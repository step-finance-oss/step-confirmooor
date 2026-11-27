use std::iter;

use anyhow::bail;
use arrayvec::ArrayVec;
use indexer_rabbitmq::geyser::{BlockMetadataNotify, SlotStatus, SlotStatusNotify};
use log::warn;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use crate::{error::*, rabbit::MessageWithMeta};

mod bucket;
use bucket::Bucket;

const RING_SIZE_USIZE: usize = 100;
pub const RING_SIZE: u64 = RING_SIZE_USIZE as u64;
const RETAINED_SLOTS: u64 = RING_SIZE * 3 / 4;

/// Events wait for both confirmation and block metadata in the bucket for their slot.
pub struct TransactionRingBuffer {
    tip: RwLock<u64>,
    ring: Box<ArrayVec<RwLock<Bucket>, RING_SIZE_USIZE>>,
    confirmed_sender: mpsc::Sender<MessageWithMeta>,
}

#[derive(Serialize, Debug, Clone)]
pub struct BucketSummary {
    pub slot: u64,
    pub tx_count: usize,
    pub status: String,
    pub block_time: Option<i64>,
    pub has_blockhash: bool,
    pub parent_slot: Option<u64>,
    pub slots_in_same_second: u8,
    pub dedup_set_len: usize,
}

impl TransactionRingBuffer {
    pub fn get_status_summary(&self) -> Vec<BucketSummary> {
        self.ring
            .iter()
            .map(|lock| {
                let bucket = lock.read();
                BucketSummary {
                    slot: bucket.slot,
                    tx_count: bucket.msgs.len(),
                    status: format!("{:?}", bucket.status),
                    block_time: bucket.block_time,
                    has_blockhash: bucket.blockhash.is_some(),
                    parent_slot: bucket.parent_slot,
                    slots_in_same_second: bucket.slots_in_same_second,
                    dedup_set_len: bucket.dedupe.len(),
                }
            })
            .collect()
    }

    pub fn new(
        tx_capacity_per_slot: usize,
        confirmed_sender: mpsc::Sender<MessageWithMeta>,
    ) -> Self {
        Self {
            tip: RwLock::new(0),
            ring: Box::new(ArrayVec::from_iter(
                iter::repeat_with(|| RwLock::new(Bucket::new(tx_capacity_per_slot)))
                    .take(RING_SIZE_USIZE),
            )),
            confirmed_sender,
        }
    }

    /// Hold the tip read lock while touching buckets so concurrent expiry cannot recycle them.
    fn live_window(&self, slot: u64) -> Option<RwLockReadGuard<'_, u64>> {
        let mut tip = self.tip.read();
        if slot > *tip {
            drop(tip);
            let mut current = self.tip.write();
            if slot > *current {
                if *current > 0 && slot - *current > 1 {
                    warn!("skipped {} slots before slot {}", slot - *current - 1, slot);
                }
                if let Some(last_expired) = slot.checked_sub(RETAINED_SLOTS) {
                    let first_expired = current.checked_sub(RETAINED_SLOTS).map_or(0, |s| s + 1);
                    // A gap can span many revolutions. Visit each physical bucket at most once.
                    let start = first_expired.max(last_expired.saturating_sub(RING_SIZE - 1));
                    for expired_slot in start..=last_expired {
                        let mut bucket = self.ring[get_slot_index(expired_slot)].write();
                        if bucket.slot <= last_expired {
                            bucket.reset_with_ack();
                        }
                    }
                }
                *current = slot;
                metrics::counter!("tip_slot").absolute(slot);
            }
            tip = RwLockWriteGuard::downgrade(current);
        }
        if tip.saturating_sub(slot) >= RETAINED_SLOTS {
            metrics::counter!("old_message").increment(1);
            metrics::gauge!("old_message_age").set((*tip - slot) as f64);
            return None;
        }
        Some(tip)
    }

    /// The caller holds a live window guard, so no other retained slot can occupy this index.
    fn bucket_for(&self, slot: u64) -> RwLockWriteGuard<'_, Bucket> {
        let mut bucket = self.ring[get_slot_index(slot)].write();
        debug_assert!(bucket.slot == 0 || bucket.slot == slot);
        bucket.slot = slot;
        bucket
    }

    pub async fn process_block_metadata(
        &self,
        meta: BlockMetadataNotify,
        ack: oneshot::Sender<()>,
    ) -> Result<()> {
        let slot = meta.slot;
        let pending = {
            let Some(_tip) = self.live_window(slot) else {
                let _ = ack.send(());
                bail!("expired block metadata for slot {slot}");
            };
            let mut same_second = 0;
            for prior in (slot.saturating_sub(5)..slot).rev() {
                let bucket = self.ring[get_slot_index(prior)].read();
                if bucket.slot != prior || bucket.block_time.is_none() {
                    continue;
                }
                if bucket.block_time == Some(meta.block_time) {
                    same_second = bucket.slots_in_same_second.saturating_add(1);
                }
                break;
            }
            self.bucket_for(slot)
                .update_meta(meta.blockhash, meta.block_time, same_second, ack)
        };
        self.send_confirmed(pending).await
    }

    pub async fn process_status(
        &self,
        ss: SlotStatusNotify,
        ack: oneshot::Sender<()>,
    ) -> Result<()> {
        if matches!(ss.status, SlotStatus::Rooted) {
            let _ = ack.send(());
            return Ok(());
        }

        let slot = ss.slot;
        let pending = {
            let Some(tip) = self.live_window(slot) else {
                let _ = ack.send(());
                bail!("expired status for slot {slot}");
            };
            let (mut parent, child_pending) = {
                let mut bucket = self.bucket_for(slot);
                if bucket.parent_slot.is_none() {
                    bucket.parent_slot = ss.parent.filter(|parent| *parent < slot);
                }
                let pending = bucket.update_status(ss.status, Some(ack));
                let parent = if matches!(bucket.status, SlotStatus::Confirmed) {
                    bucket.parent_slot
                } else {
                    None
                };
                (parent, pending)
            };
            // A processed notification can supply a missing parent link after confirmation.
            // Mark retained ancestors even if their own transactions or statuses arrive later.
            let mut pending = Vec::new();
            while let Some(parent_slot) = parent {
                if *tip - parent_slot >= RETAINED_SLOTS {
                    break;
                }
                let mut bucket = self.bucket_for(parent_slot);
                if matches!(bucket.status, SlotStatus::Confirmed) {
                    break;
                }
                metrics::counter!("ancestry_traversal_updates").increment(1);
                pending.extend(bucket.update_status(SlotStatus::Confirmed, None));
                parent = bucket.parent_slot;
            }
            pending.extend(child_pending);
            pending
        };
        self.send_confirmed(pending).await
    }

    pub async fn process_other_message(&self, msg: MessageWithMeta) -> Result<()> {
        let slot = msg.get_slot();
        let ready = {
            let Some(_tip) = self.live_window(slot) else {
                let _ = msg.acker.send(());
                bail!("expired message for slot {slot}");
            };
            self.bucket_for(slot).add_msg(msg)
        };
        if let Some(msg) = ready {
            metrics::counter!("messages_sent_single").increment(1);
            self.confirmed_sender.send(msg).await?;
        }
        Ok(())
    }

    async fn send_confirmed(&self, pending: Vec<MessageWithMeta>) -> Result<()> {
        for msg in pending {
            self.confirmed_sender.send(msg).await?;
        }
        Ok(())
    }
}

fn get_slot_index(slot: u64) -> usize {
    (slot % RING_SIZE) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_rabbitmq::geyser::{Message, SlotStatistics};
    use serde_json::json;
    use solana_transaction_status::EncodedTransaction;

    // These distinct 64-byte signatures both start with slot 1000 in little-endian order.
    const SIGNATURE_A: &str =
        "5e3Sihk1dZrnbAzjvrM67Uo1jv88j44HbrXHMYRk5rRPbcnbfcRUsbYevRPcFwe86A2aD8b8vM1jm2AArVd9nZcC";
    const SIGNATURE_B: &str =
        "5e3Sihk1dZrnckTMwMB6GpJf8n35yukDxwVoHXjbrvHRGHbsAYRn8Jt6AJEsmJzh4c9kj14e1fiFMDbBPKhVbHAh";

    fn transaction(slot: u64, signature: &str) -> Message {
        // Use the historical Geyser JSON shape, including Step's account capture fields.
        let owner = "11111111111111111111111111111111";
        serde_json::from_value(json!({
            "slot": slot, "indexInBlock": 0, "blockTime": null, "slotSecondIdx": null,
            "transaction": {
                "signatures": [signature],
                "message": {
                    "accountKeys": [{"pubkey": owner, "signer": true, "writable": true}],
                    "instructions": [], "recentBlockhash": owner
                }
            },
            "meta": {
                "err": null, "status": {"Ok": null}, "fee": 0,
                "preBalances": [1], "postBalances": [1],
                "preOwners": [owner], "postOwners": [owner],
                "preDatum": ["AQ=="], "postDatum": ["Ag=="]
            }
        }))
        .expect("valid historical transaction fixture")
    }

    async fn submit(
        ring: &TransactionRingBuffer,
        data: Message,
        key: &str,
    ) -> Result<oneshot::Receiver<()>> {
        let (acker, receipt) = oneshot::channel();
        ring.process_other_message(MessageWithMeta {
            data,
            routing_key: key.into(),
            acker,
        })
        .await?;
        Ok(receipt)
    }

    async fn status(
        ring: &TransactionRingBuffer,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<oneshot::Receiver<()>> {
        let (ack, receipt) = oneshot::channel();
        ring.process_status(
            SlotStatusNotify {
                slot,
                parent,
                status,
            },
            ack,
        )
        .await?;
        Ok(receipt)
    }

    async fn meta(
        ring: &TransactionRingBuffer,
        slot: u64,
        time: i64,
    ) -> Result<oneshot::Receiver<()>> {
        let (ack, receipt) = oneshot::channel();
        ring.process_block_metadata(
            BlockMetadataNotify {
                slot,
                block_height: slot,
                blockhash: format!("replay-{slot}"),
                block_time: time,
            },
            ack,
        )
        .await?;
        Ok(receipt)
    }

    fn assert_transaction(
        output: &mut mpsc::Receiver<MessageWithMeta>,
        key: &str,
        slot: u64,
        time: i64,
        second_index: u8,
        expected_sig: &str,
    ) {
        let msg = output.try_recv().expect("expected a confirmed transaction");
        assert_eq!(msg.routing_key, key);
        let Message::TransactionNotify(tx) = msg.data else {
            panic!("expected a transaction")
        };
        assert_eq!(tx.transaction.slot, slot);
        assert_eq!(tx.transaction.block_time, Some(time));
        assert_eq!(tx.transaction.slot_second_idx, Some(second_index));
        let wire = serde_json::to_value(&tx).unwrap();
        assert_eq!(wire["meta"]["preDatum"], json!(["AQ=="]));
        assert_eq!(wire["meta"]["postDatum"], json!(["Ag=="]));
        let EncodedTransaction::Json(tx) = tx.transaction.transaction.transaction else {
            panic!("expected a JSON transaction");
        };
        assert_eq!(tx.signatures, [expected_sig]);
        let _ = msg.acker.send(());
    }

    fn assert_idle(output: &mut mpsc::Receiver<MessageWithMeta>) {
        assert!(matches!(
            output.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn confirmed_transaction_replay() -> Result<()> {
        let (sender, mut output) = mpsc::channel(16);
        let ring = TransactionRingBuffer::new(4, sender);
        let slot = 1_000;
        let time = 1_700_000_000;

        let mut first = submit(&ring, transaction(slot, SIGNATURE_A), "parent.transaction").await?;
        let mut duplicate = submit(&ring, transaction(slot, SIGNATURE_A), "duplicate").await?;
        assert_eq!(duplicate.try_recv(), Ok(()));
        let _parent_meta = meta(&ring, slot, time).await?;

        let fork = slot + 1;
        let mut fork_receipt =
            submit(&ring, transaction(fork, SIGNATURE_A), "fork.transaction").await?;
        let _fork_meta = meta(&ring, fork, time).await?;
        assert_eq!(
            status(&ring, fork, Some(slot), SlotStatus::Rooted)
                .await?
                .try_recv(),
            Ok(())
        );

        let child = slot + 2;
        let _child_meta = meta(&ring, child, time + 1).await?;
        let _child = submit(&ring, transaction(child, SIGNATURE_A), "child.transaction").await?;
        assert_idle(&mut output);
        let _confirmed = status(&ring, child, Some(slot), SlotStatus::Confirmed).await?;
        // Both buckets flush on one status. The input ack travels with the outgoing event.
        assert!(matches!(
            first.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_transaction(
            &mut output,
            "parent.transaction",
            slot,
            time,
            0,
            SIGNATURE_A,
        );
        assert_eq!(first.try_recv(), Ok(()));
        assert_transaction(
            &mut output,
            "child.transaction",
            child,
            time + 1,
            0,
            SIGNATURE_A,
        );
        assert_idle(&mut output);

        let mut late_duplicate = submit(&ring, transaction(slot, SIGNATURE_A), "duplicate").await?;
        assert_eq!(late_duplicate.try_recv(), Ok(()));
        let _late = submit(&ring, transaction(slot, SIGNATURE_B), "late.transaction").await?;
        assert_transaction(&mut output, "late.transaction", slot, time, 0, SIGNATURE_B);
        let stats = Message::SlotStatisticsNotify(SlotStatistics {
            slot,
            ..Default::default()
        });
        let mut stats_receipt = submit(&ring, stats.clone(), "slot.stats").await?;
        let mut duplicate_stats = submit(&ring, stats, "duplicate.stats").await?;
        assert_eq!(duplicate_stats.try_recv(), Ok(()));
        let stats = output.try_recv()?;
        assert_eq!(stats.routing_key, "slot.stats");
        let Message::SlotStatisticsNotify(payload) = stats.data else {
            panic!("expected statistics")
        };
        assert_eq!(
            (payload.slot, payload.block_time, payload.slot_second_idx),
            (slot, Some(time), Some(0))
        );
        stats.acker.send(()).unwrap();
        assert_eq!(stats_receipt.try_recv(), Ok(()));

        // Status before metadata releases its buffered event exactly once.
        let next = child + 1;
        let _next = submit(&ring, transaction(next, SIGNATURE_B), "next.transaction").await?;
        let _next_status = status(&ring, next, Some(child), SlotStatus::Confirmed).await?;
        assert_idle(&mut output);
        let _next_meta = meta(&ring, next, time + 1).await?;
        assert_transaction(
            &mut output,
            "next.transaction",
            next,
            time + 1,
            1,
            SIGNATURE_B,
        );
        assert_eq!(meta(&ring, next, time + 2).await?.try_recv(), Ok(()));
        assert_eq!(
            status(&ring, next, Some(child), SlotStatus::Confirmed)
                .await?
                .try_recv(),
            Ok(())
        );
        let _after_duplicate_meta = submit(
            &ring,
            transaction(next, SIGNATURE_A),
            "after_duplicate.transaction",
        )
        .await?;
        assert_transaction(
            &mut output,
            "after_duplicate.transaction",
            next,
            time + 1,
            1,
            SIGNATURE_A,
        );
        assert!(matches!(
            fork_receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let _advance = meta(&ring, fork + RETAINED_SLOTS, time + 100).await?;
        assert_eq!(fork_receipt.try_recv(), Ok(()));
        assert!(submit(&ring, transaction(fork, SIGNATURE_A), "expired")
            .await
            .is_err());
        assert_idle(&mut output);
        Ok(())
    }

    #[tokio::test]
    async fn late_parent_link_confirms_ancestors_inside_window() -> Result<()> {
        let (sender, mut output) = mpsc::channel(4);
        let ring = TransactionRingBuffer::new(2, sender);
        let _genesis = meta(&ring, 0, 10).await?;
        let _pending = submit(&ring, transaction(0, SIGNATURE_A), "genesis").await?;
        let _child = status(&ring, 2, None, SlotStatus::Confirmed).await?;
        let _link = status(&ring, 2, Some(1), SlotStatus::Processed).await?;
        let _parent = status(&ring, 1, Some(0), SlotStatus::Processed).await?;
        assert_transaction(&mut output, "genesis", 0, 10, 0, SIGNATURE_A);

        let _late = meta(&ring, 1, 10).await?;
        let _tx = submit(&ring, transaction(1, SIGNATURE_A), "parent").await?;
        assert_transaction(&mut output, "parent", 1, 10, 1, SIGNATURE_A);
        assert_idle(&mut output);
        Ok(())
    }

    #[tokio::test]
    async fn large_gap_expires_pending_deliveries_and_reuses_bucket() -> Result<()> {
        let (sender, mut output) = mpsc::channel(2);
        let ring = TransactionRingBuffer::new(2, sender);
        let old = 103;
        let mut old_receipt = submit(&ring, transaction(old, SIGNATURE_A), "expired").await?;
        let mut old_meta = meta(&ring, old, 123).await?;
        let new = old + 10 * RING_SIZE;
        let _advance = status(&ring, new + 1, None, SlotStatus::Processed).await?;
        assert_eq!(old_receipt.try_recv(), Ok(()));
        assert_eq!(old_meta.try_recv(), Ok(()));
        let _tx = submit(&ring, transaction(new, SIGNATURE_A), "reused").await?;
        // The ancient parent aliases this bucket's index.
        let _confirmed = status(&ring, new, Some(old), SlotStatus::Confirmed).await?;
        assert_idle(&mut output);
        let _meta = meta(&ring, new, 456).await?;
        assert_transaction(&mut output, "reused", new, 456, 0, SIGNATURE_A);
        assert_idle(&mut output);
        Ok(())
    }

    #[tokio::test]
    async fn closed_publisher_leaves_input_unacknowledged() -> Result<()> {
        let (sender, output) = mpsc::channel(1);
        let ring = TransactionRingBuffer::new(1, sender);
        let slot = 20;
        let mut receipt = submit(&ring, transaction(slot, SIGNATURE_A), "transaction").await?;
        let _confirmed = status(&ring, slot, None, SlotStatus::Confirmed).await?;
        drop(output);
        assert!(meta(&ring, slot, 123).await.is_err());
        assert_eq!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
        Ok(())
    }
}
