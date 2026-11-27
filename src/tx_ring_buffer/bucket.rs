use indexer_rabbitmq::geyser::SlotStatus;
use log::warn;
use tokio::sync::oneshot;

use crate::{dedupe::SlotDedupe, rabbit::MessageWithMeta};

/// Pending events and chain information for one slot.
pub(super) struct Bucket {
    pub slot: u64,
    pub status: SlotStatus,
    pub blockhash: Option<String>,
    pub block_time: Option<i64>,
    pub parent_slot: Option<u64>,
    pub slots_in_same_second: Option<u8>,
    ack_metadata: Option<oneshot::Sender<()>>,
    ack_processed: Option<oneshot::Sender<()>>,
    ack_confirmed: Option<oneshot::Sender<()>>,
    pub msgs: Vec<MessageWithMeta>,
    pub dedupe: SlotDedupe,
}

impl Bucket {
    pub fn new(capacity: usize) -> Self {
        Self {
            slot: 0,
            status: SlotStatus::Processed,
            blockhash: None,
            block_time: None,
            parent_slot: None,
            slots_in_same_second: None,
            ack_metadata: None,
            ack_processed: None,
            ack_confirmed: None,
            msgs: Vec::with_capacity(capacity),
            dedupe: SlotDedupe::new(capacity),
        }
    }

    pub fn add_msg(&mut self, mut msg: MessageWithMeta) -> Option<MessageWithMeta> {
        if self.dedupe.is_duplicate(&msg.data) {
            metrics::counter!("duplicate_tx").increment(1);
            let _ = msg.acker.send(());
            return None;
        }

        if let Some(index) = self.confirmed_index() {
            msg.set_blocktime(self.block_time);
            msg.set_slot_in_seconds(index);
            return Some(msg);
        }

        self.msgs.push(msg);
        None
    }

    pub fn reset_with_ack(&mut self) {
        if !self.msgs.is_empty() {
            warn!(
                "expiring {} pending messages in slot {}",
                self.msgs.len(),
                self.slot
            );
        }
        for msg in self.msgs.drain(..) {
            let _ = msg.acker.send(());
        }
        for ack in [
            self.ack_processed.take(),
            self.ack_confirmed.take(),
            self.ack_metadata.take(),
        ]
        .into_iter()
        .flatten()
        {
            let _ = ack.send(());
        }

        self.slot = 0;
        self.slots_in_same_second = None;
        self.status = SlotStatus::Processed;
        self.block_time = None;
        self.blockhash = None;
        self.parent_slot = None;
        self.dedupe.clear();
    }

    pub fn update_meta(
        &mut self,
        blockhash: String,
        block_time: i64,
        ack: oneshot::Sender<()>,
    ) -> Vec<MessageWithMeta> {
        if self.blockhash.is_some() {
            let _ = ack.send(());
            return Vec::new();
        }
        self.blockhash = Some(blockhash);
        self.block_time = Some(block_time);
        self.ack_metadata = Some(ack);
        if !matches!(self.status, SlotStatus::Confirmed) && !self.msgs.is_empty() {
            metrics::counter!("meta_before_status").increment(1);
        }
        self.flush_ready("meta")
    }

    pub fn update_status(
        &mut self,
        status: SlotStatus,
        ack: Option<oneshot::Sender<()>>,
    ) -> Vec<MessageWithMeta> {
        let retained_ack = match status {
            SlotStatus::Processed => &mut self.ack_processed,
            SlotStatus::Confirmed => &mut self.ack_confirmed,
            SlotStatus::Rooted => {
                unreachable!("rooted status is handled before accessing a bucket")
            },
        };
        if let Some(ack) = ack {
            if retained_ack.is_some() {
                let _ = ack.send(());
            } else {
                *retained_ack = Some(ack);
            }
        }
        if matches!(status, SlotStatus::Processed) {
            return Vec::new();
        }
        self.status = status;
        if self.blockhash.is_none() && !self.msgs.is_empty() {
            metrics::counter!("status_before_meta").increment(1);
        }
        self.flush_ready("status")
    }

    fn confirmed_index(&self) -> Option<u8> {
        if matches!(self.status, SlotStatus::Confirmed) {
            self.slots_in_same_second
        } else {
            None
        }
    }

    pub(super) fn flush_ready(&mut self, trigger: &'static str) -> Vec<MessageWithMeta> {
        let Some(index) = self.confirmed_index() else {
            return Vec::new();
        };
        if self.msgs.is_empty() {
            return Vec::new();
        }
        metrics::histogram!("messages_flushed", "chain_type" => trigger)
            .record(self.msgs.len() as f64);
        self.msgs
            .drain(..)
            .map(|mut msg| {
                msg.set_blocktime(self.block_time);
                msg.set_slot_in_seconds(index);
                msg
            })
            .collect()
    }
}
