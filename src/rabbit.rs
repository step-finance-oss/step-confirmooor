use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::tx_ring_buffer::TransactionRingBuffer;
use anyhow::{anyhow, bail, ensure, Context, Result};
use indexer_rabbitmq::geyser::Message;
use indexer_rabbitmq::geyser::QueueKind;
use indexer_rabbitmq::{
    geyser::{CommittmentLevel, Consumer, Network, QueueType, StartupType},
    suffix::Suffix,
};
use lapin::options::{
    BasicAckOptions, BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
};
use lapin::publisher_confirm::Confirmation;
use lapin::types::FieldTable;
use lapin::uri::AMQPUri;
use lapin::{BasicProperties, Connection, ExchangeKind};
use log::{error, info, trace, warn};
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};

type TaskQueue = deadqueue::limited::Queue<MessageWithMeta>;

#[derive(Debug)]
pub struct MessageWithMeta {
    /// the message data
    pub data: Message,
    /// the routing key the message was delivered with
    pub routing_key: String,
    /// the acker for the message
    pub acker: oneshot::Sender<()>,
}

impl MessageWithMeta {
    pub fn set_blocktime(&mut self, blocktime: Option<i64>) {
        match self.data {
            Message::TransactionNotify(ref mut m) => {
                m.transaction.block_time = blocktime;
            },
            Message::SlotStatisticsNotify(ref mut m) => {
                m.block_time = blocktime;
            },
            //these aren't used, but for completeness...
            Message::AccountUpdate(ref mut m) => {
                m.block_time = blocktime;
            },
            Message::InstructionNotify(ref mut m) => {
                m.block_time = blocktime;
            },
            _ => (),
        }
    }

    pub fn get_slot(&self) -> u64 {
        match &self.data {
            Message::TransactionNotify(m) => m.transaction.slot,
            Message::BlockMetadataNotify(m) => m.slot,
            Message::SlotStatusNotify(m) => m.slot,
            Message::SlotStatisticsNotify(m) => m.slot,
            Message::AccountUpdate(m) => m.slot,
            Message::InstructionNotify(m) => m.slot,
        }
    }

    pub fn set_slot_in_seconds(&mut self, slot_in_seconds: u8) {
        match self.data {
            Message::TransactionNotify(ref mut m) => {
                m.transaction.slot_second_idx = Some(slot_in_seconds);
            },
            Message::SlotStatisticsNotify(ref mut m) => {
                m.slot_second_idx = Some(slot_in_seconds);
            },
            _ => (),
        }
    }
}

pub struct RabbitFactory {
    con: Connection,
}

impl RabbitFactory {
    pub async fn new(uri: AMQPUri) -> Result<Self> {
        let con = amqp_connect(uri, "conf-mgmt-rabbit-factory").await?;
        Ok(Self { con })
    }

    pub async fn create_sender(
        &self,
        network: Network,
        exchange_suffix: &Suffix,
        queue_suffix: &Suffix,
        confirm_level: CommittmentLevel,
        send_channel_buffer: usize,
        tasks: &mut JoinSet<Result<()>>,
    ) -> Result<mpsc::Sender<MessageWithMeta>> {
        let q = QueueType::new(
            network,
            StartupType::Normal,
            exchange_suffix,
            queue_suffix,
            confirm_level,
            QueueKind::All,
        )?;

        warn!("creating sender {:?}", q);
        // The upstream producer does not enable AMQP publisher confirms.
        let channel = self.con.create_channel().await?;
        channel
            .exchange_declare(
                q.exchange(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;

        let (send, mut rcv) = mpsc::channel::<MessageWithMeta>(send_channel_buffer);
        tasks.spawn(async move {
            let mut batch = Vec::new();
            let mut pending = Vec::new();
            while rcv.recv_many(&mut batch, send_channel_buffer).await > 0 {
                // Issue the batch in order so the broker can confirm multiple messages per round trip.
                for msg in batch.drain(..) {
                    let payload = serde_json::to_vec(&msg.data)?;
                    let confirm = channel
                        .basic_publish(
                            q.exchange(),
                            &msg.routing_key,
                            BasicPublishOptions::default(),
                            &payload,
                            BasicProperties::default(),
                        )
                        .await
                        .context("failed to publish confirmed data")?;
                    pending.push((confirm, msg.acker));
                }
                for (confirm, acker) in pending.drain(..) {
                    ensure!(
                        matches!(confirm.await?, Confirmation::Ack(None)),
                        "broker did not confirm publication"
                    );
                    acker
                        .send(())
                        .map_err(|_| anyhow!("input acknowledgement task stopped"))?;
                    metrics::counter!("out_messages_total").increment(1);
                }
            }
            bail!("publisher input closed")
        });

        Ok(send)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_receiver(
        &self,
        network: Network,
        exchange_suffix: &Suffix,
        queue_suffix: &Suffix,
        confirm_level: CommittmentLevel,
        queue_kind: QueueKind,
        prefetch: u16,
        shard_for_queue_name: Option<usize>,
        shard_numbers_to_bind: Option<Vec<usize>>,
    ) -> Result<Receiver> {
        let rcv_q = QueueType::new_with_prefetch_and_shards(
            network,
            StartupType::Normal,
            exchange_suffix,
            queue_suffix,
            confirm_level,
            queue_kind,
            prefetch,
            shard_for_queue_name,
            shard_numbers_to_bind,
        )?;
        let consumer = Consumer::new(&self.con, rcv_q, "reader").await?;
        let receiver = Receiver::new(consumer);
        Ok(receiver)
    }
}

#[derive(Debug)]
pub enum ThreadType {
    Transactions,
    Chain,
}

pub struct Receiver {
    consumer: Consumer,
}

impl Receiver {
    fn new(consumer: Consumer) -> Self {
        Self { consumer }
    }

    async fn read(
        &mut self,
        failures: &mpsc::Sender<anyhow::Error>,
    ) -> Result<Option<MessageWithMeta>, indexer_rabbitmq::Error> {
        let resp = self.consumer.read().await?;
        let ret = resp.map(|r| {
            let (tx, rx) = oneshot::channel::<()>();
            let ack = r.acker;
            let failures = failures.clone();
            spawn(async move {
                let result = match rx.await {
                    Ok(()) => ack
                        .ack(BasicAckOptions::default())
                        .await
                        .context("failed to acknowledge input delivery"),
                    // Closing this connection requeues unsettled inputs against a fresh ring.
                    Err(_) => Err(anyhow!(
                        "input delivery was abandoned before acknowledgement"
                    )),
                };
                if let Err(err) = result {
                    let _ = failures.try_send(err);
                }
            });
            MessageWithMeta {
                data: r.data,
                acker: tx,
                routing_key: r.routing_key,
            }
        });
        Ok(ret)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_read_loop(
        mut self,
        buffer: Arc<TransactionRingBuffer>,
        my_slot_counter: Arc<AtomicU64>,
        other_slot_counter: Arc<AtomicU64>,
        thread_type: ThreadType,
        tasks: &mut JoinSet<Result<()>>,
        settlement_failure: mpsc::Sender<anyhow::Error>,
        work_queue: Arc<TaskQueue>,
        workers_per_queue: usize,
        allow_x_ahead: i128,
    ) {
        let thread_type_string = format!("{:?}", thread_type).to_lowercase();
        info!("Create read loop {}", thread_type_string);

        let read_task_work_queue = work_queue.clone();
        let read_task_thread_type_string = thread_type_string.clone();
        tasks.spawn(async move {
            info!("Begin read loop {:?}", read_task_thread_type_string);
            loop {
                trace!(
                    "read loop {:?} waiting for message",
                    read_task_thread_type_string
                );
                let msg = match self.read(&settlement_failure).await {
                    Ok(Some(msg)) => msg,
                    Ok(None) => bail!("{} consumer stopped", read_task_thread_type_string),
                    // The pinned consumer has already rejected malformed payloads for dead lettering.
                    Err(indexer_rabbitmq::Error::Other("error deser message")) => {
                        metrics::counter!("malformed_deliveries_total").increment(1);
                        continue;
                    },
                    Err(err) => return Err(err).context("failed to read RabbitMQ delivery"),
                };
                let msg_slot = msg.get_slot();

                trace!("TX Rabbit thread processing slot {}", msg_slot);
                // Update the slot counter if this slot is greater
                my_slot_counter.fetch_max(msg_slot, Ordering::Relaxed);

                let start_time = Instant::now();
                let mut had_to_sleep = false;
                while (other_slot_counter.load(Ordering::Relaxed) as i128)
                    < (msg_slot as i128 - allow_x_ahead)
                {
                    // Loop until the other thread catches up
                    sleep(Duration::from_millis(1)).await;
                    had_to_sleep = true;
                }
                let elapsed = start_time.elapsed().as_secs_f64();
                if had_to_sleep {
                    metrics::histogram!("slot_catchup_wait_secs", "thread_type" => read_task_thread_type_string.clone()).record(elapsed);
                }

                let start_time = Instant::now();
                read_task_work_queue.push(msg).await;
                let elapsed = start_time.elapsed().as_secs_f64();
                metrics::histogram!("message_wait_for_work_queue_secs", "thread_type" => read_task_thread_type_string.clone()).record(elapsed);
            }
        });

        //now create WORKERS to process the messages
        for _ in 0..workers_per_queue {
            let buffer = buffer.clone();
            let work_queue = work_queue.clone();

            let thread_type_string = thread_type_string.clone();
            tasks.spawn(async move {
                loop {
                    let start_time = Instant::now();
                    let msg = work_queue.pop().await;
                    let elapsed = start_time.elapsed().as_secs_f64();
                    metrics::histogram!("work_queue_wait_for_message_secs", "thread_type" => thread_type_string.clone()).record(elapsed);
                    metrics::counter!("in_messages_total").increment(1);
                    let res = match msg.data {
                        Message::TransactionNotify(ref _m) => {
                            metrics::counter!("in_message_count", "message_type" => "tx")
                                .increment(1);
                            buffer.process_other_message(msg).await
                        },
                        Message::AccountUpdate(ref _m) => {
                            metrics::counter!("in_message_count", "message_type" => "acct")
                                .increment(1);
                            buffer.process_other_message(msg).await
                        },
                        Message::InstructionNotify(ref _m) => {
                            metrics::counter!("in_message_count", "message_type" => "ix")
                                .increment(1);
                            buffer.process_other_message(msg).await
                        },
                        Message::SlotStatisticsNotify(ref _m) => {
                            metrics::counter!("in_message_count", "message_type" => "slot_stats")
                                .increment(1);
                            buffer.process_other_message(msg).await
                        },
                        Message::BlockMetadataNotify(meta) => {
                            metrics::counter!("in_message_count", "message_type" => "block_meta")
                                .increment(1);
                            buffer.process_block_metadata(meta, msg.acker).await
                        },
                        Message::SlotStatusNotify(ss) => {
                            metrics::counter!("in_message_count", "message_type" => "slot_status")
                                .increment(1);
                            buffer.process_status(ss, msg.acker).await
                        },
                    };
                    if let Err(e) = res {
                        error!("message err (handled): {}", e);
                    }
                }
            });
        }
    }
}

/// Create a new AMQP connection from the given URL
///
/// # Errors
/// This function fails if a connection cannot be established
async fn amqp_connect(uri: AMQPUri, name: &'static str) -> Result<lapin::Connection> {
    lapin::Connection::connect_uri(
        uri,
        lapin::ConnectionProperties::default()
            .with_connection_name(
                format!(
                    "{}@{}",
                    name,
                    hostname::get()
                        .context("Failed to get system hostname")?
                        .into_string()
                        .map_err(|_| anyhow!("Failed to parse system hostname"))?,
                )
                .into(),
            )
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio),
    )
    .await
    .context("Failed to connect to the AMQP server")
}
