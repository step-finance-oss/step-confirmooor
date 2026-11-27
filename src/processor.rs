use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, Arc};

use axum::{response::IntoResponse, routing::get, Extension, Json, Router};
use lapin::uri::AMQPUri;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::rabbit::*;
use crate::tx_ring_buffer::{TransactionRingBuffer, MIN_CHAIN_PREFETCH};
use anyhow::{anyhow, bail, ensure, Context, Result};
use indexer_rabbitmq::geyser::QueueKind;
use indexer_rabbitmq::{
    geyser::{CommittmentLevel, Network},
    suffix::Suffix,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use log::*;

type TaskQueue = deadqueue::limited::Queue<MessageWithMeta>;

/// The args passed into our processor
#[derive(clap::Parser)]
pub(crate) struct ProcessArgs {
    /// The address of an AMQP server to connect to
    #[clap(long, env)]
    pub amqp_host: String,

    /// The vhost on the server
    #[clap(long, env)]
    pub amqp_vhost: String,

    /// The port of an AMQP server to connect to
    #[clap(long, env, default_value = "5671")]
    pub amqp_port: u16,

    /// The user of an AMQP server to connect to
    #[clap(long, env)]
    pub amqp_user: Option<String>,

    /// The password of an AMQP server to connect to
    #[clap(long, env, hide_env_values = true)]
    pub amqp_pass: Option<String>,

    /// Use TLS for the AMQP connection
    #[clap(long, env, default_value = "true")]
    pub amqp_secure: String,

    /// Listener for health, buffer status, and metrics
    #[clap(long, env, default_value = "127.0.0.1:8080")]
    pub http_bind_addr: SocketAddr,

    /// max txs per slot
    #[clap(long, env)]
    pub max_tx_per_slot: usize,

    /// prefetch for tx queue
    #[clap(long, env, default_value = "8000")]
    pub tx_queue_prefetch: u16,

    /// prefetch for chain queue
    #[clap(long, env, default_value = "500")]
    pub chain_queue_prefetch: u16,

    /// message send channel buffer size
    #[clap(long, env, default_value = "100")]
    pub send_channel_buffer: usize,

    /// workers per queue
    #[clap(long, env, default_value = "10")]
    pub workers_per_queue: usize,

    /// allow tx x ahead
    #[clap(long, env, default_value = "10")]
    pub allow_tx_x_ahead: i128,

    /// allow chain x ahead
    #[clap(long, env, default_value = "0")]
    pub allow_chain_x_ahead: i128,

    /// The network to listen to events for
    #[clap(long, env)]
    pub network: Network,

    //the suffix of the queue we create/use
    #[clap(flatten)]
    pub queue_suffix: Suffix,

    /// The number of threads to use.  Defaults to available core count.
    #[clap(short = 'j', env)]
    pub thread_count: Option<usize>,

    /// The total number of shards being used by the geyser plugin. This must match the plugin.
    #[clap(long, env, default_value = "12")]
    pub shard_total: usize,

    /// The number of confirmooors processing shards, this must be able to divide into SHARD_TOTAL
    #[clap(long, env, default_value = "1")]
    pub conf_shards: usize,

    /// The 0-based number of this particular shard instance
    #[clap(long, env, default_value = "0")]
    pub my_shard: usize,
}

impl ProcessArgs {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.conf_shards > 0
                && self.shard_total > 0
                && self.shard_total.is_multiple_of(self.conf_shards),
            "CONF_SHARDS must be a positive divisor of SHARD_TOTAL"
        );
        ensure!(
            self.my_shard < self.conf_shards,
            "MY_SHARD must be less than CONF_SHARDS"
        );
        ensure!(
            self.thread_count != Some(0) && self.workers_per_queue > 0,
            "thread and worker counts must be positive"
        );
        ensure!(
            self.tx_queue_prefetch > 0
                && self.chain_queue_prefetch > 0
                && self.send_channel_buffer > 0,
            "queue prefetch and send buffer sizes must be positive"
        );
        ensure!(
            self.chain_queue_prefetch >= MIN_CHAIN_PREFETCH,
            "CHAIN_QUEUE_PREFETCH must be at least {MIN_CHAIN_PREFETCH} to advance the retained slot window"
        );
        ensure!(
            self.allow_chain_x_ahead >= 0 && self.allow_tx_x_ahead >= 0,
            "slot lead allowances must be nonnegative"
        );
        Ok(())
    }

    fn amqp_uri(&self) -> Result<AMQPUri> {
        let scheme = if self
            .amqp_secure
            .parse::<bool>()
            .context("AMQP_SECURE must be true or false")?
        {
            "amqps"
        } else {
            "amqp"
        };
        ensure!(
            !self.amqp_host.is_empty() && !self.amqp_host.contains(['@', '/', '?', '#']),
            "AMQP_HOST must contain only the broker host"
        );
        ensure!(
            self.amqp_vhost.is_empty() || self.amqp_vhost.starts_with(['/', '?']),
            "AMQP_VHOST must be empty or a URI path or query"
        );
        // Parse the existing encoded vhost and query syntax without embedding credentials in a URL.
        let mut uri: AMQPUri = format!("{scheme}://localhost{}", self.amqp_vhost)
            .parse()
            .map_err(|_| anyhow!("invalid AMQP_VHOST path or query"))?;
        uri.authority.host = self.amqp_host.clone();
        uri.authority.port = self.amqp_port;
        match (&self.amqp_user, &self.amqp_pass) {
            (Some(user), Some(pass)) => {
                uri.authority.userinfo.username.clone_from(user);
                uri.authority.userinfo.password.clone_from(pass);
            },
            (None, None) => {},
            _ => bail!("AMQP_USER and AMQP_PASS must be supplied together"),
        }
        Ok(uri)
    }
}

/// main processing fn
pub(crate) async fn process(args: ProcessArgs) -> Result<()> {
    info!("Process");

    // Initialize metrics exporter
    let recorder = PrometheusBuilder::new()
        .add_global_label("shard", args.my_shard.to_string())
        .build_recorder();

    let prometheus_handle = recorder.handle();

    metrics::set_global_recorder(recorder)
        .map_err(|_| anyhow!("failed to install metrics recorder"))?;

    let rf = RabbitFactory::new(args.amqp_uri()?).await?;
    let mut tasks = JoinSet::new();
    let (settlement_failure, mut settlement_failures) = mpsc::channel(1);

    // Every worker sees the legacy unsuffixed route and advances its stream watermark.
    // Select one owner per transaction after delivery.
    let shard_numbers = (0..args.shard_total).collect();
    let workers = WorkerPartition {
        count: args.conf_shards,
        index: args.my_shard,
    };

    let tx_work_queue = Arc::new(TaskQueue::new(args.tx_queue_prefetch.into()));

    //our main receiver grabs msgs off the queue for processing
    let txn_receiver = rf
        .create_receiver(
            args.network,
            &Suffix::ProductionUnchecked, //always rcv from prod named exch
            &args.queue_suffix,
            CommittmentLevel::Processed,
            QueueKind::Transaction,
            args.tx_queue_prefetch,
            Some(args.my_shard),
            Some(shard_numbers),
        )
        .await?;

    let chain_work_queue = Arc::new(TaskQueue::new(args.chain_queue_prefetch.into()));

    let chain_msg_receiver = rf
        .create_receiver(
            args.network,
            &Suffix::ProductionUnchecked, //always rcv from prod named exch
            &args.queue_suffix,
            CommittmentLevel::Processed,
            QueueKind::ChainData,
            args.chain_queue_prefetch,
            Some(args.my_shard),
            None,
        )
        .await?;

    debug!("created receivers");

    //create senders
    let send_confirmed = rf
        .create_sender(
            args.network,
            &args.queue_suffix,
            &args.queue_suffix,
            CommittmentLevel::Confirmed,
            args.send_channel_buffer,
            &mut tasks,
        )
        .await?;

    debug!("created senders");

    //create buffer logic
    let buffer = Arc::new(TransactionRingBuffer::new(
        args.max_tx_per_slot,
        send_confirmed,
    ));

    debug!("created buffer");

    //metrics

    let tx_slot_counting_u64 = Arc::new(AtomicU64::new(0));
    let chain_slot_counting_u64 = Arc::new(AtomicU64::new(0));

    //wire everything up
    txn_receiver.spawn_read_loop(
        buffer.clone(),
        tx_slot_counting_u64.clone(),
        chain_slot_counting_u64.clone(),
        ThreadType::Transactions,
        workers,
        &mut tasks,
        settlement_failure.clone(),
        tx_work_queue,
        args.workers_per_queue,
        args.allow_tx_x_ahead,
    );

    chain_msg_receiver.spawn_read_loop(
        buffer.clone(),
        chain_slot_counting_u64,
        tx_slot_counting_u64,
        ThreadType::Chain,
        workers,
        &mut tasks,
        settlement_failure,
        chain_work_queue,
        args.workers_per_queue,
        args.allow_chain_x_ahead,
    );

    debug!("spawned read loops");

    // Start Axum webserver in background
    tasks.spawn(async move {
        async fn healthcheck() -> impl IntoResponse {
            (axum::http::StatusCode::OK, "Ok")
        }
        async fn buffer_status(
            Extension(buffer): Extension<Arc<TransactionRingBuffer>>,
        ) -> impl IntoResponse {
            let summary = buffer.get_status_summary();
            Json(summary)
        }
        async fn metrics(Extension(handle): Extension<PrometheusHandle>) -> impl IntoResponse {
            handle.render()
        }
        let app = Router::new()
            .route("/healthcheck", get(healthcheck))
            .route("/buffer-status", get(buffer_status))
            .route("/metrics", get(metrics))
            .layer(Extension(buffer))
            .layer(Extension(prometheus_handle));
        axum::Server::try_bind(&args.http_bind_addr)?
            .serve(app.into_make_service())
            .await
            .context("HTTP server failed")?;
        bail!("HTTP server stopped")
    });

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    select! {
        _ = sigterm.recv() => info!("Received SIGTERM"),
        _ = sigint.recv() => info!("Received SIGINT"),
        Some(err) = settlement_failures.recv() => return Err(err),
        Some(result) = tasks.join_next() => {
            result.context("worker task failed")??;
            bail!("worker task stopped");
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn process_args() -> ProcessArgs {
        ProcessArgs::try_parse_from([
            "confirmooor",
            "--amqp-host",
            "broker.example",
            "--amqp-vhost",
            "/a%2Fb?heartbeat=15",
            "--amqp-user",
            "operator",
            "--amqp-pass",
            "test@/:#%value",
            "--max-tx-per-slot",
            "10",
            "--network",
            "devnet",
        ])
        .unwrap()
    }

    #[test]
    fn amqp_credentials_do_not_change_host_or_path() {
        let mut args = process_args();
        let uri = args.amqp_uri().unwrap();
        assert_eq!(uri.authority.host, "broker.example");
        assert_eq!(uri.authority.userinfo.password, "test@/:#%value");
        assert_eq!(uri.vhost, "a/b");
        assert_eq!(uri.query.heartbeat, Some(15));
        args.amqp_user = None;
        assert!(args.amqp_uri().is_err());
    }

    #[test]
    fn invalid_sharding_and_unbounded_prefetch_are_rejected() {
        let mut args = process_args();
        args.conf_shards = 0;
        assert!(args.validate().is_err());
        args.conf_shards = 1;
        args.chain_queue_prefetch = MIN_CHAIN_PREFETCH - 1;
        assert!(args.validate().is_err());
        args.chain_queue_prefetch = 1;
        assert!(args.validate().is_err());
        args.chain_queue_prefetch = MIN_CHAIN_PREFETCH;
        assert!(args.validate().is_ok());
        args.tx_queue_prefetch = 0;
        assert!(args.validate().is_err());
    }
}
