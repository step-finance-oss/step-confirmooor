# Step Confirmooor

Step Finance used Confirmooor between its Solana Geyser plugin and its transaction parsers. The plugin emitted processed transactions, slot status, and block metadata to RabbitMQ. Confirmooor held the transactions until their slot was confirmed and block metadata was available, then published them with the original routing key and the slot’s block time. This repository is an **unmaintained reference implementation**.

The Agave capture patch exposes selected pre- and post-transaction account data to a Geyser plugin. Confirmooor decides when to forward those transactions to consumers at confirmed commitment. The code here reads Step’s historical RabbitMQ JSON format, pinned to the Geyser plugin and Agave 2.2.15 in `Cargo.lock`. The local replay uses that format. Compatibility with the Agave 4.4.1 capture preview has not been verified. [Geyser publisher](https://github.com/step-finance/indexer-geyser-plugin/tree/bdd538e0d82ee1941281cbdcffa6ee4d7d529ac2) · [Agave capture preview](https://github.com/step-finance-oss/agave/releases/tag/v4.4.1-step-capture.1) · [Downstream parsers](https://github.com/step-finance-oss/step-ingestooor)

## Replay a confirmation locally

Install Rust with rustup and a native C toolchain. Run this command from the repository root.

```sh
cargo test --locked tx_ring_buffer::tests::confirmed_transaction_replay -- --exact
```

The replay uses synthetic messages and the worker’s actual ring buffer. It runs without a validator, RabbitMQ, or credentials. It checks that a processed transaction stays buffered, that confirmation and metadata release it with the expected routing key, block time, and captured pre/post account bytes, and that a second copy of its signature produces no second output. It also covers parent-slot confirmation and an unconfirmed fork that ages out. A successful run reports **one test passed**. Run `cargo test --locked` for the full suite or `cargo build --locked` to build the worker.

## Run the RabbitMQ worker

1. Start a local RabbitMQ broker that accepts AMQP connections on `127.0.0.1:5672`. Copy the example configuration and start the worker.

   ```sh
   cp .env.example .env
   cargo run --locked
   ```

2. The example consumes from the topic exchange `devnet.processed.messages`. It expects both sharded transaction traffic and chain notifications. A broker alone leaves the worker idle. The `NETWORK` setting selects the broker exchange names.
3. Bind a queue to `devnet.confirmed.messages.debug.your_name` before sending input. Use `#` to observe all output. The output exchange name follows `SUFFIX`. Local diagnostics are available at `http://127.0.0.1:8080/healthcheck`, `/metrics`, and `/buffer-status`.
4. For multiple workers, match `SHARD_TOTAL` to the publisher, set `CONF_SHARDS` to the worker count, and give each worker a distinct zero-based `MY_SHARD`. For a single worker covering every shard, set `CONF_SHARDS=1` and `MY_SHARD=0`. The local example uses plaintext and guest credentials. Use TLS and broker-scoped credentials for a remote connection.

During snapshot preparation, a local RabbitMQ 4.4.1 broker delivered synthetic processed transactions, metadata, and a confirmation to the worker. A bound queue received one confirmed transaction with its original routing key, captured pre/post account bytes, and the slot’s block time. Duplicate inputs sent before and after confirmation produced no extra delivery.

## Delivery semantics

- A slot becomes eligible after an explicit confirmation or confirmation of a known descendant. Output also requires block metadata. Rooted notifications are ignored.
- The 100-bucket ring retains the most recent 75 slot numbers. It acknowledges and discards expired input. Within that window it deduplicates JSON transactions by their full first signature per slot. This state lives in memory.
- Forwarded inputs are acknowledged after the broker confirms publication. Consumers should tolerate duplicates across restarts. Output messages are transient, and broker confirmation does not guarantee a queue was bound. Debug-suffix input queues auto-delete when their final consumer disconnects.
- The worker paces the transaction and chain streams relative to one another. Progress depends on both streams advancing. `MAX_TX_PER_SLOT` controls initial allocation, not a hard limit. The health endpoint reports process liveness.

## Dependencies and licensing

The 27 November 2026 lockfile audit reported two vulnerabilities in transitive dependencies from the historical Solana stack, plus ten unmaintained and three unsound-dependency warnings. The relay reads signatures and does not sign transactions. A fork intended for deployment should reassess these dependencies. [ed25519-dalek advisory](https://rustsec.org/advisories/RUSTSEC-2022-0093.html) · [curve25519-dalek advisory](https://rustsec.org/advisories/RUSTSEC-2024-0344.html)

Original Step Finance source is available under the [MIT license](LICENSE). The linked RabbitMQ library carries AGPL-3.0-or-later terms. See [third-party notices](THIRD_PARTY_NOTICES.md).
