# Step Confirmooor

Step Finance used Confirmooor between its Solana Geyser plugin and its transaction parsers. The plugin emitted processed transactions, slot status, and block metadata to RabbitMQ. Confirmooor held the transactions until their slot was confirmed and block metadata was available, then published them with the original routing key and the slot’s block time. This repository is an **unmaintained reference implementation**.

## Agave 4.4.1 compatibility

Confirmooor runs as a RabbitMQ consumer. Its working input is Step’s historical Agave 2.2.15 transaction JSON, slot statuses, and block metadata. The pinned historical publisher emits that format. [Publisher source](https://github.com/step-finance/indexer-geyser-plugin/tree/bdd538e0d82ee1941281cbdcffa6ee4d7d529ac2) · [Downstream parsers](https://github.com/step-finance-oss/step-ingestooor)

The example Geyser plugin on stock Agave 4.4.1 writes transaction summaries to JSONL. Step’s 4.4.1 capture patch additionally supplies selected pre- and post-execution account states. In a decoder check, Confirmooor accepted a historical Step transaction and rejected all 68 example records from each of three ledger replays (stock, Step capture disabled, Step capture enabled). [Agave 4.4.1](https://github.com/anza-xyz/agave/releases/tag/v4.4.1) · [Capture replay](https://github.com/step-finance-oss/agave/blob/71a0920/examples/geyser-account-capture/REPLAY.md)

A publisher port must build against 4.4.1’s V4 Geyser transaction callback, encode the full historical transaction envelope and publish it alongside parent-linked slot statuses and block metadata. To carry captured accounts, the publisher must use Step’s capture callback, preserve account-key order, and reproduce the historical owner and data encoding (nonempty data is zstd-compressed, then base64-encoded). The worker groups messages by slot. A port must keep competing banks at the same slot separate through confirmation before feeding this format, or extend the format and worker to carry bank identity. End-to-end Agave 4.4.1 delivery requires that port. [Historical encoder](https://github.com/step-finance/indexer-geyser-plugin/blob/bdd538e0d82ee1941281cbdcffa6ee4d7d529ac2/crates/plugin/src/plugin.rs#L249) · [Capture interface](https://github.com/step-finance-oss/agave/blob/71a0920/geyser-account-capture-interface/src/lib.rs)

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

2. The example consumes from the topic exchange `devnet.processed.messages`. It accepts historical unsuffixed transaction routes, numbered transaction routes, and chain notifications. A broker alone leaves the worker idle. The `NETWORK` setting selects the broker exchange names.
3. Bind a queue to `devnet.confirmed.messages.debug.your_name` before sending input. Use `#` to observe all output. The output exchange name follows `SUFFIX`. Local diagnostics are available at `http://127.0.0.1:8080/healthcheck`, `/metrics`, and `/buffer-status`.
4. For multiple workers, match `SHARD_TOTAL` to the publisher, set `CONF_SHARDS` to the worker count, and give each worker a distinct zero-based `MY_SHARD`. Each worker receives the transaction feed and all chain notifications. Exactly one worker handles each transaction, by slot modulo worker count for the historical unsuffixed route, or by shard number modulo worker count for a numbered route. Slot statistics also have one owner per slot. For a single worker, set `CONF_SHARDS=1` and `MY_SHARD=0`. The local example uses plaintext and guest credentials. Use TLS and broker-scoped credentials for a remote connection.

A local replay against RabbitMQ 4.4.1 ran two workers with a chain prefetch of 301. They delivered three transactions exactly once during the run across historical and numbered routes. The replay checked six duplicate inputs, preservation of routing keys and captured account data, and indices 0 and 1 for two slots whose metadata arrived in reverse order.

## Delivery semantics

- A slot becomes eligible after an explicit confirmation or confirmation of a known descendant. Output requires block metadata and a known `slotSecondIdx`. Genesis starts at zero. A slot starts at zero when its parent has a different block time, or increments its parent’s index when both times match. Late metadata or parent links release waiting transactions. At startup or after a gap, the first partial second can expire if its ancestry never arrives. Rooted notifications are ignored.
- The 100-bucket ring retains the most recent 75 slot numbers. It acknowledges and discards expired input. Within that window it deduplicates JSON transactions by their full first signature per slot. This state lives in memory.
- Forwarded inputs are acknowledged after the broker confirms publication. Consumers should tolerate duplicates across restarts. Output messages are transient, and broker confirmation does not guarantee a queue was bound. Debug-suffix input queues auto-delete when their final consumer disconnects.
- `CHAIN_QUEUE_PREFETCH` defaults to 500 and must be at least 301. The 75-slot window can hold four chain deliveries per slot (processed status, confirmation, metadata, and statistics). The extra credit lets the broker deliver the next slot and release expired inputs.
- The worker paces the transaction and chain streams relative to one another. Progress depends on both streams advancing. `MAX_TX_PER_SLOT` controls initial allocation, not a hard limit. The health endpoint reports process liveness.

## Dependencies and licensing

The 27 November 2026 lockfile audit reported two vulnerabilities in transitive dependencies from the historical Solana stack, plus ten unmaintained and three unsound-dependency warnings. The relay reads signatures and does not sign transactions. A fork intended for deployment should reassess these dependencies. [ed25519-dalek advisory](https://rustsec.org/advisories/RUSTSEC-2022-0093.html) · [curve25519-dalek advisory](https://rustsec.org/advisories/RUSTSEC-2024-0344.html)

Original Step Finance source is available under the [MIT license](LICENSE). The linked RabbitMQ library carries AGPL-3.0-or-later terms. See [third-party notices](THIRD_PARTY_NOTICES.md).
