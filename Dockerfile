FROM rust:1.100.0-bookworm AS builder

WORKDIR /workdir
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo install --locked --path . --root .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN update-ca-certificates

COPY --from=builder /workdir/bin/indexer-confirmation-mgmt /usr/local/bin/indexer-confirmation-mgmt
WORKDIR /usr/local/bin

CMD ["./indexer-confirmation-mgmt"]
