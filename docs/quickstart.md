# K2I Quickstart

This quickstart gives you two paths:

1. Run the local Docker proof that exercises Kafka, Schema Registry, K2I, Iceberg REST metadata, and DuckDB.
2. Build and run K2I manually with a small config.

## Prerequisites

- Rust 1.94+ for building from source.
- Docker and Docker Compose for local E2E validation.
- Access to Kafka and an Iceberg-compatible catalog for manual runs.

## 1. Run The Local Proof

The fastest way to see what K2I does is the Docker E2E flow.

```bash
scripts/e2e-docker-iceberg.sh
```

A passing run ends with:

```text
ok: DuckDB iceberg_scan validated real Iceberg metadata
```

For the local Iceberg load profile:

```bash
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```

For the read-state and schema-evolution correctness flow:

```bash
scripts/e2e-docker.sh
```

## 2. Build K2I

```bash
git clone https://github.com/osodevops/k2i.git
cd k2i
cargo build --release
```

Verify the binary:

```bash
./target/release/k2i --version
./target/release/k2i --help
man ./docs/man/man1/k2i.1
```

## 3. Create A Config

Use `config/example.toml` as the complete reference. This is a small REST-catalog example:

```toml
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i-ingestion"
auto_offset_reset = "earliest"

[kafka.format]
type = "protobuf"
schema_registry_url = "http://localhost:8081"
subject_strategy = "topic_name"
message_type = "example.events.v1.Event"

[schema_evolution]
mode = "auto-additive"
on_breaking_change = "pause"

[buffer]
max_size_mb = 500
flush_interval_seconds = 30
flush_batch_size = 10000

[iceberg]
catalog_type = "rest"
warehouse_path = "s3://warehouse/events"
database_name = "analytics"
table_name = "events"
rest_uri = "http://localhost:8181"

[transaction_log]
log_dir = "./transaction_logs"

[monitoring]
metrics_port = 9090
health_port = 8080

[rpc]
enabled = true
socket_path = "./run/k2i.sock"
```

For raw payloads, use:

```toml
[kafka.format]
type = "raw"
```

`json` is currently raw-compatible in the ingestion loop, not a full typed JSON projection.

## 4. Validate And Run

```bash
k2i validate --config config.toml
k2i ingest --config config.toml
```

Check health and metrics:

```bash
k2i status --url http://localhost:8080
curl -s http://localhost:8080/health
curl -s http://localhost:8080/readyz
curl -s http://localhost:9090/metrics | grep k2i
```

## 5. Regenerate CLI Man Pages

After changing CLI help, flags, or subcommands:

```bash
cargo run -p k2i-cli -- completions man --output-dir docs/man/man1
cargo test -p k2i-cli --test man_pages --no-fail-fast
```

## Next Reading

- [Kafka to Iceberg](./kafka-to-iceberg.md)
- [Configuration](./configuration.md)
- [Schema Registry Protobuf](./schema-registry-protobuf.md)
- [DuckDB Iceberg Validation](./duckdb-iceberg-validation.md)
- [Production Readiness](./production-readiness.md)
