# K2I Repository Guide

This repository is **K2I**, a Rust workspace for a single-process Kafka-to-Iceberg ingestion engine. It consumes Kafka messages, decodes configured payload formats, keeps recent rows in an in-memory Arrow hot buffer, writes flushes as Parquet files, records durable progress in a transaction log, and exposes CLI, health, metrics, and optional local read-state RPC surfaces.

Use this file as the repo-specific guide for future agent work. `crates/k2i-core/src/config.rs`, `crates/k2i-core/src/engine/ingestion.rs`, and `config/example.toml` are the source of truth when docs disagree.

## What This Repo Builds

K2I is intended to fill the gap between complex real-time stream processors and slower batch pipelines:

- Input: one Kafka topic consumed by a Kafka consumer group.
- Decode path: raw bytes by default, JSON configured as a raw-compatible path today, and Confluent Schema Registry Protobuf for schema-aware ingestion.
- Hot path: decoded rows are stored in an Arrow hot buffer and exposed through local read-state RPC for sub-second freshness.
- Cold path: buffer snapshots are converted to Parquet and written through the Iceberg writer.
- Durability: a transaction log records offsets, flush stages, committed data files, schema evolution events, idempotency records, checkpoints, and maintenance events.
- Catalog layer: `CatalogOperations` supports REST, Glue, Hive, and Nessie implementations, with caveats below for full Iceberg metadata compatibility.
- Operations: health endpoints, readiness/liveness split, Prometheus-style metrics, graceful shutdown, and maintenance task types.

K2I is not a general stream processing framework. It does not implement joins, windows, multi-source ETL, or CDC delete semantics.

## Workspace Layout

```text
.
|-- Cargo.toml
|-- Cargo.lock
|-- Dockerfile
|-- README.md
|-- AGENTS.md
|-- claude.md
|-- config/example.toml
|-- crates/
|   |-- k2i-core/         # core ingestion library
|   |-- k2i-cli/          # k2i binary and HTTP server
|   |-- k2i-rpc/          # read-state protocol types and frame codec
|   |-- k2i-rpc-server/   # Unix socket read RPC server
|   `-- k2i-e2e-runner/   # Docker E2E producer/verifier
|-- docker/e2e/           # local Kafka/Schema Registry/K2I E2E stacks
|-- docs/
`-- scripts/
```

Workspace members:

- `crates/k2i-core`: config, Kafka, format decoders, schema evolution, hot buffer, read registry, Iceberg writer/catalogs, txlog, health, metrics, and maintenance.
- `crates/k2i-cli`: CLI commands, HTTP health/metrics server, engine startup, optional RPC server startup, completions, and generated man pages.
- `crates/k2i-rpc`: bincode-framed protocol types for read state.
- `crates/k2i-rpc-server`: Unix socket server for the read-state protocol.
- `crates/k2i-e2e-runner`: Docker E2E scenario runner that registers schemas, produces Protobuf records, reads RPC state, validates Parquet with DuckDB, and validates Iceberg metadata with DuckDB `iceberg_scan`.

## Common Commands

Run commands from the repository root.

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --no-fail-fast
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p k2i-cli -- validate --config config/example.toml
cargo run -p k2i-cli -- ingest --config config/example.toml
cargo run -p k2i-cli -- status --url http://localhost:8080
cargo run -p k2i-cli -- completions man --output-dir docs/man/man1
scripts/e2e-docker.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh
scripts/e2e-docker-iceberg.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```

## Configuration Source Of Truth

The authoritative config structs are in `crates/k2i-core/src/config.rs`. The best example is `config/example.toml`.

Important sections:

- `[kafka]`: `bootstrap_servers` is `Vec<String>`. Required fields are `bootstrap_servers`, `topic`, and `consumer_group`.
- `[kafka.format]`: serde-tagged enum with `type = "raw"`, `type = "json"`, or `type = "protobuf"`.
- Protobuf format fields: `schema_registry_url`, `subject_strategy`, `message_type`, `cache_ttl_seconds`, and `latest_on_startup`.
- `[kafka.security]`: optional protocol, SASL, and SSL certificate fields passed to `rdkafka`.
- `[schema_evolution]`: runtime behavior for Protobuf drift. Defaults are `mode = "auto-additive"`, `on_breaking_change = "pause"`, and `schema_update_min_interval_seconds = 60`.
- `[iceberg]`: required `catalog_type`, `warehouse_path`, `database_name`, and `table_name`; optional REST/Hive/AWS/S3/catalog-manager/table-management settings.
- `[buffer]`: TTL, max size, flush interval, flush batch size, and memory alignment.
- `[transaction_log]`: log directory, checkpoint cadence, and max retained log files. Protobuf registry stale cache lives below `transaction_log.log_dir/schema-cache`.
- `[maintenance]`: task enablement and intervals for compaction, expiration, orphan cleanup, and statistics.
- `[monitoring]`: health port, metrics port, log level, and log format.
- `[rpc]`: optional local Unix socket read-state server.

Avoid older doc examples that use `group_id`, `warehouse`, `database`, `table`, `[storage]`, or `[server]`.

## Runtime Flow

The intended main flow is:

```text
Kafka poll
  -> MessageDecoder
  -> Protobuf schema resolution/evolution check when configured
  -> Assign table read LSNs
  -> Append Arrow rows to HotBuffer
  -> Append txlog OffsetMarker entries
  -> Flush HotBuffer when threshold/time/full
  -> Keep flushed rows in read registry as in-flight visibility
  -> IcebergWriter writes Parquet and optional catalog commit
  -> Register committed data file for read RPC
  -> Append txlog DataFileCommitted and IdempotencyRecord
  -> Commit Kafka offset after durable write/log records
```

Main orchestration lives in `crates/k2i-core/src/engine/ingestion.rs`.

Important ordering invariants:

- Do not assign final read LSNs until schema compatibility is accepted.
- Do not commit Kafka offsets until the writer succeeds and required txlog records are appended.
- Keep in-flight flushed rows visible to read clients until their committed data file is registered.
- On breaking schema changes, pause before offset commit so incompatible data can be handled by an operator.

## Health And Metrics

HTTP endpoints are implemented in `crates/k2i-cli/src/server.rs`.

- `/health`: JSON status and component map.
- `/healthz`: liveness. Returns 200 for healthy or degraded, 503 for unhealthy.
- `/readyz`: readiness. Returns 200 only when operational and no readiness blockers exist.
- `/metrics`: Prometheus text.

Tracked health components include `kafka`, `buffer`, `iceberg`, `catalog`, `txlog`, and `schema`. Schema pauses degrade `schema` and block readiness while preserving liveness.

Current server metric names include:

- `k2i_messages_consumed_total`
- `k2i_errors_total`
- `k2i_flushes_total`
- `k2i_rows_flushed_total`
- `k2i_backpressure_events_total`
- `k2i_iceberg_commits_total`
- `k2i_buffer_size_bytes`
- `k2i_buffer_record_count`
- `k2i_flush_duration_seconds`

## Core Modules

### `format`

`crates/k2i-core/src/format` contains message decoding.

- `RawDecoder` preserves Kafka key/value bytes and metadata.
- `ProtobufDecoder` parses Confluent Schema Registry Protobuf frames, resolves descriptors, builds Arrow/Iceberg projections, and decodes dynamic messages.
- `HttpSchemaRegistryClient` caches by ID and subject in memory, and falls back to a stale disk cache under `transaction_log.log_dir/schema-cache`.
- Additive schema diffing lives beside the Protobuf decoder. Type changes and other incompatible changes are breaking.

Protobuf notes:

- Subject strategies: `topic_name`, `record_name`, and `topic_record_name`.
- `message_type` is required for record-based strategies and when the schema contains multiple message types.
- Enums are projected as `Utf8` names today.
- `google.protobuf.Timestamp` is projected as UTC microsecond timestamp.
- Oneofs are represented as JSON helper columns.

### `kafka`

Files:

- `crates/k2i-core/src/kafka/consumer.rs`
- `crates/k2i-core/src/kafka/offset.rs`

The consumer uses `rdkafka::StreamConsumer`, manual commits, `enable.auto.offset.store=false`, cooperative-sticky assignment, optional SASL/SSL fields, pause/resume for backpressure, and async offset commits.

### `buffer`

Files:

- `crates/k2i-core/src/buffer/hot_buffer.rs`
- `crates/k2i-core/src/buffer/eviction.rs`

`HotBuffer` stores Arrow-compatible rows and indexes for low-latency lookups. It can append decoded `RecordBatch` values, expose hot rows as Arrow IPC for read RPC, and produce snapshots for flush.

### `read`

`crates/k2i-core/src/read/mod.rs` owns `TableReadRegistry`.

It manages table-scoped read LSNs, committed data file references, in-flight flush visibility, active scan TTL, schema IPC export, and table scans. The registry is scoped to the single configured table.

### `txlog`

Files:

- `crates/k2i-core/src/txlog/entries.rs`
- `crates/k2i-core/src/txlog/log.rs`
- `crates/k2i-core/src/txlog/recovery.rs`

Transaction logs are newline-delimited JSON files named `txlog-<uuid>.jsonl`. Recovery reconstructs max offsets, last snapshot, incomplete flushes, committed files, and orphan files. Checkpointing rotates logs and keeps up to `max_log_files`.

### `iceberg`

Important files:

- `writer.rs`: Arrow to Parquet, object-store upload, mock or catalog commit.
- `factory.rs`: `CatalogFactory`, `CatalogOperations`, REST client, and catalog registry.
- `rest_api.rs`: REST catalog request/response types.
- `glue.rs`, `hive.rs`, `nessie.rs`: catalog implementations.
- `transaction_coordinator.rs`: CAS retry and idempotency tracking.
- `metadata_cache.rs`: TTL cache for snapshot/schema/table/manifest metadata.
- `table_manager.rs`: create/load/schema validation helper.
- `schema_evolution.rs`: older JSON schema inference helper, separate from the Protobuf runtime evolution path.

`CatalogOperations` is the preferred abstraction for catalog-facing work. `catalog.rs` is an older placeholder and should not be extended for new integration work unless intentionally refactoring.

### `maintenance`

Files:

- `compaction.rs`
- `expiration.rs`
- `orphan.rs`
- `statistics.rs`
- `scheduler.rs`

Maintenance tasks exist and can be given txlog/catalog/object-store/config dependencies, but scheduler wiring should be reviewed before assuming fully active production maintenance.

### `k2i-rpc`

The read-state protocol uses magic bytes `K2IR`, protocol version `1`, bincode payloads, and a max frame size from config. Requests include health, table listing, schema fetch, scan begin, and scan end. Responses include table summaries, Arrow IPC schema bytes, read states, ACKs, and structured errors.

Keep protocol structs append-only where possible. Breaking wire changes require a protocol version bump.

### `k2i-rpc-server`

The server listens on a Unix socket, serves the single configured table through `TableReadRegistry`, enforces frame limits, and cleans up the socket file on shutdown.

### `k2i-e2e-runner`

The Docker E2E runner registers Protobuf schemas, produces Confluent-framed Kafka records, queries K2I read RPC, verifies Arrow IPC hot data, checks `/health` and `/readyz`, and validates flushed Parquet with DuckDB.

## Docker E2E

Run:

```bash
scripts/e2e-docker.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh
scripts/e2e-docker-iceberg.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```

The correctness flow covers v1 Protobuf ingest, Parquet flush validation, nullable v2 schema evolution, breaking v3 rejection, schema readiness blocking, and post-pause ingestion protection.

The load flow defaults to 100,000 rows and asserts full visibility plus zero `k2i_errors_total`.

The Iceberg flow starts `apache/iceberg-rest-fixture`, commits appends through the official `iceberg-rust` REST path, and validates the latest metadata file with DuckDB `iceberg_scan`. The Iceberg load flow defaults to 100,000 rows, requires every row to flush cold, validates direct Parquet reads, validates snapshot growth, and checks `k2i_errors_total` stays zero.

## Man Pages

K2I follows the OSO Rust CLI convention used by projects such as `slack-cli`, `xero-cli`, and `keito-cli`: man pages are generated from `clap` definitions with `clap_mangen`.

Regenerate checked-in pages after changing CLI help text, flags, subcommands, or version:

```bash
cargo run -p k2i-cli -- completions man --output-dir docs/man/man1
```

Generated pages live under `docs/man/man1/`. The integration test `crates/k2i-cli/tests/man_pages.rs` verifies the recursive page set and cross-references.

## Documentation Positioning Rules

Release-facing docs should position K2I as:

> Kafka to Apache Iceberg ingestion in one Rust binary.

Use precise current-scope language:

- Say `standalone Kafka-to-Iceberg ingestion engine`, not Kafka Connect plugin.
- Say `production-oriented` unless the caveats below have been closed for the target deployment.
- Say `exactly-once-style durability design` instead of unqualified exactly-once claims.
- Separate hot visibility from cold Iceberg visibility. Hot reads come from local read-state RPC; query engines see cold data after Parquet writes and Iceberg catalog commits.
- Mention DuckDB validation as local Docker E2E coverage with direct Parquet reads and DuckDB `iceberg_scan` against real Iceberg REST metadata.
- Do not claim K2I is a stream processor, CDC tool, multi-source ETL engine, or remote read-serving API.
- Do not claim GCS/Azure writer support as complete; configuration exists, writer creation still needs backend wiring.
- Do not claim unattended maintenance is complete without reviewing scheduler wiring for the deployment.

Current user-facing docs are under `docs/`. Older PRDs, competitive research, and website drafts live under `docs/archive/` and may contain superseded claims.

## Current Implementation Caveats

These caveats matter for feature planning:

1. **Multi-partition flush/commit semantics need continued hardening.** Review mixed-partition batches before making strong exactly-once claims across all partition layouts.
2. **Recovery state is computed but not fully applied.** Startup recovery identifies offsets and orphan files, but Kafka seeking/deduplication and startup orphan cleanup need further wiring.
3. **Kafka commits are async.** `SmartKafkaConsumer::commit_offset` uses `CommitMode::Async`, so it confirms librdkafka accepted the request, not that the broker durably completed it.
4. **Transaction log appends flush but do not fsync every entry.** `sync()` and rotation call stronger sync paths; per-entry power-loss durability is not guaranteed.
5. **`json` format is not a full typed JSON ingest path yet.** The engine currently treats `raw` and `json` through the raw decoder.
6. **`skip-message` schema policy pauses.** This is deliberate to avoid advancing offsets past incompatible data without durable dead-letter accounting.
7. **GCS and Azure object-store creation are declared but not implemented in the writer.**
8. **Hot buffer TTL/eviction behavior should be reviewed.** TTL config exists, but active per-record eviction is not the main freshness mechanism.
9. **Maintenance needs production wiring review.** Do not assume compaction/expiration/orphan cleanup are fully scheduled with all required catalog/object-store handles.

## Design Invariants To Preserve

- Commit Kafka offsets only after data is durably handled by the writer and txlog records required for recovery/idempotency are written.
- Keep ingestion single-process unless explicitly changing product direction.
- Preserve manual Kafka offset management.
- Treat transaction log entries as recovery contracts. Update recovery tests if entry ordering changes.
- Keep object-store writes immutable and commit metadata after upload.
- Prefer `CatalogOperations` for catalog changes.
- Schema evolution must be additive or safely promotable unless a feature explicitly handles destructive changes.
- Read RPC is local and table-scoped; do not expose it remotely without an auth and threat-model pass.
- Maintenance tasks must be safe by default and must not delete recent or currently referenced files.

## Feature Planning Checklist

Before implementing a new feature, answer:

- Is it CLI-only, core-only, or both?
- Does it affect offset commit timing, recovery, idempotency, or read LSN assignment?
- Does it need per-partition handling?
- Does it need real catalog integration or is no-catalog Parquet output sufficient?
- Does it need config changes in `config.rs`, `config/example.toml`, and docs?
- Does it need health, readiness, or metrics updates?
- Can it be tested with unit tests, a local filesystem object store, a mock `CatalogOperations`, or Docker E2E?

For high-risk work, add tests around:

- config deserialization and validation;
- format decoding and schema evolution;
- read registry LSN and hot/cold visibility behavior;
- transaction log entry ordering and recovery;
- writer file path, Parquet output, and catalog commit behavior;
- CLI command behavior and HTTP endpoint output;
- Docker E2E scenarios for real Kafka/Schema Registry flows.

## Useful File Map

```text
crates/k2i-core/src/config.rs
crates/k2i-core/src/engine/ingestion.rs
crates/k2i-core/src/format/mod.rs
crates/k2i-core/src/format/protobuf/mod.rs
crates/k2i-core/src/format/protobuf/registry.rs
crates/k2i-core/src/kafka/consumer.rs
crates/k2i-core/src/buffer/hot_buffer.rs
crates/k2i-core/src/read/mod.rs
crates/k2i-core/src/txlog/*.rs
crates/k2i-core/src/iceberg/writer.rs
crates/k2i-core/src/iceberg/factory.rs
crates/k2i-core/src/iceberg/transaction_coordinator.rs
crates/k2i-core/src/iceberg/table_manager.rs
crates/k2i-core/src/maintenance/*.rs
crates/k2i-core/src/metrics/prometheus.rs
crates/k2i-core/src/health.rs
crates/k2i-rpc/src/lib.rs
crates/k2i-rpc-server/src/lib.rs
crates/k2i-e2e-runner/src/main.rs
crates/k2i-cli/src/commands/*.rs
crates/k2i-cli/src/server.rs
docker/e2e/README.md
docs/production-readiness.md
```

## Recommended Next Architecture Fixes

1. Harden multi-partition flush and offset commit behavior.
2. Apply recovery state to Kafka seeking/deduplication and startup orphan cleanup.
3. Add an explicit schema-pause operator resume or migration workflow.
4. Decide whether Protobuf enums should remain `Utf8` names or use Arrow dictionary projection.
5. Wire maintenance scheduling with catalog/object-store handles, or document it as manual-only.
6. Add CI/nightly coverage for the Docker E2E load and Iceberg load profiles.
