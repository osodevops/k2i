# K2I FAQ

## What is K2I?

K2I is a standalone Rust service that consumes a Kafka topic and writes the data to an Apache Iceberg table. It keeps recent rows visible through a local Arrow-backed read path and flushes Parquet files through Iceberg catalog commits.

## Is K2I a Kafka Connect plugin?

No. K2I is not a Kafka Connect plugin. It has its own Kafka consumer, configuration file, transaction log, CLI, health server, metrics server, writer, and optional read-state RPC server.

## How is K2I different from a Kafka Connect Iceberg sink?

Kafka Connect sinks run inside a Connect cluster and follow the Connect lifecycle. K2I is a single service/container. Choose K2I when you want a narrow, self-hosted Kafka-to-Iceberg process with local E2E validation and an optional hot-read path.

## How is K2I different from Flink's Iceberg sink?

Flink is a stream processor and is better for joins, windows, stateful transformations, and complex event-time logic. K2I intentionally avoids those features and focuses on final-form Kafka events that can be written directly to Iceberg.

## How is K2I different from Confluent TableFlow?

TableFlow is a managed Confluent-native path. K2I is open source and self-hosted. K2I is a better fit when you want to run the ingestion process yourself and validate output locally with Docker and DuckDB.

## Is K2I a CDC tool like Moonlink?

No. K2I is Kafka-native and append-oriented. It does not implement CDC delete/update semantics or deletion vectors today.

## Does K2I provide exactly-once delivery?

K2I is designed for exactly-once-style durability through manual Kafka offset management, transaction-log records, idempotency records, immutable Parquet writes, and atomic Iceberg commits. Remaining hardening work is tracked in [Production Readiness](./production-readiness.md).

## What happens if K2I crashes during a flush?

K2I records offsets, flush stages, committed data files, and idempotency information in the transaction log. Recovery state is computed from those records. Startup recovery application and orphan cleanup still need additional hardening before broad production rollout.

## How fresh is data in K2I?

There are two paths. Hot rows can be visible through the local read-state RPC before the next Iceberg commit. Cold Iceberg visibility depends on flush thresholds, Parquet file writes, and catalog commit timing.

## What does the read-state RPC expose?

The optional Unix socket read-state RPC exposes table summaries, Arrow IPC schema bytes, hot Arrow IPC rows, committed data file references, and scan lifecycle messages for the single configured table.

## Can DuckDB read the Iceberg tables written by K2I?

Yes. The Docker Iceberg E2E validates direct Parquet reads and DuckDB `iceberg_scan` against real Iceberg REST metadata written by K2I.

## Which Iceberg catalogs does K2I support?

Configuration and abstractions exist for REST, Glue, Hive, and Nessie. The real metadata path currently validated end-to-end is REST via the Iceberg REST fixture.

## How does K2I handle Protobuf schema evolution?

K2I resolves Confluent-framed Protobuf schemas through Schema Registry. In `auto-additive` mode, it can add compatible nullable fields. Breaking changes pause readiness and avoid advancing offsets past incompatible data.

## What schema changes are rejected?

Removed fields, incompatible type changes, unsafe required-field additions, and other changes that cannot be safely projected are treated as breaking.

## Can K2I auto-create or reset a table locally?

The CLI includes table management commands. Use `k2i table --help` and review [Commands](./commands.md). For production, validate table lifecycle behavior against the exact catalog backend.

## What local E2E tests should I run before a release?

```bash
scripts/e2e-docker.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh
scripts/e2e-docker-iceberg.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```

## What remains to harden before broad production rollout?

The key follow-ups are multi-partition commit hardening, startup recovery application, async Kafka commit acknowledgement, per-entry fsync decisions, GCS/Azure writer wiring, maintenance scheduler wiring, and CI/nightly coverage for heavy Docker load profiles.
