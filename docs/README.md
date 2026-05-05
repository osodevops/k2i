# K2I Documentation

K2I is a standalone Rust service for Kafka-to-Apache-Iceberg ingestion. It consumes a configured Kafka topic, decodes raw or Confluent-framed Protobuf messages, keeps recent rows visible through an Arrow-backed local read path, and writes Parquet data files through Iceberg catalog commits.

The current release docs are organized around the working implementation and local verification flows. Historical PRDs, research notes, and older website drafts live under [archive](./archive/).

## Start Here

| Guide | Use It For |
|---|---|
| [Kafka to Iceberg](./kafka-to-iceberg.md) | Main explanation of the K2I data path |
| [Quickstart](./quickstart.md) | Local Docker proof and first manual run |
| [Configuration](./configuration.md) | Complete TOML reference |
| [Architecture](./architecture.md) | System design, ordering, and hot/cold visibility |
| [Comparisons](./comparisons.md) | K2I vs Kafka Connect, Flink, Spark, TableFlow, and Moonlink |
| [FAQ](./faq.md) | Short answers for common user questions |

## Implementation Deep Dives

| Guide | Use It For |
|---|---|
| [DuckDB Iceberg Validation](./duckdb-iceberg-validation.md) | Docker E2E, direct Parquet reads, and DuckDB `iceberg_scan` |
| [Schema Registry Protobuf](./schema-registry-protobuf.md) | Confluent Protobuf decoding and schema evolution behavior |
| [Iceberg REST Catalog](./iceberg-rest-catalog.md) | REST catalog commits and catalog backend caveats |
| [Commands](./commands.md) | CLI command reference and E2E scripts |
| [Man Pages](./man/man1/k2i.1) | Generated man pages for every CLI command and subcommand |
| [Deployment](./deployment.md) | Deployment patterns and operational notes |
| [Troubleshooting](./troubleshooting.md) | Common issues and recovery guidance |
| [Production Readiness](./production-readiness.md) | Verification status, caveats, and follow-up issues |

## Quick Local Proof

```bash
# Correctness flow: Protobuf evolution, read-state RPC, DuckDB Parquet checks
scripts/e2e-docker.sh

# Real Iceberg REST metadata and DuckDB iceberg_scan
scripts/e2e-docker-iceberg.sh

# 100,000-row Iceberg load profile
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```

The Iceberg E2E success line is:

```text
ok: DuckDB iceberg_scan validated real Iceberg metadata
```

## Current Release Scope

K2I is production-oriented, but the docs intentionally keep caveats visible:

- one configured Kafka topic and one configured Iceberg table per process today;
- REST catalog real-metadata path validated locally; Glue, Hive, and Nessie abstractions require backend-specific validation;
- hot reads are local read-state RPC, while query engines see data after an Iceberg commit;
- exactly-once-style durability is designed around manual Kafka offsets, transaction-log records, idempotency records, immutable Parquet writes, and atomic Iceberg commits;
- multi-partition hardening, startup recovery application, async Kafka commit acknowledgement, per-entry fsync behavior, GCS/Azure writer wiring, and maintenance scheduler wiring remain production follow-ups.

See [Production Readiness](./production-readiness.md) before broad rollout.
