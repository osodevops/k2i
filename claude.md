# K2I - Kafka to Iceberg Streaming Ingestion Engine

## What This Project Is

K2I is a production-grade streaming ingestion engine written in Rust that bridges the latency-cost gap in modern data pipelines. It consumes events from Apache Kafka, buffers them in-memory using Apache Arrow for sub-second query freshness, and writes them to Apache Iceberg tables in Parquet format with exactly-once semantics.

## Why We Built It

The data engineering world forces a painful trade-off:
- **Real-time systems** (Flink, KSQL): millisecond latency but high cost and operational complexity
- **Batch systems** (Spark, Airflow): low cost but minutes-to-hours of latency

K2I delivers **sub-second data freshness at batch economics** through a single-process architecture that eliminates distributed coordination overhead. It is inspired by Moonlink (pg_mooncake) but purpose-built for Kafka-native workloads rather than CDC.

## Core Concepts

### Hot/Cold Data Separation
- **Hot buffer**: In-memory Arrow RecordBatches with DashMap indexes for <1ms query lookups
- **Cold storage**: Parquet files on object storage (S3/GCS/Azure) committed to Iceberg catalogs

### Single-Process Simplicity
No cluster coordination, no ZooKeeper, no JobManager. One binary, one config file, production-ready. This is a deliberate architectural choice — not a limitation.

### Transaction Log Durability
Append-only write-ahead log with CRC32 checksums ensures crash recovery without data loss. Offset ranges are tracked for idempotency, enabling exactly-once semantics without distributed transactions.

### Defensive Design
Backpressure propagation from buffer to Kafka consumer, circuit breakers for downstream failures, exponential backoff retry, and graceful degradation under load.

## Architecture

```
Kafka Brokers
     ↓
SmartKafkaConsumer (rdkafka + backpressure + retry)
     ↓
Hot Buffer (Arrow RecordBatch + DashMap indexes)
     ↓
Transaction Log (append-only WAL with CRC32)
     ↓
IcebergWriter (Parquet + catalog commits via CAS)
     ↓
Object Storage (S3/GCS/Azure) + Iceberg Catalog (REST/Glue/Hive/Nessie)
```

## Codebase Structure

```
crates/
├── k2i-core/src/          # Core library (~19,887 LOC)
│   ├── kafka/             # Smart consumer with backpressure
│   ├── buffer/            # Hot buffer (Arrow + indexes)
│   ├── iceberg/           # Writer + catalog integration
│   ├── txlog/             # Transaction log (WAL)
│   ├── engine/            # Orchestration & ingestion loop
│   ├── maintenance/       # Compaction, expiration, cleanup
│   ├── metrics/           # Prometheus instrumentation
│   ├── config.rs          # TOML configuration
│   ├── error.rs           # Error types (thiserror)
│   ├── health.rs          # HTTP health endpoints
│   └── circuit_breaker.rs # Fault tolerance
├── k2i-cli/src/           # CLI binary
│   ├── main.rs            # Entry point + signal handling
│   ├── server.rs          # Axum HTTP servers
│   └── commands/          # ingest, status, maintenance
docs/
├── whitepaper.md          # Technical deep-dive (1520 lines)
├── architecture.md        # System design
├── configuration.md       # Config reference
├── quickstart.md          # 5-minute Docker Compose setup
├── commands.md            # CLI reference
├── deployment.md          # Production deployment
└── troubleshooting.md     # Common issues
config/
└── example.toml           # Reference configuration
```

## Tech Stack

- **Language**: Rust 1.85+ (edition 2021)
- **Async Runtime**: Tokio
- **HTTP**: Axum
- **Kafka**: rdkafka (librdkafka bindings, SASL/SSL)
- **Columnar**: Apache Arrow 54
- **File Format**: Parquet 54 (Snappy/Zstd/LZ4)
- **Table Format**: Iceberg 0.7
- **Object Storage**: object_store 0.11
- **Concurrency**: DashMap 6
- **Metrics**: Prometheus 0.13
- **Logging**: tracing + JSON
- **Testing**: testcontainers, proptest

## Key Design Decisions

1. **Exactly-once via WAL + idempotency tracking** — no external state store needed
2. **Manual Kafka offset commits** — only after durable persistence confirmed
3. **Arrow as the in-memory format** — zero-copy reads, columnar compression, direct Parquet conversion
4. **DashMap indexes** — lock-free concurrent lookups by business key and partition/offset
5. **Flush triggers** — time (30s) OR size (500MB) OR count (10K rows), whichever fires first
6. **CAS catalog commits** — optimistic concurrency with exponential backoff on conflict
7. **Background maintenance** — compaction, snapshot expiration, orphan cleanup without blocking ingestion

## Crash Recovery Guarantees

| Crash Point | Recovery Behavior |
|-------------|-------------------|
| Before FlushStart | Resume from last committed offset |
| After FlushStart | Re-execute flush from buffer |
| After ParquetWritten | File exists as orphan, cleanup later |
| After IcebergSnapshot | Skip on recovery (already committed) |
| After FlushComplete | Fully committed, advance offset |

## Development Guidelines

- Run `cargo build` to build the workspace
- Run `cargo test` for unit tests
- Integration tests require Docker (testcontainers for Kafka, MinIO, Iceberg REST catalog)
- Configuration lives in TOML files (see `config/example.toml`)
- All errors use `thiserror` for typed errors, `anyhow` for context propagation
- Metrics follow Prometheus naming conventions (`k2i_messages_total`, `k2i_flush_duration_seconds`)
- Health endpoints: port 8080 (`/health`, `/healthz`, `/readyz`)
- Metrics endpoint: port 9090 (`/metrics`)

## What K2I Is NOT

- Not a stream processing framework (no joins, windows, or transformations)
- Not a CDC tool (Kafka-native, not database replication)
- Not a multi-source ETL pipeline
- Not a distributed system (single-process by design)
