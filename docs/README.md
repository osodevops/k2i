# K2I Documentation

K2I (Kafka to Iceberg) is a production-grade streaming ingestion engine that bridges the latency-cost trade-off in data pipelines. It consumes events from Apache Kafka, buffers them in-memory with Apache Arrow for sub-second query freshness, and writes them to Apache Iceberg tables in Parquet format.

## Overview

K2I is a single-process, embedded Rust application inspired by the Moonlink architecture from pg_mooncake. It provides:

- **Sub-second data freshness** via in-memory hot buffer
- **Cost-efficient analytics** through Iceberg's columnar format
- **Exactly-once semantics** with transaction log recovery
- **Automatic maintenance** including compaction, snapshot expiration, and orphan cleanup

## Key Features

| Feature | Description |
|---------|-------------|
| Hot Buffer | Arrow-based in-memory storage with hash indexes for O(1) lookups |
| Iceberg Writer | Atomic commits with support for REST, Hive, Glue, and Nessie catalogs |
| Transaction Log | Append-only log for crash recovery and exactly-once semantics |
| Backpressure | Smart consumer that pauses when buffer is full |
| Circuit Breaker | Fault tolerance for catalog operations |
| Prometheus Metrics | Full observability with counters, gauges, and histograms |

## Documentation

| Guide | Description |
|-------|-------------|
| [Quickstart](./quickstart.md) | Get K2I running in 5 minutes |
| [Configuration](./configuration.md) | Complete configuration reference |
| [Architecture](./architecture.md) | System design and internals |
| [Commands](./commands.md) | CLI command reference |
| [Deployment](./deployment.md) | Production deployment guide |
| [Troubleshooting](./troubleshooting.md) | Common issues and solutions |

## Quick Example

```bash
# Validate configuration
k2i validate --config config.toml

# Start ingestion
k2i ingest --config config.toml

# Check status
k2i status --url http://localhost:8080

# Run manual compaction
k2i maintenance compact --config config.toml
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    K2I Ingestion Engine                     │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌────────────┐  ┌──────────────────────┐ │
│  │ Kafka        │  │ Hot Buffer │  │ Iceberg Writer       │ │
│  │ Consumer     │→ │ (Arrow +   │→ │ (Parquet + Catalog)  │ │
│  │ (rdkafka)    │  │ DashMap)   │  │                      │ │
│  └──────────────┘  └────────────┘  └──────────────────────┘ │
│         ↓                ↓                    ↓              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Transaction Log (crash recovery, exactly-once)      │    │
│  └─────────────────────────────────────────────────────┘    │
│         ↓                                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ HTTP Servers: /health /healthz /readyz /metrics     │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Requirements

- Rust 1.75 or later (for building from source)
- Apache Kafka 2.8+
- Iceberg-compatible catalog (REST, Hive, AWS Glue, or Nessie)
- Object storage (S3, GCS, Azure Blob, or local filesystem)

## License

Apache 2.0

## Support

For issues and feature requests, visit: https://github.com/osodevops/k2i
