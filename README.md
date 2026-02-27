<p align="center">
  <h1 align="center">K2I</h1>
  <p align="center">
    High-performance streaming ingestion from Kafka to Apache Iceberg
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/k2i/actions/workflows/test.yml">
    <img src="https://github.com/osodevops/k2i/actions/workflows/test.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/k2i/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0">
  </a>
  <a href="https://github.com/osodevops/k2i/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/k2i" alt="Release">
  </a>
</p>

---

**K2I** (Kafka to Iceberg) is a production-grade streaming ingestion engine written in Rust that bridges the latency-cost trade-off in data pipelines. It consumes events from Apache Kafka, buffers them in-memory using Apache Arrow for **sub-second query freshness**, and writes them to Apache Iceberg tables in Parquet format with **exactly-once semantics**.

## Features

- **Sub-second data freshness** - In-memory hot buffer with Arrow columnar storage
- **Exactly-once semantics** - Write-ahead transaction log for crash recovery
- **Multiple catalog backends** - REST, Hive Metastore, AWS Glue, Nessie
- **Smart backpressure** - Automatic consumer pausing when buffer is full
- **Automated maintenance** - Compaction, snapshot expiration, orphan cleanup
- **Production observability** - Prometheus metrics, health endpoints, structured logging
- **Single-process simplicity** - No distributed coordination overhead

## Installation

Download the latest binary from the [GitHub Releases](https://github.com/osodevops/k2i/releases) page.

### macOS (Homebrew)

```bash
brew install osodevops/tap/k2i
```

### Linux / macOS (Shell Installer)

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/osodevops/k2i/releases/latest/download/k2i-cli-installer.sh | sh
```

### Linux (Manual)

Download the appropriate binary for your architecture from [releases](https://github.com/osodevops/k2i/releases):

```bash
# Example for x86_64
curl -LO https://github.com/osodevops/k2i/releases/latest/download/k2i-cli-x86_64-unknown-linux-gnu.tar.xz
tar -xJf k2i-cli-x86_64-unknown-linux-gnu.tar.xz
sudo mv k2i /usr/local/bin/
```

### Docker

```bash
docker pull ghcr.io/osodevops/k2i:latest
docker run --rm -v /path/to/config:/etc/k2i ghcr.io/osodevops/k2i:latest ingest --config /etc/k2i/config.toml
```

See the image on [GitHub Container Registry](https://github.com/osodevops/k2i/pkgs/container/k2i).

### From Source

```bash
git clone https://github.com/osodevops/k2i.git
cd k2i
cargo build --release
```

Binary location: `target/release/k2i`

## Quick Start

### 1. Create Configuration

Create a configuration file `config.toml`:

```toml
[kafka]
bootstrap_servers = "localhost:9092"
topic = "events"
group_id = "k2i-ingestion"

[buffer]
max_size_mb = 500
flush_interval_seconds = 30

[iceberg]
catalog_type = "rest"
rest_uri = "http://localhost:8181"
warehouse = "s3://my-bucket/warehouse"
database = "analytics"
table = "events"

[storage]
type = "s3"
bucket = "my-bucket"
region = "us-east-1"

[server]
health_port = 8080
metrics_port = 9090
```

### 2. Validate Configuration

```bash
k2i validate --config config.toml
```

### 3. Start Ingestion

```bash
k2i ingest --config config.toml
```

### 4. Monitor Health

```bash
# Health check
curl http://localhost:8080/health

# Prometheus metrics
curl http://localhost:9090/metrics
```

## Why K2I?

### The Problem

Modern data architectures face a fundamental tension:

| Approach | Latency | Cost | Complexity |
|----------|---------|------|------------|
| Real-time streaming (Kafka + KSQL) | Milliseconds | High | High |
| Micro-batch (Spark Streaming) | Seconds-Minutes | Medium | Medium |
| Batch ETL (Airflow + Spark) | Minutes-Hours | Low | Low |

Streaming data into Iceberg creates additional challenges:
- **Small file problem** - Each micro-batch creates new files, degrading query performance
- **Exactly-once complexity** - Coordinating Kafka, object storage, and catalog commits
- **Operational burden** - Manual compaction, snapshot expiration, orphan cleanup

### The Solution

K2I resolves these trade-offs through:

1. **Hot/Cold Architecture** - In-memory Arrow buffer for immediate queries, Parquet files for cost-efficient analytics
2. **Write-Ahead Logging** - Transaction log ensures exactly-once semantics and crash recovery
3. **Single-Process Design** - No distributed coordination, deterministic behavior, simple operations
4. **Automated Maintenance** - Background compaction, expiration, and cleanup

### Feature Comparison

| Feature | K2I | Spark Streaming | Flink | Kafka Connect |
|---------|-----|-----------------|-------|---------------|
| Single process | Yes | No | No | Per-connector |
| Sub-second latency | Yes | Minutes | Seconds | Seconds |
| Exactly-once | Yes | Yes | Yes | Depends |
| Auto compaction | Yes | No | No | No |
| Hot buffer queries | Yes | No | No | No |
| Memory footprint | Low | High | High | Medium |
| Operational complexity | Low | High | High | Medium |

### When NOT to Use K2I

- **Complex transformations** - Use Apache Flink for stream processing with joins, aggregations
- **Multi-source ingestion** - K2I is optimized for Kafka; use Flink for diverse sources
- **CDC replication** - For database change data capture with deletes, consider Moonlink

## Architecture

```
+-----------------------------------------------------------------------------+
|                           K2I Ingestion Engine                              |
+-----------------------------------------------------------------------------+
|                                                                             |
|   +------------------+    +------------------+    +------------------+      |
|   | SmartKafka       |    |    Hot Buffer    |    | Iceberg Writer   |      |
|   | Consumer         |--->| (Arrow + Index)  |--->| (Parquet)        |      |
|   |                  |    |                  |    |                  |      |
|   | - rdkafka        |    | - RecordBatch    |    | - Catalog        |      |
|   | - Backpressure   |    | - DashMap Index  |    | - Object Store   |      |
|   | - Retry Logic    |    | - TTL Eviction   |    | - Atomic Commit  |      |
|   +------------------+    +------------------+    +------------------+      |
|            |                       |                       |               |
|            v                       v                       v               |
|   +---------------------------------------------------------------------+  |
|   |                      Transaction Log                                |  |
|   | - Append-only entries with CRC32 checksums                          |  |
|   | - Periodic checkpoints for fast recovery                            |  |
|   | - Idempotency records for exactly-once semantics                    |  |
|   +---------------------------------------------------------------------+  |
|                                                                             |
+-----------------------------------------------------------------------------+
```

## Documentation

| Document | Description |
|----------|-------------|
| [Whitepaper](docs/whitepaper.md) | Comprehensive technical whitepaper |
| [Quick Start](docs/quickstart.md) | Get started in 5 minutes |
| [Configuration](docs/configuration.md) | Complete configuration reference |
| [Architecture](docs/architecture.md) | System design and internals |
| [Commands](docs/commands.md) | CLI command reference |
| [Deployment](docs/deployment.md) | Production deployment guide |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and solutions |

## CLI Reference

```bash
# Validate configuration
k2i validate --config config.toml

# Start ingestion
k2i ingest --config config.toml

# Check service status
k2i status --url http://localhost:8080

# Run manual compaction
k2i maintenance compact --config config.toml

# Expire old snapshots
k2i maintenance expire-snapshots --config config.toml

# Clean orphan files
k2i maintenance cleanup-orphans --config config.toml
```

## Performance

| Metric | Target | Notes |
|--------|--------|-------|
| Query freshness (hot) | < 1ms | In-memory Arrow buffer |
| Query freshness (cold) | 30s | Configurable flush interval |
| Flush latency (P50) | 200ms | End-to-end flush cycle |
| Flush latency (P99) | 800ms | Including catalog commit |
| Throughput | 10-100K msg/s | Configuration dependent |
| Memory usage | 200MB - 2GB | Based on buffer size |

## Catalog Support

K2I supports multiple Iceberg catalog backends:

| Catalog | Configuration | Features |
|---------|---------------|----------|
| **REST** | `catalog_type = "rest"` | OAuth2, Bearer token, custom headers |
| **Hive Metastore** | `catalog_type = "hive"` | Thrift protocol, schema sync |
| **AWS Glue** | `catalog_type = "glue"` | IAM roles, cross-account access |
| **Nessie** | `catalog_type = "nessie"` | Git-like branching, time travel |

## Building from Source

**Requirements:**
- Rust 1.85+ (for edition 2024)
- CMake
- OpenSSL development libraries

> **Kerberos support:** If you need Kerberos (GSSAPI) authentication, add `"gssapi"` to the rdkafka features in `Cargo.toml` and install `cyrus-sasl` (`brew install cyrus-sasl` on macOS, `libsasl2-dev` on Debian/Ubuntu). See [Building with Kerberos Support](docs/configuration.md#building-with-kerberos-support) for details.

```bash
# Clone the repository
git clone https://github.com/osodevops/k2i.git
cd k2i

# Build release binary
cargo build --release

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -p k2i-cli -- --help
```

## Running Tests

```bash
# Unit tests
cargo test --lib

# All tests
cargo test

# With coverage
cargo tarpaulin --out Html
```

## Project Structure

```
k2i/
├── crates/
│   ├── k2i-core/           # Core library
│   │   ├── src/
│   │   │   ├── kafka/      # Smart Kafka consumer
│   │   │   ├── buffer/     # Hot buffer (Arrow + indexes)
│   │   │   ├── iceberg/    # Iceberg writer
│   │   │   ├── txlog/      # Transaction log
│   │   │   ├── catalog/    # Catalog backends
│   │   │   └── maintenance/# Compaction, expiration
│   │   └── tests/
│   └── k2i-cli/            # CLI binary
├── config/                  # Example configurations
└── docs/                    # Documentation
```

## Looking for Enterprise Apache Kafka Support?

[OSO](https://oso.sh) engineers are solely focused on deploying, operating, and maintaining Apache Kafka platforms. If you need SLA-backed support or advanced features for compliance and security, our **Enterprise Edition** extends the core tool with capabilities designed for large-scale, regulated environments.

### K2I: Enterprise Edition

| Feature Category | Enterprise Capability |
|------------------|----------------------|
| **Security & Compliance** | AES-256 Encryption (client-side encryption at rest) |
| | GDPR Compliance Tools (PII masking, data retention policies) |
| | Audit Logging (comprehensive trail of all operations) |
| | Role-Based Access Control (granular permissions) |
| **Advanced Integrations** | Schema Registry Integration (Avro/Protobuf with ID mapping) |
| | Secrets Management (Vault / AWS Secrets Manager integration) |
| | SSO / OIDC (Okta, Azure AD, Google Auth) |
| **Scale & Operations** | Multi-Table Support (single process, multiple destinations) |
| | Log Shipping (Datadog, Splunk, Grafana Loki) |
| | Advanced Metrics & Dashboard (throughput, latency, drill-down UI) |
| **Support** | 24/7 SLA-Backed Support & dedicated Kafka consulting |

Need help resolving operational issues or planning a data lakehouse strategy? Our team of experts can help you design, deploy, and operate K2I at scale.

**[Talk with an expert today](https://oso.sh/contact/)** or email us at **enquiries@oso.sh**.

## Contributing

We welcome contributions of all kinds!

- **Report Bugs:** Found a bug? Open an [issue on GitHub](https://github.com/osodevops/k2i/issues).
- **Suggest Features:** Have an idea? [Request a feature](https://github.com/osodevops/k2i/issues/new).
- **Contribute Code:** Check out our [good first issues](https://github.com/osodevops/k2i/labels/good%20first%20issue) for beginner-friendly tasks.
- **Improve Docs:** Help us improve the documentation by submitting pull requests.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

K2I is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgments

K2I draws inspiration from the [Moonlink architecture](https://www.mooncake.dev/moonlink/) by Mooncake Labs.

Built with these excellent Rust crates:
- [rdkafka](https://crates.io/crates/rdkafka) - Kafka consumer (librdkafka bindings)
- [arrow](https://crates.io/crates/arrow) - In-memory columnar storage
- [parquet](https://crates.io/crates/parquet) - File format encoding
- [iceberg](https://crates.io/crates/iceberg) - Table format operations
- [tokio](https://tokio.rs) - Async runtime
- [axum](https://crates.io/crates/axum) - HTTP servers

---

<p align="center">
  Made with care by <a href="https://oso.sh">OSO</a>
</p>
