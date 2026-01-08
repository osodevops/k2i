# K2I Quickstart Guide

Get K2I running in 5 minutes with this quickstart guide.

## Prerequisites

Before you begin, ensure you have:

- Rust 1.75+ installed (`rustup update stable`)
- Docker and Docker Compose (for local testing)
- Access to a Kafka cluster
- Access to an Iceberg catalog (REST, Hive, Glue, or Nessie)

## Installation

### Build from Source

```bash
# Clone the repository
git clone https://github.com/osodevops/k2i.git
cd k2i

# Build release binary
cargo build --release

# Binary will be at target/release/k2i
```

### Verify Installation

```bash
./target/release/k2i --version
# k2i 0.1.0

./target/release/k2i --help
```

## Local Development Setup

For local testing, use Docker Compose to run Kafka and an Iceberg REST catalog.

### docker-compose.yml

```yaml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    hostname: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

  iceberg-rest:
    image: tabulario/iceberg-rest:0.10.0
    ports:
      - "8181:8181"
    environment:
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_REGION: us-east-1
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: true
    depends_on:
      - minio

volumes:
  minio-data:
```

### Start Services

```bash
# Start all services
docker-compose up -d

# Create MinIO bucket
docker exec -it <minio-container> mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec -it <minio-container> mc mb local/warehouse

# Create Kafka topic
docker exec -it <kafka-container> kafka-topics --create \
  --topic events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

## Configuration

Create a configuration file `config.toml`:

```toml
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i-ingestion"
batch_size = 1000
batch_timeout_ms = 5000
auto_offset_reset = "earliest"

[iceberg]
catalog_type = "rest"
rest_uri = "http://localhost:8181"
warehouse_path = "s3://warehouse/"
database_name = "raw"
table_name = "events"
target_file_size_mb = 128
compression = "snappy"

# MinIO S3 configuration
s3_endpoint = "http://localhost:9000"
aws_region = "us-east-1"
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"

[buffer]
ttl_seconds = 60
max_size_mb = 100
flush_interval_seconds = 30
flush_batch_size = 5000

[transaction_log]
log_dir = "./transaction_logs"
checkpoint_interval_entries = 10000
checkpoint_interval_seconds = 300

[maintenance]
compaction_enabled = true
compaction_interval_seconds = 3600
snapshot_expiration_enabled = true
snapshot_retention_days = 7

[monitoring]
metrics_port = 9090
health_port = 8080
log_level = "info"
log_format = "text"
```

## Validate Configuration

Before starting, validate your configuration:

```bash
k2i validate --config config.toml
# Configuration is valid
```

## Start Ingestion

Run the ingestion engine:

```bash
k2i ingest --config config.toml
```

You should see output like:

```
INFO k2i::engine: Starting K2I ingestion engine
INFO k2i::kafka: Connecting to Kafka brokers: localhost:9092
INFO k2i::kafka: Subscribed to topic: events
INFO k2i::buffer: Hot buffer initialized (max: 100MB, TTL: 60s)
INFO k2i::iceberg: Connected to REST catalog at http://localhost:8181
INFO k2i::server: Health endpoint listening on 0.0.0.0:8080
INFO k2i::server: Metrics endpoint listening on 0.0.0.0:9090
```

## Produce Test Data

In a separate terminal, produce some test messages:

```bash
# Using kafka-console-producer
docker exec -it <kafka-container> kafka-console-producer \
  --topic events \
  --bootstrap-server localhost:9092

# Type JSON messages:
{"event_id": "1", "event_type": "click", "timestamp": "2024-01-08T10:00:00Z"}
{"event_id": "2", "event_type": "view", "timestamp": "2024-01-08T10:00:01Z"}
{"event_id": "3", "event_type": "purchase", "timestamp": "2024-01-08T10:00:02Z"}
```

## Check Status

Check the health and metrics of the running instance:

```bash
# Health check
k2i status --url http://localhost:8080

# Output:
# Health Status: Healthy
# Components:
#   kafka: Healthy
#   buffer: Healthy
#   iceberg: Healthy
#   catalog: Healthy
#   txlog: Healthy
# Metrics:
#   messages_consumed: 3
#   rows_flushed: 0
#   buffer_flushes: 0
#   errors: 0
```

## View Prometheus Metrics

Access Prometheus metrics at http://localhost:9090/metrics:

```
# HELP k2i_messages_total Total number of messages consumed from Kafka
# TYPE k2i_messages_total counter
k2i_messages_total 3

# HELP k2i_buffer_size_bytes Current size of the hot buffer in bytes
# TYPE k2i_buffer_size_bytes gauge
k2i_buffer_size_bytes 1024

# HELP k2i_buffer_record_count Current number of records in the hot buffer
# TYPE k2i_buffer_record_count gauge
k2i_buffer_record_count 3
```

## Graceful Shutdown

Stop the ingestion engine gracefully with Ctrl+C (SIGINT) or SIGTERM:

```
^C
INFO k2i::engine: Received shutdown signal, flushing buffer...
INFO k2i::buffer: Flushing 3 records to Iceberg
INFO k2i::iceberg: Committed snapshot 12345
INFO k2i::engine: Shutdown complete
```

## Run Manual Maintenance

Execute maintenance tasks manually:

```bash
# Compact small files
k2i maintenance compact --config config.toml

# Expire old snapshots
k2i maintenance expire-snapshots --config config.toml

# Clean orphan files
k2i maintenance clean-orphans --config config.toml
```

## Next Steps

- Read the [Configuration Reference](./configuration.md) for all available options
- Learn about the [Architecture](./architecture.md) for deeper understanding
- See [Deployment](./deployment.md) for production setup
- Check [Troubleshooting](./troubleshooting.md) for common issues

## Quick Reference

| Command | Description |
|---------|-------------|
| `k2i ingest` | Start the ingestion engine |
| `k2i status` | Check health and metrics |
| `k2i validate` | Validate configuration |
| `k2i maintenance compact` | Run file compaction |
| `k2i maintenance expire-snapshots` | Remove old snapshots |
| `k2i maintenance clean-orphans` | Delete orphan files |

| Endpoint | Description |
|----------|-------------|
| `http://localhost:8080/health` | Full health status (JSON) |
| `http://localhost:8080/healthz` | Kubernetes liveness probe |
| `http://localhost:8080/readyz` | Kubernetes readiness probe |
| `http://localhost:9090/metrics` | Prometheus metrics |
