# K2I Configuration Reference

K2I uses TOML configuration files. This document describes all available configuration options.

## Configuration File Location

By default, K2I looks for `config.toml` in the current directory. Override with:

```bash
k2i ingest --config /path/to/config.toml
```

## Complete Configuration Example

```toml
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i-ingestion"
batch_size = 1000
batch_timeout_ms = 5000
session_timeout_ms = 30000
heartbeat_interval_ms = 3000
max_poll_interval_ms = 300000
auto_offset_reset = "earliest"

[kafka.security]
protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-256"
sasl_username = "user"
sasl_password = "password"
ssl_ca_location = "/path/to/ca.pem"
ssl_cert_location = "/path/to/cert.pem"
ssl_key_location = "/path/to/key.pem"

[iceberg]
catalog_type = "rest"
warehouse_path = "s3://bucket/warehouse"
database_name = "raw"
table_name = "events"
target_file_size_mb = 512
compression = "snappy"
rest_uri = "http://localhost:8181"
aws_region = "us-east-1"
aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
s3_endpoint = "http://localhost:9000"

[[iceberg.partition_spec]]
source_field = "event_timestamp"
transform = "day"

[buffer]
ttl_seconds = 60
max_size_mb = 500
flush_interval_seconds = 30
flush_batch_size = 10000
memory_alignment_bytes = 64

[transaction_log]
log_dir = "./transaction_logs"
checkpoint_interval_entries = 10000
checkpoint_interval_seconds = 300
max_log_files = 10

[maintenance]
compaction_enabled = true
compaction_interval_seconds = 3600
compaction_threshold_mb = 100
compaction_target_mb = 512
snapshot_expiration_enabled = true
snapshot_retention_days = 7
orphan_cleanup_enabled = true
orphan_retention_days = 3

[monitoring]
metrics_port = 9090
health_port = 8080
log_level = "info"
log_format = "json"
```

---

## Kafka Configuration

### [kafka]

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bootstrap_servers` | Array[String] | Yes | - | Kafka broker addresses |
| `topic` | String | Yes | - | Kafka topic to consume |
| `consumer_group` | String | Yes | - | Consumer group ID |
| `batch_size` | Integer | No | 1000 | Messages per poll batch |
| `batch_timeout_ms` | Integer | No | 5000 | Poll timeout in milliseconds |
| `session_timeout_ms` | Integer | No | 30000 | Consumer session timeout |
| `heartbeat_interval_ms` | Integer | No | 3000 | Heartbeat interval |
| `max_poll_interval_ms` | Integer | No | 300000 | Max time between polls (5 min) |
| `auto_offset_reset` | String | No | "earliest" | Offset reset policy: `earliest` or `latest` |

#### Notes

- `max_poll_interval_ms` must exceed the longest possible flush time to avoid consumer group rebalances
- `session_timeout_ms` should be at least 3x `heartbeat_interval_ms`
- Use `earliest` to process all historical data, `latest` for new messages only

### [kafka.security]

Optional section for authenticated Kafka clusters.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `protocol` | String | No | "PLAINTEXT" | Security protocol |
| `sasl_mechanism` | String | Conditional | - | SASL mechanism (required if protocol uses SASL) |
| `sasl_username` | String | Conditional | - | SASL username |
| `sasl_password` | String | Conditional | - | SASL password |
| `ssl_ca_location` | String | No | - | Path to CA certificate |
| `ssl_cert_location` | String | No | - | Path to client certificate |
| `ssl_key_location` | String | No | - | Path to client private key |

#### Security Protocols

| Protocol | Description |
|----------|-------------|
| `PLAINTEXT` | No encryption or authentication |
| `SSL` | TLS encryption, no SASL |
| `SASL_PLAINTEXT` | SASL authentication, no encryption |
| `SASL_SSL` | SASL authentication with TLS encryption |

#### SASL Mechanisms

| Mechanism | Description |
|-----------|-------------|
| `PLAIN` | Simple username/password |
| `SCRAM-SHA-256` | Salted challenge-response (recommended) |
| `SCRAM-SHA-512` | Stronger variant of SCRAM |

> **Kerberos (GSSAPI):** Kerberos authentication is not included in the default build. To enable it, you must build k2i from source with the `gssapi` feature. See [Building with Kerberos Support](#building-with-kerberos-support) below.

---

## Iceberg Configuration

### [iceberg]

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `catalog_type` | String | Yes | - | Catalog type: `rest`, `hive`, `glue`, `nessie` |
| `warehouse_path` | String | Yes | - | Base path for Iceberg tables |
| `database_name` | String | Yes | - | Database/namespace name |
| `table_name` | String | Yes | - | Table name |
| `target_file_size_mb` | Integer | No | 512 | Target Parquet file size |
| `compression` | String | No | "snappy" | Compression: `snappy`, `zstd`, `lz4`, `gzip`, `none` |

#### Catalog-Specific Options

**REST Catalog:**
| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `rest_uri` | String | Yes | REST catalog endpoint URL |

**Hive Metastore:**
| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `hive_metastore_uri` | String | Yes | Thrift metastore URI |

**AWS Glue:**
| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `aws_region` | String | Yes | AWS region |
| `aws_access_key_id` | String | No | AWS access key (or use IAM role) |
| `aws_secret_access_key` | String | No | AWS secret key |

**Nessie:**
| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `rest_uri` | String | Yes | Nessie server endpoint |

#### Object Storage Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `aws_region` | String | No | - | AWS region for S3 |
| `aws_access_key_id` | String | No | - | AWS access key ID |
| `aws_secret_access_key` | String | No | - | AWS secret access key |
| `s3_endpoint` | String | No | - | Custom S3 endpoint (for MinIO, LocalStack) |

#### Warehouse Path Formats

| Storage | Format |
|---------|--------|
| AWS S3 | `s3://bucket-name/path/` |
| Google Cloud Storage | `gs://bucket-name/path/` |
| Azure Blob Storage | `az://container/path/` |
| Local Filesystem | `file:///absolute/path/` |

### [[iceberg.partition_spec]]

Optional array of partition specifications. Multiple specs can be defined.

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `source_field` | String | Yes | Source column name |
| `transform` | String | Yes | Partition transform |

#### Partition Transforms

| Transform | Description | Example |
|-----------|-------------|---------|
| `identity` | Use value as-is | `country` |
| `year` | Extract year from timestamp | `year(event_time)` |
| `month` | Extract year-month | `month(event_time)` |
| `day` | Extract year-month-day | `day(event_time)` |
| `hour` | Extract year-month-day-hour | `hour(event_time)` |
| `bucket[N]` | Hash into N buckets | `bucket[16](user_id)` |
| `truncate[N]` | Truncate string to N chars | `truncate[2](country_code)` |

#### Example Partition Specs

```toml
# Partition by day
[[iceberg.partition_spec]]
source_field = "event_timestamp"
transform = "day"

# Partition by region
[[iceberg.partition_spec]]
source_field = "region"
transform = "identity"

# Bucket by user_id
[[iceberg.partition_spec]]
source_field = "user_id"
transform = "bucket[16]"
```

---

## Buffer Configuration

### [buffer]

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `ttl_seconds` | Integer | No | 60 | Time-to-live for buffered records |
| `max_size_mb` | Integer | No | 500 | Maximum buffer size in MB |
| `flush_interval_seconds` | Integer | No | 30 | Time-based flush trigger |
| `flush_batch_size` | Integer | No | 10000 | Count-based flush trigger |
| `memory_alignment_bytes` | Integer | No | 64 | Memory alignment for SIMD (power of 2) |

#### Flush Triggers

The buffer flushes to Iceberg when ANY of these conditions is met:
1. Time elapsed since last flush exceeds `flush_interval_seconds`
2. Record count exceeds `flush_batch_size`
3. Buffer size exceeds `max_size_mb`

#### Memory Alignment

Set `memory_alignment_bytes` to match your CPU's SIMD width:
- **64** (default): AVX-512, most modern x86_64
- **32**: AVX2
- **16**: SSE

---

## Transaction Log Configuration

### [transaction_log]

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `log_dir` | String | No | "./transaction_logs" | Directory for log files |
| `checkpoint_interval_entries` | Integer | No | 10000 | Checkpoint every N entries |
| `checkpoint_interval_seconds` | Integer | No | 300 | Checkpoint every N seconds |
| `max_log_files` | Integer | No | 10 | Maximum log files to retain |

#### Notes

- Transaction log enables exactly-once semantics and crash recovery
- Checkpoints create snapshots of system state for faster recovery
- Log files are automatically rotated based on entry count or time
- Each entry includes CRC32 checksum for integrity verification

---

## Maintenance Configuration

### [maintenance]

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `compaction_enabled` | Boolean | No | true | Enable automatic compaction |
| `compaction_interval_seconds` | Integer | No | 3600 | Compaction run interval |
| `compaction_threshold_mb` | Integer | No | 100 | Files smaller than this are compaction candidates |
| `compaction_target_mb` | Integer | No | 512 | Target size for compacted files |
| `snapshot_expiration_enabled` | Boolean | No | true | Enable snapshot expiration |
| `snapshot_retention_days` | Integer | No | 7 | Keep snapshots for N days |
| `orphan_cleanup_enabled` | Boolean | No | true | Enable orphan file cleanup |
| `orphan_retention_days` | Integer | No | 3 | Safety buffer before deleting orphans |

#### Compaction

Compaction merges small Parquet files into larger ones for better query performance:
- Files smaller than `compaction_threshold_mb` are candidates
- Multiple files are merged until reaching `compaction_target_mb`
- Runs automatically every `compaction_interval_seconds`

#### Snapshot Expiration

Removes old Iceberg snapshots:
- Keeps snapshots younger than `snapshot_retention_days`
- Reduces metadata bloat
- Does not delete data files still referenced by other snapshots

#### Orphan Cleanup

Removes unreferenced data files:
- Files not referenced by any snapshot
- Must be older than `orphan_retention_days` (safety buffer)
- Prevents storage cost from abandoned files

---

## Monitoring Configuration

### [monitoring]

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `metrics_port` | Integer | No | 9090 | Prometheus metrics port |
| `health_port` | Integer | No | 8080 | Health check endpoint port |
| `log_level` | String | No | "info" | Log level |
| `log_format` | String | No | "json" | Log format |

#### Log Levels

| Level | Description |
|-------|-------------|
| `trace` | Very detailed debugging information |
| `debug` | Debugging information |
| `info` | Informational messages (recommended for production) |
| `warn` | Warning messages |
| `error` | Error messages only |

#### Log Formats

| Format | Description |
|--------|-------------|
| `json` | Structured JSON logs (recommended for production) |
| `text` | Human-readable text logs (recommended for development) |

---

## Environment Variable Substitution

Configuration values can reference environment variables:

```toml
[kafka.security]
sasl_username = "${KAFKA_USERNAME}"
sasl_password = "${KAFKA_PASSWORD}"

[iceberg]
aws_access_key_id = "${AWS_ACCESS_KEY_ID}"
aws_secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
```

---

## CLI Overrides

Some configuration options can be overridden via CLI flags:

```bash
k2i ingest \
  --config config.toml \
  --bootstrap-servers "kafka1:9092,kafka2:9092" \
  --topic "production-events" \
  --consumer-group "k2i-prod"
```

CLI flags take precedence over config file values.

---

## Configuration Validation

Validate your configuration before running:

```bash
k2i validate --config config.toml
```

Common validation checks:
- Required fields are present
- Kafka bootstrap servers are non-empty
- Iceberg catalog type is valid
- Memory alignment is a power of 2
- Port numbers are in valid range
- Paths are accessible

---

## Example Configurations

### Minimal Configuration

```toml
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "events"
consumer_group = "k2i"

[iceberg]
catalog_type = "rest"
rest_uri = "http://localhost:8181"
warehouse_path = "s3://bucket/warehouse"
database_name = "default"
table_name = "events"
```

### Production AWS Configuration

```toml
[kafka]
bootstrap_servers = ["kafka1.prod:9092", "kafka2.prod:9092", "kafka3.prod:9092"]
topic = "events"
consumer_group = "k2i-production"
batch_size = 5000
batch_timeout_ms = 1000
max_poll_interval_ms = 600000

[kafka.security]
protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-512"
sasl_username = "${KAFKA_USERNAME}"
sasl_password = "${KAFKA_PASSWORD}"
ssl_ca_location = "/etc/kafka/ca.pem"

[iceberg]
catalog_type = "glue"
warehouse_path = "s3://data-lake-prod/iceberg/"
database_name = "analytics"
table_name = "events"
target_file_size_mb = 512
compression = "zstd"
aws_region = "us-east-1"

[[iceberg.partition_spec]]
source_field = "event_timestamp"
transform = "day"

[buffer]
ttl_seconds = 120
max_size_mb = 2000
flush_interval_seconds = 60
flush_batch_size = 50000

[transaction_log]
log_dir = "/var/lib/k2i/txlog"
checkpoint_interval_entries = 50000
checkpoint_interval_seconds = 600
max_log_files = 20

[maintenance]
compaction_enabled = true
compaction_interval_seconds = 7200
compaction_threshold_mb = 128
compaction_target_mb = 512
snapshot_expiration_enabled = true
snapshot_retention_days = 30
orphan_cleanup_enabled = true
orphan_retention_days = 7

[monitoring]
metrics_port = 9090
health_port = 8080
log_level = "info"
log_format = "json"
```

### Local Development with MinIO

```toml
[kafka]
bootstrap_servers = ["localhost:9092"]
topic = "dev-events"
consumer_group = "k2i-dev"
batch_size = 100
auto_offset_reset = "earliest"

[iceberg]
catalog_type = "rest"
rest_uri = "http://localhost:8181"
warehouse_path = "s3://dev-warehouse/"
database_name = "dev"
table_name = "events"
target_file_size_mb = 32
compression = "snappy"
s3_endpoint = "http://localhost:9000"
aws_region = "us-east-1"
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"

[buffer]
ttl_seconds = 30
max_size_mb = 50
flush_interval_seconds = 10
flush_batch_size = 100

[transaction_log]
log_dir = "./dev-txlog"

[maintenance]
compaction_enabled = false
snapshot_expiration_enabled = false
orphan_cleanup_enabled = false

[monitoring]
metrics_port = 9090
health_port = 8080
log_level = "debug"
log_format = "text"
```

---

## Building with Kerberos Support

The default k2i binary supports SASL mechanisms `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`. If your Kafka cluster requires **Kerberos (GSSAPI)** authentication, you need to build from source with the `gssapi` feature enabled.

### Prerequisites

Install the SASL development libraries:

**macOS:**
```bash
brew install cyrus-sasl
```

**Debian/Ubuntu:**
```bash
sudo apt-get install libsasl2-dev
```

**RHEL/Fedora:**
```bash
sudo dnf install cyrus-sasl-devel
```

### Build

Override the rdkafka features when building:

```bash
git clone https://github.com/osodevops/k2i.git
cd k2i

# Edit Cargo.toml to add "gssapi" to the rdkafka features:
#   rdkafka = { version = "0.38", features = ["cmake-build", "ssl", "gssapi"] }

cargo build --release -p k2i-cli
```

### Docker

To build a Docker image with Kerberos support, add `libsasl2-dev` to the builder stage and `libsasl2-2` to the runtime stage in the Dockerfile, and add `"gssapi"` to the rdkafka features in `Cargo.toml` before building.

### Configuration

Once built with Kerberos support, configure GSSAPI authentication:

```toml
[kafka.security]
protocol = "SASL_SSL"
sasl_mechanism = "GSSAPI"
```

Ensure your Kerberos keytab and `krb5.conf` are properly configured on the host.
