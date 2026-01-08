# K2I CLI Command Reference

This document provides a complete reference for all K2I CLI commands.

## Global Options

These options are available for all commands:

| Option | Short | Description |
|--------|-------|-------------|
| `--config <PATH>` | `-c` | Path to configuration file (default: `config.toml`) |
| `--verbose` | `-v` | Increase logging verbosity (-v debug, -vv trace) |
| `--help` | `-h` | Print help information |
| `--version` | | Print version information |

## Commands

### k2i ingest

Run the ingestion engine to consume from Kafka and write to Iceberg.

```bash
k2i ingest [OPTIONS]
```

#### Options

| Option | Description |
|--------|-------------|
| `--bootstrap-servers <SERVERS>` | Override Kafka bootstrap servers |
| `--topic <TOPIC>` | Override Kafka topic |
| `--consumer-group <GROUP>` | Override consumer group |

#### Examples

```bash
# Start with default config
k2i ingest

# Start with custom config
k2i ingest --config /etc/k2i/production.toml

# Override Kafka settings
k2i ingest --bootstrap-servers kafka1:9092,kafka2:9092 --topic events

# Enable debug logging
k2i ingest -v

# Enable trace logging
k2i ingest -vv
```

#### Behavior

- Starts the main ingestion loop
- Spawns HTTP servers for health (8080) and metrics (9090)
- Handles graceful shutdown on SIGINT (Ctrl+C) or SIGTERM
- Flushes buffer on shutdown to prevent data loss
- Returns exit code 0 on successful shutdown

#### Signal Handling

| Signal | Action |
|--------|--------|
| SIGINT (Ctrl+C) | Graceful shutdown |
| SIGTERM | Graceful shutdown |
| SIGKILL | Immediate termination (no flush) |

---

### k2i status

Check the health and metrics of a running K2I instance.

```bash
k2i status [OPTIONS]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--url <URL>` | `http://localhost:8080` | Health endpoint URL |

#### Examples

```bash
# Check local instance
k2i status

# Check remote instance
k2i status --url http://k2i-prod.internal:8080
```

#### Output

```
Health Status: Healthy

Components:
  kafka: Healthy
  buffer: Healthy
  iceberg: Healthy
  catalog: Healthy
  txlog: Healthy

Metrics:
  messages_consumed: 1234567
  rows_flushed: 1200000
  buffer_flushes: 240
  errors: 0
  backpressure_events: 3
```

#### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Instance is healthy or degraded (operational) |
| 6 | Instance is unhealthy or unreachable |

---

### k2i maintenance

Run maintenance tasks manually. Subcommands for specific operations.

```bash
k2i maintenance <COMMAND> [OPTIONS]
```

#### Subcommands

| Command | Description |
|---------|-------------|
| `compact` | Merge small Parquet files |
| `expire-snapshots` | Remove old snapshots |
| `clean-orphans` | Delete orphan files |

---

### k2i maintenance compact

Run file compaction to merge small Parquet files into larger ones.

```bash
k2i maintenance compact [OPTIONS]
```

#### Examples

```bash
# Run compaction with default config
k2i maintenance compact

# Run compaction with custom config
k2i maintenance compact --config production.toml
```

#### Behavior

- Scans Iceberg table for files smaller than `compaction_threshold_mb`
- Merges files until reaching `compaction_target_mb`
- Creates new manifest with merged files
- Atomically commits to catalog
- Does not delete old files immediately (handled by orphan cleanup)

#### Output

```
Starting compaction...
Found 45 files eligible for compaction
Merging files into 3 new files...
  Created: data_00001.parquet (512 MB, 1,234,567 rows)
  Created: data_00002.parquet (512 MB, 1,234,568 rows)
  Created: data_00003.parquet (256 MB, 617,284 rows)
Committed snapshot: 12345678901234
Compaction complete: merged 45 files into 3
```

---

### k2i maintenance expire-snapshots

Remove old Iceberg snapshots beyond the retention period.

```bash
k2i maintenance expire-snapshots [OPTIONS]
```

#### Examples

```bash
# Expire snapshots with default retention
k2i maintenance expire-snapshots

# With custom config (retention set in config)
k2i maintenance expire-snapshots --config production.toml
```

#### Behavior

- Lists all snapshots in the table
- Identifies snapshots older than `snapshot_retention_days`
- Removes snapshot metadata entries
- Does not delete data files still referenced by other snapshots

#### Output

```
Starting snapshot expiration...
Retention period: 7 days
Found 25 snapshots total
Expiring 18 snapshots older than 2024-01-01T00:00:00Z
  Expired: snapshot 12345678901230 (2023-12-25T10:30:00Z)
  Expired: snapshot 12345678901231 (2023-12-26T11:00:00Z)
  ...
Snapshot expiration complete: removed 18 snapshots
```

---

### k2i maintenance clean-orphans

Delete orphan files not referenced by any snapshot.

```bash
k2i maintenance clean-orphans [OPTIONS]
```

#### Examples

```bash
# Clean orphan files
k2i maintenance clean-orphans

# With custom config
k2i maintenance clean-orphans --config production.toml
```

#### Behavior

- Lists all data files in the warehouse path
- Lists all files referenced by any snapshot
- Identifies orphan files (present but not referenced)
- Applies safety buffer (`orphan_retention_days`)
- Deletes orphan files older than retention period

#### Output

```
Starting orphan cleanup...
Safety buffer: 3 days
Scanning warehouse path: s3://bucket/warehouse/
Found 150 data files
Found 142 referenced files
Identified 8 orphan files
Deleting 5 orphan files older than 2024-01-05T00:00:00Z
  Deleted: data_abc123.parquet (512 MB)
  Deleted: data_def456.parquet (128 MB)
  ...
Orphan cleanup complete: deleted 5 files (640 MB recovered)
```

---

### k2i validate

Validate the configuration file without starting the engine.

```bash
k2i validate [OPTIONS]
```

#### Examples

```bash
# Validate default config
k2i validate

# Validate specific config
k2i validate --config /etc/k2i/production.toml
```

#### Behavior

Validates:
- TOML syntax
- Required fields present
- Kafka bootstrap servers non-empty
- Valid catalog type
- Valid compression codec
- Memory alignment is power of 2
- Port numbers in valid range

#### Output

Success:
```
Configuration is valid
```

Failure:
```
Configuration error: missing required field 'kafka.topic'
```

#### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Configuration is valid |
| 1 | Configuration error |

---

## Environment Variables

K2I respects these environment variables:

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Override log level (e.g., `k2i=debug,rdkafka=warn`) |
| `AWS_ACCESS_KEY_ID` | AWS access key for S3/Glue |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_REGION` | AWS region |
| `AWS_PROFILE` | AWS profile name |

Configuration values can reference environment variables:

```toml
[kafka.security]
sasl_password = "${KAFKA_PASSWORD}"
```

---

## Exit Codes

| Code | Meaning | Cause |
|------|---------|-------|
| 0 | Success | Normal completion |
| 1 | Config Error | Invalid configuration file |
| 2 | Kafka Error | Kafka connection or consumer failure |
| 3 | Iceberg Error | Catalog or commit failure |
| 4 | Storage Error | Object storage failure |
| 5 | Transaction Log Error | Log corruption or write failure |
| 6 | Health Check Error | Instance unreachable or unhealthy |
| 10 | Runtime Error | Unclassified runtime failure |
| 130 | Signal Interrupt | SIGINT or SIGTERM received |

---

## HTTP Endpoints

When running `k2i ingest`, HTTP endpoints are available:

### Health Endpoints (port 8080)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Full health status (JSON) |
| `/healthz` | GET | Kubernetes liveness probe |
| `/readyz` | GET | Kubernetes readiness probe |

#### /health Response

```json
{
  "status": "Healthy",
  "components": {
    "kafka": { "status": "Healthy" },
    "buffer": { "status": "Healthy" },
    "iceberg": { "status": "Healthy" },
    "catalog": { "status": "Healthy" },
    "txlog": { "status": "Healthy" }
  },
  "metrics": {
    "messages_consumed": 1234567,
    "rows_flushed": 1200000,
    "buffer_flushes": 240,
    "errors": 0,
    "backpressure_events": 3
  }
}
```

#### /healthz and /readyz

- Return `200 OK` if operational (healthy or degraded)
- Return `503 Service Unavailable` if unhealthy

### Metrics Endpoint (port 9090)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus metrics |

#### Sample Metrics

```
# HELP k2i_messages_total Total messages consumed from Kafka
# TYPE k2i_messages_total counter
k2i_messages_total 1234567

# HELP k2i_buffer_size_bytes Current buffer memory usage
# TYPE k2i_buffer_size_bytes gauge
k2i_buffer_size_bytes 52428800

# HELP k2i_buffer_record_count Current records in buffer
# TYPE k2i_buffer_record_count gauge
k2i_buffer_record_count 5000

# HELP k2i_flush_duration_seconds Flush operation duration
# TYPE k2i_flush_duration_seconds histogram
k2i_flush_duration_seconds_bucket{le="0.1"} 10
k2i_flush_duration_seconds_bucket{le="0.25"} 45
k2i_flush_duration_seconds_bucket{le="0.5"} 180
k2i_flush_duration_seconds_bucket{le="1.0"} 235
k2i_flush_duration_seconds_bucket{le="+Inf"} 240
k2i_flush_duration_seconds_sum 89.5
k2i_flush_duration_seconds_count 240
```

---

## Shell Completion

Generate shell completions:

```bash
# Bash
k2i --generate bash > /etc/bash_completion.d/k2i

# Zsh
k2i --generate zsh > ~/.zsh/completions/_k2i

# Fish
k2i --generate fish > ~/.config/fish/completions/k2i.fish
```

---

## Quick Reference

```bash
# Start ingestion
k2i ingest --config config.toml

# Check health
k2i status --url http://localhost:8080

# Validate config
k2i validate --config config.toml

# Manual maintenance
k2i maintenance compact --config config.toml
k2i maintenance expire-snapshots --config config.toml
k2i maintenance clean-orphans --config config.toml

# Debug logging
k2i ingest -v

# Trace logging
k2i ingest -vv

# Help
k2i --help
k2i ingest --help
k2i maintenance --help
```
