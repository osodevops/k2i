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

## Local E2E Verification Scripts

These repository scripts are not `k2i` subcommands, but they are the canonical
local verification entry points used before release:

| Script | Description |
|--------|-------------|
| `scripts/e2e-docker.sh` | Correctness flow with Kafka, Schema Registry, K2I, read-state RPC, Protobuf schema evolution, and DuckDB Parquet validation |
| `K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh` | High-volume local load profile with full read-state visibility and zero-error metrics assertions |
| `scripts/e2e-docker-iceberg.sh` | Real Iceberg REST metadata validation with DuckDB `iceberg_scan` |
| `K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh` | Production-oriented local Iceberg load profile: 100,000 rows by default, cold flush required, Parquet validation, Iceberg metadata validation, snapshot growth, DuckDB `iceberg_scan`, and zero-error metrics |

The load scripts also support `K2I_E2E_LOAD_CONCURRENCY`,
`K2I_E2E_LOAD_PARTITIONS`, and `K2I_E2E_LOAD_TIMEOUT_SECONDS`.
`scripts/e2e-docker-iceberg-load.sh` additionally supports
`K2I_E2E_ICEBERG_LOAD_MIN_DATA_FILES` and
`K2I_E2E_ICEBERG_LOAD_MIN_SNAPSHOTS` when intentionally changing the load
shape.

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
  schema: Healthy

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

### k2i table

Register or reset existing Parquet data for a K2I-readable table.

```bash
k2i table <COMMAND> [OPTIONS]
```

#### Subcommands

| Command | Description |
|---------|-------------|
| `register` | Register existing Parquet files without copying them |
| `reset` | Reset a registered table |

---

### k2i table register

Register one or more existing Parquet files as an Iceberg table and read-state
source.

```bash
k2i table register --database <DATABASE> --table <TABLE> --source <SOURCE> [OPTIONS]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--database <DATABASE>` | Required | Database/namespace name |
| `--table <TABLE>` | Required | Table name |
| `--source <SOURCE>` | Required | Parquet file, directory, or glob. Can be repeated |
| `--schema-source <SOURCE>` | `first-file` | Use the first Parquet file schema or a `.proto` file path |
| `--message-type <TYPE>` | | Fully-qualified Protobuf message type when `--schema-source` is a `.proto` file |
| `--kafka-topic <TOPIC>` | | Optional topic label for read-state provenance |

#### Examples

```bash
# Register a directory of historical Parquet files
k2i table register \
  --config config.toml \
  --database f1 \
  --table derived_state \
  --source '/data/historical/*.parquet'

# Register with an explicit Protobuf schema
k2i table register \
  --config config.toml \
  --database f1 \
  --table derived_state \
  --source /data/historical \
  --schema-source schemas/derived_state.proto \
  --message-type f1.timing.v1.DerivedState
```

#### Behavior

- Expands files, directories, and glob sources.
- Infers schema from the first Parquet file unless a `.proto` schema source is supplied.
- Registers metadata through the configured catalog path.
- Adds transaction-log records so the read-state path can recover the registered table.
- Does not copy source files.

---

### k2i table reset

Reset a registered table.

```bash
k2i table reset --database <DATABASE> --table <TABLE> [OPTIONS]
```

#### Options

| Option | Description |
|--------|-------------|
| `--database <DATABASE>` | Database/namespace name |
| `--table <TABLE>` | Table name |
| `--keep-data` | Retain source data files |

#### Example

```bash
k2i table reset --config config.toml --database f1 --table derived_state --keep-data
```

---

### k2i dev

Run a zero-cloud local development instance. This command builds an in-process
configuration that uses Kafka plus a local filesystem warehouse, a SQLite-backed
catalog, local transaction logs, and an enabled read-state Unix socket.

```bash
k2i dev --topic <TOPIC> --warehouse <PATH> [OPTIONS]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--topic <TOPIC>` | Required | Kafka topic to consume |
| `--warehouse <PATH>` | Required | Local warehouse directory |
| `--bootstrap-servers <SERVERS>` | `localhost:9092` | Kafka bootstrap servers |
| `--database <DATABASE>` | `default` | Database/namespace name |
| `--table <TABLE>` | `events` | Table name |
| `--consumer-group <GROUP>` | `k2i-dev` | Kafka consumer group |
| `--schema-registry-url <URL>` | | Optional Confluent Schema Registry URL for Protobuf values |
| `--message-type <TYPE>` | | Optional fully-qualified Protobuf message type |

#### Examples

```bash
# Raw Kafka values into a local warehouse
k2i dev --topic events --warehouse ./warehouse

# Confluent-framed Protobuf values
k2i dev \
  --topic f1.timing.derived_state \
  --warehouse ./warehouse \
  --schema-registry-url http://localhost:8081 \
  --message-type f1.timing.v1.DerivedState
```

#### Behavior

- Creates the warehouse, transaction-log, and runtime socket directories.
- Uses a local SQLite catalog at `<warehouse>/catalog.db`.
- Uses the filesystem object store rooted at the warehouse path.
- Enables read-state RPC at `<warehouse>/run/k2i.sock`.

---

### k2i completions

Generate shell completions or recursive man pages from the live `clap` command
definitions.

```bash
k2i completions <COMMAND>
```

#### Subcommands

| Command | Description |
|---------|-------------|
| `bash` | Print bash completions to stdout |
| `zsh` | Print zsh completions to stdout |
| `fish` | Print fish completions to stdout |
| `power-shell` | Print PowerShell completions to stdout |
| `man` | Generate recursive `.1` man page files |

#### Examples

```bash
# Shell completions
k2i completions bash > ~/.local/share/bash-completion/completions/k2i
k2i completions zsh > ~/.zfunc/_k2i
k2i completions fish > ~/.config/fish/completions/k2i.fish
k2i completions power-shell > k2i.ps1

# Man pages
k2i completions man --output-dir docs/man/man1
man ./docs/man/man1/k2i.1
man ./docs/man/man1/k2i-table-register.1
```

The checked-in man pages under `docs/man/man1/` are generated with this command
and should be regenerated after any CLI help, flag, subcommand, or version
change.

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
  "status": "healthy",
  "components": {
    "kafka": { "status": "healthy" },
    "buffer": { "status": "healthy" },
    "iceberg": { "status": "healthy" },
    "catalog": { "status": "healthy" },
    "txlog": { "status": "healthy" },
    "schema": { "status": "healthy" }
  }
}
```

#### /healthz and /readyz

- `/healthz` is the liveness probe. It returns `200 OK` when the process is operational (`healthy` or `degraded`) and `503 Service Unavailable` when overall health is `unhealthy`.
- `/readyz` is the readiness probe. It returns `200 OK` only when the process is operational and there are no readiness blockers. Schema pauses from manual evolution or breaking Protobuf changes return `503 Service Unavailable` even when `/healthz` still returns `200 OK`.

### Metrics Endpoint (port 9090)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus metrics |

#### Sample Metrics

```
# HELP k2i_messages_consumed_total Total messages consumed from Kafka
# TYPE k2i_messages_consumed_total counter
k2i_messages_consumed_total 1234567

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

## Quick Reference

```bash
# Start ingestion
k2i ingest --config config.toml

# Check health
k2i status --url http://localhost:8080

# Validate config
k2i validate --config config.toml

# Register existing Parquet files
k2i table register --config config.toml --database f1 --table derived_state --source '/data/*.parquet'

# Run a zero-cloud local dev instance
k2i dev --topic events --warehouse ./warehouse

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

# Full local Docker validation
scripts/e2e-docker.sh
scripts/e2e-docker-iceberg.sh
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```
