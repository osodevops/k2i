# Kafka → Iceberg CLI Tool: MVP Product Requirements Document

**Date:** January 8, 2026  
**Version:** 1.0  
**Status:** Ready for Development  
**Timeline:** 8 weeks to MVP launch

---

## Executive Summary

### What Are We Building?

A **simple, fast, automated CLI tool** that ingests events from Apache Kafka and writes them to Apache Iceberg tables with:
- ✅ Sub-second data freshness (< 1 second)
- ✅ Fully automated table maintenance (zero manual work)
- ✅ Guaranteed consistency via transaction log
- ✅ Dual partitioning (preserve Kafka + optimize Iceberg)
- ✅ Single binary deployment (CLI or embedded library)
- ✅ Open source (no vendor lock-in)

### Why Build This?

**Market Gap:** All 9 existing solutions force you to choose between simple/slow or fast/complex. **We're building simple AND fast.**

**Competitive Advantages:**
1. **Hot Buffer Architecture** - Sub-second freshness without query-time cost
2. **Integrated Maintenance** - Only open-source solution with zero maintenance overhead
3. **Transaction Log** - Simplest path to consistency guarantees
4. **Dual Partitioning** - Preserve both Kafka + Iceberg optimization (simple implementation)
5. **Embedded Deployment** - CLI tool, not distributed system (5-minute setup)

**Market Opportunity:**
- TAM: $2.25B (Kafka → Iceberg)
- Addressable: ~60% = $1.35B
- Target Year 1: 0.1-1% = $2.25M-22.5M revenue potential

### Success Criteria

**MVP Definition:** Production-ready ingestion engine that:
- ✅ Reliably ingests 100K-1M events/sec
- ✅ Delivers < 1 second freshness
- ✅ Automates all maintenance tasks
- ✅ Recovers from crashes without data loss
- ✅ Runs as single CLI binary
- ✅ Has 70%+ test coverage
- ✅ Is fully documented

---

## Part 1: Architecture Overview

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                       KAFKA → ICEBERG ENGINE                   │
└─────────────────────────────────────────────────────────────────┘

INPUT: Apache Kafka Topic
  │
  ├─ Consumer Group (parallel consumption)
  │   ├─ Partition 0 Consumer
  │   ├─ Partition 1 Consumer
  │   └─ Partition N Consumer
  │
  ↓
┌────────────────────────────────┐
│  OFFSET TRACKING               │
│  ├─ Current offset per partition
│  ├─ Last committed offset
│  └─ Backpressure (pause/resume)
└────────────────────────────────┘
  │
  ↓
┌────────────────────────────────┐
│  HOT BUFFER (Arrow)            │
│  ├─ In-memory RecordBatch      │
│  ├─ Hash Index (O(1) lookup)   │
│  ├─ TTL eviction (60s)         │
│  ├─ Size-based eviction (500MB)│
│  └─ ACCESSIBLE VIA QUERY API   │ ← Real-time data!
└────────────────────────────────┘
  │
  ├─ [Async background thread]
  │
  ↓
┌────────────────────────────────┐
│  ARROW → PARQUET CONVERSION    │
│  ├─ Columnar encoding          │
│  ├─ Snappy compression         │
│  └─ Metadata preservation      │
└────────────────────────────────┘
  │
  ↓
┌────────────────────────────────┐
│  S3/OBJECT STORAGE WRITE       │
│  ├─ Upload parquet file        │
│  ├─ Verify checksum            │
│  └─ Retry on failure           │
└────────────────────────────────┘
  │
  ├─ [Transaction log records]
  │
  ↓
┌────────────────────────────────┐
│  ICEBERG WRITER                │
│  ├─ Add file to manifest       │
│  ├─ Update metadata            │
│  ├─ Atomic snapshot commit      │
│  └─ Compare-and-set semantics  │
└────────────────────────────────┘
  │
  ├─ [Background maintenance thread]
  │
  ↓
┌────────────────────────────────┐
│  MAINTENANCE SCHEDULER         │
│  ├─ Compaction (hourly)        │
│  │  └─ Small files → 512MB     │
│  ├─ Snapshot expiration (daily)│
│  │  └─ Keep 7 days             │
│  ├─ Orphan cleanup (daily)     │
│  │  └─ Remove unreferenced     │
│  └─ Statistics update (hourly) │
│     └─ Column stats refresh    │
└────────────────────────────────┘
  │
  ↓
┌────────────────────────────────┐
│  TRANSACTION LOG (Disk)        │
│  ├─ Append-only log            │
│  ├─ Records every operation    │
│  ├─ Crash recovery             │
│  └─ Audit trail                │
└────────────────────────────────┘

OUTPUT: Apache Iceberg Table (Query-ready)
  │
  ├─ Parquet files in S3
  ├─ Metadata in Iceberg catalog
  ├─ Queryable via Spark/Trino/DuckDB
  └─ Available immediately (hot buffer + Iceberg)
```

### Component Breakdown

**1. Kafka Consumer** (Week 1)
- Consumer group management
- Partition-to-thread mapping
- Offset tracking (committed vs. current)
- Backpressure handling (pause/resume)
- Error handling + retries

**2. Hot Buffer** (Week 2)
- Arrow RecordBatch builder
- Hash index for O(1) lookups
- TTL-based eviction (default 60s)
- Size-based eviction (default 500MB)
- Memory alignment for SIMD

**3. Iceberg Writer** (Week 3)
- Arrow → Parquet conversion
- S3 file upload with verification
- Iceberg manifest creation
- Atomic snapshot updates (CAS)
- Error recovery (idempotent commits)

**4. Transaction Log** (Week 4)
- Append-only log (disk-based)
- Entry types: offset, buffer, parquet, snapshot, recovery
- Checkpoint mechanism
- Recovery logic

**5. Maintenance Scheduler** (Weeks 5-6)
- Compaction (combine small files)
- Snapshot expiration (delete old metadata)
- Orphan cleanup (remove unreferenced files)
- Statistics updates

**6. CLI & Configuration** (Week 6)
- Argument parsing
- TOML config file support
- Health check endpoint
- Graceful shutdown
- Logging infrastructure

**7. Monitoring & Observability** (Week 7)
- Prometheus metrics export
- Key metrics (messages consumed, flush duration, buffer size, errors)
- Basic alerting rules
- Distributed tracing (optional)

**8. Testing & Documentation** (Week 8)
- Unit tests (target 70%+)
- Integration tests
- Load testing (measure 1M events/sec)
- Architecture documentation
- Quick-start guide
- Configuration reference

---

## Part 2: MVP Feature Specification

### Core Features (Must Have)

#### Feature 1: Single-Source Kafka Consumer
**Description:** Reliable, scalable consumption from Kafka topic

**Requirements:**
- ✅ Connect to Kafka broker(s) with authentication (SASL/SSL)
- ✅ Consumer group support (parallel consumption across partitions)
- ✅ Configurable batch size (default: 1000 messages)
- ✅ Configurable batch timeout (default: 5 seconds)
- ✅ Automatic offset commit (configurable interval)
- ✅ Offset reset strategy (earliest/latest/none)
- ✅ Error handling with exponential backoff

**Configuration:**
```toml
[kafka]
bootstrap_servers = "localhost:9092"
topic = "events"
consumer_group = "iceberg-ingestion"
batch_size = 1000
batch_timeout_ms = 5000
session_timeout_ms = 10000
auto_offset_reset = "earliest"  # earliest | latest | none
enable_auto_commit = true
auto_commit_interval_ms = 5000
```

**API:**
```rust
pub struct KafkaConsumer {
    // Private fields
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> Result<Self>;
    pub async fn poll(&mut self) -> Result<Vec<Event>>;
    pub async fn commit_offset(&self, partition: i32, offset: i64) -> Result<()>;
}
```

---

#### Feature 2: Hot Buffer with Hash Index
**Description:** In-memory buffer for immediate data availability

**Requirements:**
- ✅ Arrow RecordBatch storage
- ✅ Hash index for O(1) point lookups
- ✅ TTL-based eviction (remove after 60s)
- ✅ Size-based eviction (flush when > 500MB)
- ✅ Query API (SELECT on hot buffer)
- ✅ Memory alignment (64-byte for SIMD)
- ✅ Thread-safe concurrent access

**Configuration:**
```toml
[hot_buffer]
ttl_seconds = 60              # Evict records after 60 seconds
max_size_mb = 500             # Evict when buffer exceeds 500MB
hash_index_enabled = true     # Enable O(1) lookups
memory_alignment_bytes = 64   # SIMD alignment
```

**API:**
```rust
pub struct HotBuffer {
    // Private fields
}

impl HotBuffer {
    pub fn append(&mut self, batch: RecordBatch) -> Result<()>;
    pub fn query(&self, filter: Expression) -> Result<RecordBatch>;
    pub fn size_bytes(&self) -> usize;
    pub fn record_count(&self) -> usize;
    pub fn clear(&mut self) -> Result<()>;
}
```

**Query Examples:**
```sql
-- Real-time queries on hot buffer
SELECT * FROM hot_buffer 
WHERE event_type = 'purchase' 
  AND timestamp > now() - interval 60 seconds;

SELECT user_id, COUNT(*) as event_count 
FROM hot_buffer 
GROUP BY user_id;
```

---

#### Feature 3: Atomic Iceberg Writer
**Description:** Reliable write-once semantics to Iceberg tables

**Requirements:**
- ✅ Arrow → Parquet conversion (Snappy compression)
- ✅ S3/object storage file upload
- ✅ SHA256 checksum verification
- ✅ Iceberg manifest updates
- ✅ Atomic snapshot commits (compare-and-set)
- ✅ Idempotent writes (no duplicates on retry)
- ✅ Schema evolution support

**Configuration:**
```toml
[iceberg]
catalog_type = "hive"          # hive | glue | nessie | rest
warehouse_path = "s3://bucket/warehouse"

# For Hive catalog
[iceberg.hive]
metastore_uri = "thrift://localhost:9083"

# For AWS Glue catalog
[iceberg.glue]
region = "us-east-1"

# For REST catalog
[iceberg.rest]
uri = "http://localhost:8181"
s3_access_key = "${AWS_ACCESS_KEY_ID}"
s3_secret_key = "${AWS_SECRET_ACCESS_KEY}"

# Write configuration
[iceberg.write]
target_file_size_mb = 512
compression = "snappy"
format_version = 2             # Iceberg format version
```

**API:**
```rust
pub struct IcebergWriter {
    // Private fields
}

impl IcebergWriter {
    pub async fn write_parquet(&self, path: &str, batch: RecordBatch) -> Result<String>;
    pub async fn commit_snapshot(&self, files: Vec<String>) -> Result<SnapshotId>;
    pub async fn get_table_metadata(&self) -> Result<TableMetadata>;
}
```

---

#### Feature 4: Transaction Log for Consistency
**Description:** Append-only log ensuring crash recovery and consistency

**Requirements:**
- ✅ Append-only log (disk-based, durability guaranteed)
- ✅ Entry types: OffsetCommit, BufferSnapshot, ParquetWritten, IcebergSnapshot, FlushComplete
- ✅ Checkpoint mechanism (periodic compaction)
- ✅ Crash recovery (automatic replay on restart)
- ✅ Checksum verification (detect corruption)
- ✅ Audit trail (complete history)

**Configuration:**
```toml
[transaction_log]
log_dir = "./transaction_logs"
checkpoint_interval_entries = 10000
checkpoint_interval_seconds = 3600
```

**Data Structures:**
```rust
pub enum TransactionEntry {
    OffsetCommit {
        topic: String,
        partition: i32,
        offset: i64,
        timestamp: DateTime<Utc>,
    },
    BufferSnapshot {
        record_count: usize,
        size_bytes: usize,
        timestamp: DateTime<Utc>,
    },
    ParquetWritten {
        file_path: String,
        record_count: usize,
        file_size: usize,
        checksum: String, // SHA256
        timestamp: DateTime<Utc>,
    },
    IcebergSnapshotCreated {
        snapshot_id: i64,
        files_added: Vec<String>,
        timestamp: DateTime<Utc>,
    },
    FlushComplete {
        kafka_offset: i64,
        iceberg_snapshot: i64,
        timestamp: DateTime<Utc>,
    },
}
```

---

#### Feature 5: Automated Maintenance
**Description:** Background tasks for table optimization and cleanup

**Requirements:**
- ✅ Compaction (combine small files into 512MB targets)
- ✅ Snapshot expiration (keep 7 days, delete older)
- ✅ Orphan cleanup (remove unreferenced files)
- ✅ Statistics update (refresh column stats)
- ✅ Configurable policies
- ✅ Prometheus metrics for each operation

**Configuration:**
```toml
[maintenance]
# Compaction
compact_enabled = true
compact_interval_seconds = 3600        # Run hourly
compact_threshold_mb = 100             # Files < 100MB trigger compaction
compact_target_mb = 512                # Target 512MB files
compact_parallel_jobs = 4

# Snapshot expiration
snapshot_expiration_enabled = true
snapshot_retention_days = 7            # Keep 7 days of snapshots
snapshot_check_interval_seconds = 86400  # Check daily

# Orphan cleanup
orphan_cleanup_enabled = true
orphan_retention_days = 3              # Keep orphans 3 days (safety)
orphan_check_interval_seconds = 86400  # Check daily

# Statistics
statistics_enabled = true
statistics_update_interval_seconds = 3600  # Update hourly
```

**Maintenance Operations:**
```rust
pub struct MaintenanceScheduler {
    // Private fields
}

impl MaintenanceScheduler {
    pub async fn compact_small_files(&self) -> Result<CompactionStats>;
    pub async fn expire_old_snapshots(&self) -> Result<ExpirationStats>;
    pub async fn cleanup_orphan_files(&self) -> Result<CleanupStats>;
    pub async fn update_statistics(&self) -> Result<StatsUpdateStats>;
}
```

---

#### Feature 6: Dual Partitioning Strategy
**Description:** Preserve Kafka partitions while optimizing for Iceberg queries

**Requirements:**
- ✅ Preserve Kafka partition info in file paths and metadata
- ✅ Support multiple Iceberg partition specs (time-based, identity, bucket, truncate)
- ✅ Preserve Kafka offsets in Parquet metadata
- ✅ Enable replay from specific Kafka partition/offset

**Configuration:**
```toml
[partitioning]
preserve_kafka_partitions = true

# Iceberg partitioning strategy
partition_strategy = "daily"     # daily | hourly | monthly | identity | bucket | truncate

# For time-based partitioning
[partitioning.time_based]
field = "event_timestamp"
interval = "day"                 # day | hour | month

# For identity partitioning
[partitioning.identity]
field = "region"

# For bucket partitioning
[partitioning.bucket]
field = "user_id"
num_buckets = 100
```

**File Path Example:**
```
s3://warehouse/events/
  event_date=2026-01-08/
    kafka_partition=0/
      data-abc123.parquet (offsets 1000-2000)
      data-def456.parquet (offsets 2001-3000)
    kafka_partition=1/
      data-ghi789.parquet (offsets 1000-2000)
    kafka_partition=2/
      data-jkl012.parquet (offsets 1000-2000)
  event_date=2026-01-09/
    kafka_partition=0/
      data-mno345.parquet
    kafka_partition=1/
      data-pqr678.parquet
    kafka_partition=2/
      data-stu901.parquet
```

---

#### Feature 7: CLI Interface
**Description:** Command-line tool for running the ingestion engine

**Requirements:**
- ✅ TOML configuration file support
- ✅ Command-line flag overrides
- ✅ Health check endpoint (HTTP)
- ✅ Graceful shutdown (SIGTERM/SIGINT)
- ✅ Structured logging (JSON format)
- ✅ Exit codes (0 success, non-zero for errors)

**CLI Usage:**
```bash
# Basic usage (requires config file)
kafka-iceberg --config config.toml

# With overrides
kafka-iceberg --config config.toml \
  --kafka-bootstrap-servers "broker1:9092,broker2:9092" \
  --kafka-topic "events" \
  --warehouse-path "s3://my-bucket/warehouse"

# Health check
curl http://localhost:8080/health

# Metrics
curl http://localhost:8080/metrics

# Version
kafka-iceberg --version
```

**Configuration File Example:**
```toml
# config.toml

[kafka]
bootstrap_servers = "localhost:9092"
topic = "events"
consumer_group = "iceberg-ingestion"

[iceberg]
catalog_type = "hive"
warehouse_path = "s3://bucket/warehouse"
database_name = "raw"
table_name = "events"

[iceberg.hive]
metastore_uri = "thrift://metastore:9083"

[hot_buffer]
ttl_seconds = 60
max_size_mb = 500

[maintenance]
compact_enabled = true
compact_interval_seconds = 3600
snapshot_retention_days = 7

[logging]
level = "info"                    # debug | info | warn | error
format = "json"                   # json | text
```

---

#### Feature 8: Monitoring & Observability
**Description:** Prometheus metrics and structured logging

**Requirements:**
- ✅ Prometheus endpoint (`:9090/metrics` default)
- ✅ Key metrics: messages_consumed_total, flush_duration_seconds, buffer_size_bytes, iceberg_commits_total, errors_total
- ✅ Structured JSON logging
- ✅ Configurable log level
- ✅ Performance profiling data

**Prometheus Metrics:**
```
# HELP kafka_messages_consumed_total Total messages consumed from Kafka
# TYPE kafka_messages_consumed_total counter
kafka_messages_consumed_total{topic="events",partition="0"} 1000000

# HELP kafka_offset_lag_gauge Current offset lag per partition
# TYPE kafka_offset_lag_gauge gauge
kafka_offset_lag_gauge{topic="events",partition="0"} 0

# HELP hot_buffer_size_bytes Current hot buffer size
# TYPE hot_buffer_size_bytes gauge
hot_buffer_size_bytes 524288000

# HELP hot_buffer_record_count Current hot buffer record count
# TYPE hot_buffer_record_count gauge
hot_buffer_record_count 100000

# HELP flush_duration_seconds Time to flush hot buffer to Iceberg
# TYPE flush_duration_seconds histogram
flush_duration_seconds_bucket{le="0.1"} 10
flush_duration_seconds_bucket{le="0.5"} 50
flush_duration_seconds_bucket{le="1.0"} 100
flush_duration_seconds_sum 250
flush_duration_seconds_count 100

# HELP iceberg_commits_total Total Iceberg snapshot commits
# TYPE iceberg_commits_total counter
iceberg_commits_total{database="raw",table="events"} 1000

# HELP errors_total Total errors
# TYPE errors_total counter
errors_total{type="kafka_consumer"} 5
errors_total{type="iceberg_writer"} 2
errors_total{type="network"} 1
```

**Logging Output (JSON):**
```json
{
  "timestamp": "2026-01-08T12:00:00Z",
  "level": "INFO",
  "message": "Flushed hot buffer to Iceberg",
  "kafka_offset": 1000000,
  "records_flushed": 100000,
  "duration_ms": 250,
  "iceberg_snapshot_id": 12345,
  "span_id": "abc123",
  "trace_id": "def456"
}
```

---

### Advanced Features (Nice to Have, Post-MVP)

#### Feature 9: Schema Registry Integration (Post-MVP)
- [ ] Confluent Schema Registry support
- [ ] Automatic schema evolution
- [ ] Schema validation before write
- [ ] Field ID mapping for Iceberg

#### Feature 10: Multiple Kafka Cluster Support (Post-MVP)
- [ ] Consume from multiple clusters
- [ ] Multi-cluster failover
- [ ] Load balancing across clusters

#### Feature 11: Dead Letter Queue (Post-MVP)
- [ ] Capture unparseable records
- [ ] DLQ topic for error handling
- [ ] Error metrics and alerting

#### Feature 12: Time Travel / Snapshot Queries (Post-MVP)
- [ ] Query historical snapshots
- [ ] Temporal queries on Iceberg
- [ ] Audit trail queries

---

## Part 3: Development Roadmap (8 Weeks)

### Week 1: Kafka Consumer + Offset Tracking

**Deliverables:**
- ✅ Kafka consumer group implementation
- ✅ Partition assignment and rebalancing
- ✅ Offset tracking (committed vs. current)
- ✅ Backpressure (pause/resume consumer)
- ✅ Error handling with retries
- ✅ Unit tests (80%+ coverage)
- ✅ Integration test with local Kafka

**Testing:**
```bash
# Run local Kafka in Docker
docker-compose up -d

# Test consumer with 100K messages
cargo test test_consumer_100k_messages

# Test offset tracking
cargo test test_offset_tracking_accuracy

# Test backpressure
cargo test test_backpressure_pause_resume
```

**Success Metrics:**
- Consumer reliably pulls 100K records/sec
- Offset tracking accurate to 0 messages
- No data loss on pause/resume

---

### Week 2: Hot Buffer (Arrow) + Hash Index

**Deliverables:**
- ✅ Arrow RecordBatch builder
- ✅ Hash index implementation (DashMap)
- ✅ TTL-based eviction
- ✅ Size-based eviction
- ✅ Memory alignment verification
- ✅ Query API (basic SELECT)
- ✅ Unit tests (80%+ coverage)

**Testing:**
```bash
# Test hash index performance
cargo test test_hash_index_o1_lookup
# Expectation: < 1ms for 100K records

# Test TTL eviction
cargo test test_ttl_eviction_60s
# Expectation: Records evicted after 60s

# Test size-based eviction
cargo test test_size_eviction_500mb
# Expectation: Flush triggered at 500MB

# Test query API
cargo test test_query_api_select_filter
# Expectation: < 10ms response time
```

**Success Metrics:**
- Hash lookups < 1ms for O(1) verification
- TTL eviction accurate to within 1 second
- Hot buffer query responses < 10ms
- Memory alignment verified for SIMD

---

### Week 3: Iceberg Writer + Atomic Commits

**Deliverables:**
- ✅ Arrow → Parquet conversion
- ✅ S3 file upload with verification
- ✅ Iceberg manifest creation
- ✅ Atomic snapshot commits (CAS)
- ✅ Idempotent write handling
- ✅ Error recovery
- ✅ Unit tests (80%+ coverage)
- ✅ Integration test with local S3 + Iceberg

**Testing:**
```bash
# Test Parquet conversion
cargo test test_arrow_to_parquet_conversion
# Expectation: 100MB batch → valid Parquet

# Test S3 upload + verification
cargo test test_s3_upload_checksum_verification
# Expectation: Checksum matches after upload

# Test Iceberg commit
cargo test test_iceberg_atomic_commit
# Expectation: Snapshot created atomically

# Test idempotent writes
cargo test test_idempotent_writes_no_duplication
# Expectation: Retry doesn't create duplicates
```

**Success Metrics:**
- Atomic commits with zero data loss
- Parquet files valid and queryable
- Checksums 100% accurate
- No duplication on retries

---

### Week 4: Transaction Log + Crash Recovery

**Deliverables:**
- ✅ Append-only transaction log
- ✅ Entry types (offset, buffer, parquet, snapshot, complete)
- ✅ Checkpoint mechanism
- ✅ Crash recovery logic
- ✅ Checksum verification
- ✅ Replay from transaction log
- ✅ Unit tests (80%+ coverage)

**Testing:**
```bash
# Test transaction log append
cargo test test_transaction_log_append
# Expectation: All entries persisted

# Test crash recovery (clean flush)
cargo test test_recovery_clean_flush
# Expectation: Resume from last complete flush

# Test crash recovery (partial flush)
cargo test test_recovery_partial_flush
# Expectation: Complete or rollback partial flush

# Test crash recovery (replay)
cargo test test_recovery_replay_accuracy
# Expectation: All entries replayed correctly
```

**Success Metrics:**
- Transaction log 100% durable
- Crash recovery < 10 seconds
- Zero data loss in all scenarios
- No duplicate data after recovery

---

### Week 5: Automated Compaction

**Deliverables:**
- ✅ Small file detection
- ✅ Compaction logic (combine files)
- ✅ Atomic update to Iceberg manifest
- ✅ Configurable policies
- ✅ Prometheus metrics
- ✅ Unit tests (70%+ coverage)

**Testing:**
```bash
# Test compaction trigger
cargo test test_compaction_trigger_at_100mb
# Expectation: Compaction starts when files < 100MB

# Test compaction completion
cargo test test_compaction_combines_files_to_512mb
# Expectation: Result files ~512MB

# Test compaction parallelism
cargo test test_compaction_parallel_jobs_4
# Expectation: 4 parallel compactions, no conflicts
```

**Success Metrics:**
- Compaction runs automatically on schedule
- Files combined into 512MB targets
- Query performance improved after compaction
- Metrics accurately reflect compaction state

---

### Week 6: Snapshot Expiration + Orphan Cleanup

**Deliverables:**
- ✅ Snapshot expiration logic (keep 7 days)
- ✅ Orphan file detection
- ✅ Cleanup with safety interval (3 days)
- ✅ Metrics for cleanup
- ✅ Unit tests (70%+ coverage)

**Testing:**
```bash
# Test snapshot expiration
cargo test test_snapshot_expiration_keep_7_days
# Expectation: Snapshots older than 7 days deleted

# Test orphan detection
cargo test test_orphan_detection_accuracy
# Expectation: All unreferenced files detected

# Test orphan cleanup
cargo test test_orphan_cleanup_safety_3_days
# Expectation: Orphans kept 3 days before deletion
```

**Success Metrics:**
- Snapshot metadata stays bounded
- Storage costs reduced via orphan cleanup
- No premature deletion (3-day safety window)
- Metrics track cleanup effectiveness

---

### Week 7: CLI, Configuration, Monitoring

**Deliverables:**
- ✅ CLI argument parsing (structopt/clap)
- ✅ TOML config file support
- ✅ Health check endpoint (HTTP)
- ✅ Graceful shutdown (SIGTERM)
- ✅ Prometheus metrics endpoint
- ✅ Structured JSON logging
- ✅ Integration tests

**Testing:**
```bash
# Test CLI argument parsing
cargo test test_cli_config_file_parsing

# Test config override
cargo test test_cli_flag_overrides

# Test health endpoint
cargo test test_health_endpoint

# Test graceful shutdown
cargo test test_graceful_shutdown_sigterm
# Expectation: Clean shutdown, no data loss
```

**Success Metrics:**
- Single binary, runs as `kafka-iceberg --config config.toml`
- Health checks work correctly
- Prometheus metrics expose 10+ metrics
- Graceful shutdown < 5 seconds

---

### Week 8: Testing, Documentation, Launch Prep

**Deliverables:**
- ✅ Unit test coverage (target 70%+)
- ✅ Integration tests (end-to-end)
- ✅ Load testing (1M events/sec target)
- ✅ Architecture documentation (ARCHITECTURE.md)
- ✅ Quick-start guide (QUICKSTART.md)
- ✅ Configuration reference (CONFIG.md)
- ✅ Troubleshooting guide (TROUBLESHOOTING.md)
- ✅ Performance benchmarks
- ✅ GitHub setup (CI/CD, tests)

**Testing:**
```bash
# Full test suite
cargo test --all-features

# Load test (1M events/sec)
cargo run --release -- --load-test-1m-events

# Coverage report
cargo tarpaulin --out Html

# Benchmarks
cargo bench
```

**Documentation:**
- [ ] ARCHITECTURE.md - System design, component overview
- [ ] QUICKSTART.md - 5-minute setup guide
- [ ] CONFIG.md - Configuration reference (all options)
- [ ] TROUBLESHOOTING.md - Common issues and solutions
- [ ] CONTRIBUTING.md - Development setup, running tests
- [ ] CHANGELOG.md - Version history

**Success Metrics:**
- 70%+ test coverage
- 1M events/sec sustained throughput
- 70+ pages of documentation
- CI/CD pipeline green (all tests pass)

---

## Part 4: Technical Specifications

### Technology Stack

**Language:** Rust (2021 edition)

**Key Dependencies:**
```toml
# Kafka
rdkafka = "0.35"                    # Kafka consumer

# Data format
arrow = "50.0"                      # Arrow RecordBatch
parquet = "50.0"                    # Parquet writer

# Iceberg
iceberg-rust = "0.1"                # Iceberg table format

# Storage
aws-sdk-s3 = "1.0"                  # S3 client

# Async runtime
tokio = { version = "1.0", features = ["full"] }

# Data structures
dashmap = "5.5"                     # Concurrent hash map

# Configuration
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"

# CLI
clap = { version = "4.4", features = ["derive"] }

# Metrics
prometheus = "0.13"

# Logging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json"] }

# Testing
proptest = "1.4"
testcontainers = "0.15"             # Docker test containers
```

### Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **Throughput** | 100K-1M events/sec | Single instance |
| **Freshness** | < 1 second | Hot buffer latency |
| **Flush Duration** | < 1 second | Kafka offset → Iceberg commit |
| **Query Response** | < 10ms | Hot buffer queries |
| **Memory Usage** | < 2GB | For 500MB hot buffer |
| **CPU Usage** | < 50% | Single core (multi-core capable) |
| **Storage Efficiency** | 512MB Iceberg files | Optimized for query performance |
| **Compaction Duration** | < 5 minutes | For 500MB files |

### Error Handling

**Categories:**

1. **Kafka Errors** (retriable)
   - Connection timeout → Retry with backoff
   - Consumer lag → Log warning, continue
   - Offset out of range → Reset to earliest/latest

2. **Data Errors** (non-retriable)
   - Schema mismatch → Log error, send to DLQ
   - Corrupt record → Skip, increment error counter

3. **S3 Errors** (mixed)
   - 5xx errors → Retry with exponential backoff
   - 403 Forbidden → Fail immediately (auth issue)
   - 404 Not Found → Fail immediately (path issue)

4. **Iceberg Errors** (mixed)
   - Snapshot commit conflict → Retry (CAS)
   - Metadata write failure → Retry with transaction log recovery
   - Catalog connection failure → Retry with backoff

### Security Considerations

**Authentication:**
- ✅ Kafka: SASL/SCRAM, SSL/TLS
- ✅ S3: AWS credentials (SDK default chain)
- ✅ Iceberg catalog: Depends on catalog type

**Encryption:**
- ✅ TLS for all network communication
- ✅ S3 server-side encryption (default)
- ✅ Optional client-side encryption

**Authorization:**
- ✅ Kafka: Consumer group authorization
- ✅ S3: IAM policies (least privilege)
- ✅ Iceberg catalog: Role-based access

---

## Part 5: Success Criteria & Metrics

### MVP Definition (Must Have)

- ✅ **Functionality:** Reliably ingests Kafka → Iceberg at 100K-1M events/sec
- ✅ **Freshness:** < 1 second data freshness in hot buffer
- ✅ **Maintenance:** Fully automated (no manual jobs)
- ✅ **Consistency:** Zero data loss, automatic crash recovery
- ✅ **Deployment:** Single CLI binary, configurable via TOML
- ✅ **Testing:** 70%+ unit test coverage, integration tests
- ✅ **Documentation:** Quick-start, config reference, troubleshooting

### Launch Criteria

**Code Quality:**
- [ ] 70%+ test coverage
- [ ] All CI tests passing
- [ ] Code review by at least 2 engineers
- [ ] No critical security issues

**Performance:**
- [ ] Sustain 1M events/sec throughput
- [ ] < 1 second freshness verified in load test
- [ ] Memory usage < 2GB for 500MB buffer
- [ ] CPU usage < 50% at 1M events/sec

**Documentation:**
- [ ] QUICKSTART.md allows setup in 5 minutes
- [ ] All CLI flags documented
- [ ] All config options documented
- [ ] Troubleshooting guide covers common issues

**Operations:**
- [ ] Graceful startup/shutdown
- [ ] Health check endpoint working
- [ ] Prometheus metrics exposed
- [ ] Logs in JSON format for parsing

### Success Metrics (Month 1-3)

**Community:**
- [ ] 100+ GitHub stars
- [ ] 5+ beta customers testing
- [ ] 10+ issues/PRs from community

**Product:**
- [ ] < 5 critical bugs reported
- [ ] Zero data loss incidents
- [ ] 99.9% uptime in production deployments

**Business:**
- [ ] 5-10 production deployments
- [ ] 1-3 case studies written
- [ ] 1-2 conference talk submissions

---

## Part 6: Go-To-Market Strategy

### Launch Phase (Week 1-2)

**Soft Launch:**
- [ ] GitHub repo made public
- [ ] Beta access for 5 customers
- [ ] Internal documentation complete

**Hard Launch:**
- [ ] Announcement blog post
- [ ] Hacker News submission
- [ ] Twitter/LinkedIn threads
- [ ] Email to mailing list

### Messaging

**Primary:** "Real-time analytics from Kafka to Iceberg. Simple. Fast. Automated."

**Secondary:**
- "Simpler than Flink"
- "Cheaper than Confluent"
- "Works anywhere (edge to cloud)"
- "Deploy in hours, not days"

**For Teams:**
- "Sub-second freshness, zero maintenance"
- "No vendor lock-in, fully open source"
- "Proven architecture (Moonlink-inspired)"

### Positioning vs. Competitors

| Aspect | Kafka Connect | Confluent | Streambased | **Your Tool** |
|--------|---------------|-----------|-------------|--------------|
| Simplicity | ⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Freshness | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Maintenance | ⭐ | ⭐⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐⭐ |
| Cost | ⭐⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Open Source | ⭐⭐⭐⭐⭐ | ⭐ | ⭐ | ⭐⭐⭐⭐⭐ |

---

## Part 7: Risk Mitigation

### Risk 1: Moonlink Releases Kafka Support (Before/During Development)

**Probability:** Medium  
**Impact:** Medium  
**Mitigation:**
- Focus on simplicity (they'll be feature-rich but complex)
- Launch before them (8 weeks vs. their 12+ months)
- Emphasize embedded deployment (unique advantage)

---

### Risk 2: Performance Doesn't Hit 1M Events/Sec

**Probability:** Low  
**Impact:** High  
**Mitigation:**
- Use proven Arrow/Parquet libraries (production-tested)
- Profile and benchmark continuously (weekly)
- Design for parallelism from day 1 (partitioned consumers)

---

### Risk 3: Data Loss in Production

**Probability:** Very Low  
**Impact:** Critical  
**Mitigation:**
- Transaction log for all operations (auditable)
- Comprehensive crash recovery tests (week 4)
- Load testing with failures (chaos engineering)
- Beta period with 5 customers before GA

---

### Risk 4: Adoption Takes Longer Than Expected

**Probability:** Medium  
**Impact:** Medium  
**Mitigation:**
- Target startup/SMB segment (faster adoption)
- Free tier removes friction
- Build community (HN, Reddit, Slack communities)
- Create case studies early

---

## Part 8: Post-MVP Roadmap

### Month 2-3: Community & Hardening
- [ ] Schema Registry integration
- [ ] More Iceberg catalogs (Glue, Nessie, Polaris)
- [ ] Performance profiling & optimization
- [ ] Enterprise features (encryption, audit logging)

### Month 4-6: Expansion
- [ ] Multiple Kafka source support
- [ ] Dead letter queue
- [ ] Time travel queries
- [ ] SaaS variant beta

### Month 6-12: Enterprise
- [ ] Professional support offering
- [ ] Enterprise license option
- [ ] Advanced monitoring (Datadog, Prometheus)
- [ ] Multi-cluster failover

---

## Conclusion

### What We're Building

A **simple, fast, automated Kafka → Iceberg CLI tool** that solves all 4 industry problems that existing solutions don't:

1. ✅ **Sub-second freshness** (hot buffer, not batch accumulation)
2. ✅ **Zero maintenance** (fully automated background tasks)
3. ✅ **Guaranteed consistency** (transaction log)
4. ✅ **Dual partitioning** (preserve Kafka + optimize Iceberg)

### Why It Matters

- **For users:** Deploy in hours, real-time analytics, zero operational overhead
- **For us:** Own "simple real-time ingestion" category, $1.35B TAM, 6-12 month market window
- **For market:** Missing solution that's actually simple AND fast

### Timeline

**8 weeks to production-ready MVP** that can:
- Ingest 1M events/sec
- Deliver < 1 second freshness
- Automate all maintenance
- Guarantee zero data loss
- Run as single CLI binary
- Be fully documented

### Decision Point

**Build/Ship/Win.** The market is waiting. Moonlink won't provide this for 6-12 months. Move fast. 🚀

---

**Document Status:** ✅ Ready for Engineering Kickoff  
**Last Updated:** January 8, 2026  
**Next Step:** Schedule team kickoff, assign components, begin week 1 development
