# K2I: A High-Performance Streaming Ingestion Engine for Apache Iceberg

**Version 1.0 | January 2026**

**Authors:** OSO DevOps Engineering Team

---

## Abstract

Modern data architectures face a fundamental tension between query latency and storage cost. Real-time streaming systems provide millisecond freshness but at significant infrastructure complexity and cost, while batch-oriented data lakes offer cost-efficient analytics but with minutes-to-hours of data delay. K2I (Kafka to Iceberg) resolves this trade-off through a novel single-process architecture that combines in-memory hot buffering with durable cold storage, delivering sub-second query freshness with the cost efficiency of columnar storage.

This whitepaper presents K2I's architecture, implementation details, and operational characteristics. K2I consumes events from Apache Kafka, buffers them in-memory using Apache Arrow for immediate queryability, and durably persists them to Apache Iceberg tables in Parquet format. A write-ahead transaction log ensures exactly-once semantics and crash recovery without data loss.

K2I is implemented in Rust for maximum performance and memory safety, supports multiple Iceberg catalog backends (REST, Hive Metastore, AWS Glue, Nessie), and includes automated maintenance operations for production deployments.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Problem Statement](#2-problem-statement)
3. [Design Philosophy](#3-design-philosophy)
4. [System Architecture](#4-system-architecture)
5. [Core Components](#5-core-components)
6. [Data Flow & Processing Pipeline](#6-data-flow--processing-pipeline)
7. [Exactly-Once Semantics](#7-exactly-once-semantics)
8. [Crash Recovery](#8-crash-recovery)
9. [Performance Characteristics](#9-performance-characteristics)
10. [Maintenance Operations](#10-maintenance-operations)
11. [Catalog Integration](#11-catalog-integration)
12. [Observability](#12-observability)
13. [Deployment Models](#13-deployment-models)
14. [Comparison with Alternatives](#14-comparison-with-alternatives)
15. [Future Directions](#15-future-directions)
16. [Conclusion](#16-conclusion)
17. [References](#17-references)

---

## 1. Introduction

The data lakehouse paradigm has emerged as the dominant architecture for modern analytical workloads, combining the flexibility of data lakes with the performance characteristics of data warehouses. Apache Iceberg has become the de facto open table format, providing ACID transactions, schema evolution, and time travel capabilities on top of commodity object storage.

However, ingesting streaming data into Iceberg tables presents significant challenges. Traditional batch ETL introduces unacceptable latency for real-time use cases, while naive streaming approaches create excessive small files that degrade query performance and increase metadata overhead.

K2I addresses these challenges through a purpose-built streaming ingestion engine that:

- **Minimizes data latency**: Sub-second freshness via in-memory hot buffer
- **Optimizes file sizes**: Intelligent buffering produces read-optimized Parquet files
- **Ensures data integrity**: Exactly-once semantics through write-ahead logging
- **Reduces operational burden**: Automated compaction, expiration, and cleanup
- **Maximizes performance**: Native Rust implementation with zero-copy data paths

### 1.1 Inspiration

K2I draws inspiration from the [Moonlink architecture](https://www.mooncake.dev/moonlink/) developed by Mooncake Labs, which pioneered the concept of combining in-memory buffering with Iceberg's durable storage. K2I extends this approach with:

- Kafka-native ingestion (vs. CDC-focused)
- Multiple catalog backend support
- Comprehensive maintenance automation
- Production-grade observability

---

## 2. Problem Statement

### 2.1 The Latency-Cost Trade-off

Organizations face a fundamental tension when building data pipelines:

| Approach | Latency | Cost | Complexity |
|----------|---------|------|------------|
| Real-time streaming (Kafka + KSQL) | Milliseconds | High | High |
| Micro-batch (Spark Streaming) | Seconds-Minutes | Medium | Medium |
| Batch ETL (Airflow + Spark) | Minutes-Hours | Low | Low |

Real-time systems require maintaining separate infrastructure, duplicating storage, and managing complex stateful processing. Batch systems are simpler but cannot serve time-sensitive use cases.

### 2.2 The Small File Problem

Streaming data into Iceberg creates a cascade of problems:

1. **File proliferation**: Each micro-batch creates new files, potentially thousands per hour
2. **Metadata explosion**: Each file requires manifest entries, increasing planning time
3. **Query degradation**: Small files increase I/O operations and reduce scan efficiency
4. **Storage overhead**: Per-file metadata and object storage API costs accumulate

### 2.3 Exactly-Once Challenges

Achieving exactly-once semantics in streaming systems requires coordination between:

- Message consumption acknowledgment
- State persistence
- External system commits

Failures at any point can result in data loss or duplication, requiring complex distributed transactions or idempotency mechanisms.

### 2.4 Operational Complexity

Production streaming pipelines require ongoing maintenance:

- File compaction to optimize query performance
- Snapshot expiration to manage metadata growth
- Orphan file cleanup from failed operations
- Schema evolution handling
- Monitoring and alerting

---

## 3. Design Philosophy

K2I is built on four foundational principles:

### 3.1 Single-Process Simplicity

Unlike distributed streaming frameworks (Flink, Spark Streaming), K2I runs as a single process per topic partition. This eliminates:

- Distributed coordination overhead (ZooKeeper, consensus protocols)
- Network serialization between processing stages
- Complex failure recovery across nodes
- Operational complexity of cluster management

Single-process design provides deterministic behavior, simpler debugging, and predictable resource consumption.

### 3.2 Hot/Cold Data Separation

K2I maintains two storage tiers:

**Hot Tier (In-Memory)**
- Apache Arrow columnar format
- Hash indexes for O(1) key lookup
- Sub-millisecond query latency
- Bounded by configurable memory limit

**Cold Tier (Object Storage)**
- Apache Parquet files on S3/GCS/Azure
- Optimized for analytical queries
- Unlimited retention
- Cost-efficient at scale

This separation allows immediate data availability while accumulating sufficient data for optimal file sizes.

### 3.3 Transaction Log Durability

All state-changing operations are recorded in an append-only transaction log before execution. This provides:

- **Crash recovery**: Reconstruct state from log replay
- **Exactly-once semantics**: Idempotency through operation tracking
- **Audit trail**: Complete history of data mutations
- **Debugging**: Forensic analysis of failures

### 3.4 Defensive Design

K2I assumes failures will occur and designs for resilience:

- **Backpressure propagation**: Pause consumption when buffer is full
- **Circuit breakers**: Prevent cascading failures to external systems
- **Graceful degradation**: Continue operating with reduced functionality
- **Automatic recovery**: Self-heal from transient failures

---

## 4. System Architecture

### 4.1 High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           K2I Ingestion Engine                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │
│   │  SmartKafka      │    │    Hot Buffer    │    │  Iceberg Writer  │     │
│   │  Consumer        │───▶│  (Arrow + Index) │───▶│  (Parquet)       │     │
│   │                  │    │                  │    │                  │     │
│   │  • rdkafka       │    │  • RecordBatch   │    │  • Catalog       │     │
│   │  • Backpressure  │    │  • DashMap Index │    │  • Object Store  │     │
│   │  • Retry Logic   │    │  • TTL Eviction  │    │  • Atomic Commit │     │
│   └──────────────────┘    └──────────────────┘    └──────────────────┘     │
│            │                       │                       │                │
│            │                       │                       │                │
│            ▼                       ▼                       ▼                │
│   ┌────────────────────────────────────────────────────────────────────┐   │
│   │                      Transaction Log                                │   │
│   │  • Append-only entries with CRC32 checksums                        │   │
│   │  • Periodic checkpoints for fast recovery                          │   │
│   │  • Idempotency records for exactly-once semantics                  │   │
│   └────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │
│   │  Health Check    │    │  Metrics Export  │    │  Maintenance     │     │
│   │                  │    │                  │    │  Scheduler       │     │
│   │  • /health       │    │  • Prometheus    │    │  • Compaction    │     │
│   │  • /healthz      │    │  • Counters      │    │  • Expiration    │     │
│   │  • /readyz       │    │  • Histograms    │    │  • Orphan Clean  │     │
│   └──────────────────┘    └──────────────────┘    └──────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          External Services                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │
│   │  Kafka Brokers   │    │  Iceberg Catalog │    │  Object Storage  │     │
│   │                  │    │                  │    │                  │     │
│   │  • Bootstrap     │    │  • REST          │    │  • S3            │     │
│   │  • SASL/SSL      │    │  • Hive          │    │  • GCS           │     │
│   │  • Consumer      │    │  • AWS Glue      │    │  • Azure Blob    │     │
│   │    Groups        │    │  • Nessie        │    │  • Local FS      │     │
│   └──────────────────┘    └──────────────────┘    └──────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **SmartKafkaConsumer** | Message ingestion with backpressure and retry |
| **HotBuffer** | In-memory storage with indexes and eviction |
| **IcebergWriter** | Parquet generation and catalog commits |
| **TransactionLog** | Durability and recovery coordination |
| **TransactionCoordinator** | Atomic commits with CAS semantics |
| **MaintenanceScheduler** | Background optimization tasks |
| **HealthChecker** | Component status aggregation |
| **MetricsExporter** | Prometheus metrics publication |

### 4.3 Technology Stack

K2I is implemented entirely in Rust, leveraging:

| Library | Purpose |
|---------|---------|
| `rdkafka` | Kafka consumer (librdkafka bindings) |
| `arrow` | In-memory columnar storage |
| `parquet` | File format encoding |
| `iceberg` | Table format operations |
| `tokio` | Async runtime |
| `axum` | HTTP servers |
| `prometheus` | Metrics exposition |
| `dashmap` | Concurrent hash maps |
| `tracing` | Structured logging |

---

## 5. Core Components

### 5.1 Smart Kafka Consumer

The SmartKafkaConsumer wraps `rdkafka` with production-grade features:

#### 5.1.1 Batch Processing

Messages are collected into batches before processing:

```rust
pub struct KafkaBatch {
    messages: Vec<KafkaMessage>,
    partition_offsets: HashMap<i32, i64>,
    timestamp_range: (DateTime<Utc>, DateTime<Utc>),
}
```

Batching amortizes processing overhead and enables efficient Arrow conversion.

#### 5.1.2 Backpressure Control

When the hot buffer reaches capacity, the consumer pauses:

```
┌─────────────┐     BufferFull     ┌─────────────┐
│  Consuming  │──────────────────▶│   Paused    │
│             │                    │             │
│  (polling)  │◀──────────────────│  (waiting)  │
└─────────────┘     BufferSpace    └─────────────┘
```

This prevents memory exhaustion while maintaining offset consistency.

#### 5.1.3 Retry Strategy

Transient failures trigger exponential backoff:

```
Delay = min(max_delay, base_delay × 2^attempt) + jitter

Default configuration:
  base_delay:   100ms
  max_delay:    30s
  max_attempts: 10
  jitter:       0-10% random
```

#### 5.1.4 Offset Management

Offsets are committed only after durable persistence:

1. Messages consumed from Kafka
2. Data written to transaction log
3. Flush completed to Iceberg
4. **Then** offsets committed to Kafka

This ensures no data loss on failure.

### 5.2 Hot Buffer

The hot buffer provides sub-millisecond access to recent data.

#### 5.2.1 Data Structure

```rust
pub struct HotBuffer {
    // Columnar storage
    schema: SchemaRef,
    builders: RwLock<ColumnBuilders>,
    records: RwLock<Vec<BufferedRecord>>,

    // O(1) lookup indexes
    key_index: DashMap<Vec<u8>, RowId>,
    offset_index: DashMap<(i32, i64), RowId>,

    // Management
    stats: BufferStats,
    config: BufferConfig,
}
```

#### 5.2.2 Index Design

Two concurrent hash maps enable fast lookups:

| Index | Key | Value | Use Case |
|-------|-----|-------|----------|
| `key_index` | Message key bytes | Row ID | Lookup by business key |
| `offset_index` | (partition, offset) | Row ID | Offset-based queries |

Both indexes use `DashMap` for lock-free concurrent access.

#### 5.2.3 Memory Layout

Data is stored in Arrow columnar format for:

- **Cache efficiency**: Sequential memory access
- **SIMD vectorization**: Parallel processing
- **Zero-copy conversion**: Direct Parquet encoding
- **Compression**: Run-length encoding of repeated values

Memory alignment (default 64 bytes) ensures optimal CPU utilization.

#### 5.2.4 Flush Triggers

The buffer flushes when any condition is met:

| Trigger | Configuration | Default | Rationale |
|---------|---------------|---------|-----------|
| Time | `flush_interval_seconds` | 30s | Ensure freshness SLA |
| Size | `max_size_mb` | 500MB | Prevent OOM |
| Count | `flush_batch_size` | 10,000 | Optimal file size |

#### 5.2.5 Eviction Policy

```rust
pub enum EvictionDecision {
    None,              // No action needed
    FlushTtl,          // Records exceeded TTL
    FlushSize,         // Buffer size limit reached
    FlushCount,        // Record count limit reached
    FlushInterval,     // Time interval elapsed
    EvictImmediate,    // Critical: must free memory now
}
```

### 5.3 Iceberg Writer

The Iceberg writer handles the critical path of durable persistence.

#### 5.3.1 Write Pipeline

```
Arrow RecordBatch
        │
        ▼
┌───────────────────┐
│ Parquet Encoder   │
│ • Row group: 128K │
│ • Compression     │
│ • Statistics      │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Object Storage    │
│ • Multipart upload│
│ • Retry on failure│
│ • Checksum verify │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Catalog Commit    │
│ • CAS semantics   │
│ • Conflict retry  │
│ • Snapshot create │
└───────────────────┘
```

#### 5.3.2 File Naming Convention

```
{warehouse}/{database}/{table}/
  event_date={YYYY-MM-DD}/
    kafka_partition={N}/
      part-{uuid}-{offset_min}-{offset_max}.parquet
```

This structure enables:
- Partition pruning by date
- Kafka partition isolation for replay
- Offset range identification for debugging

#### 5.3.3 Compression Options

| Codec | Compression Ratio | Speed | Use Case |
|-------|-------------------|-------|----------|
| `snappy` | Good | Fast | Default, general purpose |
| `zstd` | Best | Medium | Long-term archival |
| `lz4` | Lower | Fastest | High throughput |
| `gzip` | Good | Slow | Maximum compatibility |

#### 5.3.4 Parquet Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_file_size_mb` | 512 | Target file size |
| `row_group_size` | 128K rows | Rows per row group |
| `page_size` | 1MB | Page size for encoding |
| `dictionary_encoding` | true | Enable dictionary compression |

### 5.4 Transaction Log

The transaction log is the foundation for durability and recovery.

#### 5.4.1 File Format

```
┌──────────────────────────────────────────────────────┐
│                   Log File                            │
├──────────────────────────────────────────────────────┤
│ [Length:4 bytes][Entry:N bytes][CRC32:4 bytes]       │
│ [Length:4 bytes][Entry:N bytes][CRC32:4 bytes]       │
│ [Length:4 bytes][Entry:N bytes][CRC32:4 bytes]       │
│ ...                                                   │
└──────────────────────────────────────────────────────┘
```

Each entry is self-describing with integrity verification.

#### 5.4.2 Entry Types

```rust
pub enum TransactionEntry {
    // Offset tracking
    OffsetMarker {
        topic: String,
        partition: i32,
        offset: i64,
        record_count: u64,
        timestamp: DateTime<Utc>,
    },

    // Flush lifecycle
    FlushStart {
        batch_id: String,
        row_count: u64,
        partitions: Vec<i32>,
    },
    ParquetWritten {
        file_path: String,
        file_size_bytes: u64,
        row_count: u64,
        checksum: String,
    },
    FlushComplete {
        batch_id: String,
        kafka_offset: i64,
        iceberg_snapshot_id: i64,
        files_created: Vec<String>,
    },

    // Commit tracking
    IcebergSnapshot {
        snapshot_id: i64,
        manifest_list_path: String,
        summary: HashMap<String, String>,
    },

    // Idempotency
    IdempotencyRecord {
        offset_min: i64,
        offset_max: i64,
        snapshot_id: i64,
        file_paths: Vec<String>,
    },

    // Recovery
    Checkpoint {
        checkpoint_id: String,
        last_kafka_offset: i64,
        last_iceberg_snapshot: i64,
        entries_since_last: u64,
        timestamp: DateTime<Utc>,
    },

    // Schema changes
    SchemaEvolution {
        fields_added: Vec<String>,
        fields_removed: Vec<String>,
        old_schema_id: i64,
        new_schema_id: i64,
    },
}
```

#### 5.4.3 Checkpointing

Periodic checkpoints truncate the log and speed recovery:

```
Checkpoint triggers:
  • Every 10,000 entries (configurable)
  • Every 5 minutes (configurable)
  • Before graceful shutdown

Checkpoint contains:
  • Last committed Kafka offset per partition
  • Last committed Iceberg snapshot ID
  • Entry count since previous checkpoint
```

#### 5.4.4 Log Rotation

```
transaction_logs/
  ├── txlog-20260109-001.log    (archived)
  ├── txlog-20260109-002.log    (archived)
  ├── txlog-20260109-003.log    (current)
  └── checkpoint-20260109.json  (latest state)
```

Old log files are retained for `max_log_files` (default: 10) rotations.

---

## 6. Data Flow & Processing Pipeline

### 6.1 Write Path

The write path processes messages from Kafka to the hot buffer:

```
Step 1: Poll
┌─────────────┐
│   Kafka     │──── poll() ────▶ Vec<RawMessage>
└─────────────┘

Step 2: Batch
┌─────────────┐
│   Batcher   │──── collect() ──▶ KafkaBatch { messages, offsets }
└─────────────┘

Step 3: Convert
┌─────────────┐
│   Arrow     │──── convert() ──▶ RecordBatch
└─────────────┘

Step 4: Buffer
┌─────────────┐
│ Hot Buffer  │──── append() ───▶ Updated indexes
└─────────────┘

Step 5: Log
┌─────────────┐
│ Transaction │──── write() ────▶ OffsetMarker entry
│    Log      │
└─────────────┘
```

**Latency**: < 10ms end-to-end for buffering

### 6.2 Flush Path

The flush path persists buffered data to Iceberg:

```
Step 1: Trigger
┌─────────────┐
│   Trigger   │──── evaluate() ─▶ FlushDecision::Flush
└─────────────┘

Step 2: Log Start
┌─────────────┐
│ Transaction │──── write() ────▶ FlushStart entry
│    Log      │
└─────────────┘

Step 3: Encode
┌─────────────┐
│  Parquet    │──── encode() ───▶ Compressed bytes
│  Encoder    │
└─────────────┘

Step 4: Upload
┌─────────────┐
│  Object     │──── put() ──────▶ Immutable file
│  Storage    │
└─────────────┘

Step 5: Log File
┌─────────────┐
│ Transaction │──── write() ────▶ ParquetWritten entry
│    Log      │
└─────────────┘

Step 6: Commit
┌─────────────┐
│  Iceberg    │──── commit() ───▶ New snapshot
│  Catalog    │
└─────────────┘

Step 7: Log Complete
┌─────────────┐
│ Transaction │──── write() ────▶ FlushComplete entry
│    Log      │
└─────────────┘

Step 8: Clear
┌─────────────┐
│ Hot Buffer  │──── clear() ────▶ Memory freed
└─────────────┘

Step 9: Commit Kafka
┌─────────────┐
│   Kafka     │──── commit() ───▶ Offsets committed
└─────────────┘
```

**Latency**: 200-800ms typical flush cycle

### 6.3 Query Path

The hot buffer supports immediate queries:

```rust
// By message key
buffer.query_by_key("user-123") -> Option<KafkaMessage>

// By partition and offset
buffer.query_by_offset(partition=0, offset=12345) -> Option<KafkaMessage>

// Range query
buffer.query_range(partition=0, start=12000, end=12100) -> Vec<KafkaMessage>

// Full scan
buffer.query_all() -> Vec<KafkaMessage>
```

**Latency**: < 1ms for indexed lookups

---

## 7. Exactly-Once Semantics

K2I provides exactly-once delivery guarantees through coordinated transaction management.

### 7.1 The Challenge

Exactly-once requires atomicity across three systems:

1. **Kafka**: Consume and commit offsets
2. **Object Storage**: Upload files
3. **Iceberg Catalog**: Commit metadata

Failure at any point must not result in data loss or duplication.

### 7.2 Solution: Write-Ahead Logging + Idempotency

#### 7.2.1 Write-Ahead Logging

All operations are logged before execution:

```
FlushStart(batch_id=abc, rows=1000)
  └── If crash here: recover knows flush was attempted

ParquetWritten(file=part-001.parquet, size=50MB)
  └── If crash here: file exists but not committed

IcebergSnapshot(snapshot_id=123)
  └── If crash here: snapshot exists, mark complete

FlushComplete(batch_id=abc, snapshot=123)
  └── Safe to commit Kafka offset
```

#### 7.2.2 Idempotency Tracking

Each flush creates an idempotency record:

```rust
IdempotencyRecord {
    offset_min: 12000,
    offset_max: 12999,
    snapshot_id: 123,
    file_paths: ["part-001.parquet"],
}
```

On recovery, if the same offset range is encountered:
- Check if `IdempotencyRecord` exists for range
- If exists, skip processing (already committed)
- If not, process normally

### 7.3 Transaction Coordinator

The `TransactionCoordinator` manages atomic commits:

```rust
pub struct TransactionCoordinator {
    catalog: Arc<dyn CatalogOperations>,
    txlog: Arc<TransactionLog>,
    idempotency_cache: DashMap<IdempotencyKey, IdempotencyRecord>,
    config: TransactionCoordinatorConfig,
    stats: TransactionStats,
}
```

#### 7.3.1 Compare-and-Swap Commits

Iceberg commits use CAS (Compare-And-Swap) semantics:

```rust
pub async fn commit_snapshot(&self, files: Vec<DataFile>) -> Result<SnapshotId> {
    let expected_snapshot = self.catalog.current_snapshot().await?;

    for attempt in 1..=self.config.max_retries {
        match self.catalog.commit(expected_snapshot, files.clone()).await {
            Ok(new_snapshot) => return Ok(new_snapshot),
            Err(CasConflict { actual }) => {
                // Another writer committed; retry with new base
                expected_snapshot = actual;
                tokio::time::sleep(self.backoff(attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }

    Err(Error::MaxRetriesExceeded)
}
```

#### 7.3.2 Conflict Resolution

When CAS conflicts occur:

1. Read the new current snapshot
2. Verify no overlapping data (by partition/offset)
3. Retry commit with updated base
4. Exponential backoff between attempts

```
Backoff = min(5000ms, 100ms × 2^(attempt-1)) + jitter
```

### 7.4 Guarantees

| Scenario | Outcome |
|----------|---------|
| Crash before `FlushStart` | No data loss, resume from last offset |
| Crash after `ParquetWritten` | File exists, will be cleaned as orphan |
| Crash after `IcebergSnapshot` | Data committed, will skip on recovery |
| Crash after `FlushComplete` | Fully committed, Kafka offset safe |

---

## 8. Crash Recovery

K2I automatically recovers from crashes without data loss or duplication.

### 8.1 Recovery Process

```
Startup
    │
    ▼
┌─────────────────────────────────┐
│ 1. Load last checkpoint         │
│    • Last Kafka offsets         │
│    • Last Iceberg snapshot      │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│ 2. Replay log since checkpoint  │
│    • Process each entry         │
│    • Track incomplete flushes   │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│ 3. Identify orphan files        │
│    • FlushStart without Complete│
│    • ParquetWritten uncommitted │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│ 4. Build recovery state         │
│    • Starting offsets           │
│    • Cleanup actions            │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│ 5. Execute cleanup              │
│    • Delete orphan files        │
│    • Log cleanup actions        │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│ 6. Resume normal operation      │
│    • Seek to starting offsets   │
│    • Begin consuming            │
└─────────────────────────────────┘
```

### 8.2 Recovery State

```rust
pub struct RecoveryState {
    // Where to resume consumption
    last_kafka_offsets: HashMap<(String, i32), i64>,

    // Last committed Iceberg state
    last_iceberg_snapshot: Option<i64>,

    // Incomplete operations to clean up
    incomplete_flushes: Vec<String>,
    orphan_files: Vec<OrphanFile>,

    // Already committed (for idempotency)
    committed_files: HashSet<String>,

    // Statistics
    entries_processed: u64,
}
```

### 8.3 Orphan File Types

```rust
pub enum OrphanType {
    DataFile,      // Parquet files not in any snapshot
    Manifest,      // Avro manifest files
    ManifestList,  // Snapshot manifest lists
    Statistics,    // Table statistics files
    Unknown,       // Unclassified files
}
```

### 8.4 Recovery Guarantees

| Failure Mode | Data Impact | Recovery Action |
|--------------|-------------|-----------------|
| Process crash | None | Resume from last committed offset |
| OOM kill | None | Same as crash |
| Storage failure | Partial flush lost | Retry flush on recovery |
| Catalog failure | Uncommitted snapshot | Orphan cleanup, retry |
| Network partition | Varies | Idempotency check, retry or skip |

---

## 9. Performance Characteristics

### 9.1 Latency Profile

| Operation | P50 | P99 | Notes |
|-----------|-----|-----|-------|
| Kafka poll | 2ms | 10ms | Network dependent |
| Buffer append | 0.1ms | 0.5ms | CPU bound |
| Arrow → Parquet | 20ms | 100ms | Batch size dependent |
| Object upload | 100ms | 500ms | File size dependent |
| Catalog commit | 50ms | 200ms | Catalog dependent |
| **Total flush** | **200ms** | **800ms** | End-to-end |

### 9.2 Query Freshness

| Data State | Availability | Latency |
|------------|--------------|---------|
| Hot buffer | Immediate | < 1ms |
| Cold storage | After flush | 30s default |
| After compaction | Full optimized | Variable |

### 9.3 Throughput

Throughput depends on configuration and hardware:

| Configuration | Throughput | Notes |
|---------------|------------|-------|
| Default | 10K msg/s | Conservative settings |
| Tuned | 50K msg/s | Larger batches, LZ4 |
| Maximum | 100K+ msg/s | Dedicated hardware |

#### 9.3.1 Throughput Tuning

```toml
# High throughput configuration
[kafka]
batch_size = 5000
batch_timeout_ms = 1000

[buffer]
max_size_mb = 2000
flush_batch_size = 50000

[iceberg]
compression = "lz4"
target_file_size_mb = 256
```

### 9.4 Memory Consumption

| Component | Typical | Maximum | Configuration |
|-----------|---------|---------|---------------|
| Hot buffer | 200MB | 2GB | `buffer.max_size_mb` |
| Indexes | 20MB | 200MB | ~10% of buffer |
| Arrow batches | 10MB | 100MB | Per batch |
| Transaction log | 5MB | 50MB | Automatic |
| **Total** | **235MB** | **2.35GB** | |

### 9.5 File Size Optimization

K2I produces optimally-sized files:

| Metric | Target | Rationale |
|--------|--------|-----------|
| File size | 512MB | Query engine sweet spot |
| Row groups | 128K rows | Predicate pushdown efficiency |
| Files per hour | 1-10 | Minimal metadata overhead |

---

## 10. Maintenance Operations

K2I includes automated maintenance to ensure long-term health.

### 10.1 Compaction

Compaction merges small files into larger ones:

```
Before:
├── part-001.parquet (50MB)
├── part-002.parquet (30MB)
├── part-003.parquet (40MB)
├── part-004.parquet (60MB)
└── part-005.parquet (20MB)

After compaction:
├── compacted-001.parquet (200MB)
```

#### 10.1.1 Algorithm

```rust
pub async fn run_compaction(&self) -> Result<CompactionResult> {
    // 1. Find small files (< threshold)
    let candidates = self.find_candidates().await?;

    // 2. Group into compaction bins
    let bins = self.bin_files(candidates, self.config.target_mb);

    // 3. For each bin
    for bin in bins {
        // Read all files
        let batches = self.read_parquet_files(&bin).await?;

        // Write merged file
        let merged = self.write_merged(batches).await?;

        // Atomic commit: add new, remove old
        self.commit_compaction(merged, bin).await?;
    }

    Ok(result)
}
```

#### 10.1.2 Configuration

```toml
[maintenance]
compaction_enabled = true
compaction_interval_seconds = 3600    # Every hour
compaction_threshold_mb = 100         # Files smaller than this
compaction_target_mb = 512            # Target output size
```

### 10.2 Snapshot Expiration

Removes old snapshots to control metadata growth:

```rust
pub async fn expire_snapshots(&self) -> Result<ExpirationResult> {
    let retention_cutoff = Utc::now() - Duration::days(self.config.retention_days);

    let snapshots = self.catalog.list_snapshots().await?;
    let current = self.catalog.current_snapshot().await?;

    for snapshot in snapshots {
        // Never expire current snapshot
        if snapshot.id == current.id {
            continue;
        }

        if snapshot.timestamp < retention_cutoff {
            self.catalog.expire_snapshot(snapshot.id).await?;
        }
    }

    Ok(result)
}
```

#### 10.2.1 Configuration

```toml
[maintenance]
snapshot_expiration_enabled = true
snapshot_retention_days = 7
```

### 10.3 Orphan File Cleanup

Removes unreferenced files from storage:

```rust
pub async fn cleanup_orphans(&self) -> Result<CleanupResult> {
    // List all files in storage
    let storage_files = self.storage.list_files().await?;

    // Get referenced files from all snapshots
    let referenced = self.catalog.all_referenced_files().await?;

    // Find orphans (in storage but not referenced)
    let orphans: Vec<_> = storage_files
        .into_iter()
        .filter(|f| !referenced.contains(&f.path))
        .filter(|f| f.modified < retention_cutoff)  // Safety buffer
        .collect();

    // Delete orphans
    for orphan in orphans {
        self.storage.delete(&orphan.path).await?;
    }

    Ok(result)
}
```

#### 10.3.1 Safety Measures

- **Retention buffer**: Only delete files older than `orphan_retention_days` (default: 3)
- **Reference check**: Verify file not in any snapshot (including expired)
- **Modified time**: Don't delete recently modified files (in-flight writes)

### 10.4 Statistics Collection

Gathers column statistics for query optimization:

```rust
pub struct ColumnStatistics {
    null_count: Option<i64>,
    distinct_count: Option<i64>,
    min_value: Option<Value>,
    max_value: Option<Value>,
}
```

Statistics are extracted from Parquet footers and aggregated.

### 10.5 Maintenance Scheduler

```rust
pub struct MaintenanceScheduler {
    tasks: Vec<ScheduledTask>,
    executor: TaskExecutor,
}

pub struct ScheduledTask {
    name: String,
    interval: Duration,
    last_run: Option<DateTime<Utc>>,
    handler: Box<dyn MaintenanceHandler>,
}
```

All maintenance tasks run in the background without blocking ingestion.

---

## 11. Catalog Integration

K2I supports multiple Iceberg catalog backends through a unified interface.

### 11.1 Catalog Abstraction

```rust
#[async_trait]
pub trait CatalogOperations: Send + Sync {
    async fn create_table(&self, schema: &Schema) -> Result<TableMetadata>;
    async fn load_table(&self) -> Result<TableMetadata>;
    async fn current_snapshot(&self) -> Result<Option<Snapshot>>;
    async fn commit_snapshot(&self, files: Vec<DataFile>) -> Result<Snapshot>;
    async fn list_snapshots(&self) -> Result<Vec<Snapshot>>;
    async fn expire_snapshot(&self, snapshot_id: i64) -> Result<()>;
}
```

### 11.2 REST Catalog

Standard Iceberg REST API implementation:

```toml
[iceberg]
catalog_type = "rest"
rest_uri = "http://catalog.example.com:8181"

[iceberg.rest]
credential_type = "oauth2"
oauth2_token_endpoint = "https://auth.example.com/token"
oauth2_client_id = "k2i-client"
oauth2_client_secret = "${OAUTH_SECRET}"
oauth2_scope = "catalog:read catalog:write"
request_timeout_seconds = 30
```

#### 11.2.1 Features

- OAuth2 and Bearer token authentication
- Custom headers for proxy/gateway integration
- Connection pooling
- Automatic retry on transient failures

### 11.3 Hive Metastore

Integration with Apache Hive Metastore:

```toml
[iceberg]
catalog_type = "hive"
hive_metastore_uri = "thrift://hive-metastore:9083"
```

#### 11.3.1 Features

- Thrift protocol support
- Database/namespace mapping
- Schema synchronization

### 11.4 AWS Glue

Native AWS Glue Data Catalog integration:

```toml
[iceberg]
catalog_type = "glue"
aws_region = "us-east-1"

[iceberg.glue]
role_arn = "arn:aws:iam::123456789012:role/K2IRole"
external_id = "k2i-external-id"
catalog_id = "123456789012"
```

#### 11.4.1 Features

- IAM role assumption for cross-account access
- External ID support for security
- Custom Glue catalog ID

### 11.5 Nessie

Git-like versioned data lake catalog:

```toml
[iceberg]
catalog_type = "nessie"
nessie_uri = "http://nessie:19120/api/v1"

[iceberg.nessie]
default_branch = "main"
api_version = "v2"
```

#### 11.5.1 Features

- Branch-based isolation
- Time travel via references
- Merge and diff operations

### 11.6 Catalog Factory

Dynamic catalog creation based on configuration:

```rust
pub struct CatalogFactoryRegistry {
    factories: HashMap<CatalogType, Box<dyn CatalogFactory>>,
}

impl CatalogFactoryRegistry {
    pub fn create(&self, config: &IcebergConfig) -> Result<Arc<dyn CatalogOperations>> {
        let factory = self.factories.get(&config.catalog_type)
            .ok_or(Error::UnsupportedCatalog)?;
        factory.create(config)
    }
}
```

---

## 12. Observability

K2I provides comprehensive observability through metrics, health checks, and structured logging.

### 12.1 Prometheus Metrics

#### 12.1.1 Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `k2i_messages_total` | `topic`, `partition` | Messages consumed |
| `k2i_bytes_total` | `topic`, `partition` | Bytes consumed |
| `k2i_flushes_total` | `status` | Flush operations (success/failure) |
| `k2i_rows_flushed_total` | | Rows written to Iceberg |
| `k2i_errors_total` | `type` | Errors by category |
| `k2i_backpressure_events_total` | | Backpressure activations |
| `k2i_iceberg_commits_total` | `status` | Catalog commits |
| `k2i_cas_conflicts_total` | | CAS retry events |

#### 12.1.2 Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `k2i_buffer_size_bytes` | | Current buffer memory |
| `k2i_buffer_record_count` | | Current buffered records |
| `k2i_kafka_consumer_lag` | `topic`, `partition` | Consumer lag |
| `k2i_last_flush_timestamp` | | Unix timestamp of last flush |
| `k2i_health_status` | `component` | Component health (0=unhealthy, 1=degraded, 2=healthy) |

#### 12.1.3 Histograms

| Metric | Buckets (seconds) | Description |
|--------|-------------------|-------------|
| `k2i_flush_duration_seconds` | 0.1, 0.25, 0.5, 1, 2.5, 5, 10 | Flush operation duration |
| `k2i_parquet_write_seconds` | 0.01, 0.05, 0.1, 0.25, 0.5, 1 | Parquet encoding time |
| `k2i_catalog_commit_seconds` | 0.05, 0.1, 0.25, 0.5, 1, 2.5 | Catalog commit time |

### 12.2 Health Checks

#### 12.2.1 Endpoints

| Endpoint | Response | Use Case |
|----------|----------|----------|
| `GET /health` | Full JSON | Debugging, dashboards |
| `GET /healthz` | 200/503 | Kubernetes liveness |
| `GET /readyz` | 200/503 | Kubernetes readiness |

#### 12.2.2 Health Response

```json
{
  "status": "healthy",
  "components": {
    "kafka": { "status": "healthy", "message": "Connected to 3 brokers" },
    "buffer": { "status": "healthy", "message": "45% capacity" },
    "iceberg": { "status": "healthy", "message": "Last commit 5s ago" },
    "catalog": { "status": "healthy", "message": "REST catalog connected" },
    "txlog": { "status": "healthy", "message": "1234 entries since checkpoint" }
  },
  "uptime_seconds": 3600,
  "version": "0.1.0"
}
```

#### 12.2.3 Health States

| State | HTTP Code | Description |
|-------|-----------|-------------|
| `healthy` | 200 | All components operational |
| `degraded` | 200 | Some components impaired but functional |
| `unhealthy` | 503 | Critical failure, not operational |

### 12.3 Structured Logging

```rust
// JSON format (default for production)
{"timestamp":"2026-01-09T12:00:00Z","level":"INFO","target":"k2i::engine","message":"Flush completed","batch_id":"abc123","rows":1000,"duration_ms":250}

// Text format (for development)
2026-01-09T12:00:00Z INFO k2i::engine: Flush completed batch_id=abc123 rows=1000 duration_ms=250
```

#### 12.3.1 Log Levels

| Level | Use Case |
|-------|----------|
| `error` | Failures requiring attention |
| `warn` | Degraded conditions |
| `info` | Normal operations (default) |
| `debug` | Detailed troubleshooting |
| `trace` | Maximum verbosity |

---

## 13. Deployment Models

### 13.1 Docker

```dockerfile
FROM ghcr.io/osodevops/k2i:latest

COPY config.toml /etc/k2i/config.toml

HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl -f http://localhost:8080/healthz || exit 1

EXPOSE 8080 9090

ENTRYPOINT ["k2i", "ingest", "--config", "/etc/k2i/config.toml"]
```

### 13.2 Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: k2i
spec:
  replicas: 1  # Single process per partition set
  template:
    spec:
      containers:
      - name: k2i
        image: ghcr.io/osodevops/k2i:v0.1.0
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        ports:
        - containerPort: 8080
          name: health
        - containerPort: 9090
          name: metrics
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /readyz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        volumeMounts:
        - name: config
          mountPath: /etc/k2i
        - name: txlog
          mountPath: /var/lib/k2i/txlog
      volumes:
      - name: config
        configMap:
          name: k2i-config
      - name: txlog
        persistentVolumeClaim:
          claimName: k2i-txlog
```

### 13.3 Systemd

```ini
[Unit]
Description=K2I Kafka to Iceberg Ingestion
After=network.target

[Service]
Type=simple
User=k2i
ExecStart=/usr/local/bin/k2i ingest --config /etc/k2i/config.toml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 13.4 Scaling Considerations

| Scenario | Approach |
|----------|----------|
| Multiple topics | Deploy one K2I instance per topic |
| High-throughput topic | Deploy one K2I instance per partition subset |
| High availability | Active-passive with shared transaction log storage |

---

## 14. Comparison with Alternatives

### 14.1 Feature Matrix

| Feature | K2I | Spark Streaming | Flink | Kafka Connect |
|---------|-----|-----------------|-------|---------------|
| Single process | Yes | No | No | Per-connector |
| Sub-second latency | Yes | Minutes | Seconds | Seconds |
| Exactly-once | Yes | Yes | Yes | Depends |
| Auto compaction | Yes | No | No | No |
| Hot buffer queries | Yes | No | No | No |
| Memory footprint | Low | High | High | Medium |
| Operational complexity | Low | High | High | Medium |

### 14.2 Use Case Fit

| Use Case | Best Choice | Rationale |
|----------|-------------|-----------|
| Simple Kafka→Iceberg | **K2I** | Minimal complexity |
| Complex transformations | Flink | Stream processing capabilities |
| ML feature pipelines | Spark | ML library ecosystem |
| CDC replication | Moonlink | Delete handling |
| Multi-source ingestion | Flink | Source connectors |

### 14.3 Performance Comparison

| Metric | K2I | Spark Micro-batch | Flink |
|--------|-----|-------------------|-------|
| Latency (P50) | 200ms | 30s+ | 1-5s |
| Memory per partition | 50-200MB | 1GB+ | 500MB+ |
| Files per hour | 1-10 | 100+ | 10-50 |
| Setup time | Minutes | Hours | Hours |

---

## 15. Future Directions

### 15.1 Planned Features

| Feature | Description | Timeline |
|---------|-------------|----------|
| Schema registry integration | Avro/Protobuf with Confluent SR | Q1 2026 |
| Multi-table support | Single process, multiple destination tables | Q2 2026 |
| Delete handling | Row-level deletes via deletion vectors | Q2 2026 |
| Query API | REST API for hot buffer queries | Q3 2026 |
| Windows support | Native Windows binaries | Q3 2026 |

### 15.2 Research Areas

- **Adaptive flush triggers**: ML-based flush timing optimization
- **Tiered hot buffer**: NVMe-backed overflow for larger buffers
- **Distributed mode**: Coordinated multi-process deployment
- **Change data capture**: Direct database CDC support

---

## 16. Conclusion

K2I represents a new approach to streaming data ingestion that prioritizes simplicity without sacrificing capability. By combining single-process architecture with intelligent buffering and comprehensive durability mechanisms, K2I delivers:

- **Sub-second data freshness** through in-memory hot buffering
- **Cost-efficient storage** via optimized Parquet files on object storage
- **Data integrity** with exactly-once semantics and crash recovery
- **Operational simplicity** through automated maintenance
- **Production readiness** with comprehensive observability

K2I is ideal for organizations seeking to bridge the gap between real-time streaming and batch analytics without the complexity of distributed stream processing frameworks.

---

## 17. References

1. Apache Iceberg Specification. https://iceberg.apache.org/spec/
2. Apache Arrow Columnar Format. https://arrow.apache.org/docs/format/Columnar.html
3. Apache Parquet Format. https://parquet.apache.org/docs/
4. Mooncake Labs - Moonlink. https://www.mooncake.dev/moonlink/
5. rdkafka - Rust Kafka Client. https://github.com/fede1024/rust-rdkafka
6. Iceberg Rust Implementation. https://github.com/apache/iceberg-rust

---

## Appendix A: Configuration Reference

See [Configuration Guide](./configuration.md) for complete configuration reference.

## Appendix B: CLI Reference

See [Commands Guide](./commands.md) for complete CLI reference.

## Appendix C: Troubleshooting

See [Troubleshooting Guide](./troubleshooting.md) for common issues and solutions.

---

*K2I is open source software licensed under Apache 2.0. For contributions and issue reports, visit https://github.com/osodevops/k2i*
