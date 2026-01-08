# K2I Architecture

This document describes the internal architecture and design of K2I (Kafka to Iceberg).

## Design Philosophy

K2I is inspired by the **Moonlink architecture** from pg_mooncake, which combines:

1. **Single-process simplicity** - No distributed coordination overhead
2. **Hot/cold data separation** - Recent data in memory, historical data on object storage
3. **Transaction log durability** - Crash recovery without data loss
4. **Exactly-once semantics** - No duplicate or lost records

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         K2I Ingestion Engine                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  SmartKafka      │    │    Hot Buffer    │    │  Iceberg Writer  │  │
│  │  Consumer        │───▶│  (Arrow + Index) │───▶│  (Parquet)       │  │
│  │                  │    │                  │    │                  │  │
│  │  - rdkafka       │    │  - RecordBatch   │    │  - Catalog       │  │
│  │  - Backpressure  │    │  - DashMap Index │    │  - Object Store  │  │
│  │  - Retry Logic   │    │  - TTL Eviction  │    │  - Snapshots     │  │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘  │
│           │                       │                       │             │
│           │                       │                       │             │
│           ▼                       ▼                       ▼             │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                      Transaction Log                              │  │
│  │  - Append-only entries                                           │  │
│  │  - CRC32 checksums                                               │  │
│  │  - Periodic checkpoints                                          │  │
│  │  - Crash recovery                                                │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  Health Check    │    │  Metrics Export  │    │  Maintenance     │  │
│  │                  │    │                  │    │  Scheduler       │  │
│  │  - Components    │    │  - Prometheus    │    │  - Compaction    │  │
│  │  - Overall       │    │  - Counters      │    │  - Expiration    │  │
│  │  - Degraded      │    │  - Gauges        │    │  - Orphan Clean  │  │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                      HTTP Servers                                 │  │
│  │  Health: 8080 → /health /healthz /readyz                         │  │
│  │  Metrics: 9090 → /metrics (Prometheus format)                    │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        External Services                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  Kafka Brokers   │    │  Iceberg Catalog │    │  Object Storage  │  │
│  │                  │    │                  │    │                  │  │
│  │  - Bootstrap     │    │  - REST          │    │  - S3            │  │
│  │  - SASL/SSL      │    │  - Hive          │    │  - GCS           │  │
│  │  - Consumer      │    │  - Glue          │    │  - Azure         │  │
│  │    Groups        │    │  - Nessie        │    │  - Local FS      │  │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Smart Kafka Consumer

The Kafka consumer is built on `rdkafka` with smart features for production workloads.

```
┌────────────────────────────────────────────────────────┐
│                 SmartKafkaConsumer                     │
├────────────────────────────────────────────────────────┤
│                                                        │
│  ┌─────────────────┐    ┌─────────────────┐           │
│  │  Base Consumer  │───▶│  Batch Builder  │           │
│  │  (rdkafka)      │    │                 │           │
│  └─────────────────┘    └─────────────────┘           │
│           │                      │                     │
│           ▼                      ▼                     │
│  ┌─────────────────┐    ┌─────────────────┐           │
│  │  Backpressure   │◀───│  Retry Logic    │           │
│  │  Controller     │    │  (Exp. Backoff) │           │
│  └─────────────────┘    └─────────────────┘           │
│                                                        │
└────────────────────────────────────────────────────────┘
```

#### Features

| Feature | Description |
|---------|-------------|
| **Batching** | Collects messages into batches for efficient processing |
| **Backpressure** | Pauses consumption when hot buffer is full |
| **Retry Logic** | Exponential backoff for transient failures |
| **Offset Management** | Manual commits after persistence for exactly-once |
| **Partition Assignment** | Cooperative sticky assignment for minimal rebalancing |

#### Backpressure Mechanism

When the hot buffer reaches capacity:

1. Consumer receives `BufferFull` error from buffer
2. Consumer pauses polling via rdkafka's pause API
3. Consumer emits backpressure metric
4. Buffer flush completes, freeing space
5. Consumer resumes polling

This prevents memory exhaustion while maintaining offset consistency.

#### Retry Strategy

```
Delay = min(max_delay, base_delay * 2^attempt) + jitter

Default values:
- base_delay: 100ms
- max_delay: 30s
- max_attempts: 10
- jitter: random 0-10%
```

### 2. Hot Buffer

The hot buffer provides sub-millisecond access to recent data using Apache Arrow.

```
┌────────────────────────────────────────────────────────┐
│                     Hot Buffer                         │
├────────────────────────────────────────────────────────┤
│                                                        │
│  ┌──────────────────────────────────────────────┐     │
│  │           Arrow RecordBatch                   │     │
│  │  ┌─────┬─────┬─────┬─────┬─────┬─────┐      │     │
│  │  │ key │value│topic│part │offset│time │      │     │
│  │  ├─────┼─────┼─────┼─────┼─────┼─────┤      │     │
│  │  │ ... │ ... │ ... │ ... │ ... │ ... │      │     │
│  │  └─────┴─────┴─────┴─────┴─────┴─────┘      │     │
│  └──────────────────────────────────────────────┘     │
│                         │                              │
│         ┌───────────────┼───────────────┐             │
│         ▼               ▼               ▼             │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐      │
│  │  Key Index │  │Offset Index│  │ TTL Queue  │      │
│  │ (DashMap)  │  │ (DashMap)  │  │ (eviction) │      │
│  │            │  │            │  │            │      │
│  │ key → row  │  │(p,o) → row │  │ time-based │      │
│  └────────────┘  └────────────┘  └────────────┘      │
│                                                        │
└────────────────────────────────────────────────────────┘
```

#### Data Structure

| Component | Type | Purpose |
|-----------|------|---------|
| **RecordBatch** | Arrow | Columnar storage for Kafka messages |
| **Key Index** | DashMap | O(1) lookup by message key |
| **Offset Index** | DashMap | O(1) lookup by (partition, offset) |
| **TTL Queue** | Internal | Tracks record age for eviction |

#### Query API

```rust
// Query by message key
buffer.query_by_key("user-123") -> Option<KafkaMessage>

// Query by partition and offset
buffer.query_by_offset(0, 12345) -> Option<KafkaMessage>

// Query offset range
buffer.query_range(0, 12000, 12100) -> Vec<KafkaMessage>

// Query all buffered records
buffer.query_all() -> Vec<KafkaMessage>
```

#### Flush Triggers

The buffer flushes to Iceberg when any condition is met:

| Trigger | Condition | Purpose |
|---------|-----------|---------|
| **Time** | `flush_interval_seconds` elapsed | Ensure data freshness |
| **Size** | `max_size_mb` reached | Prevent memory exhaustion |
| **Count** | `flush_batch_size` records | Efficient file sizes |

### 3. Iceberg Writer

The Iceberg writer handles Parquet file creation and catalog commits.

```
┌────────────────────────────────────────────────────────┐
│                   Iceberg Writer                       │
├────────────────────────────────────────────────────────┤
│                                                        │
│  ┌─────────────────┐                                  │
│  │  Arrow Batch    │                                  │
│  │  (from buffer)  │                                  │
│  └────────┬────────┘                                  │
│           │                                            │
│           ▼                                            │
│  ┌─────────────────┐    ┌─────────────────┐          │
│  │ Parquet Writer  │───▶│  Object Store   │          │
│  │  - Compression  │    │  - S3/GCS/Azure │          │
│  │  - Row Groups   │    │  - File Upload  │          │
│  └─────────────────┘    └─────────────────┘          │
│           │                      │                     │
│           ▼                      ▼                     │
│  ┌─────────────────┐    ┌─────────────────┐          │
│  │ Manifest Entry  │───▶│ Catalog Commit  │          │
│  │  - File path    │    │  - Atomic CAS   │          │
│  │  - Statistics   │    │  - Snapshot ID  │          │
│  │  - Partitions   │    │  - Retry logic  │          │
│  └─────────────────┘    └─────────────────┘          │
│                                                        │
└────────────────────────────────────────────────────────┘
```

#### Write Pipeline

1. **Arrow to Parquet**: Convert RecordBatch to Parquet bytes with compression
2. **Upload**: Write Parquet file to object storage (immutable)
3. **Build Manifest**: Create manifest entry with statistics
4. **Atomic Commit**: Update Iceberg metadata via catalog (CAS semantics)
5. **Log Entry**: Record snapshot ID in transaction log

#### Catalog Abstraction

K2I supports multiple Iceberg catalogs through a unified interface:

| Catalog | Protocol | Use Case |
|---------|----------|----------|
| **REST** | HTTP/REST | Standard Iceberg REST API |
| **Hive** | Thrift | Hive Metastore integration |
| **Glue** | AWS API | AWS native data catalog |
| **Nessie** | HTTP/REST | Git-like versioned catalog |

#### Compression Options

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| `snappy` | Fast | Good | Default, general purpose |
| `zstd` | Medium | Best | Long-term storage |
| `lz4` | Fastest | Lower | High throughput |
| `gzip` | Slow | Good | Maximum compatibility |

### 4. Transaction Log

The transaction log provides durability and enables crash recovery.

```
┌────────────────────────────────────────────────────────┐
│                   Transaction Log                      │
├────────────────────────────────────────────────────────┤
│                                                        │
│  Log File Format:                                      │
│  ┌─────────────────────────────────────────────────┐  │
│  │ [Length:4][Entry:N][CRC32:4]                    │  │
│  │ [Length:4][Entry:N][CRC32:4]                    │  │
│  │ [Length:4][Entry:N][CRC32:4]                    │  │
│  │ ...                                             │  │
│  └─────────────────────────────────────────────────┘  │
│                                                        │
│  Entry Types:                                          │
│  ┌─────────────────┐  ┌─────────────────┐            │
│  │ OffsetMarker    │  │ FlushStart      │            │
│  │ - topic         │  │ - record_count  │            │
│  │ - partition     │  │ - batch_size    │            │
│  │ - offset        │  └─────────────────┘            │
│  │ - record_count  │                                 │
│  └─────────────────┘  ┌─────────────────┐            │
│                       │ FlushEnd        │            │
│  ┌─────────────────┐  │ - snapshot_id   │            │
│  │ CommitStart     │  │ - files_created │            │
│  │ - snapshot_id   │  └─────────────────┘            │
│  └─────────────────┘                                 │
│                       ┌─────────────────┐            │
│  ┌─────────────────┐  │ MaintenanceOp   │            │
│  │ CommitEnd       │  │ - op_type       │            │
│  │ - success       │  │ - details       │            │
│  │ - snapshot_id   │  └─────────────────┘            │
│  └─────────────────┘                                 │
│                       ┌─────────────────┐            │
│                       │ Checkpoint      │            │
│                       │ - state snapshot│            │
│                       └─────────────────┘            │
│                                                        │
└────────────────────────────────────────────────────────┘
```

#### Entry Types

| Entry | Purpose |
|-------|---------|
| `OffsetMarker` | Track consumed Kafka offsets |
| `FlushStart` | Mark beginning of flush operation |
| `FlushEnd` | Mark successful flush completion |
| `CommitStart` | Mark beginning of catalog commit |
| `CommitEnd` | Mark commit success/failure |
| `MaintenanceOp` | Log maintenance operations |
| `Checkpoint` | Periodic state snapshot |

#### Recovery Process

On startup, K2I:

1. Scans transaction log from last checkpoint
2. Identifies incomplete operations (FlushStart without FlushEnd)
3. Locates orphan files from failed flushes
4. Resumes from last committed offset
5. Continues normal operation

### 5. Health Check System

The health check system tracks component status for monitoring.

```
┌────────────────────────────────────────────────────────┐
│                    Health Check                        │
├────────────────────────────────────────────────────────┤
│                                                        │
│  Components:                                           │
│  ┌─────────────────────────────────────────────────┐  │
│  │  kafka    │ buffer │ iceberg │ catalog │ txlog  │  │
│  ├─────────────────────────────────────────────────┤  │
│  │ Healthy   │Healthy │Degraded │ Healthy │Healthy │  │
│  └─────────────────────────────────────────────────┘  │
│                         │                              │
│                         ▼                              │
│  ┌─────────────────────────────────────────────────┐  │
│  │              Overall Status                      │  │
│  │                                                  │  │
│  │  Rule: unhealthy > degraded > healthy           │  │
│  │  Result: DEGRADED                               │  │
│  └─────────────────────────────────────────────────┘  │
│                                                        │
└────────────────────────────────────────────────────────┘
```

#### Health States

| State | Description | Operational |
|-------|-------------|-------------|
| `Healthy` | All components functioning normally | Yes |
| `Degraded` | Some components impaired but functional | Yes |
| `Unhealthy` | Critical component failure | No |

#### HTTP Endpoints

| Endpoint | Response | Use Case |
|----------|----------|----------|
| `/health` | Full JSON status | Debugging, dashboards |
| `/healthz` | 200/503 | Kubernetes liveness |
| `/readyz` | 200/503 | Kubernetes readiness |

### 6. Circuit Breaker

The circuit breaker protects against cascading failures.

```
┌────────────────────────────────────────────────────────┐
│                   Circuit Breaker                      │
├────────────────────────────────────────────────────────┤
│                                                        │
│  States:                                               │
│                                                        │
│  ┌─────────┐    failures    ┌─────────┐              │
│  │ CLOSED  │───────────────▶│  OPEN   │              │
│  │         │                │         │              │
│  │ (normal)│◀───────────────│(blocked)│              │
│  └─────────┘    reset time  └─────────┘              │
│       ▲                          │                    │
│       │                          │ timeout            │
│       │    ┌─────────────┐       │                    │
│       │    │ HALF-OPEN   │◀──────┘                    │
│       │    │             │                            │
│       └────│ (testing)   │                            │
│   success  └─────────────┘                            │
│                  │                                     │
│                  │ failure                             │
│                  ▼                                     │
│             back to OPEN                               │
│                                                        │
└────────────────────────────────────────────────────────┘
```

#### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `failure_threshold` | 5 | Failures to open circuit |
| `reset_timeout` | 30s | Time before half-open |
| `success_threshold` | 2 | Successes to close circuit |

### 7. Metrics System

K2I exports Prometheus metrics for observability.

#### Counters

| Metric | Description |
|--------|-------------|
| `k2i_messages_total` | Messages consumed from Kafka |
| `k2i_errors_total{type}` | Errors by type (kafka, buffer, iceberg) |
| `k2i_flushes_total` | Buffer flush operations |
| `k2i_rows_flushed_total` | Rows written to Iceberg |
| `k2i_backpressure_total` | Backpressure events |
| `k2i_iceberg_commits_total` | Successful catalog commits |

#### Gauges

| Metric | Description |
|--------|-------------|
| `k2i_buffer_size_bytes` | Current buffer memory usage |
| `k2i_buffer_record_count` | Current records in buffer |
| `k2i_kafka_consumer_lag` | Consumer lag by partition |

#### Histograms

| Metric | Buckets | Description |
|--------|---------|-------------|
| `k2i_flush_duration_seconds` | 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0 | Flush operation duration |

## Data Flow

### Write Path

```
1. Kafka Consumer polls messages
         │
         ▼
2. Messages batched (batch_size or timeout)
         │
         ▼
3. Batch converted to Arrow RecordBatch
         │
         ▼
4. RecordBatch appended to Hot Buffer
         │
         ▼
5. Hash indexes updated (key, offset)
         │
         ▼
6. Offset marker written to Transaction Log
         │
         ▼
7. Offset committed to Kafka (exactly-once)
```

### Flush Path

```
1. Flush trigger fires (time/size/count)
         │
         ▼
2. FlushStart written to Transaction Log
         │
         ▼
3. Arrow RecordBatch → Parquet bytes
         │
         ▼
4. Parquet file uploaded to Object Storage
         │
         ▼
5. CommitStart written to Transaction Log
         │
         ▼
6. Atomic metadata commit to Iceberg Catalog
         │
         ▼
7. CommitEnd written to Transaction Log
         │
         ▼
8. FlushEnd written to Transaction Log
         │
         ▼
9. Hot Buffer cleared for flushed data
```

## Error Handling

### Error Hierarchy

```
Error (top-level)
├── Config(String)
├── Kafka(KafkaError)
│   ├── ConnectionFailed
│   ├── ConsumerGroup
│   ├── OffsetCommit
│   ├── PartitionAssignment
│   ├── Timeout
│   ├── BackpressureEngaged
│   ├── MessageParse
│   └── ConsumerClosed
├── Buffer(BufferError)
│   ├── BufferFull
│   ├── ArrowConversion
│   ├── SchemaMismatch
│   ├── MemoryAlignment
│   └── Empty
├── Iceberg(IcebergError)
│   ├── CatalogConnection
│   ├── TableNotFound
│   ├── SnapshotCommit
│   ├── ParquetWrite
│   ├── SchemaEvolution
│   ├── CasConflict
│   ├── FileUpload
│   └── SchemaMismatch
├── TransactionLog(TransactionLogError)
│   ├── Corrupted
│   ├── RecoveryFailed
│   ├── CheckpointFailed
│   ├── WriteFailed
│   └── ChecksumMismatch
├── Storage(String)
├── Io(std::io::Error)
├── Serialization(String)
└── Shutdown
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Configuration error |
| 2 | Kafka error |
| 3 | Iceberg error |
| 4 | Storage error |
| 5 | Transaction log error |
| 6 | Health check error |
| 10 | General runtime error |
| 130 | Signal interrupt (SIGINT/SIGTERM) |

## Crate Structure

```
k2i (workspace)
├── crates/k2i-cli           # CLI binary
│   ├── main.rs              # Entry point, command routing
│   ├── server.rs            # HTTP health/metrics servers
│   └── commands/
│       ├── ingest.rs        # Run ingestion engine
│       ├── status.rs        # Check health/metrics
│       └── maintenance.rs   # Manual maintenance tasks
│
└── crates/k2i-core          # Core library
    ├── lib.rs               # Public API
    ├── config.rs            # Configuration types
    ├── engine/              # Ingestion engine
    │   └── mod.rs           # IngestionEngine orchestration
    ├── kafka/               # Kafka consumer
    │   ├── consumer.rs      # SmartKafkaConsumer
    │   └── backpressure.rs  # Backpressure controller
    ├── buffer/              # Hot buffer
    │   ├── hot_buffer.rs    # Arrow-based storage
    │   └── index.rs         # DashMap indexes
    ├── iceberg/             # Iceberg writer
    │   ├── writer.rs        # IcebergWriter
    │   └── catalog/         # Catalog implementations
    │       ├── rest.rs      # REST catalog
    │       ├── hive.rs      # Hive metastore
    │       ├── glue.rs      # AWS Glue
    │       └── nessie.rs    # Nessie
    ├── txlog/               # Transaction log
    │   ├── log.rs           # TransactionLog
    │   └── recovery.rs      # Recovery logic
    ├── maintenance/         # Maintenance tasks
    │   ├── compaction.rs    # File compaction
    │   ├── expiration.rs    # Snapshot expiration
    │   └── cleanup.rs       # Orphan cleanup
    ├── metrics/             # Prometheus metrics
    ├── health.rs            # Health check system
    ├── circuit_breaker.rs   # Circuit breaker pattern
    └── error.rs             # Error types
```

## Performance Considerations

### Memory Usage

| Component | Typical Usage | Configurable |
|-----------|---------------|--------------|
| Hot Buffer | 100MB - 2GB | `buffer.max_size_mb` |
| Arrow RecordBatch | Per-batch | Automatic |
| Hash Indexes | ~10% of buffer | Automatic |
| Transaction Log Buffer | 1-10MB | Automatic |

### Throughput Factors

| Factor | Impact | Tuning |
|--------|--------|--------|
| Batch Size | Higher = more throughput | `kafka.batch_size` |
| Compression | zstd slower, snappy faster | `iceberg.compression` |
| Flush Interval | Shorter = lower latency | `buffer.flush_interval_seconds` |
| File Size | Larger = better query perf | `iceberg.target_file_size_mb` |

### Latency Breakdown

| Operation | Typical Latency |
|-----------|-----------------|
| Kafka poll | 1-10ms |
| Buffer append | < 1ms |
| Arrow → Parquet | 10-100ms |
| Object storage upload | 100-500ms |
| Catalog commit | 50-200ms |
| **Total flush** | **200-800ms** |
