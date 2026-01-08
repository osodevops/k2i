# Kafka → Iceberg CLI Tool - Updated PRD

## Executive Summary

Building a production-grade **Kafka → Iceberg streaming ingestion engine**, inspired by **pg_mooncake's Moonlink architecture**. The solution combines:

1. **Background worker process** (similar to Moonlink as Postgres background worker)
2. **In-memory buffering & indexing** (Apache Arrow for fast access to recent data)
3. **Columnar storage** (Iceberg format with Parquet files)
4. **Real-time sync** (sub-second freshness for hot data, Iceberg for cold storage)
5. **Metadata management** (transaction logs similar to Postgres WAL)

This enables **dual-read semantics**: query hot data in-memory with sub-second latency, or query cold data from Iceberg for cost-efficiency.

---

## Problem Statement

Existing Kafka → Iceberg solutions face fundamental challenges:

### 1. **Latency vs. Cost Trade-off**
- **Kafka-only**: Sub-second latency but expensive long-term storage
- **Batch pipelines** (Spark, Flink): Cost-efficient but 5-60 minute latency
- **Streaming engines**: Complex, multi-component architecture with operational overhead

### 2. **Architecture Complexity**
- Requires separate stream processing framework (Spark Structured Streaming, Flink, etc.)
- Multiple components: consumer, processor, writer, checkpointing, dead-letter queues
- Operational burden: monitoring, scaling, failure recovery across distributed systems

### 3. **Missing Gap: Real-Time Analytics Without Complexity**
- No single-process solution offering sub-second freshness + analytical query performance
- No elegant hybrid approach: keep hot data in-memory, cold data on Iceberg

### 4. **Data Freshness & Consistency**
- CDC replication slots can back up if downstream is slow (blocking source database)
- No effective mechanism to buffer writes without losing ordering or consistency guarantees

---

## Solution Architecture: Moonlink-Inspired Design

### Core Concept: **Embedded Real-Time Engine**

Instead of external streaming framework, embed a lightweight Rust engine that:

```
┌──────────────────────────────────────────────────────────────┐
│ Kafka → Iceberg Streaming Ingestion Engine                   │
│ (Single Process, Embedded)                                   │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Kafka     │    │  Hot Buffer  │    │  Iceberg     │  │
│  │  Consumer   │───▶│  (Arrow in   │───▶│   Writer     │  │
│  │             │    │   Memory)    │    │              │  │
│  └─────────────┘    └──────────────┘    └──────────────┘  │
│                             │                     │         │
│                             ▼                     ▼         │
│                    ┌──────────────┐    ┌──────────────┐  │
│                    │   Hash Index │    │  S3/Cloud    │  │
│                    │  (Fast seek) │    │  Storage     │  │
│                    └──────────────┘    └──────────────┘  │
│                             │                     │         │
│                             └──────┬──────────────┘         │
│                                    ▼                        │
│                           ┌──────────────────┐             │
│                           │  Transaction Log │             │
│                           │  (Metadata)      │             │
│                           └──────────────────┘             │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ Metrics, Checkpoints, Dead Letter Queues            │  │
│  │ Schema Evolution, Partitioning, Compaction          │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Architectural Layers (Moonlink Translation for Kafka)

#### **Layer 1: Kafka Replication Equivalent (Write Path)**

**Moonlink equivalent:** Postgres logical replication slot (can pause if hot buffer full)

**Kafka Adaptation:**
```
┌─────────────────────────────────────────────┐
│ Kafka Consumer Thread                       │
├─────────────────────────────────────────────┤
│ • Consumer group membership management      │
│ • Offset tracking (equivalent to WAL pos)   │
│ • Partition assignment & rebalancing        │
│ • Batch fetching from Kafka                 │
│                                              │
│ Acknowledgment strategy:                    │
│ - Commit offset only after persisting to    │
│   Transaction Log (similar to WAL writing)  │
│ - Prevents reprocessing on failure          │
└─────────────────────────────────────────────┘
```

**Key Insight from Moonlink:**
> Moonlink acknowledges to Postgres BEFORE writing to Iceberg. This prevents replication slot backup while buffering changes. We apply this pattern:

- Acknowledge to Kafka offset manager immediately
- BUT buffer in transaction log before committing Iceberg write
- If process crashes, transaction log recovery restores state
- Iceberg metadata is single source of truth (like Postgres table)

#### **Layer 2: In-Memory Buffer & Index (Hot Storage)**

**Moonlink equivalent:** In-memory hash tables for recent writes with Parquet-to-Index mapping

**Kafka Adaptation:**
```
┌────────────────────────────────────────────────┐
│ Hot Buffer Manager (Arrow-based)               │
├────────────────────────────────────────────────┤
│ Data Structure:                                │
│ ┌──────────────────────────────────────────┐ │
│ │ Apache Arrow RecordBatch                 │ │
│ │ (Columnar, vectorized, zero-copy)        │ │
│ └──────────────────────────────────────────┘ │
│                    ▼                          │
│ ┌──────────────────────────────────────────┐ │
│ │ Hash Index (memtable-style)              │ │
│ │ • Row ID → Arrow offset mapping          │ │
│ │ • Primary key → position lookup          │ │
│ │ • TTL-based eviction (recent = in hot)   │ │
│ └──────────────────────────────────────────┘ │
│                    ▼                          │
│ ┌──────────────────────────────────────────┐ │
│ │ Manifest Metadata                        │ │
│ │ • Which messages in hot buffer           │ │
│ │ • Sequence numbers for ordering          │ │
│ │ • Schema version tracking                │ │
│ └──────────────────────────────────────────┘ │
│                                                │
│ Policy: Keep N seconds of data in memory     │
│ Example: Last 60 seconds (configurable)      │
│                                                │
│ Size: Roughly 100MB-1GB for high throughput  │
│ (much smaller than traditional caches)       │
└────────────────────────────────────────────────┘
```

**Hybrid Read Strategy:**
```
Query request for data
    │
    ├─ Is record in hot buffer (recent)?
    │   └─ YES → Return from Arrow buffer (sub-millisecond)
    │   └─ NO → Query Iceberg table (with DuckDB/Presto/etc)
    │
    └─ Always consistent (same snapshot version)
```

#### **Layer 3: Iceberg Writer (Cold Storage)**

**Moonlink equivalent:** Atomic metadata updates to Iceberg manifest with immutable file writes

**Kafka Adaptation:**
```
┌────────────────────────────────────────────────┐
│ Iceberg Writer (Batch Flusher)                 │
├────────────────────────────────────────────────┤
│ • Converts Arrow batches → Parquet files       │
│ • S3/Cloud storage with object-store crate     │
│ • Transactional metadata updates               │
│                                                 │
│ Flush Triggers:                                │
│ • Batch size: 10K-100K records                 │
│ • Time-based: 60 seconds default               │
│ • Partition boundary: Natural event boundaries │
│                                                 │
│ Write Path:                                    │
│ 1. Buffer full → Arrow → Parquet               │
│ 2. Write to S3/GCS/Blob storage               │
│ 3. Update Iceberg metadata (manifest)         │
│ 4. Atomic metadata commit (CAS semantics)      │
│ 5. Append to transaction log                   │
│                                                 │
│ Concurrency:                                   │
│ • Multiple flush threads safe (Iceberg)       │
│ • Snapshot isolation (readers don't block)    │
│ • Lost write detection (CAS failure = retry)   │
└────────────────────────────────────────────────┘
```

#### **Layer 4: Transaction Log (Metadata Durability)**

**Moonlink equivalent:** Internal WAL for index writes, schema changes, manifests

**Kafka Adaptation:**
```
┌────────────────────────────────────────────────┐
│ Transaction Log (DurableLog)                   │
├────────────────────────────────────────────────┤
│ Storage: Local filesystem or S3                │
│                                                 │
│ Log Entries:                                   │
│ • OffsetCommit: "flushed to offset X"          │
│ • BufferSnapshot: "added Y records to hot"    │
│ • IcebergManifest: "added file Z.parquet"     │
│ • SchemaEvolution: "new schema version"       │
│ • CompactionTask: "merged 50 files"           │
│                                                 │
│ Recovery Process:                              │
│ 1. Scan transaction log from last offset      │
│ 2. Rebuild in-memory state                    │
│ 3. Verify Iceberg metadata matches            │
│ 4. Resume from safe point                     │
│                                                 │
│ Example Entry Format:                         │
│ {                                              │
│   "type": "OffsetCommit",                      │
│   "offset": 42000,                             │
│   "timestamp": "2024-01-08T10:30:00Z",        │
│   "icebergSnapshot": "12345-abc123"            │
│ }                                              │
└────────────────────────────────────────────────┘
```

---

## Implementation Architecture

### Process Model (Inspired by Moonlink's Postgres Integration)

```
User Query/Application
    │
    ├─ Hot Data Path:
    │  └─ Query in-memory Arrow buffer
    │     (built-in query executor for recent records)
    │
    ├─ Cold Data Path:
    │  └─ Query Iceberg via standard tools
    │     (DuckDB, Spark, Presto, etc.)
    │
    └─ Consistency Guarantee:
       └─ Both paths see same snapshot version
          (version control via transaction log)

Background Process:
    ├─ Consumer: Kafka offset tracking
    ├─ Buffer Manager: Arrow hot storage
    ├─ Writer: Iceberg flush + metadata
    ├─ Compactor: Background file merging
    └─ Monitor: Metrics & health checks
```

### Component Details

#### **1. Consumer Module (kafka/consumer.rs)**

```rust
struct KafkaConsumer {
    // Connection to Kafka broker
    rdkafka_consumer: Consumer<DefaultConsumerContext>,
    
    // Offset management (equivalent to WAL position)
    current_offset: Arc<AtomicU64>,
    
    // Batch building
    pending_batch: Vec<KafkaRecord>,
    batch_size: usize,
    batch_timeout: Duration,
    
    // Checkpoint recovery
    checkpoint_manager: CheckpointManager,
}

impl KafkaConsumer {
    async fn consume_loop(&mut self) {
        loop {
            match self.rdkafka_consumer.poll(timeout) {
                Some(msg) => {
                    self.pending_batch.push(msg);
                    
                    // Check if batch ready for flushing
                    if self.ready_to_flush() {
                        let batch = self.take_batch();
                        // Send to buffer manager
                        self.buffer_manager.append(batch).await;
                        self.current_offset = batch.max_offset();
                    }
                }
                None => {
                    // Timeout: flush partial batch
                    if !self.pending_batch.is_empty() {
                        self.flush_partial().await;
                    }
                }
            }
        }
    }
}
```

**Key Design (Moonlink pattern):**
- Track Kafka offset (like WAL position in Moonlink)
- Only commit offset after successfully persisting to buffer + transaction log
- Prevents losing data on crash, prevents reprocessing on recovery

#### **2. Hot Buffer Manager (buffer/hot_storage.rs)**

```rust
struct HotBufferManager {
    // Arrow columnar storage
    record_batch: Arc<RecordBatch>,
    
    // Hash index for fast lookups
    row_index: DashMap<PrimaryKey, RowId>,
    
    // Metadata tracking
    buffer_metadata: BufferMetadata,
    
    // Memory constraints
    max_size_bytes: usize,
    ttl: Duration,
}

impl HotBufferManager {
    async fn append(&self, records: Vec<Record>) {
        // Convert to Arrow format
        let arrow_batch = Self::to_arrow(records);
        
        // Update hash index
        for (idx, record) in records.iter().enumerate() {
            self.row_index.insert(record.key(), RowId(idx));
        }
        
        // Check eviction policy
        if self.size_exceeds_limit() {
            self.evict_old_records().await;
        }
        
        // Append to transaction log
        self.transaction_log.append(TransactionEntry::BufferSnapshot {
            row_count: records.len(),
            memory_usage: arrow_batch.get_array_memory_size(),
        }).await;
    }
    
    fn query(&self, key: &PrimaryKey) -> Option<Record> {
        // Fast lookup via hash index
        if let Some(row_id) = self.row_index.get(key) {
            return Some(self.record_batch.get_row(row_id.0));
        }
        None
    }
}
```

**Key Design (Moonlink pattern):**
- Uses Apache Arrow (columnar format like parquet)
- Hash index for point lookups (like Moonlink's disk-optimized hash)
- TTL-based eviction (recent = hot, old = cold)
- All updates tracked in transaction log

#### **3. Iceberg Writer (iceberg/writer.rs)**

```rust
struct IcebergWriter {
    // Arrow to Parquet converter
    parquet_writer: Arc<ParquetWriter>,
    
    // Cloud storage
    object_store: Arc<dyn ObjectStore>,
    
    // Iceberg catalog
    catalog: Arc<IcebergCatalog>,
    
    // Flush configuration
    batch_size: usize,
    flush_interval: Duration,
}

impl IcebergWriter {
    async fn flush(&self, buffer: &HotBufferManager) {
        let arrow_batch = buffer.get_recordbatch();
        
        // Step 1: Convert Arrow → Parquet
        let parquet_data = self.parquet_writer.write(arrow_batch).await;
        
        // Step 2: Upload to storage
        let file_path = format!("warehouse/data/{}.parquet", Uuid::new_v4());
        self.object_store.put(&file_path, parquet_data).await;
        
        // Step 3: Build Iceberg metadata
        let manifest_entry = ManifestEntry {
            file_path,
            record_count: arrow_batch.num_rows(),
            file_size: parquet_data.len(),
            schema_version: self.current_schema_version,
        };
        
        // Step 4: Atomic commit to Iceberg metadata
        let new_snapshot = self.catalog.append_file(manifest_entry).await;
        
        // Step 5: Record in transaction log
        self.transaction_log.append(TransactionEntry::IcebergManifest {
            snapshot_id: new_snapshot.id(),
            manifest_list_path: new_snapshot.manifest_list_path(),
        }).await;
        
        // Step 6: Clear hot buffer
        buffer.clear().await;
    }
}
```

**Key Design (Moonlink pattern):**
- Atomic Iceberg metadata updates (CAS semantics)
- All file writes are immutable (append-only)
- Snapshot isolation (readers can see old versions)
- All operations logged for crash recovery

#### **4. Transaction Log (checkpoint/transaction_log.rs)**

```rust
struct TransactionLog {
    // Local filesystem or S3
    storage: Arc<dyn TransactionLogStorage>,
    
    // In-memory buffer for fast appends
    buffer: Arc<Mutex<Vec<LogEntry>>>,
}

enum TransactionEntry {
    OffsetCommit { offset: u64, timestamp: DateTime },
    BufferSnapshot { row_count: usize, memory_usage: u64 },
    IcebergManifest { snapshot_id: u64, manifest_path: String },
    SchemaEvolution { version: u32, schema: String },
    CompactionTask { files_merged: u32, output_file: String },
}

impl TransactionLog {
    async fn append(&self, entry: TransactionEntry) {
        let mut buffer = self.buffer.lock().await;
        buffer.push(entry);
        
        // Flush periodically (e.g., every 1000 entries or 5 seconds)
        if buffer.len() > 1000 || self.should_flush() {
            self.flush_to_storage().await;
        }
    }
    
    async fn recover(&self) -> RecoveryState {
        // Read transaction log from storage
        let entries = self.storage.read_all().await;
        
        // Rebuild state
        let mut state = RecoveryState::default();
        for entry in entries {
            match entry {
                TransactionEntry::OffsetCommit { offset, .. } => {
                    state.last_committed_offset = offset;
                }
                TransactionEntry::IcebergManifest { snapshot_id, .. } => {
                    state.last_iceberg_snapshot = snapshot_id;
                }
                // ... handle other entries
            }
        }
        
        state
    }
}
```

**Key Design (Moonlink pattern):**
- Ordered write-ahead log (like Postgres WAL)
- Enables crash recovery without re-consuming from Kafka
- Verifiable state reconstruction
- Single source of truth for system state

---

## System Features

### 1. **Hot/Cold Hybrid Query Semantics**

**Unlike traditional streaming:**
```
Traditional:  Kafka (hot) → Wait for batch → Iceberg (cold)
              ⏱️ 5-60 minute latency

Our approach: Kafka → Hot Buffer (< 1ms) 
                   ↘
                     Query Recent (< 1ms) or Historical (seconds)
                   ↙
              Iceberg (cost-optimized)
              ⏱️ Consistent sub-second view
```

### 2. **CDC/Upsert Support (like Postgres UPDATE/DELETE)**

Problem: Kafka provides immutable append-only logs, but operational databases need ACID updates.

Solution: Implement CDC-like upsert semantics in Iceberg with delete files.

### 3. **Automatic Partitioning (like Iceberg's hidden partitions)**

```rust
pub struct PartitioningStrategy {
    // Example: partition by day + hour
    spec: vec![
        PartitionField::DatePartition("event_time".to_string()),
        PartitionField::HourPartition("event_time".to_string()),
    ],
}
```

### 4. **Automatic Compaction (like Iceberg maintenance)**

Background process monitors file sizes and merges small files automatically.

### 5. **Schema Evolution (like Iceberg's schema versioning)**

Safe schema changes without reprocessing data using field IDs instead of positions.

---

## Configuration & CLI

### Command Structure

```bash
# Basic sync: consume all topics, write to Iceberg
kafka-iceberg-cli sync \
  --kafka-brokers localhost:9092 \
  --kafka-topics events,transactions \
  --iceberg-warehouse-path s3://my-bucket/warehouse \
  --iceberg-table-name my_db.events \
  --buffer-ttl-secs 60 \
  --buffer-max-size-mb 500
```

### Configuration File (YAML)

```yaml
kafka:
  brokers:
    - localhost:9092
  topics:
    - name: events
      table: my_db.events
      key_field: event_id
      timestamp_field: event_time
    - name: transactions
      table: my_db.transactions
      key_field: tx_id
      upsert_mode: true

iceberg:
  warehouse_path: s3://my-bucket/warehouse
  catalog_type: glue  # glue, hive, jdbc, rest
  default_partition_spec:
    - name: year
      transform: year(event_time)
    - name: month
      transform: month(event_time)

buffer:
  hot_ttl_secs: 60
  hot_max_size_mb: 500
  flush_interval_secs: 30
  flush_batch_size: 10000

performance:
  consumer_threads: 4
  writer_threads: 2
  compaction_interval_mins: 60
  compaction_parallelism: 2

monitoring:
  metrics_port: 9090
  log_level: info
```

---

## Implementation Phases

### Phase 1: MVP (Weeks 1-2) - Moonlink Core

**Goal:** Basic Kafka → Iceberg with hot buffer

**Deliverables:**
- ✅ Kafka consumer with batch support
- ✅ Arrow-based hot buffer with hash index
- ✅ Basic Iceberg writer (append-only)
- ✅ Transaction log for crash recovery
- ✅ Offset management

### Phase 2: Production Hardening (Weeks 3-4)

**Goal:** Robustness, monitoring, operational safety

**Added Features:**
- ✅ Comprehensive error recovery
- ✅ Dead letter queue for poison messages
- ✅ Offset checkpoint persistence
- ✅ Prometheus metrics export
- ✅ Graceful shutdown & health checks
- ✅ Schema Registry integration

### Phase 3: Advanced Features (Weeks 5-6)

**Goal:** Parity with pg_mooncake features

**Added Features:**
- ✅ CDC/Upsert support (delete files)
- ✅ Automatic partitioning
- ✅ Background compaction
- ✅ Schema evolution
- ✅ Multiple topic configurations
- ✅ Query-time hybrid read optimization

### Phase 4: Query Layer (Weeks 7-8)

**Goal:** Embedded query capability (optional, advanced)

**Added Features:**
- ⏳ Embedded DuckDB for hot buffer queries
- ⏳ SQL interface for administrative queries
- ⏳ Built-in HTTP query endpoint
- ⏳ Integration with popular BI tools

---

## Comparison: Traditional vs. Moonlink-Inspired Approach

| Aspect | Traditional (Spark/Flink) | **Moonlink-Inspired** |
|--------|--------------------------|----------------------|
| **Latency** | 5-60 minutes | Sub-second (hot), seconds (cold) |
| **Components** | 4-6 (producer, broker, processor, writer, catalog, UI) | **1 (embedded engine)** |
| **Operational Burden** | High (distributed, monitoring, scaling) | **Low (single process)** |
| **Memory Usage** | Entire dataset in Spark | **Recent data only (hot buffer)** |
| **Consistency Model** | Eventually consistent | **Strong consistency (snapshot isolation)** |
| **Query Pattern** | Batch queries | **Hot queries (< 1ms) + analytical (seconds)** |
| **Cost** | Expensive (always-on cluster) | **Cost-optimized (pay for storage)** |
| **Data Freshness** | Delayed | **Real-time** |
| **Failure Recovery** | Complex (reprocess from checkpoint) | **Simple (replay transaction log)** |
| **Storage Format** | Parquet/Delta (after processing) | **Iceberg (immediate)** |

---

## Technology Stack

### Core Libraries

```toml
# Kafka
rdkafka = { version = "0.36", features = ["libz", "ssl"] }

# Iceberg & Arrow
iceberg = { version = "0.5", features = ["io-object-store"] }
arrow = { version = "52.0", features = ["csv", "json"] }
parquet = "52.0"

# Cloud storage
object_store = { version = "0.10", features = ["aws", "azure", "gcp"] }

# Async runtime
tokio = { version = "1", features = ["full"] }

# Data structures
dashmap = "5.5"  # Concurrent hash map for index
bytes = "1.5"    # Efficient byte handling

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
prost = "0.12"   # Protobuf support

# Monitoring
prometheus = "0.13"
tracing = "0.1"
tracing-subscriber = "0.3"

# CLI
clap = { version = "4.5", features = ["derive"] }

# Schema Registry (optional)
schema_registry_converter = "0.2"

# DuckDB (Phase 4)
# duckdb = { version = "0.10", optional = true }
```

---

## Success Metrics

### Performance Targets

| Metric | Target | Moonlink Equivalent |
|--------|--------|-------------------|
| **Write Latency** | < 100ms (buffer) | Postgres replication latency |
| **Query Latency (Hot)** | < 1ms | In-memory Postgres reads |
| **Query Latency (Cold)** | < 5 seconds | Iceberg table scans |
| **Throughput** | 100K+ msgs/sec | Postgres INSERT rate |
| **Memory (Hot)** | 100-500MB | Memtable-sized buffer |
| **Crash Recovery** | < 10 seconds | Transaction log replay |

### Operational Targets

- **Uptime:** 99.9%+ (monitored via health checks)
- **Data Loss:** 0 (transaction log guarantee)
- **Reprocessing:** 0 (offset checkpointing)
- **Schema Evolution:** Automatic (no downtime)
- **Compaction:** Automatic (background)

---

## Moonlink-Specific Design Decisions

### 1. **Why Background Worker Pattern?**

Moonlink embeds in Postgres as a background worker (separate thread, same process). Benefits:

- ✅ No network latency (in-process calls)
- ✅ Shared memory with hot buffer
- ✅ Atomic reads/writes without locking
- ✅ Simple deployment (single binary)
- ✅ Crash recovery via single transaction log

Our Kafka equivalent: Single-process Rust CLI (no distributed complexity).

### 2. **Why Arrow for Hot Buffer?**

Moonlink uses disk-optimized hash tables + Parquet-like indexing. We use Arrow because:

- ✅ **Zero-copy**: Arrow memory can be read directly as Parquet
- ✅ **Vectorized**: SIMD operations on columnar data
- ✅ **Schema evolution**: Built-in handling
- ✅ **Interoperability**: DuckDB, Polars, Spark can read directly
- ✅ **Memory efficiency**: Compress repetitive columns

### 3. **Why Transaction Log?**

Moonlink writes internal WAL (separate from Postgres WAL) for recovery. We replicate this:

- ✅ **Ordering guarantee**: Sequenced log of all state changes
- ✅ **Idempotency**: Replay safely (no double-counts)
- ✅ **Verification**: Detect data corruption on recovery
- ✅ **Audit trail**: Complete history of ingestion

### 4. **Why Snapshot Isolation?**

Iceberg's immutable snapshots enable:

- ✅ **Non-blocking reads**: Old snapshots readable while new data written
- ✅ **Time-travel**: Query data as it existed at point in time
- ✅ **Concurrent writers**: Multiple flush threads safe (optimistic locking)

---

## Open Questions & Future Directions

1. **Query Layer**: Should we embed DuckDB for SQL queries on hot data?
   - Moonlink answer: No (use Postgres queries on Iceberg view)
   - Our answer: TBD (optional Phase 4)

2. **Replication**: Multi-instance high availability?
   - Moonlink answer: Not in v0.1 (single instance per table)
   - Our answer: Plan for distributed checkpoints (shared S3 state)

3. **Ordering Guarantees**: Per-key ordering like Kafka partitions?
   - Moonlink answer: Yes (WAL preserves order)
   - Our answer: Use Kafka partition → Iceberg partition mapping

4. **Cost Model**: Should we optimize for S3 request costs?
   - Moonlink answer: Yes (S3 ListObjects optimization)
   - Our answer: Batch metadata updates, compress manifests

---

## Conclusion

This updated PRD combines:

1. **Kafka's durability & scalability** (distributed, partitioned topics)
2. **Moonlink's elegance** (single process, in-memory hot buffer, transaction log)
3. **Iceberg's analytics power** (columnstore, snapshots, schema evolution)
4. **Rust's performance** (no GC, zero-cost abstractions, concurrency)

The result is a **minimal, fast, reliable Kafka → Iceberg ingestion engine** that solves the latency/cost/complexity tradeoff.

---

## References

- [pg_mooncake Architecture](https://docs.mooncake.dev/intro/architecture)
- [Moonlink Real-time Storage Engine](https://mooncake.dev/whitepaper)
- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [Arrow Data Format](https://arrow.apache.org/)
- [Kafka Protocol](https://kafka.apache.org/protocol.html)
