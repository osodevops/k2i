# Moonlink-to-Kafka: Deep Architectural Analysis

## Executive Summary

pg_mooncake's **Moonlink engine** is a high-performance ingestion layer that replicates PostgreSQL tables into Apache Iceberg with sub-second latency. By studying its architecture, we can build an equivalent for **Kafka → Iceberg streaming**.

---

## Part 1: pg_mooncake Architecture (Reference Implementation)

### High-Level Stack

```
┌─────────────────────────────────────────────────────────┐
│ PostgreSQL (Operational Database)                       │
└─────────────────────────────────────────────────────────┘
    │
    │ Logical Replication Stream
    │ (WAL - Write-Ahead Log)
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ pg_mooncake Extension (Postgres Process)                │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐│
│  │ Moonlink (Background Worker Thread)                 ││
│  ├─────────────────────────────────────────────────────┤│
│  │ • Logical Replication Slot Consumer                ││
│  │ • In-Memory Hash Tables (recent writes)            ││
│  │ • Transaction Log (Moonlink WAL)                   ││
│  │ • Parquet Encoder (for hot buffer)                 ││
│  │ • Iceberg Metadata Manager                         ││
│  └─────────────────────────────────────────────────────┘│
│                       │                                  │
│         ┌─────────────┼─────────────┐                   │
│         │             │             │                   │
│         ▼             ▼             ▼                   │
│  ┌────────────┐  ┌──────────┐  ┌────────────┐          │
│  │ Hot Buffer │  │   Index  │  │ Trans Log  │          │
│  │ (Arrow mem)│  │  (Hash)  │  │  (Local)   │          │
│  └────────────┘  └──────────┘  └────────────┘          │
│                                                          │
│  ┌─────────────────────────────────────────────────────┐│
│  │ DuckDB (Vectorized Query Engine)                    ││
│  │ • Reads from hot buffer                             ││
│  │ • Reads from Iceberg (parquet files)                ││
│  │ • Returns results to Postgres                       ││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
    │
    └──→ S3/Cloud Storage
         • Parquet files (data)
         • Iceberg metadata (snapshots, manifests)
```

### Moonlink's Key Design Patterns

#### 1. **Dual-Path Architecture**

**Moonlink listens to TWO streams:**

```
PostgreSQL WAL (Logical Replication)
    │
    ├─ Old writes (committed)
    │  └─→ In-memory index
    │      └─→ Match with hot buffer entries
    │
    └─ New writes (real-time)
       └─→ Append to hot buffer
           └─→ Update index
               └─→ Maybe flush to Iceberg
```

**Key insight:** The hot buffer acts like a **memtable** in LSM trees (Log-Structured Merge trees). Recent writes stay in memory, old writes pushed to Iceberg.

#### 2. **Transaction Log (Moonlink's own WAL)**

Moonlink writes a **separate transaction log** (not Postgres WAL), which tracks:

```
Timeline of internal operations:

T0: 
[TxnLog Entry] "OffsetMarker: Postgres LSN=123456"
[Buffer Action] "Insert 1000 rows into hot buffer"
[Index Action]  "Update hash index for 1000 keys"

T1:
[TxnLog Entry] "FlushStart: buffer_id=batch_001"
[Buffer Action] "Read hot buffer (1000 rows)"
[Encode Action] "Convert to Parquet: batch_001.parquet"
[Upload Action] "Write to S3"

T2:
[TxnLog Entry] "IcebergSnapshot: snapshot_id=456789"
[Metadata Action] "Update manifest list"
[Index Action]  "Mark old entries for eviction"
[TxnLog Entry] "FlushComplete: batch_id=batch_001"

Recovery on crash:
→ Read TxnLog from last checkpoint
→ Verify S3 files exist for completed flushes
→ Rebuild hot buffer state
→ Resume from last LSN marker
```

**This is brilliant because:**
- ✅ Minimal overhead (append-only log)
- ✅ Enables exact crash recovery without re-reading Postgres WAL
- ✅ Allows detecting data loss (corrupt files detected at next read)
- ✅ Enables point-in-time recovery by snapshot

#### 3. **Hot Buffer with Disk-Optimized Index**

Moonlink's hot buffer uses a **dictionary-encoded columnar format** similar to Parquet, with a **hash table for fast seeks**.

```
Hot Buffer Memory Layout:

┌──────────────────────────────────────┐
│ Column 1 (user_id)                   │
│ Values: [1001, 1002, 1003, ...]      │
│ Dictionary: {1001→0, 1002→1, ...}    │
├──────────────────────────────────────┤
│ Column 2 (event_type)                │
│ Values: ["click", "purchase", ...]   │
│ Dictionary: {"click"→0, ...}         │
├──────────────────────────────────────┤
│ Column 3 (timestamp)                 │
│ Values: [T1, T2, T3, ...]            │
├──────────────────────────────────────┤
│ Hash Index                           │
│ Key → Row Offset mapping             │
│ user_id=1001 → row_offset=0          │
│ user_id=1002 → row_offset=1          │
└──────────────────────────────────────┘
```

**Benefits:**
- Dictionary encoding reduces memory (repeated values stored once)
- Columnar format enables vectorized operations
- Hash index enables O(1) point lookups on recent data
- Zero-copy conversion to Parquet (same format in memory)

#### 4. **Snapshot Isolation (Like MVCC in Postgres)**

Each Iceberg snapshot is **immutable**. Moonlink maintains:

- **Read snapshot**: Points to stable Iceberg version
- **Write snapshot**: Points to next version being built
- **Hot buffer snapshot**: Recent changes not yet in Iceberg

Readers always see consistent snapshot; writers don't block readers.

```
Timeline of snapshots:

Time T0:
- Snapshot V1 (in Iceberg): rows 1-1000
- Hot buffer: (empty)

Time T1:
- Snapshot V1 (in Iceberg): rows 1-1000
- Hot buffer: rows 1001-1500 (not visible to readers yet)

Time T2:
- Snapshot V1 (in Iceberg): rows 1-1000  ← Readers still use V1
- Hot buffer: rows 1001-1500 (staging for V2)

Time T3 (flush):
- Snapshot V2 (in Iceberg): rows 1-1500  ← Created, but V1 still readable
- Hot buffer: (cleared)

Time T4 (next write):
- Snapshot V1 (in Iceberg): rows 1-1000  ← Old, can be deleted
- Snapshot V2 (in Iceberg): rows 1-1500  ← Current, readers use this
- Hot buffer: rows 1501-2000
```

---

## Part 2: Adapting Moonlink Pattern for Kafka

### Mapping Concepts

| PostgreSQL (Moonlink) | Kafka (Our Implementation) | Key Difference |
|----------------------|---------------------------|-----------------|
| **Logical Replication Stream (WAL)** | **Kafka Consumer Stream** | Kafka offset = PostgreSQL LSN |
| **Postgres transaction boundary** | **Kafka message / batch** | Kafka has explicit offsets, Postgres has continuous stream |
| **Replication Slot** | **Consumer group + offset tracking** | Similar purpose (track progress) |
| **Hot buffer (in Moonlink)** | **Arrow RecordBatch (in hot buffer)** | Both columnar, in-memory |
| **Parquet flush** | **Parquet flush** | Identical |
| **Iceberg snapshot** | **Iceberg snapshot** | Identical |
| **Transaction log (Moonlink WAL)** | **Transaction log (durability log)** | Same function, slightly different format |

### Key Adaptation Points

#### 1. **Consumer as Replication Slot Equivalent**

```rust
// Moonlink: Postgres logical replication slot
// LSN (Log Sequence Number) = byte offset in WAL
// Example LSN: 0/1A234567 (hex format)

// Our Kafka equivalent:
struct ConsumerState {
    topic: String,
    partition: i32,
    offset: u64,  // Like LSN, but simpler (decimal)
    timestamp: DateTime,
}

// Moonlink behavior: "Consume from LSN X, acknowledge only after flush"
// Our behavior: "Consume from offset X, commit only after flush to transaction log"
```

**Key insight:** Like Moonlink's replication slot (which pauses if consumer can't keep up), Kafka consumer group pauses if we can't flush fast enough.

#### 2. **Batch Semantics**

Moonlink consumes **continuous stream** but treats it in **batches** internally.

```
Moonlink's implicit batching:
TIME → "Transaction 1" → "Transaction 2" → "Transaction 3" → "Flush!"
       └─ Accumulate in hot buffer ─────────────────────────────┘

Kafka's explicit batching:
Partition 0 offsets: [0, 1, 2, 3, 4, 5]
Poll call 1: Get messages [0-4], size ~10MB
Poll call 2: Get messages [5-10], size ~10MB
Each poll = explicit "batch"
```

**Our adaptation:**

```rust
struct KafkaConsumer {
    rdkafka_consumer: Consumer<...>,
}

impl KafkaConsumer {
    async fn consume_batch(&mut self) -> Batch {
        let mut batch = Batch::new();
        
        // Kafka poll with timeout
        while let Some(msg) = self.rdkafka_consumer.poll(timeout) {
            batch.add(msg);
            
            // Flush when batch full
            if batch.size_bytes() > 10_000_000 {
                break;
            }
        }
        
        // Return batch + metadata
        batch.with_metadata(BatchMetadata {
            min_offset: batch.first().offset(),
            max_offset: batch.last().offset(),
            timestamp: batch.last().timestamp(),
        })
    }
}
```

#### 3. **Hot Buffer Management**

```rust
// Moonlink's approach (tuned for PostgreSQL):
// • Keep last N MB of writes in hot buffer
// • Based on transaction volume, not time
// • Evict via LRU when full

// Our approach (adapted for Kafka):
// • Keep last K seconds of messages (configurable, default 60s)
// • Also evict if size > M MB (default 500MB)
// • Combined policy: min(by_time, by_size)

struct HotBufferPolicy {
    ttl_seconds: u64,           // 60 seconds default
    max_size_mb: usize,         // 500 MB default
    eviction_interval_ms: u64,  // 5 seconds default
}

impl HotBufferManager {
    async fn evict_old_records(&mut self) {
        let now = Instant::now();
        
        // Evict records older than TTL
        self.records.retain(|record| {
            now.duration_since(record.timestamp()) 
                < Duration::from_secs(self.policy.ttl_seconds)
        });
        
        // If still over size, evict oldest
        while self.current_size_bytes > self.policy.max_size_mb * 1_000_000 {
            self.records.remove(0); // FIFO eviction
        }
    }
}
```

#### 4. **Transaction Log Format**

Moonlink's transaction log is **internal format** (not standardized). For Kafka, we can use similar pattern:

```rust
// Moonlink's approach (implementation detail):
// Writes a binary log with entries like:
// • [1 byte: type] [4 bytes: LSN] [4 bytes: size] [N bytes: data]

// Our approach (JSONL for observability, binary for performance):
enum TransactionLogEntry {
    // Phase 1: Kafka acknowledgment
    OffsetCommit {
        offset: u64,
        timestamp: i64,
        record_count: u32,
    },
    
    // Phase 2: Buffer update
    BufferSnapshot {
        row_count: u32,
        memory_usage_bytes: u64,
        schema_version: u32,
        min_timestamp: i64,
        max_timestamp: i64,
    },
    
    // Phase 3: Flush started
    FlushStart {
        batch_id: String,
        buffer_row_count: u32,
    },
    
    // Phase 4: Parquet written
    ParquetWritten {
        batch_id: String,
        file_path: String,
        row_count: u32,
        file_size_bytes: u64,
    },
    
    // Phase 5: Iceberg metadata updated
    IcebergSnapshotCreated {
        snapshot_id: u64,
        manifest_list_path: String,
        row_count_total: u64,
        timestamp: i64,
    },
    
    // Phase 6: Buffer cleared
    FlushComplete {
        batch_id: String,
        duration_ms: u64,
    },
}

// Write to transaction log:
transaction_log.append(entry);

// Recovery:
for entry in transaction_log.read_from_checkpoint() {
    match entry {
        TransactionLogEntry::OffsetCommit { offset, .. } => {
            state.last_committed_offset = offset;
        }
        TransactionLogEntry::IcebergSnapshotCreated { snapshot_id, .. } => {
            state.last_iceberg_snapshot = snapshot_id;
        }
        // ... verify consistency, detect corruption
    }
}
```

---

## Part 3: Detailed Component Design

### Component 1: Smart Consumer with Backpressure

**Moonlink equivalent:** Postgres logical replication slot (can pause if hot buffer full)

```rust
pub struct SmartKafkaConsumer {
    // Kafka connection
    rdkafka_consumer: Arc<Consumer<DefaultConsumerContext>>,
    
    // Backpressure mechanism
    hot_buffer: Arc<HotBufferManager>,
    
    // State tracking (like replication slot position)
    state: Arc<RwLock<ConsumerState>>,
}

impl SmartKafkaConsumer {
    pub async fn consume_loop(&mut self) {
        loop {
            // Check if hot buffer has capacity
            if self.hot_buffer.is_full() {
                // Backpressure: pause consumption
                // (Like Postgres replication slot blocking writer)
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            
            // Poll from Kafka (respects timeout)
            match self.rdkafka_consumer.poll(timeout) {
                Some(Ok(msg)) => {
                    // Convert Kafka message to Record
                    let record = Record::from_kafka_msg(&msg);
                    
                    // Append to hot buffer (transactional)
                    self.hot_buffer.append(record).await?;
                    
                    // Update state (offset tracking)
                    self.state.write().await.offset = msg.offset();
                    self.state.write().await.timestamp = Instant::now();
                }
                Some(Err(e)) => {
                    error!("Kafka error: {}", e);
                    // Handle errors (reconnect, etc.)
                }
                None => {
                    // Timeout: maybe flush partial buffer
                    if self.hot_buffer.should_flush_on_timeout() {
                        self.hot_buffer.flush().await?;
                    }
                }
            }
        }
    }
}
```

### Component 2: Arrow-Based Hot Buffer

**Moonlink equivalent:** In-memory hash tables + Parquet-like encoding

```rust
pub struct HotBufferManager {
    // Current Arrow RecordBatch
    current_batch: Arc<Mutex<RecordBatch>>,
    
    // Hash index for fast lookups
    primary_key_index: Arc<DashMap<Vec<u8>, RowId>>,
    
    // Metadata
    metadata: Arc<RwLock<BufferMetadata>>,
    
    // Configuration
    config: HotBufferConfig,
}

#[derive(Clone)]
struct BufferMetadata {
    row_count: u64,
    memory_bytes: u64,
    min_timestamp: i64,
    max_timestamp: i64,
    schema_version: u32,
    created_at: Instant,
}

impl HotBufferManager {
    pub async fn append(&self, record: Record) -> Result<()> {
        // 1. Encode record to Arrow
        let arrow_value = Record::to_arrow(&record);
        
        // 2. Add to current batch
        let mut batch = self.current_batch.lock().await;
        let row_id = batch.add_row(arrow_value)?;
        
        // 3. Update hash index
        self.primary_key_index.insert(
            record.primary_key().to_vec(),
            RowId(row_id),
        );
        
        // 4. Update metadata
        let mut meta = self.metadata.write().await;
        meta.row_count += 1;
        meta.memory_bytes = batch.get_array_memory_size();
        meta.max_timestamp = record.timestamp;
        
        // 5. Check eviction policy
        self.maybe_evict().await?;
        
        // 6. Check flush triggers
        if self.should_flush() {
            drop(batch); // Release lock before flush
            self.flush().await?;
        }
        
        Ok(())
    }
    
    pub async fn query_point(&self, key: &[u8]) -> Option<Record> {
        // Fast hash lookup on hot data
        if let Some(row_id) = self.primary_key_index.get(key) {
            let batch = self.current_batch.lock().await;
            return batch.get_row(row_id.value().0);
        }
        None
    }
    
    async fn maybe_evict(&self) -> Result<()> {
        let meta = self.metadata.read().await;
        
        // Check time-based eviction
        let age = meta.created_at.elapsed();
        if age > Duration::from_secs(self.config.ttl_seconds) {
            drop(meta);
            self.evict_by_time().await?;
        }
        
        // Check size-based eviction
        if meta.memory_bytes > self.config.max_size_bytes {
            drop(meta);
            self.evict_by_size().await?;
        }
        
        Ok(())
    }
    
    async fn flush(&self) -> Result<()> {
        let mut batch = self.current_batch.lock().await;
        
        // Snapshot batch
        let batch_snapshot = batch.clone();
        
        // Clear for next batch
        batch.clear();
        self.primary_key_index.clear();
        
        // Send to writer (don't hold lock)
        drop(batch);
        
        self.writer.write_batch(batch_snapshot).await?;
        
        Ok(())
    }
}
```

### Component 3: Iceberg Writer with Atomic Commits

**Moonlink equivalent:** Atomic metadata updates to Iceberg manifest

```rust
pub struct IcebergWriter {
    // Iceberg table reference
    table: Arc<Table>,
    
    // Parquet writer
    parquet_writer: Arc<ParquetWriter>,
    
    // Cloud storage
    object_store: Arc<dyn ObjectStore>,
    
    // Transaction log for durability
    transaction_log: Arc<TransactionLog>,
}

impl IcebergWriter {
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<()> {
        let batch_id = Uuid::new_v4().to_string();
        
        // Step 1: Write to transaction log (flush start)
        self.transaction_log.append(
            TransactionLogEntry::FlushStart {
                batch_id: batch_id.clone(),
                buffer_row_count: batch.num_rows() as u32,
            }
        ).await?;
        
        // Step 2: Convert Arrow to Parquet
        let parquet_data = self.parquet_writer.write(&batch).await?;
        
        // Step 3: Upload to cloud storage
        let file_path = self.compute_file_path(&batch, &batch_id);
        self.object_store.put(&file_path, parquet_data.clone()).await?;
        
        // Step 4: Write to transaction log (parquet written)
        self.transaction_log.append(
            TransactionLogEntry::ParquetWritten {
                batch_id: batch_id.clone(),
                file_path: file_path.clone(),
                row_count: batch.num_rows() as u32,
                file_size_bytes: parquet_data.len() as u64,
            }
        ).await?;
        
        // Step 5: Create Iceberg manifest entry
        let manifest_entry = ManifestEntry {
            file_path: file_path.clone(),
            file_format: FileFormat::Parquet,
            row_count: batch.num_rows() as u64,
            file_size: parquet_data.len() as u64,
            spec_id: 0,
        };
        
        // Step 6: Atomic commit to Iceberg
        // (This uses Compare-And-Swap semantics)
        let new_snapshot = self.table
            .append_file(manifest_entry)
            .await
            .map_err(|e| {
                // If commit fails, file is orphaned (OK, GC will clean)
                // But transaction log has record, so we know it happened
                error!("Iceberg commit failed: {}", e);
                e
            })?;
        
        // Step 7: Write to transaction log (snapshot created)
        self.transaction_log.append(
            TransactionLogEntry::IcebergSnapshotCreated {
                snapshot_id: new_snapshot.snapshot_id(),
                manifest_list_path: new_snapshot.manifest_list_path().to_string(),
                row_count_total: self.table.current_snapshot().row_count(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            }
        ).await?;
        
        // Step 8: Write to transaction log (flush complete)
        self.transaction_log.append(
            TransactionLogEntry::FlushComplete {
                batch_id,
                duration_ms: start.elapsed().as_millis() as u64,
            }
        ).await?;
        
        Ok(())
    }
    
    fn compute_file_path(&self, batch: &RecordBatch, batch_id: &str) -> String {
        // Extract timestamp from batch for partitioning
        let min_timestamp = batch.column(TIMESTAMP_COLUMN_ID)
            .as_primitive::<TimestampMillisecondType>()
            .min()
            .unwrap();
        
        let date = DateTime::<Utc>::from_timestamp_millis(min_timestamp)
            .format("%Y/%m/%d");
        
        format!(
            "data/{}/batch-{}.parquet",
            date, batch_id
        )
    }
}
```

### Component 4: Transaction Log Recovery

```rust
pub struct TransactionLog {
    storage: Arc<dyn TransactionLogStorage>,
}

pub struct RecoveryState {
    pub last_committed_offset: u64,
    pub last_iceberg_snapshot: u64,
    pub pending_batches: Vec<PendingBatch>,
    pub verified: bool,
}

pub struct PendingBatch {
    pub batch_id: String,
    pub file_path: String,
    pub iceberg_snapshot: Option<u64>,
}

impl TransactionLog {
    pub async fn recover(&self) -> Result<RecoveryState> {
        let mut state = RecoveryState::default();
        
        // Read all entries from transaction log
        let entries = self.storage.read_all().await?;
        
        for entry in entries {
            match entry {
                TransactionLogEntry::OffsetCommit { offset, .. } => {
                    state.last_committed_offset = offset;
                }
                
                TransactionLogEntry::FlushStart { batch_id, .. } => {
                    state.pending_batches.push(PendingBatch {
                        batch_id,
                        file_path: String::new(),
                        iceberg_snapshot: None,
                    });
                }
                
                TransactionLogEntry::ParquetWritten { batch_id, file_path, .. } => {
                    if let Some(pending) = state.pending_batches.iter_mut()
                        .find(|b| b.batch_id == batch_id) {
                        pending.file_path = file_path;
                    }
                }
                
                TransactionLogEntry::IcebergSnapshotCreated { 
                    snapshot_id, 
                    .. 
                } => {
                    // Most recent entry wins
                    state.last_iceberg_snapshot = snapshot_id;
                    
                    // Mark all pending batches as committed
                    for pending in &mut state.pending_batches {
                        pending.iceberg_snapshot = Some(snapshot_id);
                    }
                }
                
                TransactionLogEntry::FlushComplete { batch_id, .. } => {
                    // Remove from pending (it's done)
                    state.pending_batches.retain(|b| b.batch_id != batch_id);
                }
                
                _ => {}
            }
        }
        
        // Verify consistency
        state.verified = self.verify_consistency(&state).await?;
        
        Ok(state)
    }
    
    async fn verify_consistency(&self, state: &RecoveryState) -> Result<bool> {
        // Check that all files in transaction log exist in S3
        for pending in &state.pending_batches {
            if let Ok(exists) = self.object_store.exists(&pending.file_path).await {
                if !exists {
                    warn!(
                        "Orphaned file in transaction log: {}",
                        pending.file_path
                    );
                }
            }
        }
        
        // Check that Iceberg snapshot matches
        let iceberg_snapshot = self.table.current_snapshot().snapshot_id();
        if iceberg_snapshot != state.last_iceberg_snapshot {
            warn!(
                "Snapshot mismatch: txlog={}, iceberg={}",
                state.last_iceberg_snapshot, iceberg_snapshot
            );
        }
        
        Ok(true)
    }
}
```

---

## Part 4: Running Example - Complete Data Flow

### Scenario: Processing E-commerce Events

```
Input: Kafka topic "ecommerce.events"
  {event_id: 1001, user_id: 101, event_type: "view", product_id: 5001, timestamp: T1}
  {event_id: 1002, user_id: 102, event_type: "click", product_id: 5001, timestamp: T2}
  {event_id: 1003, user_id: 101, event_type: "purchase", product_id: 5001, timestamp: T3}
  ... (1000 more events)

Time T0 (Message arrives):
├─ Consumer polls Kafka
├─ Batch contains 1000 messages, offset range [1000-1999]
├─ Hot buffer append loop:
│  ├─ Event 1001 → Arrow encoding → Hash index insert
│  ├─ Event 1002 → Arrow encoding → Hash index insert
│  ├─ ... 998 more ...
│  └─ Event 1999 → Arrow encoding → Hash index insert
│
├─ Transaction log: OffsetCommit { offset: 1999 }
└─ Hot buffer state:
   • row_count: 1000
   • memory_bytes: ~2MB (compressed columns)
   • min_timestamp: T1
   • max_timestamp: T999
   • in hot buffer: ALL 1000 rows

Time T1 (60 seconds later, hot buffer full):
├─ Flush trigger: 60 seconds elapsed
├─ Hot buffer lock acquired
│  ├─ Snapshot current Arrow batch
│  ├─ Clear hash index
│  ├─ Clear row storage
│  └─ Release lock (fast, no long wait)
│
├─ Writer begins async flush:
│  ├─ Convert Arrow → Parquet (batch_001.parquet, 1.8MB)
│  ├─ Upload to S3: s3://warehouse/data/2024/01/15/batch-001.parquet
│  ├─ Transaction log: ParquetWritten { batch_id: "batch_001", ... }
│  ├─ Create Iceberg manifest entry
│  ├─ Commit to Iceberg (atomic):
│  │  └─ Old snapshot: V1 (rows 1-1000)
│  │  └─ New snapshot: V2 (rows 1-2000)
│  └─ Transaction log: IcebergSnapshotCreated { snapshot_id: V2, ... }

Time T2 (Query arrives during flush):
├─ User queries: "Get events for user_id=101"
├─ Query planner checks: Is this recent?
│  ├─ Event 1001 (T1) is now in Iceberg (V2)
│  └─ Events 2000+ are in hot buffer
│
├─ Execution:
│  ├─ Query hot buffer: Events 2000-2100 for user_id=101
│  │  └─ Hash index lookup: O(1)
│  │  └─ Returns: [event 2001, 2050, ...]
│  │
│  └─ Query Iceberg (V2): Events 1-2000 for user_id=101
│     └─ DuckDB scan of parquet files
│     └─ Returns: [event 1001, 1500, ...]
│
└─ Result: All events merged, sorted by timestamp (consistent snapshot!)

Time T3 (Crash happens):
├─ Process dies unexpectedly
└─ All data safe because:
   • Events 1-1000: Flushed to Iceberg ✓
   • Events 1001-2000: In transaction log ✓
   • Hot buffer: Lost but can recover from transaction log

Time T4 (Restart):
├─ Read transaction log:
│  ├─ Identify last committed offset (1999)
│  ├─ Verify Iceberg metadata matches
│  ├─ No pending batches (FlushComplete was logged)
│
├─ Resume consumption from offset 2000
├─ Hot buffer rebuilt as new messages arrive
└─ Service restored: No data loss, No reprocessing
```

---

## Part 5: Moonlink vs Traditional Streaming

### Comparison Matrix

| Aspect | Spark Streaming | Flink | Moonlink | Our Kafka Version |
|--------|-----------------|-------|----------|------------------|
| **Architecture** | Multiple nodes | Multiple nodes | Single process | Single process |
| **Ingestion latency** | 500ms - 5s | 100ms - 1s | 10-100ms | 10-100ms |
| **Hot data locality** | Shuffle to nodes | Shuffle to nodes | In-memory hash | In-memory Arrow |
| **Query freshness** | 5-60 minutes | 100ms - 5s | < 100ms | < 100ms |
| **Storage format** | Delta/Parquet | Kafka/Delta | Iceberg | Iceberg |
| **Memory usage** | 10-100GB | 10-100GB | 100-500MB | 100-500MB |
| **Operational complexity** | High | High | Low | Low |
| **Cost** | $$$$ (cluster) | $$$ (cluster) | $ (storage) | $ (storage) |
| **Query tool support** | Spark, DW | SQL, DW | Any Iceberg reader | Any Iceberg reader |
| **Failure recovery** | Reprocess from checkpoint | Reprocess from checkpoint | Point-in-time recovery | Point-in-time recovery |
| **Suitable for** | Batch analytics | Streaming analytics | Operational analytics | Operational analytics |

---

## Part 6: Design Decisions & Trade-offs

### Decision 1: Single Process vs Distributed

**Moonlink:** Single process, embedded in PostgreSQL
**Our choice:** Single process Rust CLI

**Rationale:**
- Simplicity: No distributed consensus needed
- Latency: No network roundtrips for hot queries
- Cost: No need for multiple machines
- For HA: Run multiple instances with shared offset management (future)

**Trade-off:** Can't scale beyond single machine (1-2M msg/sec throughput)

### Decision 2: Arrow for Hot Buffer

**Moonlink:** Uses Parquet-like columnar format
**Our choice:** Apache Arrow (same idea, standardized)

**Rationale:**
- Zero-copy: Arrow memory can be directly converted to Parquet
- Vectorized: SIMD operations on columnar data
- Interop: DuckDB, Spark can read directly
- Memory: Dictionary encoding + compression

**Trade-off:** More memory overhead than custom format, but worth it for features

### Decision 3: Immutable Iceberg Only

**Moonlink:** Supports both reads from hot + Iceberg
**Our choice:** Two-path query (hot buffer OR Iceberg)

**Rationale:**
- Consistency: Same snapshot version
- Simplicity: No merge logic needed
- Correctness: No stale reads possible

**Trade-off:** Slightly more latency for cold queries (need to merge results)

---

## Conclusion

Moonlink's design is elegant because it:

1. **Embeds in the database** (no separate cluster)
2. **Buffers in-memory** (sub-second freshness)
3. **Flushes asynchronously** (no blocking writes)
4. **Uses transaction log** (crash recovery)
5. **Maintains snapshots** (consistent reads)
6. **Writes immutable files** (no overwrites)

By adapting these patterns for Kafka → Iceberg, we get:

- ✅ Sub-second ingestion latency
- ✅ Cost-optimized cold storage (Iceberg)
- ✅ Simple operations (single process)
- ✅ Crash recovery without reprocessing
- ✅ Query-time freshness (hot + cold hybrid)

This is the missing piece between Kafka's durability and Iceberg's analytics power.
