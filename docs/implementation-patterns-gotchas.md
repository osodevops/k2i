# Kafka → Iceberg Implementation: Critical Patterns & Gotchas

## Part 1: Lessons from pg_mooncake Implementation

### Gotcha 1: Offset Commit Timing is Critical

**The Problem:**

Moonlink commits to Postgres's replication slot AFTER persisting writes. But Kafka's semantics are different.

```rust
// ❌ WRONG: Commit immediately after buffer append
async fn bad_consume() {
    let msg = consumer.poll(...)?;
    hot_buffer.append(&msg)?;  // Append to hot buffer
    consumer.commit_sync()?;     // Commit immediately!
    // If crash HERE: offset committed but hot buffer lost
}

// ✅ CORRECT: Commit only after transaction log write
async fn good_consume() {
    let msg = consumer.poll(...)?;
    
    // Step 1: Add to hot buffer
    hot_buffer.append(&msg)?;
    
    // Step 2: Write to durable transaction log
    txn_log.append(
        TransactionEntry::OffsetCommit { offset: msg.offset() }
    ).await?;
    
    // Step 3: THEN commit to Kafka
    consumer.commit_async();  // Fire and forget, we have txn log
}
```

**Why it matters:**

```
Timeline of bad approach:

T0: Message arrives in Kafka offset 100
T1: Append to hot buffer ✓
T2: Commit offset 100 to Kafka ✓
T3: CRASH! Process dies
T4: Restart, recovery
    • Kafka says: "Last offset 100" 
    • Hot buffer: Empty (lost in crash)
    • Result: MISSING DATA! (offset 100 never made it to Iceberg)

Timeline of good approach:

T0: Message arrives in Kafka offset 100
T1: Append to hot buffer ✓
T2: Append to transaction log ✓ (durable on disk)
T3: Commit offset 100 to Kafka
T4: CRASH! Process dies
T5: Restart, recovery
    • Read transaction log
    • Find: "OffsetCommit: 100"
    • If hot buffer empty: Refill from transaction log
    • Result: Data safe!
```

### Gotcha 2: Arrow Memory Management & Alignment

**The Problem:**

Apache Arrow requires **64-byte memory alignment** for SIMD operations. Buffer pool misalignment causes segfaults.

```rust
// ❌ WRONG: Creating Arrow arrays without proper allocation
async fn bad_arrow_usage() {
    let values: Vec<i64> = vec![1, 2, 3, ...];
    
    // This doesn't allocate with proper alignment!
    let array = Int64Array::from(values);
    
    // Later, DuckDB tries SIMD operations → SEGFAULT
    duckdb_execute("SELECT SUM(...) FROM ...").await?;
}

// ✅ CORRECT: Use Arrow's MemoryPool
async fn good_arrow_usage() {
    use arrow::memory_pool::MemoryPool;
    
    let memory_pool = arrow::memory_pool::system::SYSTEM;
    
    // Allocate with proper alignment
    let capacity_bytes = 1_000_000;
    let mut buffer = memory_pool.allocate(
        capacity_bytes,
        64,  // 64-byte alignment required
    )?;
    
    // Now safe for SIMD operations
}

// ✅ BETTER: Use RecordBatchBuilder (handles alignment)
async fn best_arrow_usage() {
    use arrow::record_batch::RecordBatchBuilder;
    
    let mut batch_builder = RecordBatchBuilder::new();
    
    // Add fields with proper alignment
    batch_builder
        .field("event_id", Field::new("event_id", DataType::Int64, false))
        .field("user_id", Field::new("user_id", DataType::Int64, false));
    
    // Build handles alignment automatically
    let batch = batch_builder.build()?;
    
    // Safe to use with DuckDB, etc.
}
```

### Gotcha 3: Iceberg Manifest List Size Explosion

**The Problem:**

Moonlink discovered that Iceberg's manifest list can grow unboundedly with small flushes.

```
Timeline of manifest growth:

Flush 1:  1 file  → manifest list: 10 KB
Flush 2:  2 files → manifest list: 20 KB
Flush 3:  3 files → manifest list: 30 KB
...
Flush 100: 100 files → manifest list: 1 MB

Problem: List operations (scan, compact) become O(n) in file count!
```

**Solution 1: Larger Batch Sizes**

```rust
// Batch config: Aim for fewer, larger files
struct FlushConfig {
    // Write larger batches (10K+ records)
    batch_size: usize,
    
    // Wait longer between flushes (60+ seconds)
    flush_interval: Duration,
    
    // Target file size: 128-512MB in Iceberg
    target_parquet_size_mb: usize,
}

// Example: 1M events/sec = 1000 events/ms
// Batch 100K records = 100ms latency (acceptable)
// Write ~100KB Parquet file (too small!)

// Better: Batch 1M records = 1s latency
// Write ~1MB Parquet file (better for manifest list)
```

### Gotcha 4: Schema Evolution with Arrow <-> Iceberg

**The Problem:**

Arrow and Iceberg have different schema semantics:

```
Arrow:
- Schemas are positional (column 0, 1, 2)
- Adding columns changes position of all fields
- NO backward compatibility by default

Iceberg:
- Schemas are named, with IDs
- Can add/remove/reorder fields safely
- Strong backward compatibility
```

**Correct approach:**

```rust
pub struct SchemaManager {
    iceberg_table: Arc<Table>,
    current_version: u32,
}

impl SchemaManager {
    pub async fn evolve_schema(&mut self, new_fields: Vec<Field>) -> Result<()> {
        // Step 1: Create new Iceberg schema
        let old_schema = self.iceberg_table.schema();
        let mut evolved_schema = old_schema.clone();
        
        for field in new_fields {
            // Use Iceberg's schema evolution API
            // This assigns proper field IDs!
            evolved_schema.add_field(field)?;
        }
        
        // Step 2: Update Iceberg table schema
        // Iceberg handles backward compatibility
        self.iceberg_table.update_schema(&evolved_schema).await?;
        
        // Step 3: Arrow schema for NEW records
        self.arrow_schema = evolved_schema.to_arrow_schema();
        
        // Step 4: Old records still work because Iceberg maps by field ID
        // Not by position!
        
        self.current_version += 1;
        
        Ok(())
    }
}

// Example evolution:
// V1: {user_id (ID=1), event_type (ID=2)}
// V2: {user_id (ID=1), event_type (ID=2), properties (ID=3)} + nullable
// V3: {user_id (ID=1), timestamp (ID=4), event_type (ID=2), properties (ID=3)}
//      Note: timestamp inserted in middle, but by ID not position!

// When reading old Parquet from V1:
// Iceberg: "Map ID=1 → user_id, ID=2 → event_type, skip ID=3 (null)"
// Works perfectly!
```

### Gotcha 5: Kafka Consumer Group Rebalancing

**The Problem:**

Kafka can cause **rebalancing storms** if consumer is slow or if heartbeats stop.

```
Timeline of bad rebalancing:

T0: Consumer starts, joins group
T1: Hot buffer filling, flush in progress (SLOW)
T2: Kafka: "Consumer timeout, didn't fetch message"
T3: Rebalance triggered!
    • All consumers pause
    • Offsets reassigned
    • Resume consuming
T4: More flushes happen, network slow
T5: Rebalance again!
T6: Rebalance loop until service stable

Problem: Each rebalance = ~5-10 seconds of NO DATA flowing!
```

**Solution: Configure Heartbeats Properly**

```rust
pub struct ConsumerConfig {
    // Session timeout: How long without heartbeat before considered dead
    // Moonlink equivalent: Postgres backend timeout
    session_timeout_ms: u32,  // Default: 10s, Increase to 30s
    
    // Max poll interval: How long between polls before considered dead
    // If flush takes > this time → considered dead → rebalance
    max_poll_interval_ms: u32,  // Default: 5 min, OK for us
    
    // Heartbeat interval: How often to send "I'm alive"
    // Send MORE frequently if doing long flushes
    heartbeat_interval_ms: u32,  // Default: 3s, Good
}

pub struct SmartKafkaConsumer {
    consumer: Consumer<DefaultConsumerContext>,
    
    // Separate thread for heartbeats
    heartbeat_handle: JoinHandle<()>,
}

impl SmartKafkaConsumer {
    pub async fn create(config: ConsumerConfig) -> Result<Self> {
        let consumer_config = ConsumerConfig::default()
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set("heartbeat.interval.ms", config.heartbeat_interval_ms.to_string())
            .set("max.poll.interval.ms", config.max_poll_interval_ms.to_string());
        
        let consumer = Consumer::from_config(&consumer_config)?;
        
        // Spawn heartbeat thread
        let consumer_clone = consumer.clone();
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                // Send heartbeat every heartbeat_interval_ms
                consumer_clone.poll(Duration::from_millis(100));
                tokio::time::sleep(
                    Duration::from_millis(config.heartbeat_interval_ms as u64)
                ).await;
            }
        });
        
        Ok(Self {
            consumer,
            heartbeat_handle,
        })
    }
}
```

---

## Part 2: Critical Implementation Patterns

### Pattern 1: Lock-Free Hot Buffer

**Problem:** Hot buffer under high throughput (1M msg/sec) becomes lock bottleneck.

```rust
// ❌ WRONG: Single lock for entire buffer
pub struct BadHotBuffer {
    data: Mutex<Vec<Record>>,  // ALL operations wait for this lock!
}

// ✅ CORRECT: Lock-free structure with sharding
pub struct GoodHotBuffer {
    // Shard by primary key (256 shards)
    shards: [Arc<RwLock<Shard>>; 256],
}

impl GoodHotBuffer {
    pub async fn append(&self, record: Record) {
        // Hash primary key to shard
        let shard_idx = self.hash(&record.key()) % 256;
        
        // Only lock that shard (1/256th of contention)
        let mut shard = self.shards[shard_idx].write().await;
        shard.append(record);
    }
}

// Even better: Use lock-free structures
pub struct BestHotBuffer {
    // DashMap = lock-free concurrent hashmap
    // Each key has its own lock (micro-lock)
    data: DashMap<PrimaryKey, Record>,
    
    // Separate index for range queries
    timestamp_index: BTreeMap<i64, Vec<PrimaryKey>>,
}
```

### Pattern 2: Backpressure Propagation

**Problem:** Kafka producer sends 100K msg/sec, but we can only flush 10K msg/sec to Iceberg.

```rust
// ✅ CORRECT: Pause consumer when buffer full
pub async fn good_consume_loop() {
    loop {
        // Check buffer capacity BEFORE polling
        if hot_buffer.is_full() {
            // Pause Kafka consumer
            consumer.pause(&[topic_partition])?;
            
            // Wait for flush to happen
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Resume when buffer has space
            if hot_buffer.has_space() {
                consumer.resume(&[topic_partition])?;
            }
            continue;
        }
        
        // Safe to poll now
        if let Some(msg) = consumer.poll(timeout) {
            hot_buffer.append(&msg)?;
        }
    }
}
```

### Pattern 3: Idempotent Iceberg Commits

**Problem:** Network failure during Iceberg commit. Did it succeed or fail?

```rust
// ✅ CORRECT: Idempotent with Check-And-Set
pub async fn good_iceberg_write() {
    let parquet_path = write_parquet(&batch).await?;
    
    // Step 1: Upload to S3 (idempotent, same data = same path)
    storage.put(&parquet_path, &data).await?;
    
    // Step 2: Verify file exists
    if !storage.exists(&parquet_path).await? {
        return Err("Upload failed");
    }
    
    // Step 3: Create manifest entry with same batch_id
    let manifest_entry = ManifestEntry {
        file_path: parquet_path.clone(),
        batch_id: batch_id.clone(),  // Idempotent key!
    };
    
    // Step 4: Iceberg compare-and-set
    // If same batch_id already committed: returns existing snapshot (idempotent)
    // If different batch_id: creates new snapshot
    let snapshot = table.append_file_idempotent(&manifest_entry).await?;
    
    // Step 5: Verify snapshot contains our file
    if !snapshot.contains_file(&parquet_path) {
        return Err("File not in snapshot after commit!");
    }
    
    Ok(snapshot)
}
```

### Pattern 4: Transaction Log Checkpointing

**Problem:** Transaction log grows unboundedly. 30 days = 2.6 billion entries!

```rust
pub struct TransactionLog {
    // Current file
    current_file: File,
    current_size_bytes: u64,
    
    // Checkpoint tracking
    last_checkpoint: Checkpoint,
}

pub struct Checkpoint {
    timestamp: DateTime,
    
    // Snapshots of state at this point
    last_iceberg_snapshot: u64,
    last_kafka_offset: u64,
    
    // Checkpoint file path (can delete old files before this)
    checkpoint_path: String,
}

impl TransactionLog {
    pub async fn append(&mut self, entry: LogEntry) -> Result<()> {
        // Write entry to current file
        self.current_file.write_all(&serde_json::to_vec(&entry)?)?;
        self.current_size_bytes += /* entry size */;
        
        // Check if we should checkpoint
        if self.current_size_bytes > 100_000_000 {  // 100MB
            self.create_checkpoint().await?;
        }
        
        Ok(())
    }
    
    async fn create_checkpoint(&mut self) -> Result<()> {
        // 1. Create checkpoint record
        let checkpoint = Checkpoint {
            timestamp: Utc::now(),
            last_iceberg_snapshot: self.last_iceberg_snapshot.clone(),
            last_kafka_offset: self.last_kafka_offset.clone(),
            checkpoint_path: format!(
                "checkpoints/checkpoint-{}.json",
                Uuid::new_v4()
            ),
        };
        
        // 2. Write checkpoint to storage
        let checkpoint_data = serde_json::to_vec(&checkpoint)?;
        self.storage.put(&checkpoint.checkpoint_path, checkpoint_data).await?;
        
        // 3. Close current transaction log file
        drop(self.current_file);
        
        // 4. Start new transaction log file
        self.current_file = File::create(format!(
            "txn_logs/log-{}.jsonl",
            Uuid::new_v4()
        ))?;
        self.current_size_bytes = 0;
        
        // 5. Save checkpoint reference
        self.last_checkpoint = checkpoint;
        
        // 6. Optionally delete old transaction log files
        // (Keep last 2-3 checkpoint periods for safety)
        self.cleanup_old_logs().await?;
        
        Ok(())
    }
    
    pub async fn recover(&self) -> Result<RecoveryState> {
        // 1. Load latest checkpoint
        let checkpoint = self.load_latest_checkpoint().await?;
        
        // 2. Start recovery from checkpoint
        let mut state = RecoveryState {
            last_iceberg_snapshot: checkpoint.last_iceberg_snapshot,
            last_kafka_offset: checkpoint.last_kafka_offset,
        };
        
        // 3. Read ONLY the entries after checkpoint
        // (Don't process entire 30 days of history!)
        let entries = self.read_entries_after(&checkpoint).await?;
        
        for entry in entries {
            // Update state with new entries...
        }
        
        Ok(state)
    }
}
```

---

## Part 3: Monitoring & Observability

### Critical Metrics

```rust
pub struct IngestionMetrics {
    // Throughput
    messages_consumed_total: Counter,
    messages_consumed_per_sec: Gauge,
    
    // Latency (hot path)
    append_to_buffer_latency_ms: Histogram,  // Should be < 1ms
    
    // Backpressure
    buffer_pause_events: Counter,
    buffer_size_bytes: Gauge,
    buffer_full_percent: Gauge,  // % of max capacity
    
    // Flush operations
    flush_duration_seconds: Histogram,  // How long to flush?
    flush_size_records: Histogram,      // How many records flushed?
    parquet_file_size_mb: Histogram,    // Iceberg file sizes
    
    // Error tracking
    failed_appends: Counter,
    failed_flushes: Counter,
    failed_iceberg_commits: Counter,
    recovery_events: Counter,
    
    // Offset tracking
    current_kafka_offset: Gauge,
    last_committed_offset: Gauge,
    offset_lag: Gauge,  // How far behind?
    
    // Iceberg metrics
    iceberg_snapshots_created: Counter,
    iceberg_files_created: Counter,
    iceberg_manifest_size_mb: Gauge,
}

// Example alerts:
pub fn check_health(metrics: &IngestionMetrics) -> Vec<Alert> {
    let mut alerts = vec![];
    
    // Alert if offset lag > 10 minutes
    if metrics.offset_lag.value() > 600_000 {
        alerts.push(Alert {
            severity: "CRITICAL",
            message: "Offset lag exceeds 10 minutes, possible data loss",
        });
    }
    
    // Alert if buffer > 80% full
    if metrics.buffer_full_percent.value() > 80.0 {
        alerts.push(Alert {
            severity: "WARNING",
            message: "Hot buffer near capacity, backpressure active",
        });
    }
    
    // Alert if flush latency > 30 seconds
    if metrics.flush_duration_seconds.p99() > 30.0 {
        alerts.push(Alert {
            severity: "WARNING",
            message: "Flush operations slow, check Iceberg/S3 latency",
        });
    }
    
    alerts
}
```

---

## Part 4: Testing Strategies

### Test 1: Chaos Engineering - Kill Writer Thread

```rust
#[tokio::test]
async fn test_crash_during_flush() {
    let engine = IngestionEngine::new().await;
    
    // Send 1M messages
    let messages = generate_test_messages(1_000_000);
    for msg in messages {
        engine.hot_buffer.append(&msg).await?;
    }
    
    // Trigger flush
    let flush_task = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine.flush().await
        }
    });
    
    // Kill flush halfway through
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush_task.abort();
    
    // Recover
    let recovered = IngestionEngine::recover().await?;
    
    // Verify: No data loss
    let final_offset = recovered.get_last_committed_offset().await?;
    assert!(final_offset > 0, "Should have committed some data");
    
    // Verify: No corruption
    let records = recovered.query_all().await?;
    assert_eq!(records.len(), 1_000_000, "All records should be recoverable");
}
```

---

## Conclusion: Key Takeaways

1. **Offset commit timing is security-critical** - Commit only after transaction log write
2. **Arrow memory alignment matters** - Use MemoryPool for SIMD safety
3. **Manifest bloat is real** - Larger flushes + background compaction
4. **Schema evolution must use field IDs** - Not positions
5. **Rebalancing storms are common** - Configure Kafka heartbeat threads properly
6. **Iceberg commits need idempotency** - Handle retries safely
7. **Transaction log needs checkpointing** - Don't grow unboundedly
8. **Monitoring is critical** - Offset lag, buffer capacity, flush latency
9. **Crash recovery is your baseline** - Test it extensively
10. **Lock-free structures scale better** - Use DashMap, not Mutex<Vec>

These patterns directly translate from what Moonlink engineers discovered building pg_mooncake!
