# Iceberg Catalog Integration: Enhanced Product Requirements Document

**Date:** January 8, 2026  
**Version:** 2.0  
**Status:** Implementation Planning  
**Scope:** Iceberg Catalog Layer Integration (Week 3-4 refactor + ongoing)

---

## Executive Summary

This PRD extends the original Kafka → Iceberg MVP PRD with **production-ready Iceberg catalog integration** using `iceberg-rs` (Apache's official Rust implementation). Instead of a single hardcoded table, this enhancement enables:

- ✅ **Multi-catalog support** (REST, Hive Metastore, AWS Glue, Project Nessie)
- ✅ **Dynamic table management** (create, list, update, commit snapshots)
- ✅ **Catalog connection pooling** (reuse connections, handle reconnection)
- ✅ **Atomic transactions** (proper CAS semantics via iceberg-rs)
- ✅ **Metadata-driven operations** (compaction, orphan cleanup via catalog)
- ✅ **Transaction log integration** (catalog snapshots, idempotent commits)
- ✅ **Production error handling** (retry logic, circuit breakers, failover)

This is inspired by **Mooncake/Moonlink's architecture** but tuned for your embedded Kafka ingestion use case.

---

## Part 1: Architecture - Iceberg Catalog Layer

### Current Gap vs. Enhanced Design

**Current (Original PRD):**
```
Kafka → Iceberg Writer → [hardcoded catalog path] → S3 Parquet
```
- Single table
- Catalog type hardcoded in config
- No table creation logic
- No multi-namespace support
- Metadata operations are file-based

**Enhanced Design:**
```
Kafka → Hot Buffer → Iceberg Writer
                         ├─ Catalog Manager (connection pooling, lifecycle)
                         ├─ Catalog Abstraction (REST, Hive, Glue, Nessie)
                         ├─ Table Manager (CRUD, metadata)
                         ├─ Transaction Coordinator (atomic commits, idempotency)
                         └─ Metadata Cache (snapshots, manifests)
```

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│             ICEBERG CATALOG LAYER (Week 3-4 Refactor)          │
└─────────────────────────────────────────────────────────────────┘

INPUT: Hot Buffer (RecordBatch)
  │
  ↓
┌────────────────────────────────────────────────────────────────┐
│ 1. CATALOG MANAGER                                             │
│    ├─ ConnectionPool<CatalogClient>                           │
│    ├─ Catalog selection (REST | Hive | Glue | Nessie)         │
│    ├─ Lazy initialization (on-demand)                         │
│    ├─ Reconnection with backoff                               │
│    ├─ Health checks (periodic ping)                           │
│    └─ Graceful shutdown                                       │
└────────────────────────────────────────────────────────────────┘
  │
  ↓
┌────────────────────────────────────────────────────────────────┐
│ 2. TABLE MANAGER                                               │
│    ├─ Create table (if not exists)                            │
│    ├─ Load table metadata                                     │
│    ├─ Validate schema compatibility                           │
│    ├─ Handle schema evolution                                 │
│    ├─ Get current snapshot ID                                 │
│    └─ List snapshots (for time travel, debugging)            │
└────────────────────────────────────────────────────────────────┘
  │
  ↓
┌────────────────────────────────────────────────────────────────┐
│ 3. PARQUET WRITER                                              │
│    ├─ Arrow RecordBatch → Parquet conversion                 │
│    ├─ Snappy compression                                      │
│    ├─ Iceberg metadata columns (partition_spec, etc.)        │
│    ├─ S3 upload with SHA256 verification                     │
│    └─ Return file path + metrics                             │
└────────────────────────────────────────────────────────────────┘
  │
  ├─ [Transaction Log: record ParquetWritten entry]
  │
  ↓
┌────────────────────────────────────────────────────────────────┐
│ 4. TRANSACTION COORDINATOR                                     │
│    ├─ Prepare snapshot (add file to manifest)                │
│    ├─ Compare-and-set (CAS) commit                           │
│    ├─ Handle commit conflicts (retry logic)                  │
│    ├─ Update metadata.json atomically                        │
│    ├─ Verify snapshot creation                               │
│    └─ Idempotency: track committed (offset, snapshot_id)    │
└────────────────────────────────────────────────────────────────┘
  │
  ├─ [Transaction Log: record IcebergSnapshotCreated entry]
  │
  ↓
┌────────────────────────────────────────────────────────────────┐
│ 5. METADATA CACHE                                              │
│    ├─ Current snapshot ID (volatile)                          │
│    ├─ Table schema (with TTL refresh)                         │
│    ├─ Partition spec                                          │
│    ├─ Manifest entries (for compaction)                       │
│    └─ Last catalog sync timestamp                             │
└────────────────────────────────────────────────────────────────┘
  │
  ├─ [Background maintenance thread reads cache]
  │
  ↓
┌────────────────────────────────────────────────────────────────┐
│ 6. MAINTENANCE SCHEDULER (unchanged, but uses cache)          │
│    ├─ Compaction (pull small files from catalog)             │
│    ├─ Orphan cleanup (iterate manifests)                     │
│    ├─ Snapshot expiration (catalog snapshots)                │
│    └─ Statistics update                                       │
└────────────────────────────────────────────────────────────────┘

OUTPUT: Apache Iceberg Table (Queryable via any engine)
```

---

## Part 2: Iceberg Catalog Implementations

### 2.1 Catalog Type Comparison

| Aspect | REST | Hive | AWS Glue | Nessie |
|--------|------|------|----------|--------|
| **Setup Complexity** | Medium | High | Low | High |
| **Infrastructure** | External REST API | MySQL/PostgreSQL | Managed | External REST API |
| **Maturity (2025)** | ✅ Recommended | ✅ Mature | ✅ Production | ✅ Growing |
| **Multi-Engine** | ✅ Yes | ✅ Yes | ✅ Yes (AWS only) | ✅ Yes |
| **Versioning** | ❌ No | ❌ No | ❌ No | ✅ Git-like |
| **Embedded Use** | ⚠️ Requires server | ❌ Requires server | ❌ AWS only | ❌ Requires server |
| **Best For** | New deployments | Legacy systems | AWS-native | Advanced workflows |

### 2.2 Implementation Strategy (Week 3-4)

**Phase 1 (Week 3a): File-Based Catalog (Fast Path)**
- Implement using `iceberg-rs` `FileCatalog`
- Works with local S3-like storage
- No external dependency
- Great for MVP testing, embedded scenarios
- **Limitation**: Single-process only (no distributed catalog)

**Phase 2 (Week 3b): REST Catalog (Recommended for Production)**
- Implement `RestCatalog` backend
- Use `iceberg-rs` REST abstraction
- Works with Apache Polaris (open-source) or Lakekeeper
- Recommended for new deployments in 2025
- **Advantage**: Works everywhere, best ecosystem support

**Phase 3 (Week 4): AWS Glue (Enterprise AWS)**
- Implement AWS Glue catalog backend
- Use AWS SDK for Glue
- Integrates with Athena, EMR, Redshift Spectrum
- **Limitation**: AWS-only

**Phase 4 (Post-MVP): Hive Metastore & Nessie (Optional)**
- Hive Metastore for legacy Hadoop environments
- Nessie for advanced versioning workflows
- Can be added based on customer demand

---

## Part 3: Detailed Specification

### 3.1 Catalog Manager (New Component)

**Purpose:** Single point of contact for all catalog operations. Handles connection pooling, reconnection, and abstraction over different catalog types.

**Configuration:**
```toml
[iceberg]
catalog_type = "file"            # file | rest | glue | hive | nessie
database_name = "raw"
table_name = "events"

[iceberg.file]
warehouse_path = "s3://bucket/warehouse"
s3_access_key = "${AWS_ACCESS_KEY_ID}"
s3_secret_key = "${AWS_SECRET_ACCESS_KEY}"

[iceberg.rest]
uri = "http://localhost:8181"
credential_type = "bearer"
credential = "${CATALOG_TOKEN}"  # or read from .env
request_timeout_seconds = 30
max_retries = 3

[iceberg.glue]
region = "us-east-1"
role_arn = "arn:aws:iam::123456789012:role/IcebergRole"

[iceberg.hive]
metastore_uri = "thrift://localhost:9083"
warehouse_path = "s3://bucket/warehouse"

[iceberg.catalog_manager]
connection_pool_size = 5
health_check_interval_seconds = 60
reconnect_backoff_ms = [100, 500, 2000, 5000]  # exponential
```

**API:**
```rust
pub trait CatalogFactory {
    async fn create_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>>;
}

pub struct CatalogManager {
    catalog: Arc<dyn Catalog>,
    config: CatalogConfig,
    health_check_interval: Duration,
    is_healthy: Arc<AtomicBool>,
}

impl CatalogManager {
    pub async fn new(config: CatalogConfig) -> Result<Self>;
    pub async fn health_check(&self) -> Result<()>;
    pub fn is_healthy(&self) -> bool;
    pub fn catalog(&self) -> Arc<dyn Catalog>;
    pub async fn graceful_shutdown(&self) -> Result<()>;
}
```

**Implementation Details:**
```rust
pub struct FileCatalogManager {
    catalog: Arc<FileCatalog>,
    // Simple file-based catalog for MVP
}

pub struct RestCatalogManager {
    catalog: Arc<RestCatalog>,
    client: Arc<HttpClient>,
    pool: Arc<ConnectionPool>,
    health: Arc<HealthMonitor>,
}

pub struct GlueCatalogManager {
    catalog: Arc<GlueCatalog>,
    client: Arc<GlueClient>,
    aws_region: Region,
}
```

**Error Handling:**
- Retry transient failures (3xx, 5xx, timeout)
- Circuit breaker for persistent failures
- Fallback to cached metadata if catalog unavailable
- Alert ops if health check fails 3x in a row

### 3.2 Table Manager (New Component)

**Purpose:** Manage table lifecycle (create, load, validate, evolve schema).

**Configuration:**
```toml
[iceberg.table_management]
auto_create_if_missing = true
schema_mismatch_action = "fail"  # fail | evolve | warn
partition_strategy = "daily"      # daily | hourly | identity | bucket
```

**API:**
```rust
pub struct TableManager {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: Schema,
    partition_spec: PartitionSpec,
}

impl TableManager {
    pub async fn get_or_create(
        catalog: Arc<dyn Catalog>,
        namespace: &str,
        table_name: &str,
        schema: Schema,
        partition_spec: PartitionSpec,
    ) -> Result<Self>;

    pub async fn load_table(&mut self) -> Result<Table>;
    pub fn get_schema(&self) -> &Schema;
    pub fn get_partition_spec(&self) -> &PartitionSpec;
    pub async fn evolve_schema(&mut self, new_schema: Schema) -> Result<()>;
    pub fn current_snapshot_id(&self) -> Option<i64>;
    pub async fn refresh_metadata(&mut self) -> Result<()>;
}
```

**Implementation Logic:**

```rust
pub async fn get_or_create(...) -> Result<TableManager> {
    let table_ident = TableIdent::from_strs([namespace, table_name])?;
    
    // Try to load existing table
    match catalog.load_table(&table_ident).await {
        Ok(table) => {
            // Validate schema compatibility
            if table.schema() != &schema {
                return match config.schema_mismatch_action {
                    "fail" => Err(SchemaError),
                    "evolve" => {
                        // Plan schema evolution
                        // Will be applied on next commit
                    },
                    "warn" => {
                        warn!("Schema mismatch detected, continuing...");
                    }
                };
            }
            Ok(TableManager { /* ... */ })
        }
        Err(CatalogError::NotFound) => {
            // Table doesn't exist, create it
            if config.auto_create_if_missing {
                let creation = TableCreation::builder()
                    .name(table_ident)
                    .schema(schema.clone())
                    .partition_spec(partition_spec.clone())
                    .location(warehouse_path) // derived from config
                    .build()?;
                
                catalog.create_table(&creation).await?;
                Ok(TableManager { /* ... */ })
            } else {
                Err(CatalogError::NotFound)
            }
        }
        Err(e) => Err(e),
    }
}
```

### 3.3 Transaction Coordinator (Enhanced from Week 3)

**Purpose:** Atomic snapshot commits with idempotency and conflict resolution.

**Key Changes from Original PRD:**
- Uses `iceberg-rs` transaction API (proper CAS semantics)
- Tracks idempotency key per Kafka offset range
- Handles concurrent commits (snapshot conflicts)
- Records all catalog snapshots in transaction log

**API:**
```rust
pub struct TransactionCoordinator {
    catalog: Arc<dyn Catalog>,
    table: Arc<Table>,
    transaction_log: Arc<TransactionLog>,
}

impl TransactionCoordinator {
    pub async fn begin_transaction(&self) -> Result<Transaction>;
    
    pub async fn add_parquet_file(
        &mut self,
        tx: &mut Transaction,
        file_path: String,
        file_size: usize,
        record_count: usize,
        kafka_offsets: (i64, i64), // min, max
    ) -> Result<()>;

    pub async fn commit_snapshot(
        &mut self,
        tx: Transaction,
        kafka_offset_range: (i64, i64),
    ) -> Result<SnapshotCommitResult>;
}

pub struct SnapshotCommitResult {
    pub snapshot_id: i64,
    pub committed_at: DateTime<Utc>,
    pub transaction_log_entry_id: u64,
    pub files_added: usize,
}
```

**Commit Flow (Idempotent):**

```
1. Receive: ParquetFile, KafkaOffsetRange (e.g., 1000-2000)

2. Check idempotency: 
   tx_log.query("where kafka_offset_min >= 1000 and kafka_offset_max <= 2000")
   if exists: return previous snapshot_id (idempotent)

3. Begin transaction:
   tx = table.new_transaction()

4. Add files:
   tx.add_datafile(file_metadata)

5. Commit (with CAS):
   new_snapshot_id = tx.commit()
   
6. Record in tx_log:
   tx_log.append(IcebergSnapshotCreated {
     snapshot_id: new_snapshot_id,
     kafka_offset_range,
     timestamp,
   })

7. Return SnapshotCommitResult
```

**Conflict Resolution:**

```rust
pub async fn commit_snapshot(...) -> Result<SnapshotCommitResult> {
    let mut retries = 0;
    let max_retries = 3;
    
    loop {
        match self.table.new_transaction().add_datafile(...).commit().await {
            Ok(snapshot_id) => {
                self.transaction_log.append(IcebergSnapshotCreated { ... })?;
                return Ok(SnapshotCommitResult { snapshot_id, ... });
            }
            Err(TransactionError::Conflict) => {
                retries += 1;
                if retries > max_retries {
                    return Err(TransactionError::CommitConflict);
                }
                
                // Refresh table, retry
                self.table.refresh().await?;
                tokio::time::sleep(Duration::from_millis(100 * (2 ^ retries))).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 3.4 Metadata Cache (New Component)

**Purpose:** Reduce catalog queries for frequently accessed metadata.

```rust
pub struct MetadataCache {
    snapshot_id: Arc<RwLock<Option<i64>>>,
    schema: Arc<RwLock<Schema>>,
    partition_spec: Arc<RwLock<PartitionSpec>>,
    manifest_entries: Arc<RwLock<Vec<ManifestEntry>>>,
    ttl: Duration,
    last_refresh: Arc<RwLock<Instant>>,
}

impl MetadataCache {
    pub async fn get_snapshot_id(&self) -> Result<i64>;
    pub async fn get_schema(&self) -> Result<Schema>;
    pub async fn refresh_if_stale(&self, table: &Table) -> Result<()>;
    pub async fn invalidate(&self);
}
```

**TTL Strategy:**
- Snapshot ID: 1 second (very volatile)
- Schema: 5 minutes (rarely changes)
- Manifest: 1 minute (for compaction decisions)
- Explicit invalidation on commit

### 3.5 Maintenance Scheduler Updates

**Changes to compaction, orphan cleanup, etc. to use catalog:**

```rust
pub struct MaintenanceScheduler {
    catalog: Arc<dyn Catalog>,
    table: Arc<Table>,
    metadata_cache: Arc<MetadataCache>,
}

impl MaintenanceScheduler {
    pub async fn compact_small_files(&self) -> Result<CompactionStats> {
        // 1. Get manifest from catalog (via metadata_cache)
        let manifest = self.metadata_cache.get_manifest_entries().await?;
        
        // 2. Identify small files (<= 100MB)
        let small_files: Vec<_> = manifest
            .iter()
            .filter(|f| f.file_size_in_bytes <= 100 * 1024 * 1024)
            .collect();
        
        // 3. Read + merge parquet files
        let merged_batch = self.merge_parquet_files(&small_files).await?;
        
        // 4. Write new consolidated file to S3
        let new_file_path = self.write_parquet(&merged_batch).await?;
        
        // 5. Begin catalog transaction
        let mut tx = self.table.new_transaction();
        
        // 6. Remove old files
        for f in &small_files {
            tx.delete_datafile(&f.file_path);
        }
        
        // 7. Add new consolidated file
        tx.add_datafile(DataFileMetadata {
            file_path: new_file_path,
            file_size: /* ... */,
            record_count: merged_batch.num_rows(),
        });
        
        // 8. Commit snapshot
        let snapshot_id = tx.commit().await?;
        
        // 9. Invalidate cache
        self.metadata_cache.invalidate();
        
        Ok(CompactionStats { snapshot_id, files_compacted: small_files.len() })
    }
}
```

### 3.6 Transaction Log Schema Extension

**New entries for catalog integration:**

```rust
pub enum TransactionEntry {
    // Original entries
    OffsetCommit { /* ... */ },
    BufferSnapshot { /* ... */ },
    ParquetWritten { /* ... */ },
    IcebergSnapshotCreated { /* ... */ },
    FlushComplete { /* ... */ },
    
    // New catalog entries
    CatalogHealthCheck {
        catalog_type: CatalogType,
        is_healthy: bool,
        response_time_ms: u64,
        timestamp: DateTime<Utc>,
    },
    
    CatalogError {
        catalog_type: CatalogType,
        error_message: String,
        retry_count: u32,
        timestamp: DateTime<Utc>,
    },
    
    IdempotencyRecord {
        kafka_offset_range: (i64, i64),
        snapshot_id: i64,
        committed_at: DateTime<Utc>,
    },
    
    SchemaEvolution {
        old_schema: Schema,
        new_schema: Schema,
        timestamp: DateTime<Utc>,
    },
}
```

---

## Part 4: Dependencies & Versions

**Key Additions to Cargo.toml:**

```toml
# Official Apache Iceberg (Rust)
iceberg = "0.2"

# REST Catalog client (if needed beyond iceberg-rs)
reqwest = { version = "0.11", features = ["json"] }

# AWS SDK (for Glue support)
aws-sdk-glue = "1.0"
aws-config = "1.0"

# Connection pooling
bb8 = "0.8"
deadpool = "0.10"

# Metrics
prometheus = "0.13"

# Async utilities
tokio = { version = "1.0", features = ["full"] }
async-trait = "0.1"

# Error handling
thiserror = "1.0"
anyhow = "1.0"
```

---

## Part 5: Integration Timeline (Week 3-4 Refactor)

### Week 3: Catalog Abstraction & File-Based Implementation

**Monday-Tuesday (Day 1-2):**
- [ ] Set up `iceberg-rs` integration
- [ ] Design Catalog trait abstraction
- [ ] Implement FileCatalogManager
- [ ] Unit tests for file catalog

**Wednesday-Thursday (Day 3-4):**
- [ ] Implement TableManager
- [ ] Implement MetadataCache
- [ ] Integration tests with local S3 (MinIO)
- [ ] Error handling for catalog failures

**Friday (Day 5):**
- [ ] Code review & refinement
- [ ] Load testing (100K events/sec)
- [ ] Documentation (CATALOG_GUIDE.md)

**Deliverables:**
- ✅ CatalogManager trait + FileCatalogManager implementation
- ✅ TableManager (create, load, validate)
- ✅ MetadataCache (snapshot_id, schema)
- ✅ Updated IcebergWriter using new components
- ✅ Unit tests (80%+ coverage)
- ✅ Integration test end-to-end with file catalog

**Success Metrics:**
- FileCatalog operations complete in < 100ms
- Metadata cache hit rate > 95%
- Zero data loss when catalog operations fail

### Week 4: REST Catalog + Transaction Coordinator

**Monday-Tuesday (Day 1-2):**
- [ ] Implement RestCatalogManager (iceberg-rs)
- [ ] REST API client wrapper
- [ ] Implement TransactionCoordinator with idempotency
- [ ] Idempotency key storage in transaction log

**Wednesday-Thursday (Day 3-4):**
- [ ] Conflict resolution & retry logic
- [ ] Connection pooling & health checks
- [ ] Error recovery for catalog unavailability
- [ ] Integration tests with REST catalog (Polaris, Lakekeeper)

**Friday (Day 5):**
- [ ] End-to-end testing (Kafka → REST Catalog → Iceberg)
- [ ] Performance benchmarking
- [ ] Documentation (REST_CATALOG_SETUP.md)

**Deliverables:**
- ✅ RestCatalogManager with iceberg-rs
- ✅ TransactionCoordinator with idempotent commits
- ✅ Connection pooling & health monitoring
- ✅ Unit tests (80%+ coverage)
- ✅ Integration tests with external catalog

**Success Metrics:**
- REST catalog operations complete in < 500ms
- Idempotency verified (no duplicate snapshots on retry)
- Catalog unavailability handled gracefully (fallback to cache)

### Week 5-6: AWS Glue (Optional for MVP)

**Conditional (only if AWS customer priorities):**
- [ ] Implement GlueCatalogManager
- [ ] AWS IAM role integration
- [ ] Glue-specific error handling
- [ ] Integration tests with real AWS Glue

---

## Part 6: Configuration Examples

### Example 1: File-Based Catalog (Local Development)

```toml
# config.toml

[kafka]
bootstrap_servers = "localhost:9092"
topic = "events"
consumer_group = "iceberg-ingestion"

[iceberg]
catalog_type = "file"
database_name = "raw"
table_name = "events"

[iceberg.file]
warehouse_path = "s3://my-bucket/warehouse"
s3_access_key = "minioadmin"
s3_secret_key = "minioadmin"
s3_endpoint = "http://localhost:9000"

[hot_buffer]
ttl_seconds = 60
max_size_mb = 500
```

**Usage:**
```bash
kafka-iceberg --config config.toml
```

### Example 2: REST Catalog (Production)

```toml
# config.toml

[kafka]
bootstrap_servers = "broker1:9092,broker2:9092"
topic = "events"
consumer_group = "iceberg-prod"

[iceberg]
catalog_type = "rest"
database_name = "analytics"
table_name = "raw_events"

[iceberg.rest]
uri = "http://polaris.example.com:8181"
credential_type = "bearer"
credential = "${CATALOG_TOKEN}"
request_timeout_seconds = 30

[iceberg.catalog_manager]
connection_pool_size = 10
health_check_interval_seconds = 60

[hot_buffer]
ttl_seconds = 60
max_size_mb = 2048

[maintenance]
compact_enabled = true
compact_interval_seconds = 3600
```

**Usage:**
```bash
CATALOG_TOKEN=$(cat /run/secrets/polaris_token) \
  kafka-iceberg --config config.toml
```

### Example 3: AWS Glue (AWS-Native)

```toml
# config.toml

[kafka]
bootstrap_servers = "b-1.kafka-cluster.xxx.kafka.us-east-1.amazonaws.com:9092"
topic = "events"
consumer_group = "iceberg-aws"

[iceberg]
catalog_type = "glue"
database_name = "raw_data"
table_name = "events"

[iceberg.glue]
region = "us-east-1"
role_arn = "arn:aws:iam::123456789012:role/KafkaIcebergRole"
warehouse_path = "s3://my-data-lake/warehouse"

[iceberg.catalog_manager]
connection_pool_size = 5
health_check_interval_seconds = 60

[hot_buffer]
ttl_seconds = 60
max_size_mb = 2048
```

---

## Part 7: Monitoring & Observability

### New Prometheus Metrics

```
# Catalog operations
iceberg_catalog_operation_duration_seconds{operation="create_table",catalog_type="rest"} 0.245
iceberg_catalog_operation_duration_seconds{operation="load_table",catalog_type="rest"} 0.012
iceberg_catalog_operation_duration_seconds{operation="commit_snapshot",catalog_type="rest"} 0.456

# Catalog health
iceberg_catalog_health_status{catalog_type="rest"} 1  # 1=healthy, 0=unhealthy
iceberg_catalog_error_total{catalog_type="rest",error_type="timeout"} 3
iceberg_catalog_error_total{catalog_type="rest",error_type="conflict"} 1

# Transaction coordinator
iceberg_snapshot_commits_total{database="raw",table="events"} 1000
iceberg_snapshot_commit_duration_seconds{database="raw",table="events"} 0.345
iceberg_idempotent_retries_total{reason="already_committed"} 5
iceberg_idempotent_retries_total{reason="conflict"} 3

# Metadata cache
iceberg_metadata_cache_hits_total{key="snapshot_id"} 95000
iceberg_metadata_cache_misses_total{key="snapshot_id"} 5000
iceberg_metadata_cache_hit_rate{key="snapshot_id"} 0.95

# Compaction via catalog
iceberg_compaction_files_merged_total{database="raw",table="events"} 150
iceberg_compaction_duration_seconds{database="raw",table="events"} 125.3
```

### Structured Logging

```json
{
  "timestamp": "2026-01-08T14:30:00Z",
  "level": "INFO",
  "message": "Iceberg snapshot committed",
  "catalog_type": "rest",
  "database": "raw",
  "table": "events",
  "snapshot_id": 12345,
  "kafka_offset_range": [1000000, 1010000],
  "files_added": 2,
  "duration_ms": 450,
  "committed_at": "2026-01-08T14:30:00Z"
}
```

---

## Part 8: Error Handling & Resilience

### Catalog Failure Scenarios

| Scenario | Action | Recovery |
|----------|--------|----------|
| **REST catalog timeout** | Log error, retry with backoff (100ms, 500ms, 2s, 5s) | After max retries, use cached metadata for reads, fail on writes |
| **REST catalog 4xx error** | Fail immediately (client error, not retryable) | Alert ops, require manual fix |
| **REST catalog 5xx error** | Retry with exponential backoff | Fallback to cached metadata after 3 retries |
| **Table not found** | If auto_create=true, create table | If auto_create=false, fail |
| **Schema mismatch** | Handle per config (fail/evolve/warn) | Log details for troubleshooting |
| **Snapshot commit conflict** | Retry CAS with new snapshot ID | After 3 retries, fail (rare in practice) |
| **Idempotency key collision** | Return previous snapshot_id (idempotent) | No error, success response |

### Circuit Breaker Pattern

```rust
pub struct CatalogCircuitBreaker {
    failures: Arc<AtomicU32>,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    state: Arc<RwLock<CircuitState>>,
}

pub enum CircuitState {
    Closed,           // Normal operation
    Open,             // Failing, reject requests
    HalfOpen,         // Testing if catalog recovered
}

impl CatalogCircuitBreaker {
    pub async fn call<F, T>(&self, f: F) -> Result<T>
    where
        F: Fn() -> BoxFuture<'static, Result<T>>,
    {
        match *self.state.read().await {
            CircuitState::Open => {
                if self.should_retry() {
                    self.state.write().await = CircuitState::HalfOpen;
                    // Continue to attempt call
                } else {
                    return Err(CircuitBreakerOpen);
                }
            }
            _ => {}
        }
        
        match f().await {
            Ok(val) => {
                self.failures.store(0, Ordering::Relaxed);
                self.state.write().await = CircuitState::Closed;
                Ok(val)
            }
            Err(e) => {
                let failures = self.failures.fetch_add(1, Ordering::Relaxed);
                if failures > 5 {
                    self.state.write().await = CircuitState::Open;
                }
                Err(e)
            }
        }
    }
}
```

---

## Part 9: Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_catalog_create_table() {
        // Arrange
        let catalog = FileCatalogManager::new(config).await.unwrap();
        let schema = /* ... */;
        
        // Act
        let table_mgr = TableManager::get_or_create(
            catalog.catalog(),
            "raw",
            "events",
            schema,
            partition_spec,
        ).await.unwrap();
        
        // Assert
        assert_eq!(table_mgr.get_schema().fields.len(), 10);
    }

    #[tokio::test]
    async fn test_idempotent_snapshot_commit() {
        // First commit
        let result1 = coordinator.commit_snapshot(tx1, (1000, 2000)).await.unwrap();
        
        // Identical second commit should return same snapshot_id
        let result2 = coordinator.commit_snapshot(tx2, (1000, 2000)).await.unwrap();
        
        assert_eq!(result1.snapshot_id, result2.snapshot_id);
    }

    #[tokio::test]
    async fn test_metadata_cache_ttl() {
        let cache = MetadataCache::new(Duration::from_secs(1));
        
        // First fetch hits catalog
        let _ = cache.get_snapshot_id().await.unwrap();
        
        // Within TTL, should be cached
        assert!(cache.is_cached());
        
        // After TTL, should refresh
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!cache.is_cached());
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_failures() {
        let cb = CatalogCircuitBreaker::new();
        
        // Simulate 6 failures
        for _ in 0..6 {
            let _ = cb.call(|| Box::pin(async { Err("error".into()) })).await;
        }
        
        // Circuit should be open
        assert_eq!(cb.state, CircuitState::Open);
        
        // Next request should fail immediately (no retry)
        let result = cb.call(|| Box::pin(async { Ok(1) })).await;
        assert!(result.is_err());
    }
}
```

### Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    use testcontainers::*;

    #[tokio::test]
    async fn test_end_to_end_file_catalog() {
        // 1. Start MinIO (S3-compatible)
        let container = testcontainers::run_minio();
        
        // 2. Create Kafka topic
        let kafka = testcontainers::run_kafka();
        kafka.create_topic("events", 3, 1);
        
        // 3. Produce events
        kafka.produce("events", 0, "event1", "payload1");
        
        // 4. Run ingestion engine
        let mut engine = KafkaIcebergEngine::new(config).await.unwrap();
        engine.run_for(Duration::from_secs(5)).await.unwrap();
        
        // 5. Verify table in Iceberg
        let table = engine.catalog().load_table("raw.events").await.unwrap();
        let snapshot = table.current_snapshot().unwrap();
        assert_eq!(snapshot.snapshot_id, 1);
        
        // 6. Verify hot buffer contains latest data
        let buffer_data = engine.hot_buffer().query(filter).await.unwrap();
        assert_eq!(buffer_data.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_rest_catalog_with_polaris() {
        // Requires Polaris running (can use testcontainers)
        let polaris = testcontainers::run_polaris();
        
        let config = CatalogConfig {
            catalog_type: CatalogType::Rest,
            rest: RestCatalogConfig {
                uri: polaris.uri(),
                ..Default::default()
            },
            ..Default::default()
        };
        
        let catalog_mgr = RestCatalogManager::new(config).await.unwrap();
        
        // Test operations
        let table = catalog_mgr.catalog().load_table("test.table").await;
        // ... assertions
    }

    #[tokio::test]
    async fn test_idempotency_with_concurrent_commits() {
        // Simulate concurrent writes to same offset range
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let coordinator = coordinator.clone();
                tokio::spawn(async move {
                    coordinator.commit_snapshot(tx, (1000, 2000)).await
                })
            })
            .collect();
        
        let results = futures::future::join_all(handles).await;
        
        // All should succeed with same snapshot_id
        let first_snapshot = results[0].unwrap().unwrap().snapshot_id;
        for result in &results[1..] {
            assert_eq!(result.unwrap().unwrap().snapshot_id, first_snapshot);
        }
    }
}
```

---

## Part 10: Rollout Strategy

### Phase 1: MVP Launch (Week 3-4)
- **Supported catalogs:** File-based (local dev), REST (production)
- **Deployment:** Single binary, embedded in customer environments
- **Monitoring:** Prometheus metrics, structured JSON logs
- **Support:** Community support, GitHub issues

### Phase 2: Extended Support (Month 2-3)
- Add AWS Glue support (for AWS customers)
- Add Hive Metastore support (for Hadoop users)
- Enterprise features (encryption, audit logging)

### Phase 3: Advanced Features (Month 4-6)
- Add Project Nessie support (for versioning workflows)
- Multi-table transactions across Iceberg
- Time travel query API

---

## Part 11: Go-To-Market Positioning

### Key Message

**"The only embedded Kafka → Iceberg solution with real production catalogs: REST, Hive, Glue, Nessie. Deploy in hours, scale to 1M events/sec."**

### Competitive Advantages

1. **Multiple Catalogs from Day 1** - Not just file-based, real production catalogs
2. **Embedded Deployment** - No distributed infrastructure, single binary
3. **Mooncake-Inspired** - Moonlink's real-time layer + your durability focus
4. **Atomic Semantics** - Proper CAS commits via iceberg-rs
5. **Production-Ready** - Health checks, circuit breakers, idempotency, error recovery

### Target Use Cases

- **Startups:** REST Catalog + local testing
- **AWS Shops:** Glue Catalog + Athena queries
- **Hadoop Users:** Hive Metastore + existing infra
- **Advanced Users:** Nessie + branching/versioning

---

## Part 12: Success Criteria

### Technical

- ✅ File catalog: sub-100ms operations
- ✅ REST catalog: sub-500ms operations (including network)
- ✅ Idempotency: zero duplicates on retry, 100% verified
- ✅ Metadata cache: 95%+ hit rate
- ✅ Graceful degradation: use cache if catalog unavailable
- ✅ Test coverage: 80%+ unit + integration tests

### Operational

- ✅ 70%+ test coverage
- ✅ Comprehensive error handling (9+ scenarios)
- ✅ Health checks every 60s (configurable)
- ✅ Circuit breaker with half-open state
- ✅ Prometheus metrics (20+ metrics)
- ✅ Structured JSON logging

### Documentation

- ✅ CATALOG_INTEGRATION.md (architecture)
- ✅ REST_CATALOG_SETUP.md (Polaris, Lakekeeper)
- ✅ AWS_GLUE_SETUP.md (IAM, configuration)
- ✅ TROUBLESHOOTING.md (common issues + solutions)

---

## Appendix A: Mooncake/Moonlink Learnings

### Applicable Patterns

| Pattern | Mooncake | Your Tool | Notes |
|---------|----------|-----------|-------|
| **Snapshot separation** | Mooncake snapshots every 500ms, Iceberg every 5m | Hot buffer flush independently | Enables sub-second freshness |
| **Update/Delete handling** | Positional deletes in Iceberg | Same approach (supports Kafka append-only) | Mooncake adds mutation support |
| **Buffer + Index** | Arrow + hash index | Same design | Sub-second lookups |
| **Catalog abstraction** | Integrates with Iceberg catalog | Explicit multi-catalog support | You make it pluggable from start |
| **Real-time CDC** | Postgres logical replication | Kafka consumer groups | Different source, same pattern |
| **Error recovery** | Transaction log + replay | Extended transaction log | Mooncake approach + catalog snapshots |

### Where Your Tool Differs

| Aspect | Mooncake | Your Tool |
|--------|----------|-----------|
| **Source** | Postgres WAL CDC | Kafka topics |
| **Deployment** | Embedded in Postgres extension | Standalone binary or embedded library |
| **Catalog focus** | Uses Iceberg catalog (not opinionated) | Explicit multi-catalog support from start |
| **Simplicity** | Complex (Postgres integration) | Simpler (just Kafka consumer) |
| **Use case** | OLTP→OLAP mirroring | Real-time event streaming |

---

## Appendix B: Iceberg Catalog Comparison Matrix (2025)

```
┌────────────────────┬────────┬──────┬────────┬────────┐
│ Feature            │ REST   │ Hive │ Glue   │ Nessie │
├────────────────────┼────────┼──────┼────────┼────────┤
│ Setup Complexity   │ Medium │ High │ Low    │ High   │
│ Maturity           │ ✅     │ ✅   │ ✅     │ ⚠️     │
│ Multi-engine       │ ✅     │ ✅   │ ⚠️ AWS │ ✅     │
│ Versioning         │ ❌     │ ❌   │ ❌     │ ✅     │
│ Embedded Option    │ ⚠️     │ ❌   │ ❌     │ ❌     │
│ Recommended 2025   │ ✅✅   │ ✅   │ ✅     │ ✅     │
│ iceberg-rs Support │ ✅     │ ⚠️   │ ⚠️     │ ⚠️     │
└────────────────────┴────────┴──────┴────────┴────────┘
```

---

## Conclusion

This PRD transforms your Kafka → Iceberg CLI from a single-table prototype into a **production-grade, multi-catalog ingestion engine** that competes with Mooncake/Moonlink while maintaining simplicity and embedded deployability.

**Key deliverable:** Production-ready Iceberg catalog integration using `iceberg-rs`, supporting REST, Hive, Glue, and Nessie catalogs with atomic transactions, idempotency, and comprehensive error handling.

**Timeline:** 2 weeks (Week 3-4) to add full catalog support without disrupting the core ingestion pipeline.

**Outcome:** Market-ready product that works with any Iceberg catalog, giving customers flexibility while you focus on the core value: simple, fast, automated Kafka → Iceberg ingestion at 1M events/sec.

---

**Document Status:** ✅ Ready for Architecture Review  
**Last Updated:** January 8, 2026  
**Next Step:** Schedule architecture review with engineering team, finalize catalog implementation decisions

