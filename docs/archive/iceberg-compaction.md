# Product Requirements Document (PRD)
# Distributed Compaction Engine for Apache Iceberg
## Built with Rust & Apache DataFusion

---

**Document Version:** 1.0
**Date:** January 30, 2026
**Author:** OSO Engineering
**Status:** Draft

---

## Executive Summary

This PRD defines the requirements for building a high-performance, distributed compaction engine for Apache Iceberg tables using Rust and Apache DataFusion. The engine addresses the critical "small file problem" that degrades query performance in data lakes, delivering 5-10x faster compaction than Apache Spark while reducing operational costs by 80-90%.

The solution leverages Rust's memory safety and performance characteristics combined with DataFusion's vectorized query execution to provide an embedded compaction service that eliminates the need for separate Spark clusters, reduces infrastructure complexity, and dramatically improves table maintenance efficiency.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Goals & Success Metrics](#2-goals--success-metrics)
3. [User Personas](#3-user-personas)
4. [Functional Requirements](#4-functional-requirements)
5. [System Architecture](#5-system-architecture)
6. [Technical Specifications](#6-technical-specifications)
7. [Compaction Strategies](#7-compaction-strategies)
8. [API Design](#8-api-design)
9. [Non-Functional Requirements](#9-non-functional-requirements)
10. [Integration Points](#10-integration-points)
11. [Deployment Architecture](#11-deployment-architecture)
12. [Milestones & Roadmap](#12-milestones--roadmap)
13. [Risks & Mitigations](#13-risks--mitigations)
14. [Appendix](#appendix)

---

## 1. Problem Statement

### 1.1 The Small File Problem

Apache Iceberg's architecture—while enabling powerful features like ACID transactions, time travel, and schema evolution—creates significant performance overhead over time. Every write operation generates a new **snapshot**, and streaming workloads that write data continuously accumulate files rapidly.

**Key Pain Points:**

| Problem | Impact | Root Cause |
|---------|--------|------------|
| File Proliferation | Query latency increases 10-100x | Streaming writes create many small files (< 100MB) |
| Metadata Bloat | Planning time degrades | Thousands of manifest entries to scan |
| Read Amplification | I/O overhead increases | Delete files accumulate, requiring merge-on-read |
| Storage Fragmentation | Costs increase | Suboptimal file sizes waste storage IOPS |

### 1.2 Current Solutions Are Insufficient

**Apache Spark-based Compaction:**
- Requires dedicated cluster infrastructure
- JVM startup overhead (30-60 seconds cold start)
- High memory consumption for metadata-heavy workloads
- Costly at scale (~$50/TB for managed services)

**Managed Services (AWS S3 Tables, Databricks):**
- Limited configurability
- Vendor lock-in concerns
- Higher operational costs ($50/TB typical)

### 1.3 Opportunity

A Rust-based compaction engine can deliver:
- **5.5x faster** bin-pack compaction vs. Spark
- **90% cost reduction** ($5/TB vs. $50/TB)
- **Memory efficiency** enabling processing on standard hardware
- **Embedded deployment** eliminating cluster management overhead

---

## 2. Goals & Success Metrics

### 2.1 Primary Goals

| Goal | Description | Priority |
|------|-------------|----------|
| **G1** | Reduce compaction time by 5x vs. Spark baseline | P0 |
| **G2** | Process delete-heavy workloads without OOM failures | P0 |
| **G3** | Achieve < $10/TB compaction cost | P0 |
| **G4** | Support all Iceberg compaction strategies | P1 |
| **G5** | Enable embedded deployment without external dependencies | P1 |
| **G6** | Provide production-ready observability | P1 |

### 2.2 Success Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Bin-pack compaction throughput | > 500 GB/hour on m5.4xlarge | Benchmark suite |
| Sort compaction throughput | > 250 GB/hour on m5.4xlarge | Benchmark suite |
| Memory efficiency | < 50% utilization for 200GB workload | Resource monitoring |
| Delete file processing | 60K files without OOM | Stress test |
| Cost per TB compacted | < $10 | AWS billing analysis |
| API latency (job submission) | < 100ms p99 | APM metrics |

### 2.3 Non-Goals (Out of Scope v1.0)

- Real-time incremental compaction during writes
- Cross-region compaction orchestration
- Custom file format support beyond Parquet
- GUI management interface

---

## 3. User Personas

### 3.1 Primary Persona: Platform Engineer

**Name:** Sarah, Data Platform Lead
**Context:** Manages Iceberg-based lakehouse for 50+ data engineers
**Pain Points:**
- Spark compaction jobs are expensive and unreliable
- Must maintain separate infrastructure for table maintenance
- Difficult to tune compaction for different table characteristics

**Needs:**
- Simple API for scheduling compaction
- Cost-effective execution
- Integration with existing observability stack

### 3.2 Secondary Persona: Streaming Engineer

**Name:** Alex, Real-Time Data Engineer
**Context:** Operates Kafka-to-Iceberg streaming pipelines
**Pain Points:**
- Tables degrade within hours due to small file accumulation
- Cannot run compaction during business hours without impacting queries
- Delete-heavy tables (CDC) cause Spark OOM

**Needs:**
- Continuous background compaction
- Resource isolation from query workloads
- Reliable delete file processing

---

## 4. Functional Requirements

### 4.1 Core Compaction Operations

#### FR-1: Bin-Pack Compaction
**Description:** Merge small data files into larger files based on target file size.

**Acceptance Criteria:**
- [ ] Merge files below `min-file-size-bytes` threshold
- [ ] Target output files of configurable size (default: 512MB)
- [ ] Preserve partition boundaries
- [ ] Support filtered compaction (partition/time-range scoping)

**Input Parameters:**
```
table_identifier: String
target_file_size_bytes: u64 (default: 536870912)
min_file_size_bytes: u64 (default: 104857600)
max_file_size_bytes: u64 (default: 1073741824)
filter_expression: Option<Expression>
```

#### FR-2: Sort-Based Compaction
**Description:** Rewrite data files with sorted order for improved query performance.

**Acceptance Criteria:**
- [ ] Sort by table's defined sort order
- [ ] Support custom sort expressions
- [ ] Maintain target file size constraints
- [ ] Generate properly partitioned output

**Input Parameters:**
```
table_identifier: String
sort_order: SortOrder
target_file_size_bytes: u64
```

#### FR-3: Z-Order Compaction
**Description:** Apply Z-order clustering for multi-dimensional query optimization.

**Acceptance Criteria:**
- [ ] Accept list of columns for Z-value generation
- [ ] Interleave column bits for spatial locality
- [ ] Support up to 8 clustering columns
- [ ] Validate column data types (numeric, string, timestamp)

#### FR-4: Delete File Compaction
**Description:** Merge delete files with data files, physically removing deleted rows.

**Acceptance Criteria:**
- [ ] Process position delete files
- [ ] Process equality delete files
- [ ] Handle mixed delete file types
- [ ] Support configurable delete file threshold

**Performance Target:** Process 40K delete files without OOM on 64GB RAM.

### 4.2 Table Maintenance Operations

#### FR-5: Snapshot Expiration
**Description:** Remove snapshots older than retention policy.

**Acceptance Criteria:**
- [ ] Expire by age (`max_age_ms`)
- [ ] Expire by count (`retain_last`)
- [ ] Clean up associated manifest files
- [ ] Report expired snapshot count

#### FR-6: Orphan File Cleanup
**Description:** Remove data files not referenced by any snapshot.

**Acceptance Criteria:**
- [ ] Scan storage for unreferenced files
- [ ] Configurable dry-run mode
- [ ] Age threshold for safety (default: 3 days)
- [ ] Report bytes reclaimed

#### FR-7: Manifest Rewrite
**Description:** Optimize manifest file layout for scan planning.

**Acceptance Criteria:**
- [ ] Rewrite manifests to target size
- [ ] Cluster manifest entries by partition
- [ ] Reduce manifest count for small tables

### 4.3 Execution Control

#### FR-8: File Group Management
**Description:** Organize compaction work into parallelizable file groups.

**Parameters:**
```
max_file_group_size_bytes: u64 (default: 107374182400) // 100GB
max_concurrent_file_group_rewrites: u32 (default: 5)
partial_progress_enabled: bool (default: true)
partial_progress_max_commits: u32 (default: 10)
```

#### FR-9: Job Ordering
**Description:** Control execution order of file groups.

**Options:**
- `bytes-asc`: Smallest groups first
- `bytes-desc`: Largest groups first
- `files-asc`: Fewest files first
- `files-desc`: Most files first
- `none`: No ordering (default)

#### FR-10: Conflict Resolution
**Description:** Handle concurrent modifications during compaction.

**Acceptance Criteria:**
- [ ] Detect commit conflicts
- [ ] Retry failed commits with configurable attempts
- [ ] Support partial progress for conflict isolation
- [ ] Report conflict statistics

---

## 5. System Architecture

### 5.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Compaction Engine                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
│  │   REST API  │    │  Scheduler  │    │   Metrics Exporter      │ │
│  │   (Axum)    │    │  (Tokio)    │    │   (Prometheus)          │ │
│  └──────┬──────┘    └──────┬──────┘    └───────────┬─────────────┘ │
│         │                  │                       │               │
│         └──────────────────┼───────────────────────┘               │
│                            │                                        │
│  ┌─────────────────────────▼─────────────────────────────────────┐ │
│  │                   Job Coordinator                              │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │ │
│  │  │ File Group  │  │  Conflict   │  │  Progress Tracker   │   │ │
│  │  │  Planner    │  │  Handler    │  │                     │   │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘   │ │
│  └─────────────────────────┬─────────────────────────────────────┘ │
│                            │                                        │
│  ┌─────────────────────────▼─────────────────────────────────────┐ │
│  │                   Execution Layer                              │ │
│  │  ┌──────────────────────────────────────────────────────────┐ │ │
│  │  │               Apache DataFusion                          │ │ │
│  │  │  ┌─────────┐  ┌──────────┐  ┌────────────────────────┐  │ │ │
│  │  │  │ Logical │  │ Physical │  │ Vectorized Execution   │  │ │ │
│  │  │  │ Planner │  │ Planner  │  │ (Arrow RecordBatches)  │  │ │ │
│  │  │  └─────────┘  └──────────┘  └────────────────────────┘  │ │ │
│  │  └──────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────┬─────────────────────────────────────┘ │
│                            │                                        │
│  ┌─────────────────────────▼─────────────────────────────────────┐ │
│  │                    Storage Layer                               │ │
│  │  ┌─────────────────────────────────────────────────────────┐  │ │
│  │  │              iceberg-rust + OpenDAL                      │  │ │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────┐  │  │ │
│  │  │  │   S3    │  │  GCS    │  │  Azure  │  │  Local FS │  │  │ │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └───────────┘  │  │ │
│  │  └─────────────────────────────────────────────────────────┘  │ │
│  └───────────────────────────────────────────────────────────────┘ │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Iceberg Catalog                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │
│  │  REST API   │  │  AWS Glue   │  │    Hive     │  │  Nessie   │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Component Responsibilities

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| REST API | Job submission, status queries, metrics | Axum, Tower |
| Scheduler | Cron-based and event-driven job triggering | Tokio, cron crate |
| Job Coordinator | Work distribution, progress tracking | Custom Rust |
| File Group Planner | Partition analysis, group formation | iceberg-rust |
| Conflict Handler | Optimistic concurrency, retry logic | iceberg-rust |
| Execution Layer | Query plan execution | DataFusion |
| Storage Layer | File I/O abstraction | OpenDAL |

### 5.3 Data Flow

```
1. Job Submission
   │
   ▼
2. Load Table Metadata (Catalog)
   │
   ▼
3. Scan Manifest Files
   │
   ▼
4. Identify Compaction Candidates
   │  - Files below min size
   │  - Files with delete associations
   │  - Unoptimized partitions
   │
   ▼
5. Form File Groups
   │  - Respect max group size
   │  - Maintain partition boundaries
   │
   ▼
6. Execute File Groups (Parallel)
   │  ┌─────────────────────────────┐
   │  │  For each file group:       │
   │  │  1. Read source files       │
   │  │  2. Apply transformations   │
   │  │  3. Write output files      │
   │  │  4. Commit to catalog       │
   │  └─────────────────────────────┘
   │
   ▼
7. Report Results
```

---

## 6. Technical Specifications

### 6.1 Core Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `iceberg-rust` | 0.9.x | Iceberg table operations |
| `datafusion` | 44.x | Query execution engine |
| `datafusion-iceberg` | 0.9.x | DataFusion-Iceberg integration |
| `arrow` | 54.x | In-memory columnar format |
| `parquet` | 54.x | Parquet file I/O |
| `tokio` | 1.x | Async runtime |
| `axum` | 0.7.x | HTTP framework |
| `opendal` | 0.50.x | Storage abstraction |

### 6.2 Execution Model

**Threading Model:**
```rust
// Tokio runtime configuration
let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_cpus::get())
    .enable_all()
    .build()?;
```

**DataFusion Session Configuration:**
```rust
let config = SessionConfig::new()
    .with_target_partitions(execution_parallelism)
    .with_batch_size(8192)
    .with_parquet_pruning(true)
    .with_collect_statistics(true);

let ctx = SessionContext::new_with_config(config);
```

### 6.3 Memory Management

| Parameter | Default | Description |
|-----------|---------|-------------|
| `memory_limit` | 70% system RAM | Maximum memory for execution |
| `spill_threshold` | 80% of limit | Threshold to begin spilling |
| `batch_size` | 8192 rows | Arrow batch size |
| `parquet_row_group_size` | 1M rows | Output Parquet row group |

### 6.4 File I/O Specifications

**Read Configuration:**
```rust
ParquetReadOptions {
    pushdown_filters: true,
    reorder_filters: true,
    enable_page_index: true,
    schema_adapter: true,
}
```

**Write Configuration:**
```rust
ParquetWriterOptions {
    compression: Compression::ZSTD(ZstdLevel::try_new(3)?),
    write_batch_size: 1024,
    data_page_size: 1024 * 1024,  // 1MB
    max_row_group_size: 1_000_000,
}
```

---

## 7. Compaction Strategies

### 7.1 Strategy Comparison

| Strategy | Speed | Query Benefit | Write Amplification | Use Case |
|----------|-------|---------------|---------------------|----------|
| Bin-pack | Fastest | Moderate | Low (1-2x) | Streaming tables |
| Sort | Medium | High | Medium (2-3x) | Range query tables |
| Z-Order | Slowest | Highest | High (3-4x) | Multi-dimensional queries |

### 7.2 Bin-Pack Algorithm

```
Input: Set of data files F, target size T, min size M
Output: Set of file groups G

1. Filter F to files where size < T
2. Sort filtered files by partition, then by size descending
3. For each partition P:
   a. Initialize current_group = []
   b. Initialize current_size = 0
   c. For each file f in P:
      - If current_size + f.size > T:
        - Emit current_group
        - Reset current_group, current_size
      - Add f to current_group
      - current_size += f.size
   d. If current_size >= M:
      - Emit current_group
4. Return G
```

### 7.3 Sort Strategy Implementation

```rust
// DataFusion logical plan for sorted compaction
let plan = LogicalPlanBuilder::scan(table_name, source, None)?
    .sort(sort_exprs)?  // Apply table sort order
    .build()?;

// Physical plan with repartitioning
let physical = ctx.create_physical_plan(&plan).await?;
let repartitioned = RepartitionExec::try_new(
    physical,
    Partitioning::RoundRobinBatch(output_parallelism),
)?;
```

### 7.4 Delete File Processing

**Position Delete Handling:**
```rust
// Read position deletes
let delete_positions: HashSet<(String, i64)> =
    read_position_deletes(&delete_files).await?;

// Filter during scan
let filtered = source_batch.filter(|row| {
    !delete_positions.contains(&(row.file_path(), row.pos()))
})?;
```

**Equality Delete Handling:**
```rust
// Build anti-join predicate from equality deletes
let delete_predicates = equality_deletes
    .iter()
    .map(|d| build_equality_predicate(d))
    .collect::<Vec<_>>();

// Apply as filter pushdown
let plan = LogicalPlanBuilder::scan(table_name, source, None)?
    .filter(not(or_predicates(delete_predicates)))?
    .build()?;
```

---

## 8. API Design

### 8.1 REST API Endpoints

#### Submit Compaction Job
```http
POST /v1/tables/{namespace}/{table}/compact
Content-Type: application/json

{
  "strategy": "binpack",
  "options": {
    "target_file_size_bytes": 536870912,
    "max_concurrent_file_group_rewrites": 5,
    "partial_progress_enabled": true
  },
  "filter": {
    "partition_filter": "date >= '2026-01-01'"
  }
}
```

**Response:**
```json
{
  "job_id": "cmp-abc123",
  "status": "SUBMITTED",
  "submitted_at": "2026-01-30T09:20:00Z",
  "estimated_file_groups": 15
}
```

#### Get Job Status
```http
GET /v1/jobs/{job_id}
```

**Response:**
```json
{
  "job_id": "cmp-abc123",
  "status": "RUNNING",
  "progress": {
    "file_groups_total": 15,
    "file_groups_completed": 8,
    "bytes_processed": 85899345920,
    "files_rewritten": 1247,
    "files_added": 156
  },
  "started_at": "2026-01-30T09:20:05Z",
  "current_duration_ms": 185000
}
```

#### List Jobs
```http
GET /v1/jobs?table={namespace}.{table}&status=RUNNING
```

### 8.2 Rust SDK Interface

```rust
use iceberg_compactor::{CompactionEngine, CompactionOptions, Strategy};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize engine with catalog
    let engine = CompactionEngine::builder()
        .with_catalog(catalog)
        .with_parallelism(16)
        .with_memory_limit_gb(48)
        .build()
        .await?;

    // Configure compaction
    let options = CompactionOptions::builder()
        .strategy(Strategy::BinPack)
        .target_file_size_mb(512)
        .filter("date >= '2026-01-01'")
        .build();

    // Execute compaction
    let result = engine
        .compact_table("analytics.events", options)
        .await?;

    println!("Compacted {} files into {} files",
             result.rewritten_files_count,
             result.added_files_count);

    Ok(())
}
```

### 8.3 CLI Interface

```bash
# Bin-pack compaction
iceberg-compact binpack \
  --catalog rest \
  --catalog-uri http://localhost:8181 \
  --table analytics.events \
  --target-file-size 512MB \
  --parallelism 16

# Sort compaction with filter
iceberg-compact sort \
  --table analytics.events \
  --sort-by "event_time ASC, user_id ASC" \
  --filter "date >= '2026-01-01'" \
  --dry-run

# Z-order clustering
iceberg-compact zorder \
  --table analytics.events \
  --columns user_id,event_type,region
```

---

## 9. Non-Functional Requirements

### 9.1 Performance Requirements

| Requirement | Specification | Validation |
|-------------|---------------|------------|
| **NFR-1** | Bin-pack 100GB in < 5 minutes (m5.4xlarge) | Benchmark |
| **NFR-2** | Sort 100GB in < 15 minutes (m5.4xlarge) | Benchmark |
| **NFR-3** | Process 60K delete files without OOM (64GB) | Stress test |
| **NFR-4** | Startup time < 5 seconds | Cold start test |
| **NFR-5** | API response < 100ms p99 | Load test |

### 9.2 Reliability Requirements

| Requirement | Specification |
|-------------|---------------|
| **NFR-6** | Graceful shutdown with in-progress file group completion |
| **NFR-7** | Automatic retry on transient failures (3 attempts) |
| **NFR-8** | Checkpoint progress for job resumption |
| **NFR-9** | No data loss on crash (atomic commits) |

### 9.3 Observability Requirements

| Requirement | Specification |
|-------------|---------------|
| **NFR-10** | Prometheus metrics endpoint (`/metrics`) |
| **NFR-11** | Structured JSON logging (tracing crate) |
| **NFR-12** | OpenTelemetry trace propagation |
| **NFR-13** | Health check endpoint (`/health`) |

**Key Metrics:**
```
# Compaction throughput
compactor_bytes_processed_total{table, strategy}
compactor_files_rewritten_total{table, strategy}
compactor_files_added_total{table, strategy}

# Performance
compactor_job_duration_seconds{table, strategy, status}
compactor_file_group_duration_seconds{table}

# Resources
compactor_memory_usage_bytes
compactor_active_file_groups

# Errors
compactor_errors_total{table, error_type}
compactor_commit_conflicts_total{table}
```

### 9.4 Security Requirements

| Requirement | Specification |
|-------------|---------------|
| **NFR-14** | Support AWS IAM, GCP Service Accounts, Azure AD |
| **NFR-15** | TLS 1.3 for API endpoints |
| **NFR-16** | Catalog credential pass-through |
| **NFR-17** | No credentials in logs |

---

## 10. Integration Points

### 10.1 Catalog Integrations

| Catalog | Priority | Authentication |
|---------|----------|----------------|
| Iceberg REST Catalog | P0 | OAuth2, Bearer Token |
| AWS Glue | P0 | IAM Role, Access Keys |
| Hive Metastore | P1 | Kerberos, Simple |
| Nessie | P1 | Bearer Token |
| Polaris | P2 | OAuth2 |

### 10.2 Storage Integrations

| Storage | Priority | Features |
|---------|----------|----------|
| AWS S3 | P0 | SSE, Multipart, Express One Zone |
| S3-Compatible (MinIO) | P0 | Path-style, Virtual-hosted |
| Google Cloud Storage | P1 | IAM, HMAC |
| Azure Blob Storage | P1 | SAS, Managed Identity |
| Local Filesystem | P1 | Development/testing |

### 10.3 Orchestration Integrations

| System | Integration Type |
|--------|-----------------|
| Apache Airflow | REST API + Custom Operator |
| Dagster | REST API + Resource |
| Kubernetes CronJob | CLI + Container Image |
| AWS Step Functions | Lambda trigger |

---

## 11. Deployment Architecture

### 11.1 Standalone Deployment

```yaml
# docker-compose.yml
version: '3.8'
services:
  compactor:
    image: ghcr.io/oso/iceberg-compactor:latest
    environment:
      - CATALOG_URI=http://rest-catalog:8181
      - CATALOG_WAREHOUSE=s3://my-warehouse
      - AWS_REGION=us-east-1
      - EXECUTION_PARALLELISM=16
      - MEMORY_LIMIT_GB=48
    ports:
      - "8080:8080"
    deploy:
      resources:
        limits:
          cpus: '16'
          memory: 64G
```

### 11.2 Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: iceberg-compactor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: iceberg-compactor
  template:
    metadata:
      labels:
        app: iceberg-compactor
    spec:
      serviceAccountName: iceberg-compactor
      containers:
      - name: compactor
        image: ghcr.io/oso/iceberg-compactor:latest
        resources:
          requests:
            cpu: "8"
            memory: "32Gi"
          limits:
            cpu: "16"
            memory: "64Gi"
        ports:
        - containerPort: 8080
        env:
        - name: CATALOG_URI
          valueFrom:
            configMapKeyRef:
              name: compactor-config
              key: catalog_uri
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: iceberg-compactor
spec:
  selector:
    app: iceberg-compactor
  ports:
  - port: 8080
    targetPort: 8080
```

### 11.3 Embedded Deployment (Library Mode)

```rust
// Embed compactor in existing Rust application
use iceberg_compactor::EmbeddedCompactor;

let compactor = EmbeddedCompactor::new(catalog)
    .with_background_scheduler(schedule)
    .build();

// Compactor runs as background task
compactor.start().await;
```

---

## 12. Milestones & Roadmap

### Phase 1: Foundation (Weeks 1-4)

| Milestone | Deliverables | Success Criteria |
|-----------|--------------|------------------|
| M1.1 | Project scaffolding, CI/CD | Tests pass, builds publish |
| M1.2 | iceberg-rust integration | Load/commit table metadata |
| M1.3 | DataFusion integration | Execute scan queries |
| M1.4 | Basic bin-pack compaction | 100GB compaction completes |

### Phase 2: Core Features (Weeks 5-8)

| Milestone | Deliverables | Success Criteria |
|-----------|--------------|------------------|
| M2.1 | Sort compaction | Sorted output verified |
| M2.2 | Delete file processing | Position + equality deletes |
| M2.3 | File group parallelism | Concurrent execution |
| M2.4 | Partial progress commits | Conflict isolation works |

### Phase 3: Production Readiness (Weeks 9-12)

| Milestone | Deliverables | Success Criteria |
|-----------|--------------|------------------|
| M3.1 | REST API | OpenAPI spec compliant |
| M3.2 | CLI tool | All commands functional |
| M3.3 | Observability | Prometheus + tracing |
| M3.4 | Benchmark suite | Performance targets met |

### Phase 4: Ecosystem (Weeks 13-16)

| Milestone | Deliverables | Success Criteria |
|-----------|--------------|------------------|
| M4.1 | Z-order compaction | Clustering verified |
| M4.2 | Multi-catalog support | Glue + Hive tested |
| M4.3 | Container images | Multi-arch published |
| M4.4 | Documentation | User guide complete |

---

## 13. Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| iceberg-rust API instability | Medium | High | Pin versions, contribute upstream fixes |
| Memory pressure on large tables | Medium | High | Implement spill-to-disk, streaming execution |
| Catalog commit conflicts | High | Medium | Partial progress, exponential backoff |
| DataFusion performance regression | Low | Medium | Benchmark CI, version pinning |
| S3 API rate limiting | Medium | Medium | Request batching, exponential backoff |
| Complex delete file scenarios | Medium | High | Extensive test coverage, fallback modes |

---

## Appendix

### A. Benchmark Reference Data

**Test Environment:** AWS m5.4xlarge (16 vCPU, 64GB RAM), GP3 EBS

| Scenario | Data Size | Files | Rust Engine | Spark | Speedup |
|----------|-----------|-------|-------------|-------|---------|
| Bin-pack (no compression) | 193GB | 17,358 | 277s | 1,533s | 5.5x |
| Bin-pack (ZSTD) | 200GB | 18,000 | 221s | 1,805s | 8.2x |
| Sort compaction | 200GB | 18,000 | 780s | 1,612s | 2.1x |
| High-entropy deletes | 60K files | 40K deletes | 490s | OOM | ∞ |

### B. Configuration Reference

```toml
# compactor.toml

[catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://my-warehouse"

[storage]
type = "s3"
region = "us-east-1"
# endpoint = "http://minio:9000"  # For S3-compatible

[execution]
parallelism = 16
output_parallelism = 16
memory_limit_gb = 48
batch_size = 8192

[compaction]
default_target_file_size_mb = 512
default_min_file_size_mb = 100
partial_progress_enabled = true
max_concurrent_file_group_rewrites = 5

[observability]
metrics_port = 9090
log_level = "info"
log_format = "json"
```

### C. Glossary

| Term | Definition |
|------|------------|
| **Bin-pack** | Compaction strategy that merges small files without sorting |
| **File Group** | Unit of compaction work containing files from single partition |
| **Manifest** | Iceberg metadata file listing data files |
| **Position Delete** | Delete marker specifying file path and row position |
| **Equality Delete** | Delete marker specifying column values to delete |
| **Snapshot** | Point-in-time view of table data |
| **Z-Order** | Space-filling curve for multi-dimensional clustering |

---

**Document Approval**

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Product Owner | | | |
| Tech Lead | | | |
| Engineering Manager | | | |
