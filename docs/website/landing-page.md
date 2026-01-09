# K2I Microsite Landing Page

> **Note:** This document contains all the content sections for the K2I marketing website.
> - Whitepaper: Links to `/whitepaper` (separate page)
> - Documentation: Links to `docs.k2i.dev` (subdomain)
> - GitHub: Links to `github.com/osodevops/k2i`

---

## Navigation

```
[Logo: K2I]     [Product]  [Whitepaper]  [Docs]  [Blog]  [GitHub]     [Get Started]
```

---

# SECTION 1: Hero

## Headline

```
Real-time streaming ingestion for Apache Iceberg
```

## Subheadline

```
K2I bridges the gap between Kafka and your data lakehouse.
Sub-second freshness. Exactly-once delivery. Zero operational overhead.
```

## Hero Description

K2I (Kafka to Iceberg) is a high-performance streaming ingestion engine that consumes events from Apache Kafka and writes them to Apache Iceberg tables with sub-second latency. Built in Rust for maximum performance, K2I eliminates the complexity of streaming pipelines while delivering the reliability your data team needs.

## Primary CTA

```
[Get Started]  [Read the Whitepaper]
```

## Hero Visual

```
+------------------+                      +------------------+
|                  |                      |                  |
|      KAFKA       |                      |     ICEBERG      |
|                  |                      |                  |
|  +------------+  |                      |  +------------+  |
|  | topic-1    |  |                      |  | table-1    |  |
|  +------------+  |                      |  +------------+  |
|  | topic-2    |  |     +----------+     |  | table-2    |  |
|  +------------+  |---->|   K2I    |---->|  +------------+  |
|  | topic-3    |  |     +----------+     |  | table-3    |  |
|  +------------+  |      < 1 sec         |  +------------+  |
|                  |                      |                  |
+------------------+                      +------------------+
     Events                                   Analytics
```

## Badge/Social Proof Bar

```
[GitHub Stars: 200+]  [Apache 2.0 License]  [Built with Rust]
```

---

# SECTION 2: The Problem

## Section Headline

```
The streaming-to-lakehouse gap is costing you
```

## Problem Statement

Modern data teams want Apache Iceberg for their analytics workloads. Open table formats, time travel, schema evolution, and cost-efficient object storage. But getting data from Kafka into Iceberg is harder than it should be.

## The Three Problems (Card Layout)

### Card 1: Latency vs. Cost

```
+------------------------------------------+
|  [Icon: Clock/Dollar]                    |
|                                          |
|  THE LATENCY-COST TRADE-OFF              |
|                                          |
|  Real-time streaming: milliseconds,      |
|  but expensive and complex.              |
|                                          |
|  Batch ETL: cost-efficient,              |
|  but hours of delay.                     |
|                                          |
|  You shouldn't have to choose.           |
+------------------------------------------+
```

### Card 2: Small File Hell

```
+------------------------------------------+
|  [Icon: Files/Warning]                   |
|                                          |
|  THE SMALL FILE PROBLEM                  |
|                                          |
|  Streaming into Iceberg creates          |
|  thousands of tiny files per hour.       |
|                                          |
|  Query performance degrades.             |
|  Metadata explodes.                      |
|  Storage costs spiral.                   |
|                                          |
|  Compaction becomes a full-time job.     |
+------------------------------------------+
```

### Card 3: Exactly-Once Nightmares

```
+------------------------------------------+
|  [Icon: Shield/Check]                    |
|                                          |
|  EXACTLY-ONCE IS HARD                    |
|                                          |
|  Coordinating Kafka offsets,             |
|  object storage writes, and              |
|  catalog commits is complex.             |
|                                          |
|  One failure = data loss or duplication. |
|                                          |
|  You need guarantees, not hope.          |
+------------------------------------------+
```

## Comparison Table

| Current Approach | Latency | Files/Hour | Operational Burden |
|------------------|---------|------------|-------------------|
| Spark Streaming | 30s - 5min | 100-1000+ | High (cluster ops) |
| Flink + Iceberg Sink | 1-10s | 50-500 | High (stateful ops) |
| Kafka Connect | 5-60s | 100-1000+ | Medium |
| **K2I** | **< 1 second** | **1-10** | **Minimal** |

---

# SECTION 3: The Solution

## Section Headline

```
K2I: Real-time Iceberg, simplified
```

## Solution Statement

K2I is a purpose-built streaming ingestion engine that solves the Kafka-to-Iceberg problem once and for all. A single Rust binary that runs anywhere, handles everything, and just works.

## Three Pillars (Feature Highlights)

### Pillar 1: Sub-Second Freshness

**Headline:** Query your data in under a second

**Description:** K2I maintains an in-memory hot buffer using Apache Arrow. New events are immediately queryable while being accumulated for optimal file writes. No more waiting for batch windows.

**Key Metric:**
```
< 1 second
end-to-end latency from Kafka to queryable
```

### Pillar 2: Exactly-Once Delivery

**Headline:** Never lose or duplicate a single event

**Description:** K2I uses a write-ahead transaction log to coordinate Kafka consumption, object storage writes, and catalog commits. Crash at any point? K2I recovers automatically and resumes exactly where it left off.

**Key Metric:**
```
Zero data loss
even through crashes and restarts
```

### Pillar 3: Zero Operations

**Headline:** Set it and forget it

**Description:** K2I handles compaction, snapshot expiration, and orphan file cleanup automatically. No cron jobs. No manual intervention. No 3am pages about metadata table bloat.

**Key Metric:**
```
1 binary
no cluster, no dependencies, no ops
```

---

# SECTION 4: How It Works

## Section Headline

```
A single process that does everything right
```

## Architecture Description

K2I is a single-process, embedded engine inspired by the Moonlink architecture. It combines in-memory hot buffering with durable cold storage, delivering real-time freshness with lakehouse economics.

## Architecture Diagram

```
+-----------------------------------------------------------------------------+
|                              K2I ENGINE                                     |
+-----------------------------------------------------------------------------+
|                                                                             |
|   INGEST                    BUFFER                      PERSIST             |
|                                                                             |
|   +------------------+      +------------------+      +------------------+  |
|   |                  |      |                  |      |                  |  |
|   |  Kafka Consumer  |----->|   Hot Buffer     |----->|  Iceberg Writer  |  |
|   |                  |      |   (Arrow)        |      |  (Parquet)       |  |
|   |  - Backpressure  |      |                  |      |                  |  |
|   |  - Retry logic   |      |  - O(1) lookups  |      |  - Atomic commit |  |
|   |  - Batch collect |      |  - TTL eviction  |      |  - CAS conflicts |  |
|   |                  |      |  - Memory bound  |      |  - Multi-catalog |  |
|   +------------------+      +------------------+      +------------------+  |
|            |                        |                        |              |
|            |                        |                        |              |
|            v                        v                        v              |
|   +---------------------------------------------------------------------+   |
|   |                      TRANSACTION LOG                                |   |
|   |                                                                     |   |
|   |   Append-only  |  CRC32 checksums  |  Periodic checkpoints         |   |
|   |   Crash recovery  |  Exactly-once  |  Idempotency tracking         |   |
|   +---------------------------------------------------------------------+   |
|                                                                             |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                          YOUR INFRASTRUCTURE                                |
+-----------------------------------------------------------------------------+
|                                                                             |
|   +------------------+      +------------------+      +------------------+  |
|   |  Kafka           |      |  Iceberg Catalog |      |  Object Storage  |  |
|   |                  |      |                  |      |                  |  |
|   |  Any Kafka 2.8+  |      |  - REST API      |      |  - Amazon S3     |  |
|   |  MSK, Confluent  |      |  - Hive Metastore|      |  - Google GCS    |  |
|   |  Redpanda, etc.  |      |  - AWS Glue      |      |  - Azure Blob    |  |
|   |                  |      |  - Nessie        |      |  - MinIO / Local |  |
|   +------------------+      +------------------+      +------------------+  |
|                                                                             |
+-----------------------------------------------------------------------------+
```

## Data Flow Steps

### Step 1: Consume
K2I pulls messages from Kafka in intelligent batches. If the buffer is full, backpressure automatically pauses consumption. No data is acknowledged until it's durably logged.

### Step 2: Buffer
Events are converted to Apache Arrow columnar format and held in an in-memory hot buffer. Hash indexes enable O(1) lookups by key or offset. Data is immediately queryable.

### Step 3: Flush
When the buffer reaches size, time, or count thresholds, K2I encodes records as Parquet and uploads to object storage. The transaction log tracks every step.

### Step 4: Commit
K2I atomically commits the new data files to your Iceberg catalog. Only after successful catalog commit are Kafka offsets acknowledged. Exactly-once, guaranteed.

### Step 5: Maintain
Background tasks automatically compact small files, expire old snapshots, and clean up orphans. Your table stays healthy without manual intervention.

---

# SECTION 5: Key Features

## Section Headline

```
Everything you need for production streaming
```

## Feature Grid (2x3 or 3x2)

### Feature 1: Hot/Cold Architecture

**Icon:** Temperature/Layers

**Title:** Hot Buffer + Cold Storage

**Description:** Sub-millisecond queries on recent data via in-memory Arrow buffer. Cost-efficient analytics on historical data via Parquet on object storage. Best of both worlds.

### Feature 2: Write-Ahead Logging

**Icon:** Document/Shield

**Title:** Transaction Log

**Description:** Every operation is logged before execution. Crash at any point and K2I recovers automatically. No manual intervention, no data loss, no duplicates.

### Feature 3: Smart Backpressure

**Icon:** Gauge/Pause

**Title:** Intelligent Flow Control

**Description:** When the buffer fills up, K2I automatically pauses Kafka consumption. No OOM crashes, no dropped messages. Resume seamlessly when capacity returns.

### Feature 4: Multi-Catalog Support

**Icon:** Database/Grid

**Title:** Your Catalog, Your Choice

**Description:** Native support for REST, Hive Metastore, AWS Glue, and Nessie catalogs. OAuth2, IAM roles, and custom auth. Works with your existing infrastructure.

### Feature 5: Automated Maintenance

**Icon:** Wrench/Clock

**Title:** Self-Maintaining Tables

**Description:** Built-in compaction merges small files. Snapshot expiration controls metadata growth. Orphan cleanup removes failed operation debris. All automatic.

### Feature 6: Production Observability

**Icon:** Chart/Eye

**Title:** Full Visibility

**Description:** Prometheus metrics for throughput, latency, buffer utilization, and catalog health. Structured JSON logging. Kubernetes-ready health endpoints.

---

# SECTION 6: Performance

## Section Headline

```
Benchmarks that matter
```

## Key Metrics (Large Numbers)

```
+--------------------+    +--------------------+    +--------------------+
|                    |    |                    |    |                    |
|     < 1 sec        |    |    100K msg/s      |    |     1-10 files     |
|                    |    |                    |    |                    |
|  Query Freshness   |    |    Throughput      |    |    Per Hour        |
|                    |    |    (tuned)         |    |    (vs. 1000+)     |
+--------------------+    +--------------------+    +--------------------+
```

## Performance Table

| Metric | Value | Notes |
|--------|-------|-------|
| **Query Freshness (Hot)** | < 1ms | In-memory Arrow buffer |
| **Query Freshness (Cold)** | 30 seconds | Configurable flush interval |
| **Flush Latency (P50)** | 200ms | End-to-end flush cycle |
| **Flush Latency (P99)** | 800ms | Including catalog commit |
| **Throughput (Default)** | 10K msg/s | Conservative settings |
| **Throughput (Tuned)** | 100K+ msg/s | Larger batches, LZ4 compression |
| **Memory (Typical)** | 200-500MB | Based on buffer configuration |
| **Memory (Maximum)** | 2GB | Configurable limit |
| **Files Per Hour** | 1-10 | vs. 100-1000+ with Spark/Flink |

## Performance Diagram: File Comparison

```
Files created per hour (streaming 10K msg/s):

Spark Streaming (30s batches)
|████████████████████████████████████████| 1,200 files

Flink + Iceberg Sink
|██████████████████████| 720 files

Kafka Connect
|████████████████████████████████████████| 1,000+ files

K2I
|██| 4-6 files

Fewer files = faster queries = lower costs
```

---

# SECTION 7: Comparison

## Section Headline

```
Why K2I over alternatives?
```

## Comparison Table

| Capability | K2I | Spark Streaming | Apache Flink | Kafka Connect |
|------------|-----|-----------------|--------------|---------------|
| **Architecture** | Single process | Distributed cluster | Distributed cluster | Connect workers |
| **Latency** | Sub-second | 30s - minutes | 1-10 seconds | 5-60 seconds |
| **Exactly-once** | Built-in | Requires checkpointing | Requires checkpointing | Connector-dependent |
| **Auto compaction** | Built-in | Manual / separate job | Manual / separate job | No |
| **Hot buffer queries** | Yes | No | No | No |
| **Memory footprint** | 200MB - 2GB | 4GB+ per executor | 2GB+ per task manager | 1GB+ per worker |
| **Operational complexity** | Deploy binary | Cluster management | Cluster + state management | Connect cluster |
| **Small file handling** | Intelligent buffering | Creates many files | Creates many files | Creates many files |
| **Catalog support** | REST, Hive, Glue, Nessie | Limited | REST, Hive | Limited |
| **License** | Apache 2.0 | Apache 2.0 | Apache 2.0 | Apache 2.0 |

## When to Choose K2I

**Choose K2I when you need:**
- Simple Kafka-to-Iceberg ingestion without cluster overhead
- Sub-second data freshness
- Exactly-once guarantees without complexity
- Minimal operational burden
- Optimal file sizes without manual compaction

**Consider alternatives when you need:**
- Complex stream transformations (Flink)
- ML feature engineering (Spark)
- Multiple source connectors (Kafka Connect)
- Database CDC with deletes (Moonlink/Debezium)

---

# SECTION 8: Use Cases

## Section Headline

```
Built for real-world data pipelines
```

## Use Case Cards

### Use Case 1: Real-Time Product Analytics

**Scenario:** E-commerce platform tracking user behavior

**Before:** Spark batch jobs with 15-minute delay. Dashboard data always stale. Marketing can't react to trends.

**After:** K2I streams clickstream events to Iceberg with sub-second freshness. Real-time dashboards. Instant A/B test insights.

```
Kafka (clickstream) --> K2I --> Iceberg --> Trino/Dremio --> Dashboard
```

### Use Case 2: IoT Sensor Ingestion

**Scenario:** Manufacturing company with 10,000 sensors

**Before:** Flink cluster costs $50K/month. Complex state management. DevOps team stretched thin.

**After:** Single K2I instance per topic. 90% cost reduction. One binary to manage.

```
Sensors --> Kafka --> K2I --> Iceberg (S3) --> Analytics
```

### Use Case 3: Financial Event Sourcing

**Scenario:** Fintech company with strict audit requirements

**Before:** Worried about data loss. Manual reconciliation processes. Compliance concerns.

**After:** K2I's transaction log provides exactly-once guarantees. Complete audit trail. Automatic recovery.

```
Transaction Events --> Kafka --> K2I (with txlog) --> Iceberg --> Compliance Reports
```

### Use Case 4: Log Analytics Pipeline

**Scenario:** SaaS platform ingesting application logs

**Before:** Thousands of small Parquet files. Query performance degraded. Constant compaction jobs.

**After:** K2I's intelligent buffering creates optimal file sizes. Auto-compaction handles the rest.

```
App Logs --> Kafka --> K2I --> Iceberg --> Trino --> Grafana
```

---

# SECTION 9: Integrations

## Section Headline

```
Works with your existing stack
```

## Integration Diagram

```
                    DATA SOURCES
         +------+  +-------+  +--------+
         |Apps  |  |Services|  |IoT    |
         +--+---+  +---+---+  +---+----+
            |          |          |
            v          v          v
         +-------------------------+
         |        KAFKA            |
         |  MSK | Confluent |      |
         |  Redpanda | Strimzi     |
         +------------+------------+
                      |
                      v
              +-------+-------+
              |      K2I      |
              +-------+-------+
                      |
         +------------+------------+
         |                         |
         v                         v
+--------+--------+     +----------+---------+
|  ICEBERG CATALOG |     |   OBJECT STORAGE   |
|                  |     |                    |
| - REST Catalog   |     | - Amazon S3        |
| - Hive Metastore |     | - Google GCS       |
| - AWS Glue       |     | - Azure Blob       |
| - Nessie         |     | - MinIO            |
| - Tabular        |     | - Local filesystem |
+--------+---------+     +----------+---------+
         |                          |
         +------------+-------------+
                      |
                      v
              +-------+-------+
              |    ICEBERG    |
              |    TABLES     |
              +-------+-------+
                      |
    +-----------------+-----------------+
    |         |         |         |
    v         v         v         v
+------+ +-------+ +-------+ +--------+
|Trino | |Spark  | |Dremio | |DuckDB  |
+------+ +-------+ +-------+ +--------+
    |         |         |         |
    v         v         v         v
+------+ +-------+ +-------+ +--------+
|Preset| |Jupyter| |Tableau| |Superset|
+------+ +-------+ +-------+ +--------+
```

## Integration Categories

### Kafka Distributions
- Amazon MSK
- Confluent Cloud / Platform
- Redpanda
- Strimzi (Kubernetes)
- Apache Kafka (self-managed)

### Iceberg Catalogs
- REST Catalog (standard API)
- Hive Metastore
- AWS Glue Data Catalog
- Nessie (git-like versioning)
- Tabular

### Object Storage
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO
- Local filesystem (development)

### Query Engines
- Trino / Starburst
- Apache Spark
- Dremio
- DuckDB
- Snowflake (via Iceberg)
- Databricks (via Iceberg)

### BI & Visualization
- Preset / Apache Superset
- Tableau
- Looker
- Metabase
- Grafana

---

# SECTION 10: Getting Started

## Section Headline

```
From zero to streaming in 5 minutes
```

## Installation Options

### Option 1: Homebrew (macOS)

```bash
brew install osodevops/tap/k2i
```

### Option 2: Shell Installer (Linux/macOS)

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/osodevops/k2i/releases/latest/download/k2i-cli-installer.sh | sh
```

### Option 3: Docker

```bash
docker pull ghcr.io/osodevops/k2i:latest
```

### Option 4: Download Binary

Download from [GitHub Releases](https://github.com/osodevops/k2i/releases) for:
- Linux x86_64
- macOS Intel
- macOS Apple Silicon

## Quick Start

### Step 1: Create Configuration

```toml
# config.toml
[kafka]
bootstrap_servers = "localhost:9092"
topic = "events"
group_id = "k2i-ingestion"

[buffer]
max_size_mb = 500
flush_interval_seconds = 30

[iceberg]
catalog_type = "rest"
rest_uri = "http://localhost:8181"
warehouse = "s3://my-bucket/warehouse"
database = "analytics"
table = "events"

[storage]
type = "s3"
bucket = "my-bucket"
region = "us-east-1"
```

### Step 2: Validate

```bash
k2i validate --config config.toml
```

### Step 3: Run

```bash
k2i ingest --config config.toml
```

### Step 4: Monitor

```bash
# Health check
curl http://localhost:8080/health

# Prometheus metrics
curl http://localhost:9090/metrics
```

## Primary CTAs

```
[Read the Docs]  [View on GitHub]  [Join Discord]
```

---

# SECTION 11: The Whitepaper

## Section Headline

```
Deep dive into the architecture
```

## Whitepaper Teaser

The K2I whitepaper provides comprehensive technical details on:

- **Hot/Cold Architecture:** How Arrow buffering enables sub-second freshness
- **Transaction Log Design:** Write-ahead logging for exactly-once semantics
- **Crash Recovery:** Automatic state reconstruction without data loss
- **Performance Characteristics:** Latency, throughput, and memory profiles
- **Catalog Integration:** REST, Hive, Glue, and Nessie implementations
- **Maintenance Operations:** Compaction, expiration, and orphan cleanup

## Key Technical Claims

```
+------------------------+    +------------------------+    +------------------------+
|                        |    |                        |    |                        |
|    Sub-second          |    |    Exactly-once        |    |    Zero-copy           |
|    freshness           |    |    delivery            |    |    data paths          |
|                        |    |                        |    |                        |
|  Arrow hot buffer      |    |  Write-ahead log       |    |  Rust + Arrow          |
|  with hash indexes     |    |  with CRC32 checksums  |    |  native performance    |
|                        |    |                        |    |                        |
+------------------------+    +------------------------+    +------------------------+
```

## CTA

```
[Read the Full Whitepaper ->]
```

Link: `/whitepaper`

---

# SECTION 12: Open Source

## Section Headline

```
Open source. Open table format. Open future.
```

## Open Source Statement

K2I is fully open source under the Apache 2.0 license. No vendor lock-in. No proprietary formats. Your data stays yours.

Built on open standards:
- **Apache Kafka:** The standard for event streaming
- **Apache Iceberg:** The open table format for analytics
- **Apache Arrow:** The standard for in-memory columnar data
- **Apache Parquet:** The standard for columnar storage

## GitHub Stats (Dynamic Badges)

```
[Stars: XXX]  [Forks: XXX]  [Contributors: XXX]  [License: Apache 2.0]
```

## CTA

```
[Star on GitHub]  [View Source]  [Contribute]
```

---

# SECTION 13: Enterprise

## Section Headline

```
Need more? We've got you covered.
```

## Enterprise Description

[OSO](https://oso.sh) provides enterprise support and advanced features for organizations running K2I at scale.

## Enterprise Features Table

| Category | Capability |
|----------|------------|
| **Security** | AES-256 encryption at rest |
| | GDPR compliance tools (PII masking, retention policies) |
| | Audit logging for all operations |
| | Role-based access control |
| **Integrations** | Schema Registry (Avro/Protobuf with ID mapping) |
| | Secrets management (Vault, AWS Secrets Manager) |
| | SSO/OIDC (Okta, Azure AD, Google) |
| **Scale** | Multi-table support (single process, multiple destinations) |
| | Advanced metrics and dashboards |
| | Log shipping (Datadog, Splunk, Loki) |
| **Support** | 24/7 SLA-backed support |
| | Dedicated Kafka consulting |
| | Architecture review and optimization |

## CTA

```
[Talk to Sales]  [Contact: enquiries@oso.sh]
```

---

# SECTION 14: Footer

## Footer Layout

```
+-----------------------------------------------------------------------------+
|                                                                             |
|  K2I                          Product           Resources        Company    |
|  Real-time streaming          How it Works      Documentation    About OSO  |
|  ingestion for Iceberg        Features          Whitepaper       Blog       |
|                               Comparison        GitHub           Contact    |
|  [GitHub] [Discord] [Twitter] Pricing           Changelog        Careers    |
|                                                                             |
+-----------------------------------------------------------------------------+
|                                                                             |
|  Apache 2.0 License  |  Made with care by OSO  |  Privacy  |  Terms        |
|                                                                             |
+-----------------------------------------------------------------------------+
```

## Footer Links

### Product
- How it Works → `/#how-it-works`
- Features → `/#features`
- Comparison → `/#comparison`
- Pricing → `/pricing` (if applicable)

### Resources
- Documentation → `https://docs.k2i.dev`
- Whitepaper → `/whitepaper`
- GitHub → `https://github.com/osodevops/k2i`
- Changelog → `https://github.com/osodevops/k2i/releases`

### Company
- About OSO → `https://oso.sh/about`
- Blog → `/blog`
- Contact → `https://oso.sh/contact`
- Careers → `https://oso.sh/careers`

### Social
- GitHub → `https://github.com/osodevops/k2i`
- Discord → `https://discord.gg/osodevops`
- Twitter → `https://twitter.com/osodevops`

---

# ADDITIONAL PAGES

## Page: /whitepaper

The whitepaper gets its own dedicated page. Content is the full technical whitepaper from `docs/whitepaper.md`.

**Page Structure:**
1. Hero with title and abstract
2. Table of contents (sticky sidebar)
3. Full whitepaper content
4. CTA to get started / contact

## Page: /blog

Blog posts about K2I, streaming, Iceberg, and data engineering.

**Suggested Initial Posts:**
1. "Introducing K2I: Real-time Kafka to Iceberg"
2. "How K2I Solves the Small File Problem"
3. "Exactly-Once Semantics Without the Complexity"
4. "K2I vs. Spark Streaming: A Performance Comparison"
5. "Running K2I in Production on Kubernetes"

## Subdomain: docs.k2i.dev

Full documentation hosted separately.

**Documentation Structure:**
- Getting Started
  - Installation
  - Quick Start
  - Configuration
- Concepts
  - Architecture
  - Hot Buffer
  - Transaction Log
  - Exactly-Once Semantics
- Guides
  - Deployment (Docker, Kubernetes, Systemd)
  - Catalog Configuration
  - Performance Tuning
  - Monitoring & Alerting
- Reference
  - CLI Commands
  - Configuration Options
  - Metrics Reference
  - API Reference
- Troubleshooting
  - Common Issues
  - FAQ

---

# DESIGN NOTES

## Visual Style Recommendations

1. **Color Palette:**
   - Primary: Deep blue (#1a365d) - trust, reliability
   - Accent: Bright teal (#38b2ac) - modern, fresh
   - Background: Light gray (#f7fafc) - clean, readable
   - Dark mode support

2. **Typography:**
   - Headlines: Inter or similar sans-serif
   - Body: System font stack for performance
   - Code: JetBrains Mono or Fira Code

3. **Imagery:**
   - Minimal, technical illustrations
   - Architecture diagrams with consistent style
   - No stock photos
   - Subtle gradient backgrounds

4. **Layout:**
   - Single-page scroll with anchor navigation
   - Full-width sections with contained content
   - Generous whitespace
   - Mobile-first responsive design

## Key Messaging Hierarchy

1. **Primary Message:** Real-time streaming ingestion for Iceberg
2. **Supporting Messages:**
   - Sub-second freshness
   - Exactly-once delivery
   - Zero operational overhead
3. **Proof Points:**
   - Single binary
   - Transaction log
   - Auto-maintenance
   - Multi-catalog support

## Call-to-Action Strategy

1. **Primary CTA:** Get Started (link to docs quickstart)
2. **Secondary CTA:** Read the Whitepaper
3. **Tertiary CTA:** Star on GitHub
4. **Enterprise CTA:** Talk to Sales

---

# SEO METADATA

## Homepage

```html
<title>K2I - Real-time Streaming Ingestion for Apache Iceberg</title>
<meta name="description" content="K2I bridges Kafka and Apache Iceberg with sub-second latency, exactly-once delivery, and zero operational overhead. Open source, built in Rust.">
<meta name="keywords" content="kafka, iceberg, streaming, data lakehouse, real-time, parquet, arrow, rust">
```

## Whitepaper

```html
<title>K2I Whitepaper - Architecture of a Real-time Iceberg Ingestion Engine</title>
<meta name="description" content="Technical deep dive into K2I's hot/cold architecture, transaction log design, exactly-once semantics, and production deployment patterns.">
```

## Open Graph

```html
<meta property="og:title" content="K2I - Real-time Kafka to Iceberg">
<meta property="og:description" content="Sub-second streaming ingestion for Apache Iceberg. Open source, exactly-once, zero ops.">
<meta property="og:image" content="https://k2i.dev/og-image.png">
<meta property="og:url" content="https://k2i.dev">
```
