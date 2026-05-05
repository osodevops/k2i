# K2I Architecture

K2I is a single-process Kafka-to-Iceberg ingestion engine. One process consumes one configured Kafka topic and writes one configured Iceberg table today.

The architecture combines:

- a Kafka consumer with manual offset management and backpressure;
- a decoder layer for raw values and Confluent-framed Protobuf;
- an Arrow hot buffer and optional local read-state RPC;
- a Parquet writer and Iceberg catalog commit path;
- an append-only transaction log used for recovery and idempotency records;
- HTTP health/readiness and Prometheus metrics.

## System Overview

```mermaid
flowchart LR
    kafka[(Kafka topic)]
    registry[(Schema Registry)]
    client[Local read client]
    metrics[Health / Metrics]

    subgraph k2i[K2I Rust process]
        consumer[Kafka consumer<br/>manual offsets + backpressure]
        decoder[Decoder<br/>raw / Protobuf]
        hot[Arrow hot buffer<br/>read LSNs]
        wal[Transaction log<br/>offsets + idempotency]
        writer[Parquet writer]
        rpc[Read-state RPC<br/>Unix socket]
    end

    store[(Object storage<br/>Parquet data files)]
    catalog[(Iceberg catalog<br/>REST / Glue / Hive / Nessie)]
    engines[Query engines<br/>DuckDB / Trino / Spark]

    kafka --> consumer --> decoder --> hot --> writer --> store
    registry --> decoder
    hot --> rpc --> client
    decoder --> wal
    writer --> wal
    writer --> catalog
    catalog --> engines
    store --> engines
    k2i --> metrics
```

## Runtime Flow

```text
Kafka poll
  -> decode raw or Protobuf payload
  -> schema compatibility check when Protobuf is configured
  -> assign read LSNs
  -> append rows to the Arrow hot buffer
  -> append transaction-log offset markers
  -> flush when time, size, or count threshold fires
  -> write Parquet data file
  -> commit Iceberg append
  -> record committed file and idempotency data
  -> commit Kafka offsets after durable handling
```

## Write Ordering

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as K2I Consumer
    participant D as Decoder
    participant H as Hot Buffer
    participant T as Transaction Log
    participant W as Iceberg Writer
    participant O as Object Store
    participant I as Iceberg Catalog

    K->>C: Poll messages
    C->>D: Decode payloads
    D->>H: Append rows and assign read LSNs
    D->>T: Record offset markers
    H->>W: Flush snapshot
    W->>O: Write Parquet data file
    W->>I: Commit Iceberg append
    W->>T: Record data file and idempotency
    C->>K: Commit Kafka offsets after durable handling
```

Important invariants:

- Do not advance Kafka offsets past data that has not been durably handled.
- Do not assign final read LSNs until schema compatibility is accepted.
- Keep flushed rows visible to read clients until their committed data file is registered.
- Treat transaction-log entries as recovery contracts.

## Hot And Cold Visibility

```mermaid
flowchart TB
    record[Kafka record accepted]
    hot[Hot path<br/>Arrow buffer + read-state RPC]
    flush[Flush trigger<br/>time / size / count]
    cold[Cold path<br/>Parquet + Iceberg snapshot]
    query_hot[Local freshness reads]
    query_cold[Lakehouse queries]

    record --> hot --> query_hot
    hot --> flush --> cold --> query_cold
```

Hot-path reads are local. The optional read-state RPC listens on a Unix socket and is intended for sidecars or co-located readers.

Cold-path reads happen through query engines after K2I writes Parquet and commits an Iceberg snapshot.

## Component Responsibilities

| Component | Responsibility |
|---|---|
| Kafka consumer | Poll Kafka, batch messages, pause/resume for backpressure, commit offsets after durable handling |
| Decoder | Preserve raw values or decode Confluent-framed Protobuf through Schema Registry |
| Schema evolution | Accept compatible additive Protobuf changes and pause readiness on breaking changes |
| Hot buffer | Store recent rows in Arrow-compatible structures and expose snapshots for flush |
| Read registry | Track read LSNs, hot rows, in-flight flush visibility, committed files, and scan lifetimes |
| Iceberg writer | Convert buffer snapshots to Parquet and commit append metadata through the catalog path |
| Transaction log | Record offsets, flush stages, data files, schema events, idempotency records, and checkpoints |
| HTTP server | Expose `/health`, `/healthz`, `/readyz`, and `/metrics` |
| CLI | Validate config, run ingestion, inspect status, manage table/dev commands, generate man pages |

## Protobuf Schema Evolution

```mermaid
stateDiagram-v2
    [*] --> LoadLatestSchema
    LoadLatestSchema --> Ingesting
    Ingesting --> SameSchema: schema ID known
    Ingesting --> CompareSchema: new schema ID
    CompareSchema --> AutoAdditive: optional fields added
    AutoAdditive --> UpdateProjection
    UpdateProjection --> Ingesting
    CompareSchema --> BreakingChange: removed field / type change / unsafe required field
    BreakingChange --> Paused: readiness blocked, offsets not advanced
    Paused --> OperatorAction
    OperatorAction --> LoadLatestSchema
```

See [Schema Registry Protobuf](./schema-registry-protobuf.md).

## Iceberg Catalog Commit Flow

```mermaid
flowchart TD
    batch[Flush batch]
    parquet[Write Parquet data file]
    filemeta[Build Iceberg data file metadata]
    load[Load current table metadata]
    append[Create append operation]
    commit[Commit through catalog]
    snapshot[New Iceberg snapshot]
    txlog[Record committed file in txlog]

    batch --> parquet --> filemeta --> load --> append --> commit --> snapshot --> txlog
```

The REST catalog path is validated locally with the Apache Iceberg REST fixture and DuckDB `iceberg_scan`. Glue, Hive, and Nessie catalog abstractions exist, but should be validated against the exact backend before production use.

See [Iceberg REST Catalog](./iceberg-rest-catalog.md).

## Current Caveats

- Multi-partition flush and offset commit behavior needs continued hardening.
- Startup recovery state is computed, but Kafka seeking/deduplication and startup orphan cleanup need further wiring.
- Kafka commits are async in the current helper.
- Transaction-log entries are flushed, but not every entry is fsynced individually.
- GCS and Azure object-store configuration is declared, but writer creation is not complete for those backends.
- Maintenance commands and task implementations exist; scheduler wiring should be reviewed for each deployment.

See [Production Readiness](./production-readiness.md).
