# Docker E2E Harness

Run from the repository root:

```bash
scripts/e2e-docker.sh
```

For the heavier local load profile:

```bash
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh
```

For real Iceberg metadata validation:

```bash
scripts/e2e-docker-iceberg.sh
```

For the production-grade local load profile with real Iceberg metadata
validation:

```bash
K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh
```

The load script uses the same stack with four Kafka partitions by default,
larger ingest batches, and a 5,000-row flush threshold. Override
`K2I_E2E_LOAD_MESSAGES`, `K2I_E2E_LOAD_CONCURRENCY`, or
`K2I_E2E_LOAD_PARTITIONS` to scale the run. The default production-grade local
profile produces 100,000 Protobuf rows, waits for full hot/cold visibility, and
asserts that `k2i_errors_total` remains zero.

The harness starts:

- Kafka (`confluentinc/cp-kafka`)
- Confluent Schema Registry
- a schema-init runner that pre-registers the startup Protobuf schema
- the real `k2i` container built from this repo
- a Rust E2E runner with DuckDB Python installed

The Iceberg variant also starts `apache/iceberg-rest-fixture` with a shared
local warehouse volume. K2I commits appended files with iceberg-rust
`Transaction::fast_append`, then the runner validates the resulting metadata
with DuckDB `iceberg_scan`.

The Iceberg load variant combines both profiles: it produces 100,000 rows by
default, requires all rows to flush to cold storage, checks the read-state data
file list, validates Parquet with DuckDB, validates the latest Iceberg snapshot
with DuckDB `iceberg_scan`, asserts metadata and snapshot growth, and confirms
`k2i_errors_total` stayed at zero. Override `K2I_E2E_LOAD_TIMEOUT_SECONDS`,
`K2I_E2E_ICEBERG_LOAD_MIN_DATA_FILES`, or
`K2I_E2E_ICEBERG_LOAD_MIN_SNAPSHOTS` when intentionally changing the load
shape.

The init runner registers the v1 Protobuf subject before K2I starts, exercising
`latest_on_startup`. The main runner registers evolved schemas through Schema
Registry, produces real Confluent-framed Protobuf Kafka values, reads K2I
through the Unix socket RPC, and uses DuckDB
`read_parquet(..., union_by_name=true)` to verify flushed files.

Covered flow:

1. Protobuf v1 rows are visible in the hot Arrow IPC buffer.
2. v1 rows flush to Parquet and DuckDB counts them.
3. Protobuf v2 adds nullable `team`; hot Arrow IPC exposes the new column.
4. v1/v2 Parquet files are queried together by DuckDB with schema union, and
   `team` values/nulls are verified.
5. Protobuf v3 changes `lap int32` to `string`; K2I rejects it and logs the
   breaking schema change.
6. `/health` reports schema degradation, `/readyz` is blocked, and valid data
   produced after the schema pause is not ingested.

The default and load harnesses still keep the Parquet `read_parquet` validation
as a fast control path. Use the Iceberg variant whenever changes touch catalog
commit behavior or metadata compatibility.
