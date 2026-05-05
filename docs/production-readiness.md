# Production Readiness Review

This document captures the current stabilization state after the read-state protocol, DuckDB/Docker E2E harness, and Protobuf schema evolution PRDs. Treat it as the review map for turning the current implementation into mergeable PRs and explicit follow-up issues.

The release-facing docs now use this document as the claims backstop. README and user-facing docs should say "production-oriented" unless the caveats below have been closed or explicitly accepted for a deployment.

## Current Status

The implemented path now covers:

- Confluent-framed Protobuf decoding through Schema Registry.
- Additive Protobuf schema evolution with transaction-log records.
- Startup latest-schema resolution and stale disk cache under `<transaction_log.log_dir>/schema-cache`.
- Breaking-change protection that blocks readiness before assigning read LSNs or committing Kafka offsets.
- Optional Unix socket read-state RPC returning committed Parquet files plus hot Arrow IPC rows.
- Real Iceberg REST metadata commits through iceberg-rust, validated with DuckDB `iceberg_scan`.
- Docker E2E correctness, local load, Iceberg metadata, and Iceberg load harnesses with Kafka, Schema Registry, K2I, DuckDB, and the Rust E2E runner.

The code has been verified locally, but the production gaps below should be tracked before calling the whole system production complete.

## Suggested PR Split

1. **Read-state protocol and RPC**
   - `crates/k2i-rpc`
   - `crates/k2i-rpc-server`
   - `crates/k2i-core/src/read`
   - Engine read LSN assignment, in-flight flush visibility, and optional CLI RPC startup.

2. **Docker E2E and DuckDB validation**
   - `docker/e2e`
   - `crates/k2i-e2e-runner`
   - `scripts/e2e-docker.sh`
   - `scripts/e2e-docker-load.sh`
   - `scripts/e2e-docker-iceberg.sh`
   - `scripts/e2e-docker-iceberg-load.sh`
   - Docker build context and local verification docs.

3. **Protobuf decoding and schema evolution**
   - `crates/k2i-core/src/format`
   - `crates/k2i-core/src/format/protobuf`
   - Schema Registry client/cache, descriptor projection, additive diffing, readiness behavior, and txlog schema evolution records.

4. **Catalog/Iceberg integration refinements**
   - REST/Nessie/factory/writer changes needed by the above flows.
   - Keep this separate if the diff contains broader catalog behavior beyond the PRDs.

5. **Docs and examples**
   - `README.md`
   - `AGENTS.md`
   - `config/example.toml`
   - `docs/README.md`
   - `docs/kafka-to-iceberg.md`
   - `docs/comparisons.md`
   - `docs/duckdb-iceberg-validation.md`
   - `docs/schema-registry-protobuf.md`
   - `docs/iceberg-rest-catalog.md`
   - `docs/faq.md`
   - Existing current docs under `docs/*`
   - `docs/man/man1/*`
   - E2E and operator runbooks.

6. **Historical docs archive**
   - Older PRDs, competitive research, website drafts, and superseded planning docs have been moved under `docs/archive/`.
   - Treat archive content as context only; it may contain older claims that are not release documentation.

## Verification Checklist

Latest local verification for the implementation:

| Check | Result |
|-------|--------|
| `cargo fmt --all --check` | Pass |
| `git diff --check` | Pass |
| `cargo check --workspace` | Pass |
| `cargo clippy --workspace --all-targets -- -D warnings` | Pass |
| `cargo test --workspace --no-fail-fast` | Pass, 285 passed, 3 ignored |
| `cargo run -p k2i-cli -- completions man --output-dir docs/man/man1` | Pass, 22 pages generated |
| `scripts/e2e-docker.sh` | Pass |
| `K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-load.sh` | Pass |
| `scripts/e2e-docker-iceberg.sh` | Pass |
| `K2I_E2E_LOAD_MESSAGES=100000 scripts/e2e-docker-iceberg-load.sh` | Pass |
| Docs claim scan and `git diff --check` | Pass for release docs after content PR |

Docker correctness flow verified:

- Protobuf v1 rows visible through hot read RPC.
- v1 rows flushed to Parquet and counted with DuckDB.
- Protobuf v2 nullable field addition visible in hot Arrow IPC.
- v1/v2 Parquet files queried together with DuckDB `read_parquet(..., union_by_name=true)`.
- Protobuf v3 breaking type change rejected.
- `/health` reports schema degradation, `/readyz` returns 503, and valid data after the pause is not ingested.

Docker load flow verified:

- 100,000 Confluent-framed Protobuf rows produced and ingested.
- Full visibility reached through the read-state path.
- `k2i_errors_total` stayed at zero.
- DuckDB verified flushed Parquet rows when a flush occurred.

Docker Iceberg flow verified:

- K2I starts against `apache/iceberg-rest-fixture`.
- Appended Parquet files are committed with real Iceberg REST metadata.
- DuckDB `iceberg_scan` validates the latest Iceberg metadata file.

Docker Iceberg load flow:

- Produces 100,000 Confluent-framed Protobuf rows by default.
- Requires every row to flush cold before validation.
- Validates read-state data files, direct Parquet reads, Iceberg metadata, Iceberg snapshot growth, and DuckDB `iceberg_scan`.
- Asserts `k2i_errors_total` stayed at zero.

## Operator Notes

- `/healthz` is liveness: it stays 200 for degraded but operational states.
- `/readyz` is readiness: it returns 503 when schema evolution requires operator action.
- A breaking Protobuf schema change pauses ingestion before read LSN assignment and before Kafka offset commit.
- `on_breaking_change = "skip-message"` currently pauses instead of skipping because skipping would advance offsets past incompatible data.
- Schema Registry responses are cached in memory and on disk. The disk cache is a stale fallback, not a replacement for Schema Registry availability.
- Read RPC is local Unix socket only. It is intended for co-located readers, sidecars, or local adapters, not direct remote exposure.

## Production Follow-Ups

Track these as explicit issues before broad production rollout:

1. Decide whether Protobuf enum fields should remain `Utf8` enum names or become Arrow dictionaries with stable Iceberg projection.
2. Add an operator resume/migration workflow for schema pauses, including an explicit command or documented restart criteria.
3. Decide whether `skip-message` should become a real dead-letter/skip workflow. Do not implement simple offset skipping without durable accounting.
4. Gate the Docker load and Iceberg load profiles in CI or nightly automation. The 100,000-row runs are useful but may be too heavy for every PR.
5. Add JSON/raw regression coverage if those formats need the same confidence level as Protobuf.
6. Continue hardening multi-partition offset commit and recovery behavior under mixed-partition batches.
