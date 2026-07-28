# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-07-28

### Added

- Implemented `create_gcs_store()` via `object_store::gcp::GoogleCloudStorageBuilder`, falling through to Application Default Credentials (Workload Identity on GKE) when `gcs_service_account_path` is unset.
- Implemented `create_azure_store()` via `object_store::azure::MicrosoftAzureBuilder`, falling through to `DefaultAzureCredential` (Managed Identity on AKS) when `azure_access_key` is unset.
- Documented that omitting `aws_access_key_id`/`aws_secret_access_key` activates the `AmazonS3Builder` default credential chain (env vars → IMDS → IRSA), unblocking EKS with IRSA and EC2 instance profiles without explicit config.
- Added `gcs_bucket_name`, `gcs_service_account_path`, `azure_container_name`, `azure_storage_account_name`, and `azure_access_key` fields to `IcebergConfig` for credential overrides and the Azure-required account name.

### Security

- `Secret` now redacts in `serde` serialization as well as `Debug`, emitting `REDACTED` in place of the value. `Config` derives `Serialize`, so previously any code that dumped or echoed the configuration would have emitted credentials in the clear. This follows the `secrecy` crate's convention of not implementing `Serialize` for secret-wrapped strings, so that emitting one must be a conscious act. The trade-off is deliberate and documented: a serialized `Config` no longer round-trips, since reading it back yields the literal `REDACTED` marker.

### Changed

- `Config::validate` now rejects cloud warehouse settings that cannot be satisfied, rather than deferring the failure to the first flush: `azure_storage_account_name` is required for `az://` and `abfs://` paths, and `s3://`/`gs://` paths must name a bucket. A long-running ingest previously reported healthy and only failed minutes later, on its first write.
- Warehouse-path parsing (bucket, Azure container, in-bucket prefix) is now defined once in `config` and shared with the Iceberg writer, so what validation accepts is exactly what the writer can build a store from.
- Bumped the workspace version to 0.3.0 to absorb the semver-major addition of public fields on the externally-constructible `IcebergConfig` struct. The 0.x convention treats a minor bump (0.2 → 0.3) as the breaking-change boundary.
- Upgraded the official Apache Iceberg Rust client from 0.7 to 0.10.0 and the Arrow/Parquet ecosystem from 54 to 58.
- Removed the temporary standalone REST `update_schema` fallback now that `Transaction::update_schema()` is available in `iceberg-rust` 0.10.0.
- Simplified `OfficialRestCommitter` by delegating all catalog operations to the official `RestCatalog` transaction APIs.

### Fixed

- Aligned cloud object store uploads with the warehouse path recorded by the catalog/txlog. For cloud backends the store is rooted at the bucket, so `IcebergWriter` derives an in-bucket prefix from `warehouse_path` (e.g. `warehouse` for `s3://bucket/warehouse`) and applies it when addressing the store. Uploads previously landed at `s3://bucket/data/...` while the catalog recorded `s3://bucket/warehouse/data/...`, leaving every committed file unreadable. Preexisting on S3; the same handling now covers GCS and Azure. The prefix is applied **only** at upload time — paths handed to the catalog, transaction log, and read path stay warehouse-relative, since those consumers join them against `warehouse_path` themselves.
- Azure container parsing now handles the Hadoop ABFS form `abfs://container@account.dfs.core.windows.net/path` by extracting the container before the `@`, instead of treating the whole `container@account` segment as the container.
- `K2I_MONITORING_LOG_FORMAT` now takes effect. The tracing subscriber is configured before the full config is loaded and read the TOML value directly, so the environment override was silently ignored for all output.
- `K2I_RPC_ENABLED` no longer treats an unrecognized value as `false`. `K2I_RPC_ENABLED=yes` previously disabled the RPC server that the TOML had enabled; unparseable values now warn and preserve the configured value.
- Added `K2I_*` overrides for the remaining cloud object-store fields, including the Azure-required `azure_storage_account_name`, which could not previously be set by environment-only deployments.
- The unrecognized-variable warning no longer fires for `K2I_E2E_*` and the other harness variables that share the engine's environment during end-to-end runs.
- Aligned Parquet writer properties with the parquet 58 API (`set_max_row_group_row_count`).
- Avoided manual OAuth2, route resolution, and multipart namespace encoding logic previously needed for the schema-update fallback.

### Testing

- Added container-backed S3 round-trip tests (MinIO) covering a prefixed warehouse (`s3://bucket/warehouse`), a multi-segment prefix, and a bucket-root warehouse. Each asserts that joining `warehouse_path` with the writer's reported path resolves to a real stored object, and that nothing was written to the doubled-prefix or bucket-root locations. These reproduce the warehouse-prefix defect above; they fail against the previous behaviour.
- CI's integration-tests job now passes `--include-ignored`. Every container-backed test is marked `#[ignore = "requires Docker"]`, so the job provisioned Docker and then ran no Docker test at all — including the pre-existing Kafka integration tests.
- The Tests workflow now also triggers on `docs/**` and `config/**`, since a test asserts that every `K2I_*` variable is documented in `docs/kubernetes.md`.

### Documentation

- Removed the `docs/configuration.md` claim that config values support `${VAR}` shell substitution. No such mechanism exists — following it would have authenticated with the literal string `${VAR}`. Replaced with the two real mechanisms: `{ file = "..." }` refs and `K2I_*` overrides.
- Updated `README.md`, `docs/architecture.md`, and `docs/configuration.md`, which still described GCS and Azure as declared-but-unwired.

### Requirements

- Raised the documented minimum supported Rust version to 1.94, matching `iceberg` 0.10.0 and the updated AWS SDK dependency graph.

## [0.2.2] - 2026-07-13

### Changed

- Unified REST catalog lifecycle and snapshot operations on the official Apache Iceberg client, retaining only a narrow schema-update fallback until the pinned client exposes that transaction API.
- Made the schema-update fallback follow Iceberg REST runtime URI and warehouse-prefix negotiation, multipart namespace encoding, custom headers, and configured bearer or OAuth2 authentication.

### Fixed

- Preserved the real manifest-list path through catalog commits, enforced caller snapshot preconditions, and rejected unsupported file removals instead of silently omitting them.
- Preserved complex Iceberg struct, list, and map types as canonical JSON so unchanged Protobuf schemas are not incorrectly classified as breaking changes.
- Corrected explicit OAuth2 token endpoint handling and serialized concurrent route and token initialization.
- Preserved the public K2I 0.2 REST adapter and protocol types for patch-release API compatibility.
- Updated direct and transitive dependencies to clear 16 RustSec findings, including the current Prometheus, AWS-LC, `bytes`, `quinn-proto`, `rustls-webpki`, and `time` fixes.
- Made the security audit blocking, with narrowly documented exceptions for advisories pinned by upstream dependencies or confined to unused and test-only code paths.

### Requirements

- Raised the documented minimum supported Rust version to 1.88, matching the existing Apache Iceberg and AWS SDK dependency graph.

### Verified

- Full workspace formatting, Clippy, unit, integration, RPC, documentation, and semantic-version compatibility checks.
- Docker Iceberg correctness flow with real REST metadata and DuckDB `iceberg_scan` validation.
- 100,000-row Docker Iceberg load flow with full cold visibility across 20 data files and 20 snapshots.

## [0.2.1] - 2026-07-07

### Added

- Published the `ghcr.io/osodevops/k2i` Docker image as a multi-platform manifest list for `linux/amd64` and `linux/arm64`, built natively per architecture (no QEMU emulation) and verified by CI.

### Fixed

- Fixed Iceberg REST snapshot commits failing against catalogs backed by `apache/iceberg-rust` when snapshot summary properties included `operation`.

## [0.2.0] - 2026-05-05

### Added

- Added Confluent-framed Protobuf decoding through Schema Registry.
- Added additive Protobuf schema evolution with readiness blocking on breaking changes.
- Added read-state RPC crates and Unix socket serving for local hot/cold table views.
- Added Arrow hot-read visibility and committed data file tracking.
- Added real Iceberg REST metadata commits through the official Rust Iceberg implementation.
- Added Docker E2E flows for correctness, local load, Iceberg metadata validation, and Iceberg load validation.
- Added DuckDB direct Parquet and DuckDB `iceberg_scan` validation in local E2E.
- Added table/backfill/dev CLI surfaces and generated recursive man pages.
- Added shell completion and man-page generation commands.
- Added production-oriented release docs, FAQ, comparisons, and SEO research outputs.
- Added Apache 2.0 license file.

### Changed

- Repositioned public docs around "Kafka to Apache Iceberg in one Rust binary".
- Moved older PRDs, research, and website drafts under `docs/archive/`.
- Tightened release claims around exactly-once-style durability, hot vs cold freshness, catalog backend validation, and maintenance scheduler caveats.
- Updated production-readiness docs with the current verification matrix and follow-up list.

### Verified

- `cargo fmt --all --check`
- `git diff --check`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --no-fail-fast`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p k2i-cli --test man_pages --no-fail-fast`
- `scripts/e2e-docker-iceberg.sh`
- Previous 100,000-row Docker Iceberg load validation with DuckDB `iceberg_scan`

## [0.1.0] - 2026-01-09

### Added

- Initial K2I repository and release workflow setup.
