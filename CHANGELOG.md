# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Upgraded the official Apache Iceberg Rust client from 0.7 to 0.10.0 and the Arrow/Parquet ecosystem from 54 to 58.
- Removed the temporary standalone REST `update_schema` fallback now that `Transaction::update_schema()` is available in `iceberg-rust` 0.10.0.
- Simplified `OfficialRestCommitter` by delegating all catalog operations to the official `RestCatalog` transaction APIs.

### Fixed

- Avoided manual OAuth2, route resolution, and multipart namespace encoding logic previously needed for the schema-update fallback.
- Aligned Parquet writer properties with the parquet 58 API (`set_max_row_group_row_count`).

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
