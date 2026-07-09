# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
