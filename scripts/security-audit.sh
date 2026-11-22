#!/usr/bin/env bash
set -euo pipefail

# Keep accepted advisories explicit and fail on every new vulnerability,
# unmaintained dependency, unsound dependency, or yanked crate.
#
# Runtime exceptions pinned by current upstream dependency ranges:
# - The current iceberg, object_store, and AWS SDK dependency graph pins the
#   affected XML and legacy webpki versions. K2I parses responses only from explicitly
#   configured storage services, does not configure CRLs, and still verifies
#   TLS certificates (RUSTSEC-2026-0194, -0195, -0098, -0099, -0104, and -0382).
# - SQLx's target-specific MySQL graph includes rsa, but K2I enables SQLite and
#   PostgreSQL catalogs and performs no RSA private-key operations
#   (RUSTSEC-2023-0071).
# - The AWS SDK keeps its deprecated hyper 0.14 connector in the graph (h2 0.3
#   and rustls 0.21). K2I builds the SDK config with BehaviorVersion::latest(),
#   so requests use aws-smithy-runtime's default hyper 1.x client and the patched
#   h2 0.4.16, rustls 0.23.46, and rustls-webpki 0.103.17 stack; the older
#   connector is compiled but not selected, and traffic goes only to explicitly
#   configured AWS endpoints over verified TLS (RUSTSEC-2026-0258, -0346, and
#   -0349).
# - SQLx 0.8 bundles SQLite through libsqlite3-sys 0.30.1. Its CVE needs a
#   crafted database plus sibling -wal file written by a less trusted local
#   party before K2I opens the database; K2I creates and opens only its own
#   catalog database under the operator-controlled warehouse path, so writing
#   those files already implies control of the catalog. The patched
#   libsqlite3-sys requires SQLx 0.9, whose dynamic-SQL API changes are a code
#   migration rather than a dependency bump (RUSTSEC-2026-0459).
#
# Development and maintenance exceptions:
# - tokio-tar is reachable only through dev-only testcontainers and never ships
#   in the K2I binary (RUSTSEC-2025-0111).
# - bincode 1 is frozen into local RPC protocol version 1; replacing it requires
#   an intentional wire-version migration (RUSTSEC-2025-0141).
# - paste and rustls-pemfile are unmaintained transitive crates with no reported
#   vulnerability (RUSTSEC-2024-0436 and RUSTSEC-2025-0134).
# - chrono is unmaintained with no reported vulnerability; K2I uses it for
#   timestamps and the arrow/iceberg graph still requires it, so replacing it is
#   a code migration, not a dependency bump (RUSTSEC-2026-0543).
# - humantime and pin-utils are unmaintained transitive crates with no reported
#   vulnerability; humantime is pinned by object_store 0.11 and pin-utils is used
#   only through the AWS SDK's pin_mut! macro (RUSTSEC-2026-0542 and
#   RUSTSEC-2026-0441).
readonly -a accepted_advisories=(
  RUSTSEC-2026-0194
  RUSTSEC-2026-0195
  RUSTSEC-2026-0098
  RUSTSEC-2026-0099
  RUSTSEC-2026-0104
  RUSTSEC-2026-0382
  RUSTSEC-2026-0258
  RUSTSEC-2026-0346
  RUSTSEC-2026-0349
  RUSTSEC-2026-0459
  RUSTSEC-2023-0071
  RUSTSEC-2025-0111
  RUSTSEC-2025-0141
  RUSTSEC-2024-0436
  RUSTSEC-2025-0134
  RUSTSEC-2026-0543
  RUSTSEC-2026-0542
  RUSTSEC-2026-0441
)

audit_args=(--deny warnings)
for advisory in "${accepted_advisories[@]}"; do
  audit_args+=(--ignore "$advisory")
done

exec cargo audit "${audit_args[@]}"
