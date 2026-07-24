#!/usr/bin/env bash
set -euo pipefail

# Keep accepted advisories explicit and fail on every new vulnerability,
# unmaintained dependency, unsound dependency, or yanked crate.
#
# Runtime exceptions pinned by current upstream dependency ranges:
# - The current iceberg, object_store, and AWS SDK dependency graph pins the
#   affected XML and legacy webpki versions. K2I parses responses only from explicitly
#   configured storage services, does not configure CRLs, and still verifies
#   TLS certificates (RUSTSEC-2026-0194, -0195, -0098, -0099, and -0104).
# - SQLx's target-specific MySQL graph includes rsa, but K2I enables SQLite and
#   PostgreSQL catalogs and performs no RSA private-key operations
#   (RUSTSEC-2023-0071).
#
# Development and maintenance exceptions:
# - tokio-tar is reachable only through dev-only testcontainers and never ships
#   in the K2I binary (RUSTSEC-2025-0111).
# - bincode 1 is frozen into local RPC protocol version 1; replacing it requires
#   an intentional wire-version migration (RUSTSEC-2025-0141).
# - paste and rustls-pemfile are unmaintained transitive crates with no reported
#   vulnerability (RUSTSEC-2024-0436 and RUSTSEC-2025-0134).
# - K2I does not install the custom rand logger needed to reach
#   RUSTSEC-2026-0097.
readonly -a accepted_advisories=(
  RUSTSEC-2026-0194
  RUSTSEC-2026-0195
  RUSTSEC-2023-0071
  RUSTSEC-2026-0098
  RUSTSEC-2026-0099
  RUSTSEC-2026-0104
  RUSTSEC-2025-0111
  RUSTSEC-2025-0141
  RUSTSEC-2024-0436
  RUSTSEC-2025-0134
  RUSTSEC-2026-0097
)

audit_args=(--deny warnings)
for advisory in "${accepted_advisories[@]}"; do
  audit_args+=(--ignore "$advisory")
done

exec cargo audit "${audit_args[@]}"
