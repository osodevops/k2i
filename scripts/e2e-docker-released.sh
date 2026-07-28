#!/usr/bin/env bash
set -euo pipefail

# Run the Iceberg E2E against a PUBLISHED image rather than a local build, so a
# release can be validated as the artifact users actually pull.
#
#   scripts/e2e-docker-released.sh                      # ghcr.io/osodevops/k2i:latest
#   scripts/e2e-docker-released.sh v0.3.0               # a specific tag
#   K2I_RELEASE_IMAGE=my/image:tag scripts/e2e-docker-released.sh
#
# A green release workflow proves the image built; this proves it runs.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/e2e/compose.iceberg.yml"
OVERLAY_FILE="${ROOT_DIR}/docker/e2e/compose.published-image.yml"
PROJECT_NAME="${K2I_E2E_PROJECT_NAME:-k2i-e2e-released}"

if [[ -n "${1:-}" ]]; then
  export K2I_RELEASE_IMAGE="ghcr.io/osodevops/k2i:${1}"
fi
: "${K2I_RELEASE_IMAGE:=ghcr.io/osodevops/k2i:latest}"
export K2I_RELEASE_IMAGE

cleanup() {
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" -f "${OVERLAY_FILE}" \
    down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

echo "Validating published image: ${K2I_RELEASE_IMAGE}"
docker pull "${K2I_RELEASE_IMAGE}"

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" -f "${OVERLAY_FILE}" up \
  --abort-on-container-exit \
  --exit-code-from e2e-runner \
  e2e-runner

echo "ok: ${K2I_RELEASE_IMAGE} passed the DuckDB iceberg_scan validation"
