#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/e2e/compose.yml"
PROJECT_NAME="${K2I_E2E_PROJECT_NAME:-k2i-e2e}"

cleanup() {
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

cleanup

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up \
  --build \
  --abort-on-container-exit \
  --exit-code-from e2e-runner \
  e2e-runner

if ! docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" logs --no-color k2i \
  | grep -q "breaking schema change"; then
  echo "expected K2I logs to include a breaking schema change error" >&2
  exit 1
fi

echo "ok: K2I logged the breaking schema change"
