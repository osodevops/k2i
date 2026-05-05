#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/e2e/compose.yml"
LOAD_COMPOSE_FILE="$ROOT_DIR/docker/e2e/compose.load.yml"
PROJECT_NAME="${K2I_E2E_PROJECT_NAME:-k2i-e2e-load}"

cleanup() {
  docker compose \
    -p "$PROJECT_NAME" \
    -f "$COMPOSE_FILE" \
    -f "$LOAD_COMPOSE_FILE" \
    down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup

docker compose \
  -p "$PROJECT_NAME" \
  -f "$COMPOSE_FILE" \
  -f "$LOAD_COMPOSE_FILE" \
  up \
  --build \
  --abort-on-container-exit \
  --exit-code-from e2e-runner \
  e2e-runner
