#!/usr/bin/env bash
set -euo pipefail

# Every workflow job that compiles the workspace needs the same system headers.
#
# rdkafka-sys bundles librdkafka, which requires curl/curl.h and the SASL headers
# at build time. This has drifted twice: the librdkafka 2.12 upgrade added the
# headers to test.yml but missed release.yml and docker-publish.yml, and because
# those two only run on tag pushes the breakage stayed invisible until a release
# was cut and failed before publishing anything.
#
# Fail fast on any Rust-building workflow whose apt-get line lacks a header.

readonly -a required_packages=(
  cmake
  libcurl4-openssl-dev
  libsasl2-dev
  libssl-dev
  pkg-config
)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly WORKFLOW_DIR="${ROOT_DIR}/.github/workflows"

failures=0

while IFS= read -r workflow; do
  # Only workflows that install system packages compile the workspace.
  grep -q 'apt-get install' "${workflow}" || continue

  while IFS= read -r line; do
    for package in "${required_packages[@]}"; do
      if [[ "${line}" != *"${package}"* ]]; then
        echo "error: $(basename "${workflow}") installs system packages but omits '${package}'"
        echo "       ${line#"${line%%[![:space:]]*}"}"
        failures=$((failures + 1))
      fi
    done
  done < <(grep 'apt-get install' "${workflow}")
done < <(find "${WORKFLOW_DIR}" -name '*.yml' -o -name '*.yaml' | sort)

if [[ "${failures}" -gt 0 ]]; then
  echo
  echo "Every workflow job that builds the workspace must install: ${required_packages[*]}"
  exit 1
fi

echo "ok: all workflow dependency installs are consistent"
