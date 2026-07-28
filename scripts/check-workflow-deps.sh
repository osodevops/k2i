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

# cargo-dist builds the release binaries in a job it generates itself, installing
# the apt packages listed in dist-workspace.toml rather than any step written in
# the workflow. That list drifted independently of the workflows and broke the
# Linux release artifact after the workflows themselves were already fixed.
readonly DIST_CONFIG="${ROOT_DIR}/dist-workspace.toml"

if [[ -f "${DIST_CONFIG}" ]] && grep -q '\[dist.dependencies.apt\]' "${DIST_CONFIG}"; then
  apt_section="$(awk '/^\[dist\.dependencies\.apt\]/{flag=1;next}/^\[/{flag=0}flag' "${DIST_CONFIG}")"
  for package in "${required_packages[@]}"; do
    if ! grep -q "^${package} " <<<"${apt_section}"; then
      echo "error: dist-workspace.toml [dist.dependencies.apt] omits '${package}'"
      failures=$((failures + 1))
    fi
  done
fi

if [[ "${failures}" -gt 0 ]]; then
  echo
  echo "Every job that builds the workspace must install: ${required_packages[*]}"
  echo "That means .github/workflows/*.yml apt-get lines AND dist-workspace.toml."
  exit 1
fi

echo "ok: workflow and cargo-dist dependency installs are consistent"
