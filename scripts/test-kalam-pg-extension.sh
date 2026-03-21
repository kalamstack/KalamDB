#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PG_VERSION="${1:-pg16}"
PACKAGE="kalam-pg-extension"
FEATURES="${KALAM_PG_EXTENSION_FEATURES:-embedded}"

cd "$ROOT_DIR"

echo "==> Running nextest for ${PACKAGE} (${PG_VERSION}, ${FEATURES})"
cargo nextest run \
  -p "$PACKAGE" \
  --features "${PG_VERSION} ${FEATURES}" \
  --no-default-features

echo "==> Running pgrx suite for ${PACKAGE} (${PG_VERSION}, ${FEATURES})"
RUST_TEST_THREADS=1 cargo pgrx test "$PG_VERSION" \
  -p "$PACKAGE" \
  --no-default-features \
  -F "$FEATURES"