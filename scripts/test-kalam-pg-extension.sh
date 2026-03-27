#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PG_VERSION="${1:-pg16}"
PACKAGE="kalam-pg-extension"
FEATURES="${KALAM_PG_EXTENSION_FEATURES:-}"

cd "$ROOT_DIR"

echo "==> Running nextest for ${PACKAGE} (${PG_VERSION}, ${FEATURES})"
if [ -n "$FEATURES" ]; then
  cargo nextest run \
    -p "$PACKAGE" \
    --features "${PG_VERSION} ${FEATURES}" \
    --no-default-features
else
  cargo nextest run \
    -p "$PACKAGE" \
    --features "${PG_VERSION}" \
    --no-default-features
fi

echo "==> Running pgrx suite for ${PACKAGE} (${PG_VERSION}, ${FEATURES})"
if [ -n "$FEATURES" ]; then
  RUST_TEST_THREADS=1 cargo pgrx test "$PG_VERSION" \
    -p "$PACKAGE" \
    --no-default-features \
    -F "$FEATURES"
else
  RUST_TEST_THREADS=1 cargo pgrx test "$PG_VERSION" \
    -p "$PACKAGE" \
    --no-default-features
fi