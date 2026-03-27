#!/usr/bin/env bash
# Build the kalamdb-pg Docker image (PostgreSQL 16 + pg_kalam extension).
#
# The Dockerfile is a 2-stage build:
#   Stage 1 (builder) — Compiles the pgrx extension from source
#   Stage 2 (runtime) — Copies artifacts into stock postgres:16-bookworm
#
# Usage (from any directory):
#   ./pg/docker/build.sh              # build using cache
#   ./pg/docker/build.sh --rebuild    # force full rebuild (no cache)
#
# After building:
#   cd pg/docker && docker compose up -d
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE_NAME="${KALAMDB_PG_IMAGE:-kalamdb-pg:latest}"

DOCKER_ARGS=()
if [[ "${1:-}" == "--rebuild" ]]; then
    DOCKER_ARGS+=(--no-cache)
fi

echo "==> Building $IMAGE_NAME ..."
docker build \
    -f "$REPO_ROOT/pg/docker/Dockerfile" \
    -t "$IMAGE_NAME" \
    ${DOCKER_ARGS[@]+"${DOCKER_ARGS[@]}"} \
    "$REPO_ROOT"

echo ""
echo "Done. Image: $IMAGE_NAME"
echo "Start with: cd pg/docker && docker compose up -d"
