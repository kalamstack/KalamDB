#!/usr/bin/env bash
# Build the pg-kalam Docker image (PostgreSQL 16 + pg_kalam extension).
#
# The Dockerfile is a 2-stage build:
#   Stage 1 (builder) — Compiles the pgrx extension from source
#   Stage 2 (runtime) — Copies artifacts into the selected stock postgres image
#
# Usage (from any directory):
#   ./pg/docker/build.sh              # build using cache
#   ./pg/docker/build.sh --rebuild    # force full rebuild (no cache)
#
# After building:
#   cd pg/docker && docker compose up -d
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE_NAME="${KALAMDB_PG_IMAGE:-pg-kalam:latest}"
IMAGE_DESCRIPTION_FILE="$REPO_ROOT/pg/docker/image-description.txt"
OCI_IMAGE_DESCRIPTION="$(tr '\n' ' ' < "$IMAGE_DESCRIPTION_FILE" | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//')"
PG_MAJOR="${PG_MAJOR:-16}"
PG_EXTENSION_FLAVOR="${PG_EXTENSION_FLAVOR:-pg${PG_MAJOR}}"
RUST_BASE_IMAGE="${RUST_BASE_IMAGE:-public.ecr.aws/docker/library/rust:1.92-bookworm}"
POSTGRES_BASE_IMAGE="${POSTGRES_BASE_IMAGE:-public.ecr.aws/docker/library/postgres:${PG_MAJOR}-bookworm}"
PGRX_VERSION="${PGRX_VERSION:-0.17.0}"

DOCKER_ARGS=()
if [[ "${1:-}" == "--rebuild" ]]; then
    DOCKER_ARGS+=(--no-cache)
fi

EXPECTED_FLAVOR="pg${PG_MAJOR}"
if [[ "$PG_EXTENSION_FLAVOR" != "$EXPECTED_FLAVOR" ]]; then
    echo "PG_EXTENSION_FLAVOR must match PG_MAJOR (expected $EXPECTED_FLAVOR, got $PG_EXTENSION_FLAVOR)" >&2
    exit 1
fi

echo "==> Building $IMAGE_NAME ..."
docker build \
    --build-arg PG_MAJOR="$PG_MAJOR" \
    --build-arg PG_EXTENSION_FLAVOR="$PG_EXTENSION_FLAVOR" \
    --build-arg PGRX_VERSION="$PGRX_VERSION" \
    --build-arg OCI_IMAGE_DESCRIPTION="$OCI_IMAGE_DESCRIPTION" \
    --build-arg RUST_BASE_IMAGE="$RUST_BASE_IMAGE" \
    --build-arg POSTGRES_BASE_IMAGE="$POSTGRES_BASE_IMAGE" \
    -f "$REPO_ROOT/pg/docker/Dockerfile" \
    -t "$IMAGE_NAME" \
    ${DOCKER_ARGS[@]+"${DOCKER_ARGS[@]}"} \
    "$REPO_ROOT"

echo ""
echo "Done. Image: $IMAGE_NAME"
echo "Start with: cd pg/docker && docker compose up -d"
