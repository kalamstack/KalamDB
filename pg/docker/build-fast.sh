#!/usr/bin/env bash
# Fast iterative build for pg_kalam PostgreSQL extension.
#
# Compiles in a Docker container with persistent cargo cache volumes,
# then packages into a lightweight runtime image.
#
# Performance:
#   First build   — ~14 min (downloads & compiles everything)
#   Subsequent    — ~30-60s (incremental compilation, cached deps)
#   Runtime image — <5s (just COPY of prebuilt artifacts)
#
# Usage:
#   ./pg/docker/build-fast.sh                        # compile + build runtime image
#   ./pg/docker/build-fast.sh --compile               # compile only, output to artifacts/
#   ./pg/docker/build-fast.sh --runtime               # runtime image only (artifacts must exist)
#   ./pg/docker/build-fast.sh --rebuild-base          # force rebuild the builder base image
#   ./pg/docker/build-fast.sh --server                # also build KalamDB server from source (→ kalamdb:local)
#   ./pg/docker/build-fast.sh --server-image <img>    # print export for using an existing server image
#
# After building:
#   cd pg/docker && docker compose up -d
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
IMAGE_NAME="${KALAMDB_PG_IMAGE:-pg-kalam:latest}"
IMAGE_DESCRIPTION_FILE="$SCRIPT_DIR/image-description.txt"
OCI_IMAGE_DESCRIPTION="$(tr '\n' ' ' < "$IMAGE_DESCRIPTION_FILE" | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//')"
PG_MAJOR="${PG_MAJOR:-16}"
PG_EXTENSION_FLAVOR="${PG_EXTENSION_FLAVOR:-pg${PG_MAJOR}}"
BUILDER_IMAGE="pg-kalam-builder-pg${PG_MAJOR}"
RUST_BASE_IMAGE="${RUST_BASE_IMAGE:-public.ecr.aws/docker/library/rust:1.92-bookworm}"
POSTGRES_BASE_IMAGE="${POSTGRES_BASE_IMAGE:-public.ecr.aws/docker/library/postgres:${PG_MAJOR}-bookworm}"
PGRX_VERSION="${PGRX_VERSION:-0.18.0}"

DO_COMPILE=true
DO_RUNTIME=true
REBUILD_BASE=false
DO_BUILD_SERVER=false
SERVER_IMAGE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --compile)        DO_RUNTIME=false ;;
        --runtime)        DO_COMPILE=false ;;
        --rebuild-base)   REBUILD_BASE=true ;;
        --server)         DO_BUILD_SERVER=true ;;
        --server-image)   SERVER_IMAGE="$2"; shift ;;
        *)                echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

EXPECTED_FLAVOR="pg${PG_MAJOR}"
if [[ "$PG_EXTENSION_FLAVOR" != "$EXPECTED_FLAVOR" ]]; then
    echo "PG_EXTENSION_FLAVOR must match PG_MAJOR (expected $EXPECTED_FLAVOR, got $PG_EXTENSION_FLAVOR)"
    exit 1
fi

# ── Step 0: (Optional) Build KalamDB server from source ─────────────────
build_server() {
    echo "==> Building KalamDB server image from source (→ kalamdb:local) ..."
    docker build \
        -f "$REPO_ROOT/docker/build/Dockerfile" \
        -t kalamdb:local \
        "$REPO_ROOT"
    echo "    Done. Tagged: kalamdb:local"
    echo ""
    echo "    To use this image with the test stack:"
    echo "      export KALAMDB_IMAGE=kalamdb:local"
}

# ── Step 1: Ensure builder base image exists ──────────────────────────────
ensure_builder_base() {
    if $REBUILD_BASE || ! docker image inspect "$BUILDER_IMAGE" &>/dev/null; then
        echo "==> Building builder base image ($BUILDER_IMAGE) ..."
        docker build \
            --build-arg RUST_BASE_IMAGE="$RUST_BASE_IMAGE" \
            --build-arg PG_MAJOR="$PG_MAJOR" \
            --build-arg PGRX_VERSION="$PGRX_VERSION" \
            -f "$SCRIPT_DIR/Dockerfile.builder-base" \
            -t "$BUILDER_IMAGE" \
            "$REPO_ROOT"
        echo "    Builder base ready."
    else
        echo "==> Builder base image exists ($BUILDER_IMAGE), reusing."
    fi
}

# ── Step 2: Compile extension with cached volumes ─────────────────────────
compile_extension() {
    echo "==> Compiling pg_kalam extension (cached) ..."
    mkdir -p "$ARTIFACTS_DIR"

    docker run --rm \
        -v "$REPO_ROOT:/src:ro" \
        -v pg-kalam-cargo-registry:/usr/local/cargo/registry \
        -v pg-kalam-cargo-git:/usr/local/cargo/git \
        -v pg-kalam-target:/target-cache \
        -v "$ARTIFACTS_DIR:/artifacts" \
        -e CARGO_TARGET_DIR=/target-cache \
        -e CARGO_INCREMENTAL=1 \
        -e PG_MAJOR="$PG_MAJOR" \
        -e PG_EXTENSION_FLAVOR="$PG_EXTENSION_FLAVOR" \
        -e RUSTFLAGS="-Cdebuginfo=0" \
        "$BUILDER_IMAGE" \
        bash -c '
            cd /src && \
            cargo pgrx install \
                -p kalam-pg-extension \
                -c "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" \
                --no-default-features \
                --profile release-pg \
                -F "${PG_EXTENSION_FLAVOR}" && \
            cp "/usr/lib/postgresql/${PG_MAJOR}/lib/pg_kalam.so" /artifacts/ && \
            cp "/usr/share/postgresql/${PG_MAJOR}/extension/pg_kalam.control" /artifacts/ && \
            cp "/usr/share/postgresql/${PG_MAJOR}/extension/pg_kalam--"*.sql /artifacts/
        '

    echo "    Artifacts:"
    ls -lh "$ARTIFACTS_DIR"/pg_kalam*
}

# ── Step 3: Build lightweight runtime image ───────────────────────────────
build_runtime() {
    if [[ ! -f "$ARTIFACTS_DIR/pg_kalam.so" ]]; then
        echo "ERROR: $ARTIFACTS_DIR/pg_kalam.so not found."
        echo "Run with --compile first or without flags to compile + build."
        exit 1
    fi

    echo "==> Building runtime image ($IMAGE_NAME) ..."
    docker build \
        --build-arg PG_MAJOR="$PG_MAJOR" \
        --build-arg OCI_IMAGE_DESCRIPTION="$OCI_IMAGE_DESCRIPTION" \
        --build-arg POSTGRES_BASE_IMAGE="$POSTGRES_BASE_IMAGE" \
        -f "$SCRIPT_DIR/Dockerfile.runtime" \
        -t "$IMAGE_NAME" \
        "$SCRIPT_DIR"

    echo "    Done. Image: $IMAGE_NAME"
}

# ── Main ──────────────────────────────────────────────────────────────────
if $DO_BUILD_SERVER; then
    build_server
fi

if [[ -n "$SERVER_IMAGE" ]]; then
    if ! docker image inspect "$SERVER_IMAGE" &>/dev/null; then
        echo "==> Pulling server image $SERVER_IMAGE ..."
        docker pull "$SERVER_IMAGE"
    fi
    echo "==> Server image ready: $SERVER_IMAGE"
    echo "    To use it with the test/dev stack:"
    echo "      export KALAMDB_IMAGE=$SERVER_IMAGE"
fi

if $DO_COMPILE; then
    ensure_builder_base
    compile_extension
fi

if $DO_RUNTIME; then
    build_runtime
fi

echo ""
if $DO_BUILD_SERVER; then
    echo "KalamDB server image: kalamdb:local"
    echo "Extension image:      $IMAGE_NAME"
    echo ""
    echo "Run tests with:"
    echo "  KALAMDB_IMAGE=kalamdb:local ./pg/test.sh"
else
    echo "Start with: cd pg/docker && docker compose up -d"
fi
