#!/usr/bin/env bash
# ==========================================================================
# pg/test.sh — pg_kalam end-to-end test runner
# ==========================================================================
#
# Ensures Docker images are available, runs all Rust e2e tests (which mirror
# the README workflows), and tears down the test stack on exit.
#
# Usage (from repo root OR from pg/):
#   ./pg/test.sh
#   ./pg/test.sh --build                # rebuild extension image first
#   ./pg/test.sh --build-server         # also build KalamDB server from source
#   ./pg/test.sh --server-image <img>   # use a specific KalamDB server image
#   ./pg/test.sh --pg-image <img>       # use a specific pg_kalam extension image
#   ./pg/test.sh --no-cleanup           # leave Docker stack running after tests
#   ./pg/test.sh --filter <pattern>     # nextest filter (default: test(e2e))
#   ./pg/test.sh --help
# ==========================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker/docker-compose.test.yml"
COMPOSE_PROJECT="kalam-e2e"

# ── Defaults ──────────────────────────────────────────────────────────────
BUILD_EXTENSION=false
BUILD_SERVER=false
NO_CLEANUP=false
export KALAMDB_IMAGE="${KALAMDB_IMAGE:-}"
export KALAMDB_PG_IMAGE="${KALAMDB_PG_IMAGE:-kalamdb-pg:latest}"
NEXTEST_FILTER="test(e2e)"
USE_NO_FAIL_FAST=false

# ── Argument parsing ───────────────────────────────────────────────────────
usage() {
    sed -n '/^# ====$/,/^# ====$/p' "$0" 2>/dev/null || head -25 "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build)            BUILD_EXTENSION=true ;;
        --build-server)     BUILD_SERVER=true ;;
        --server-image)     KALAMDB_IMAGE="$2"; shift ;;
        --pg-image)         KALAMDB_PG_IMAGE="$2"; export KALAMDB_PG_IMAGE; shift ;;
        --no-cleanup)       NO_CLEANUP=true ;;
        --filter)           NEXTEST_FILTER="$2"; shift ;;
        --no-fail-fast)     USE_NO_FAIL_FAST=true ;;
        --fail-fast)        USE_NO_FAIL_FAST=false ;;
        -h|--help)          usage; exit 0 ;;
        *)                  echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# ── Cleanup trap ──────────────────────────────────────────────────────────
cleanup() {
    local exit_code=$?
    echo ""
    if $NO_CLEANUP; then
        echo "==> --no-cleanup set: leaving Docker stack running."
        echo "    Stop it with:"
        echo "      docker compose -f pg/docker/docker-compose.test.yml -p kalam-e2e down -v"
    else
        echo "==> Tearing down e2e Docker stack ..."
        docker compose \
            -f "$COMPOSE_FILE" \
            -p "$COMPOSE_PROJECT" \
            down -v --remove-orphans 2>/dev/null || true
        echo "    Stack removed."
    fi
    exit $exit_code
}
trap cleanup EXIT

# ── Helpers ───────────────────────────────────────────────────────────────
step() { echo ""; echo "==> $*"; }
ok()   { echo "    OK: $*"; }
die()  { echo ""; echo "ERROR: $*" >&2; exit 1; }

image_exists() { docker image inspect "$1" &>/dev/null; }

# ── Print config ──────────────────────────────────────────────────────────
echo "========================================================"
echo " pg_kalam End-to-End Tests"
echo "========================================================"
echo " Repo root:        $REPO_ROOT"
echo " Extension image:  ${KALAMDB_PG_IMAGE}"
echo " Server image:     ${KALAMDB_IMAGE:-jamals86/kalamdb:latest (default)}"
echo " Nextest filter:   $NEXTEST_FILTER"
echo " Cleanup:          $( $NO_CLEANUP && echo 'disabled (--no-cleanup)' || echo 'enabled' )"
echo "========================================================"

# ── Step 1: Build KalamDB server image (optional) ────────────────────────
if $BUILD_SERVER; then
    step "Building KalamDB server image from source ..."
    docker build \
        -f "$REPO_ROOT/docker/build/Dockerfile" \
        -t kalamdb:local \
        "$REPO_ROOT"
    export KALAMDB_IMAGE="kalamdb:local"
    ok "Server image built: kalamdb:local"
elif [[ -n "$KALAMDB_IMAGE" ]]; then
    if ! image_exists "$KALAMDB_IMAGE"; then
        step "Pulling server image $KALAMDB_IMAGE ..."
        docker pull "$KALAMDB_IMAGE" || die "Could not pull $KALAMDB_IMAGE"
    fi
    ok "Server image: $KALAMDB_IMAGE"
else
    # Use DockerHub default; pull if not cached
    if ! image_exists "jamals86/kalamdb:latest"; then
        step "Pulling jamals86/kalamdb:latest ..."
        docker pull jamals86/kalamdb:latest \
            || die "Could not pull jamals86/kalamdb:latest. Build it with --build-server or pass --server-image."
    fi
    ok "Server image: jamals86/kalamdb:latest (cached)"
fi

# ── Step 2: Build pg_kalam extension image ────────────────────────────────
if $BUILD_EXTENSION; then
    step "Building pg_kalam extension image ($KALAMDB_PG_IMAGE) ..."
    "$SCRIPT_DIR/docker/build-fast.sh"
    ok "Extension image built: $KALAMDB_PG_IMAGE"
elif ! image_exists "$KALAMDB_PG_IMAGE"; then
    step "Extension image $KALAMDB_PG_IMAGE not found — building it now ..."
    echo "    (pass --build to force a rebuild on subsequent runs)"
    "$SCRIPT_DIR/docker/build-fast.sh"
    ok "Extension image built: $KALAMDB_PG_IMAGE"
else
    ok "Extension image: $KALAMDB_PG_IMAGE (cached)"
fi

# ── Step 3: Pre-clean any leftover stack ──────────────────────────────────
step "Cleaning up any leftover e2e stack ..."
docker compose \
    -f "$COMPOSE_FILE" \
    -p "$COMPOSE_PROJECT" \
    down -v --remove-orphans 2>/dev/null || true
ok "Leftover stack cleared"

# ── Step 4: Check prerequisite tools ─────────────────────────────────────
step "Checking prerequisites ..."

if ! command -v cargo-nextest &>/dev/null && ! cargo nextest --version &>/dev/null 2>&1; then
    die "cargo-nextest is required. Install it with:
    cargo install cargo-nextest --locked"
fi
ok "cargo-nextest available"

if ! docker info &>/dev/null; then
    die "Docker is not running. Start Docker Desktop and retry."
fi
ok "Docker daemon reachable"

# ── Step 5: Run e2e tests ─────────────────────────────────────────────────
step "Running e2e tests ..."
echo "    Command: cargo nextest run -p kalam-pg-extension --features e2e -E '$NEXTEST_FILTER'$( $USE_NO_FAIL_FAST && printf ' --no-fail-fast' )"
echo "    The first test will start the Docker Compose stack automatically."
echo ""

cd "$REPO_ROOT"

# Export image vars so the Rust TestEnv/docker compose picks them up
[[ -n "${KALAMDB_IMAGE:-}" ]] && export KALAMDB_IMAGE

cargo nextest run \
    -p kalam-pg-extension \
    --features e2e \
    -E "$NEXTEST_FILTER" \
    --test-threads 1 \
    $( $USE_NO_FAIL_FAST && printf '%s ' '--no-fail-fast' )
    2>&1

# If we reach here, all tests passed (set -euo pipefail would have exited otherwise)
echo ""
echo "========================================================"
echo " All e2e tests passed!"
echo "========================================================"
