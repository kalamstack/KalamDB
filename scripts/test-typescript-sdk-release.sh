#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${TS_SDK_RELEASE_TMP_DIR:-$ROOT_DIR/ts-sdk-release}"
SERVER_URL="${KALAMDB_URL:-http://localhost:8080}"
SERVER_USER="${KALAMDB_USER:-admin}"
SERVER_PASSWORD="${KALAMDB_PASSWORD:-kalamdb123}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
JWT_SECRET="sdk-test-secret-key-minimum-32-characters-long"
SERVER_LOG="${TS_SDK_SERVER_LOG:-$ROOT_DIR/ts-sdk-server.log}"
TEST_OUTPUT="${TS_SDK_TEST_OUTPUT:-$ROOT_DIR/ts-sdk-test-output.txt}"
SERVER_BIN="${KALAMDB_SERVER_BIN:-}"
SKIP_SERVER_START="${KALAMDB_SKIP_SERVER_START:-false}"
SKIP_AUTH_SETUP="${KALAMDB_SKIP_AUTH_SETUP:-false}"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

: > "$SERVER_LOG"

if [[ "$SKIP_SERVER_START" != "true" ]]; then
    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR/data" "$WORK_DIR/logs"
    cp "$ROOT_DIR/backend/server.example.toml" "$WORK_DIR/server.toml"

    perl -0pi -e 's|data_path = "\./data"|data_path = "'"$WORK_DIR"'/data"|g; s|logs_path = "\./logs"|logs_path = "'"$WORK_DIR"'/logs"|g; s|jwt_secret = ".*"|jwt_secret = "'"$JWT_SECRET"'"|g' "$WORK_DIR/server.toml"

    if [[ -n "$SERVER_BIN" ]]; then
        SERVER_CMD=("$SERVER_BIN" "$WORK_DIR/server.toml")
    else
        SERVER_CMD=(cargo run --manifest-path "$ROOT_DIR/backend/Cargo.toml" --bin kalamdb-server -- "$WORK_DIR/server.toml")
    fi

    (
        cd "$ROOT_DIR"
        KALAMDB_SERVER_HOST=0.0.0.0 \
        KALAMDB_JWT_SECRET="$JWT_SECRET" \
        "${SERVER_CMD[@]}" > "$SERVER_LOG" 2>&1
    ) &
    SERVER_PID=$!
fi

for i in {1..60}; do
    if curl -sf "$SERVER_URL/health" > /dev/null 2>&1 \
        || curl -sf "$SERVER_URL/v1/api/healthcheck" > /dev/null 2>&1; then
        echo "✅ TypeScript SDK test server ready (${i}s)"
        break
    fi
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "❌ TypeScript SDK test server died"
        cat "$SERVER_LOG" || true
        exit 1
    fi
    echo "  Waiting for TypeScript SDK test server... ($i/60)"
    sleep 1
done

if ! curl -sf "$SERVER_URL/health" > /dev/null 2>&1 \
    && ! curl -sf "$SERVER_URL/v1/api/healthcheck" > /dev/null 2>&1; then
    echo "❌ Timed out waiting for TypeScript SDK test server"
    cat "$SERVER_LOG" || true
    exit 1
fi

if [[ "$SKIP_AUTH_SETUP" != "true" ]]; then
    curl -fsS "$SERVER_URL/v1/api/auth/setup" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"$SERVER_USER\",\"password\":\"$SERVER_PASSWORD\",\"root_password\":\"$ROOT_PASSWORD\"}" \
        >/dev/null
fi

(
    cd "$ROOT_DIR/link/sdks/typescript"
    chmod +x ./test.sh
    KALAMDB_URL="$SERVER_URL" \
    KALAMDB_USER="$SERVER_USER" \
    KALAMDB_PASSWORD="$SERVER_PASSWORD" \
    ./test.sh
) 2>&1 | tee "$TEST_OUTPUT"