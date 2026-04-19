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
AUTH_TMP_DIR=""

json_token_from_file() {
    local path="$1"
    node -e 'const fs = require("fs"); const body = JSON.parse(fs.readFileSync(process.argv[1], "utf8")); process.stdout.write(body.access_token || "");' "$path"
}

sql_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

try_login() {
    local user="$1"
    local password="$2"
    local body_file="$3"
    local status
    status=$(curl -sS -o "$body_file" -w '%{http_code}' \
        -H 'Content-Type: application/json' \
        -d "{\"user\":\"$user\",\"password\":\"$password\"}" \
        "$SERVER_URL/v1/api/auth/login")
    [[ "$status" == "200" ]]
}

ensure_test_auth_ready() {
    if [[ "$SKIP_AUTH_SETUP" == "true" ]]; then
        return 0
    fi

    AUTH_TMP_DIR="$(mktemp -d)"
    local status_body="$AUTH_TMP_DIR/status.json"
    local login_body="$AUTH_TMP_DIR/login.json"
    local root_login_body="$AUTH_TMP_DIR/root-login.json"
    local user_check_body="$AUTH_TMP_DIR/user-check.json"
    local user_sql_body="$AUTH_TMP_DIR/user-sql.json"
    trap 'if [[ -n "${AUTH_TMP_DIR:-}" ]]; then rm -rf "$AUTH_TMP_DIR"; AUTH_TMP_DIR=""; fi' RETURN

    if curl -fsS "$SERVER_URL/v1/api/auth/status" > "$status_body" 2>/dev/null; then
        if grep -Eq '"needs_setup"[[:space:]]*:[[:space:]]*true' "$status_body"; then
            curl -fsS "$SERVER_URL/v1/api/auth/setup" \
                -H "Content-Type: application/json" \
                -d "{\"user\":\"$SERVER_USER\",\"password\":\"$SERVER_PASSWORD\",\"root_password\":\"$ROOT_PASSWORD\",\"email\":null}" \
                >/dev/null
            return 0
        fi
    fi

    if try_login "$SERVER_USER" "$SERVER_PASSWORD" "$login_body"; then
        return 0
    fi

    if [[ "$SERVER_USER" == "root" ]]; then
        echo "Configured root credentials failed for $SERVER_URL" >&2
        cat "$login_body" >&2
        return 1
    fi

    echo "⚠️  Configured test user '$SERVER_USER' failed to log in; trying root bootstrap path."
    if ! try_login root "$ROOT_PASSWORD" "$root_login_body"; then
        echo "Could not authenticate test user '$SERVER_USER' or root against $SERVER_URL" >&2
        echo "Test user response:" >&2
        cat "$login_body" >&2
        echo "Root response:" >&2
        cat "$root_login_body" >&2
        return 1
    fi

    local root_token
    root_token="$(json_token_from_file "$root_login_body")"
    if [[ -z "$root_token" ]]; then
        echo "Root login succeeded but no access token was returned." >&2
        cat "$root_login_body" >&2
        return 1
    fi

    local user_sql
    user_sql="$(sql_escape "$SERVER_USER")"
    local password_sql
    password_sql="$(sql_escape "$SERVER_PASSWORD")"
    local check_sql
    check_sql="SELECT username FROM system.users WHERE username = '$user_sql' LIMIT 1"
    curl -sS -o "$user_check_body" \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $root_token" \
        -d "{\"sql\":\"$check_sql\"}" \
        "$SERVER_URL/v1/api/sql" >/dev/null

    local repair_sql
    if grep -q "\"user\":\"$SERVER_USER\"" "$user_check_body"; then
        repair_sql="ALTER USER '$user_sql' SET PASSWORD '$password_sql'; ALTER USER '$user_sql' SET ROLE 'dba';"
    else
        repair_sql="CREATE USER '$user_sql' WITH PASSWORD '$password_sql' ROLE 'dba'"
    fi

    local repair_status
    repair_status=$(curl -sS -o "$user_sql_body" -w '%{http_code}' \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $root_token" \
        -d "{\"sql\":\"$repair_sql\"}" \
        "$SERVER_URL/v1/api/sql")
    if [[ "$repair_status" != "200" ]] && ! grep -Eiq 'already exists|duplicate|conflict|idempotent' "$user_sql_body"; then
        echo "Failed to ensure TypeScript SDK test user '$SERVER_USER'." >&2
        cat "$user_sql_body" >&2
        return 1
    fi

    if try_login "$SERVER_USER" "$SERVER_PASSWORD" "$login_body"; then
        return 0
    fi

    echo "⚠️  Test user '$SERVER_USER' still cannot log in after root bootstrap; falling back to root for this run."
    SERVER_USER=root
    SERVER_PASSWORD="$ROOT_PASSWORD"
    if ! try_login "$SERVER_USER" "$SERVER_PASSWORD" "$login_body"; then
        echo "Root fallback also failed for $SERVER_URL" >&2
        cat "$login_body" >&2
        return 1
    fi
}

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

if [[ "$SKIP_SERVER_START" != "true" ]]; then
    : > "$SERVER_LOG"
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

if [[ -s "$SERVER_LOG" ]]; then
    echo "Recent TypeScript SDK server log output:"
    tail -n 40 "$SERVER_LOG" || true
fi

ensure_test_auth_ready

(
    echo "Running @kalamdb/client tests..."
    cd "$ROOT_DIR/link/sdks/typescript/client"
    chmod +x ./test.sh
    KALAMDB_URL="$SERVER_URL" \
    KALAMDB_USER="$SERVER_USER" \
    KALAMDB_PASSWORD="$SERVER_PASSWORD" \
    ./test.sh

    echo "Running @kalamdb/consumer tests..."
    cd "$ROOT_DIR/link/sdks/typescript/consumer"
    npm install --no-audit --no-fund
    npm test
) 2>&1 | tee "$TEST_OUTPUT"