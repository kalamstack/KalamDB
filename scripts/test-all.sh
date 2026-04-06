#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USE_NO_FAIL_FAST=false
SERVER_URL="${KALAMDB_SERVER_URL:-http://127.0.0.1:8080}"
TMP_DIR="${TMPDIR:-/tmp}"

usage() {
    cat <<'EOF'
Usage: ./scripts/test-all.sh [options]

Options:
  --server-url <url>   KalamDB server URL to target (default: http://127.0.0.1:8080)
  --no-fail-fast       Pass --no-fail-fast to nextest-based suites
  --fail-fast          Force fail-fast mode (default)
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server-url)
            SERVER_URL="$2"
            shift 2
            ;;
        --no-fail-fast)
            USE_NO_FAIL_FAST=true
            shift
            ;;
        --fail-fast)
            USE_NO_FAIL_FAST=false
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "Unknown option: $1"
            ;;
    esac
done

export KALAMDB_SERVER_URL="$SERVER_URL"
export KALAMDB_ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
export KALAMDB_ADMIN_USER="${KALAMDB_ADMIN_USER:-admin}"
export KALAMDB_ADMIN_PASSWORD="${KALAMDB_ADMIN_PASSWORD:-$KALAMDB_ROOT_PASSWORD}"
export KALAMDB_URL="${KALAMDB_URL:-$KALAMDB_SERVER_URL}"
export KALAMDB_USER="${KALAMDB_USER:-root}"
export KALAMDB_PASSWORD="${KALAMDB_PASSWORD:-$KALAMDB_ROOT_PASSWORD}"
export KALAM_URL="${KALAM_URL:-$KALAMDB_URL}"
export KALAM_USER="${KALAM_USER:-$KALAMDB_USER}"
export KALAM_PASS="${KALAM_PASS:-$KALAMDB_PASSWORD}"

step() {
    echo
    echo "==> $*"
}

die() {
    echo
    echo "ERROR: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "Missing required command '$1'"
}

json_token_from_file() {
    local path="$1"
    node -e 'const fs = require("fs"); const body = JSON.parse(fs.readFileSync(process.argv[1], "utf8")); process.stdout.write(body.access_token || "");' "$path"
}

sql_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

ensure_admin_user() {
    local admin_login_body="$TMP_DIR/kalamdb-admin-login.json"
    local root_login_body="$TMP_DIR/kalamdb-root-login.json"
    local user_check_body="$TMP_DIR/kalamdb-admin-check.json"
    local user_sql_body="$TMP_DIR/kalamdb-admin-sql.json"

    local admin_status
    admin_status=$(curl -sS -o "$admin_login_body" -w '%{http_code}' \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"$KALAMDB_ADMIN_USER\",\"password\":\"$KALAMDB_ADMIN_PASSWORD\"}" \
        "$KALAMDB_SERVER_URL/v1/api/auth/login")
    if [[ "$admin_status" == "200" ]]; then
        echo "    Admin test user '$KALAMDB_ADMIN_USER' already available"
        return
    fi

    local root_status
    root_status=$(curl -sS -o "$root_login_body" -w '%{http_code}' \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"root\",\"password\":\"$KALAMDB_ROOT_PASSWORD\"}" \
        "$KALAMDB_SERVER_URL/v1/api/auth/login")
    [[ "$root_status" == "200" ]] || die "Unable to authenticate as root to prepare admin test user"

    local root_token
    root_token=$(json_token_from_file "$root_login_body")
    [[ -n "$root_token" ]] || die "Unable to read root access token while preparing admin test user"

    local check_sql
    check_sql="SELECT username FROM system.users WHERE username = '$(sql_escape "$KALAMDB_ADMIN_USER")' LIMIT 1"
    curl -sS -o "$user_check_body" \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $root_token" \
        -d "{\"sql\":\"$check_sql\"}" \
        "$KALAMDB_SERVER_URL/v1/api/sql" >/dev/null

    local admin_exists=false
    if grep -q "\"username\":\"$KALAMDB_ADMIN_USER\"" "$user_check_body"; then
        admin_exists=true
    fi

    local admin_password_sql
    admin_password_sql=$(sql_escape "$KALAMDB_ADMIN_PASSWORD")
    local admin_user_sql
    admin_user_sql=$(sql_escape "$KALAMDB_ADMIN_USER")

    local sql
    if [[ "$admin_exists" == true ]]; then
        sql="ALTER USER $admin_user_sql SET PASSWORD '$admin_password_sql'; ALTER USER $admin_user_sql SET ROLE 'dba';"
    else
        sql="CREATE USER $admin_user_sql WITH PASSWORD '$admin_password_sql' ROLE 'dba'"
    fi

    local sql_status
    sql_status=$(curl -sS -o "$user_sql_body" -w '%{http_code}' \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer $root_token" \
        -d "{\"sql\":\"$sql\"}" \
        "$KALAMDB_SERVER_URL/v1/api/sql")
    if [[ "$sql_status" != "200" ]]; then
        if ! grep -q 'already exists' "$user_sql_body"; then
            die "Failed to ensure admin test user: $(cat "$user_sql_body")"
        fi
    fi

    admin_status=$(curl -sS -o "$admin_login_body" -w '%{http_code}' \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"$KALAMDB_ADMIN_USER\",\"password\":\"$KALAMDB_ADMIN_PASSWORD\"}" \
        "$KALAMDB_SERVER_URL/v1/api/auth/login")
    [[ "$admin_status" == "200" ]] || die "Admin test user '$KALAMDB_ADMIN_USER' is still not usable after bootstrap"

    echo "    Prepared admin test user '$KALAMDB_ADMIN_USER'"
}

run_npm_test() {
    local dir="$1"
    local label="$2"

    step "$label"
    (
        cd "$ROOT_DIR/$dir"
        npm install --no-audit --no-fund
        if [[ "$dir" == "ui" ]]; then
            npm test -- --run
        else
            npm test
        fi
    )
}

run_typescript_sdk_tests() {
    step "Running TypeScript client SDK tests"
    (
        cd "$ROOT_DIR/link/sdks/typescript/client"
        chmod +x ./test.sh
        ./test.sh
    )

    step "Running TypeScript consumer SDK tests"
    (
        cd "$ROOT_DIR/link/sdks/typescript/consumer"
        npm install --no-audit --no-fund
        npm test
    )
}

step "Checking required tools"
require_cmd cargo
require_cmd curl
require_cmd npm
require_cmd flutter

if ! cargo nextest --version >/dev/null 2>&1; then
    die "cargo-nextest is required. Install it with: cargo install cargo-nextest --locked"
fi

step "Checking KalamDB server at $KALAMDB_SERVER_URL"
if ! curl -sf "$KALAMDB_SERVER_URL/health" >/dev/null 2>&1; then
    die "KalamDB server is not reachable at $KALAMDB_SERVER_URL. Start it first with: cd backend && cargo run"
fi

step "Ensuring admin test user"
ensure_admin_user

step "Clearing shared test token cache"
rm -f "$TMP_DIR/kalamdb_test_tokens.json" "$TMP_DIR/kalamdb_test_tokens.lock"

cd "$ROOT_DIR"

step "Running Rust workspace tests with CLI e2e coverage"
if $USE_NO_FAIL_FAST; then
    cargo nextest run \
        --workspace \
        --all-targets \
        --features "kalam-cli/e2e-tests" \
        --no-fail-fast
else
    cargo nextest run \
        --workspace \
        --all-targets \
        --features "kalam-cli/e2e-tests"
fi

step "Running feature-gated FDW import tests"
if $USE_NO_FAIL_FAST; then
    cargo nextest run \
        -p kalam-pg-fdw \
        --features import-foreign-schema \
        --test import_foreign_schema \
        --no-fail-fast
else
    cargo nextest run \
        -p kalam-pg-fdw \
        --features import-foreign-schema \
        --test import_foreign_schema
fi

step "Running PostgreSQL extension end-to-end tests"
if $USE_NO_FAIL_FAST; then
    "$ROOT_DIR/pg/test.sh" --no-fail-fast
else
    "$ROOT_DIR/pg/test.sh" --fail-fast
fi

run_typescript_sdk_tests
run_npm_test "ui" "Running admin UI tests"

step "Running Dart SDK tests"
(
    cd "$ROOT_DIR/link/sdks/dart"
    ./test.sh
)

echo
echo "========================================================"
echo " All core KalamDB test suites passed"
echo "========================================================"