#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KALAMDB_URL="${KALAMDB_URL:-http://127.0.0.1:8080}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
SQL_FILE="$SCRIPT_DIR/activity-feed.sql"
ENV_FILE="$SCRIPT_DIR/.env.local"
ACCESS_TOKEN=""
DEMO_TOKEN=""
ADMIN_USER="admin"
ADMIN_PASSWORD="kalamdb123"

log() {
    echo "[setup] $*"
}

fail() {
    echo "[setup][error] $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

show_help() {
    cat <<EOF
Usage: ./setup.sh [--server URL] [--password ROOT_PASSWORD]

Creates the demo schema, ensures the local demo user exists, seeds a few rows,
and writes .env.local for the Vite app.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server)
            KALAMDB_URL="$2"
            shift 2
            ;;
        --password)
            ROOT_PASSWORD="$2"
            shift 2
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            fail "Unknown option: $1"
            ;;
    esac
done

require_cmd curl
require_cmd jq

try_login() {
    local user="$1"
    local password="$2"
    local response
    local http_code
    local body
    local payload

    payload="$(jq -cn --arg user "$user" --arg password "$password" '{user: $user, password: $password}')"
    response="$(curl -sS -w "\n%{http_code}" -X POST "$KALAMDB_URL/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "$payload")"

    http_code="$(echo "$response" | tail -1)"
    body="$(echo "$response" | sed '$d')"

    if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        return 1
    fi

    ACCESS_TOKEN="$(echo "$body" | jq -r '.access_token // empty')"
    [[ -n "$ACCESS_TOKEN" ]]
}

server_needs_setup() {
    local response
    response="$(curl -fsS "$KALAMDB_URL/v1/api/auth/status")"
    [[ "$(echo "$response" | jq -r '.needs_setup // false')" == "true" ]]
}

run_initial_setup() {
    local payload
    local response
    local http_code
    local body

    log "Server requires initial setup - creating bootstrap DBA user '$ADMIN_USER'"

    payload="$(jq -cn \
        --arg user "$ADMIN_USER" \
        --arg password "$ADMIN_PASSWORD" \
        --arg root_password "$ROOT_PASSWORD" \
        '{user: $user, password: $password, root_password: $root_password}')"
    response="$(curl -sS -w "\n%{http_code}" -X POST "$KALAMDB_URL/v1/api/auth/setup" \
        -H "Content-Type: application/json" \
        -d "$payload")"

    http_code="$(echo "$response" | tail -1)"
    body="$(echo "$response" | sed '$d')"

    if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        fail "Initial server setup failed: $body"
    fi
}

ensure_access_token() {
    if server_needs_setup; then
        run_initial_setup
    fi

    if try_login "$ADMIN_USER" "$ADMIN_PASSWORD"; then
        return 0
    fi

    try_login root "$ROOT_PASSWORD" || fail "Failed to obtain admin or root access token"
}

curl -fsS "$KALAMDB_URL/health" >/dev/null || fail "KalamDB is not reachable at $KALAMDB_URL"

ensure_access_token

execute_root_sql() {
    local sql="$1"
    curl -fsS -X POST "$KALAMDB_URL/v1/api/sql" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -d "{\"sql\": $(jq -Rs . <<<"$sql")}" >/dev/null
}

execute_root_sql_allow_exists() {
    local sql="$1"
    local response
    response="$(curl -sS -w "\n%{http_code}" -X POST "$KALAMDB_URL/v1/api/sql" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -d "{\"sql\": $(jq -Rs . <<<"$sql")}")"
    local http_code
    http_code="$(echo "$response" | tail -1)"
    local body
    body="$(echo "$response" | sed '$d')"

    if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
        return 0
    fi

    if echo "$body" | grep -Eiq 'already exists|duplicate|conflict|idempotent'; then
        return 0
    fi

    echo "$body" >&2
    return 1
}

log "Applying schema"
SQL_BUFFER=""
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue
    SQL_BUFFER+="$line "
    if [[ "$line" =~ \;[[:space:]]*$ ]]; then
        statement="${SQL_BUFFER%;*}"
        SQL_BUFFER=""
        execute_root_sql_allow_exists "$statement"
    fi
done < "$SQL_FILE"

log "Ensuring demo-user exists"
execute_root_sql_allow_exists "CREATE USER 'demo-user' WITH PASSWORD 'demo123' ROLE user"

DEMO_TOKEN="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"user":"demo-user","password":"demo123"}' | jq -r '.access_token // empty')"
[[ -n "$DEMO_TOKEN" ]] || fail "Failed to obtain demo-user token"

execute_demo_sql() {
    local sql="$1"
    curl -fsS -X POST "$KALAMDB_URL/v1/api/sql" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $DEMO_TOKEN" \
        -d "{\"sql\": $(jq -Rs . <<<"$sql")}" >/dev/null
}

log "Seeding demo-user rows"
row_count="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/sql" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $DEMO_TOKEN" \
    -d '{"sql":"SELECT COUNT(*) AS total FROM demo.activity_feed"}' | jq -r '(try .results[0].rows[0].total catch empty) // (try .results[0].rows[0][0] catch 0)')"

if [[ "${row_count:-0}" == "0" ]]; then
    execute_demo_sql "INSERT INTO demo.activity_feed (service, level, actor, message) VALUES ('api', 'ok', 'system', 'Subscriptions are live for this browser session')"
    execute_demo_sql "INSERT INTO demo.activity_feed (service, level, actor, message) VALUES ('payments', 'warn', 'ops-bot', 'Retry queue grew above the morning baseline')"
    execute_demo_sql "INSERT INTO demo.activity_feed (service, level, actor, message) VALUES ('search', 'critical', 'pager', 'Cold shard promoted and traffic recovered')"
fi

if [[ ! -f "$ENV_FILE" ]]; then
    cat > "$ENV_FILE" <<EOF
VITE_KALAMDB_URL=$KALAMDB_URL
VITE_KALAMDB_USER=demo-user
VITE_KALAMDB_PASSWORD=demo123
EOF
fi

log "Realtime dashboard is ready"
echo
echo "Next: npm install && npm run dev"
