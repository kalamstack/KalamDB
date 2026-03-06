#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KALAMDB_URL="${KALAMDB_URL:-http://127.0.0.1:8080}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
SQL_FILE="$SCRIPT_DIR/activity-feed.sql"
ENV_FILE="$SCRIPT_DIR/.env.local"
ACCESS_TOKEN=""
DEMO_TOKEN=""

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

curl -fsS "$KALAMDB_URL/health" >/dev/null || fail "KalamDB is not reachable at $KALAMDB_URL"

log "Logging in as root"
ACCESS_TOKEN="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"root\",\"password\":\"$ROOT_PASSWORD\"}" | jq -r '.access_token // empty')"
[[ -n "$ACCESS_TOKEN" ]] || fail "Failed to obtain root access token"

execute_root_sql() {
    local sql="$1"
    curl -fsS -X POST "$KALAMDB_URL/v1/api/sql" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -d "{\"sql\": $(jq -Rs . <<<"$sql")}" >/dev/null
}

log "Applying schema"
SQL_BUFFER=""
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue
    SQL_BUFFER+="$line "
    if [[ "$line" =~ \;[[:space:]]*$ ]]; then
        statement="${SQL_BUFFER%;*}"
        SQL_BUFFER=""
        execute_root_sql "$statement" || true
    fi
done < "$SQL_FILE"

log "Ensuring demo-user exists"
execute_root_sql "CREATE USER 'demo-user' WITH PASSWORD 'demo123' ROLE user" || true

DEMO_TOKEN="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"demo-user","password":"demo123"}' | jq -r '.access_token // empty')"
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
VITE_KALAMDB_USERNAME=demo-user
VITE_KALAMDB_PASSWORD=demo123
EOF
fi

log "Realtime dashboard is ready"
echo
echo "Next: npm install && npm run dev"
