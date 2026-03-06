#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KALAMDB_URL="${KALAMDB_URL:-http://127.0.0.1:8080}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
SQL_FILE="$SCRIPT_DIR/chat-app.sql"
ENV_FILE="$SCRIPT_DIR/.env.local"
ACCESS_TOKEN=""

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

Creates the user-scoped chat table, the live agent event stream, and the local
admin user used by the UI and the agent.
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

ACCESS_TOKEN="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"root\",\"password\":\"$ROOT_PASSWORD\"}" | jq -r '.access_token // empty')"
[[ -n "$ACCESS_TOKEN" ]] || fail "Failed to obtain root access token"

execute_sql() {
  local sql="$1"
  curl -sS -w "\n%{http_code}" -X POST "$KALAMDB_URL/v1/api/sql" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -d "{\"sql\": $(jq -Rs . <<<"$sql")}" 
}

execute_sql_allow_exists() {
  local sql="$1"
  local response
  response="$(execute_sql "$sql")"
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

execute_sql_required() {
  local sql="$1"
  local response
  response="$(execute_sql "$sql")"
  local http_code
  http_code="$(echo "$response" | tail -1)"
  local body
  body="$(echo "$response" | sed '$d')"

  if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
    return 0
  fi

  echo "$body" >&2
  return 1
}

execute_sql_optional() {
  local sql="$1"
  execute_sql "$sql" >/dev/null || true
}

log "Applying schema"
execute_sql_optional "DROP TOPIC chat_demo.ai_inbox"
SQL_BUFFER=""
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue
  SQL_BUFFER+="$line "
  if [[ "$line" =~ \;[[:space:]]*$ ]]; then
    statement="${SQL_BUFFER%;*}"
    SQL_BUFFER=""
    execute_sql_allow_exists "$statement"
  fi
done < "$SQL_FILE"

log "Ensuring demo admin user exists"
execute_sql_allow_exists "CREATE USER 'admin' WITH PASSWORD 'kalamdb123' ROLE dba"

count_result="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/sql" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -d '{"sql":"SELECT COUNT(*) AS total FROM chat_demo.messages"}')"
row_count="$(echo "$count_result" | jq -r '(try .results[0].rows[0].total catch empty) // (try .results[0].rows[0][0] catch 0)')"

if [[ "${row_count:-0}" == "0" ]]; then
  execute_sql_allow_exists "EXECUTE AS USER 'admin' (INSERT INTO chat_demo.messages (room, role, author, sender_username, content) VALUES ('main', 'assistant', 'KalamDB Copilot', 'admin', 'AI reply: The worker will stream its thinking through chat_demo.agent_events before it commits a full assistant reply.'))"
fi

cat > "$ENV_FILE" <<EOF
VITE_KALAMDB_URL=$KALAMDB_URL
VITE_KALAMDB_USERNAME=admin
VITE_KALAMDB_PASSWORD=kalamdb123

KALAMDB_URL=$KALAMDB_URL
KALAMDB_USERNAME=admin
KALAMDB_PASSWORD=kalamdb123
EOF

log "Chat demo is ready"
echo
echo "Next: npm install && npm run agent && npm run dev"
