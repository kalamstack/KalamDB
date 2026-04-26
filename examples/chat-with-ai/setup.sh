#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KALAMDB_URL="${KALAMDB_URL:-http://127.0.0.1:8080}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
SQL_FILE="$SCRIPT_DIR/chat-app.sql"
ENV_FILE="$SCRIPT_DIR/.env.local"
ACCESS_TOKEN=""
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

log "Creating topic for agent consumption"
execute_sql_allow_exists "CREATE TOPIC chat_demo.ai_inbox"
execute_sql_allow_exists "ALTER TOPIC chat_demo.ai_inbox ADD SOURCE chat_demo.messages ON INSERT"

log "Ensuring demo admin user exists"
execute_sql_allow_exists "CREATE USER '$ADMIN_USER' WITH PASSWORD '$ADMIN_PASSWORD' ROLE dba"

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
VITE_KALAMDB_USER=$ADMIN_USER
VITE_KALAMDB_PASSWORD=$ADMIN_PASSWORD

KALAMDB_URL=$KALAMDB_URL
KALAMDB_USER=$ADMIN_USER
KALAMDB_PASSWORD=$ADMIN_PASSWORD
EOF

log "Chat demo is ready"
echo
echo "Next: npm install && npm run agent && npm run dev"
