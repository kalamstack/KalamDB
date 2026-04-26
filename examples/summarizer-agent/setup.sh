#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KALAMDB_URL="${KALAMDB_URL:-http://localhost:8080}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD-kalamdb123}"
SQL_FILE="$SCRIPT_DIR/setup.sql"
ENV_FILE="$SCRIPT_DIR/.env.local"
ACCESS_TOKEN=""
SAMPLE_BLOG_ID=""
FORCE_ENV_WRITE=0
ROOT_PASSWORD_EXPLICIT=0
ADMIN_USER="admin"
ADMIN_PASSWORD="kalamdb123"

if [[ -n "${KALAMDB_ROOT_PASSWORD+x}" ]]; then
  ROOT_PASSWORD_EXPLICIT=1
fi

log_info() {
  echo "[setup] $*"
}

log_warn() {
  echo "[setup][warn] $*"
}

log_success() {
  echo "[setup][ok] $*"
}

log_error() {
  echo "[setup][error] $*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "Missing required command: $1"
    exit 1
  fi
}

check_server() {
  log_info "Checking server: $KALAMDB_URL"
  if ! curl -fsS "$KALAMDB_URL/health" >/dev/null; then
    log_error "KalamDB is not reachable at $KALAMDB_URL"
    log_error "Start it first: cd backend && cargo run"
    exit 1
  fi
}

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

  if [[ -z "$ROOT_PASSWORD" ]]; then
    log_error "Initial server setup requires a non-empty root password. Set KALAMDB_ROOT_PASSWORD or pass --password."
    exit 1
  fi

  log_info "Server requires initial setup - creating bootstrap DBA user '$ADMIN_USER'"

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
    log_error "Initial server setup failed"
    log_error "$body"
    exit 1
  fi
}

ensure_access_token() {
  if server_needs_setup; then
    run_initial_setup
  fi

  if try_login "$ADMIN_USER" "$ADMIN_PASSWORD"; then
    return 0
  fi

  log_info "Logging in as root"
  if try_login root "$ROOT_PASSWORD"; then
    return 0
  fi

  log_error "Could not get access token. Check KALAMDB_ROOT_PASSWORD or initialize the server via /v1/api/auth/setup."
  exit 1
}

execute_sql_raw() {
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
    echo "$body"
    return 0
  fi

  echo "$body"
  return 1
}

execute_sql_allow_exists() {
  local sql="$1"
  local output
  if output="$(execute_sql_raw "$sql")"; then
    return 0
  fi

  if echo "$output" | grep -Eiq "already exists|duplicate|conflict|idempotent"; then
    log_info "Ignoring idempotent error for: $sql"
    return 0
  fi

  log_error "SQL failed: $sql"
  log_error "$output"
  return 1
}

execute_sql_file() {
  log_info "Applying schema + topic routes from $(basename "$SQL_FILE")"

  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue

    SQL_BUFFER+="${line} "
    if [[ "$line" =~ \;[[:space:]]*$ ]]; then
      local stmt
      stmt="${SQL_BUFFER%;*}"
      stmt="$(echo "$stmt" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      SQL_BUFFER=""

      [[ -z "$stmt" ]] && continue
      execute_sql_allow_exists "$stmt"
    fi
  done < "$SQL_FILE"
}

verify() {
  log_info "Verifying sample row"
  local result
  result="$(execute_sql_raw "SELECT blog_id, content, summary, created, updated FROM blog.blogs ORDER BY created DESC LIMIT 1")"

  local row_count
  row_count="$(echo "$result" | jq -r '.results[0].row_count // 0')"
  if [[ "${row_count}" -lt 1 ]]; then
    log_error "Expected at least one blog row, but none were found."
    exit 1
  fi

  SAMPLE_BLOG_ID="$(echo "$result" | jq -r '(try .results[0].rows[0].blog_id catch empty) // (try .results[0].rows[0][0] catch empty)')"
  if [[ -z "$SAMPLE_BLOG_ID" || "$SAMPLE_BLOG_ID" == "null" ]]; then
    log_error "Failed to extract sample blog_id from verification query."
    exit 1
  fi

  log_info "Sample blog_id: $SAMPLE_BLOG_ID"

  log_info "Verifying failure sink table blog.summary_failures"
  local failure_result
  if ! failure_result="$(execute_sql_raw "SELECT COUNT(*) AS c FROM blog.summary_failures")"; then
    log_error "Expected table blog.summary_failures to exist, but verification query failed."
    exit 1
  fi

  local failure_count
  failure_count="$(echo "$failure_result" | jq -r '(try .results[0].rows[0].c catch empty) // (try .results[0].rows[0][0] catch empty)')"
  if [[ -z "$failure_count" || "$failure_count" == "null" ]]; then
    log_error "Could not read row count for blog.summary_failures."
    exit 1
  fi

  log_info "summary_failures row_count=$failure_count"
}

generate_env_file() {
  if [[ -f "$ENV_FILE" && "$FORCE_ENV_WRITE" -ne 1 ]]; then
    log_warn ".env.local already exists - keeping current file"
    log_info "Use --force-env to overwrite"
    return 0
  fi

  log_info "Writing .env.local"
  cat > "$ENV_FILE" <<EOF
# KalamDB Connection (generated by setup.sh)
KALAMDB_URL=$KALAMDB_URL
KALAMDB_USER=$ADMIN_USER
KALAMDB_PASSWORD=$ADMIN_PASSWORD

# Agent settings
KALAMDB_TOPIC=blog.summarizer
KALAMDB_GROUP=blog-summarizer-agent
EOF

  log_success "Wrote $(basename "$ENV_FILE")"
}

show_help() {
  cat <<HELP
Setup summarizer-agent example

Usage:
  ./setup.sh [--server URL] [--password ROOT_PASSWORD] [--force-env]

Options:
  --server     KalamDB server URL (default: http://localhost:8080)
  --password   Root password (default: kalamdb123; localhost fallback: empty)
  --force-env  Overwrite existing .env.local
HELP
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)
      KALAMDB_URL="$2"
      shift 2
      ;;
    --password)
      ROOT_PASSWORD="$2"
      ROOT_PASSWORD_EXPLICIT=1
      shift 2
      ;;
    --force-env)
      FORCE_ENV_WRITE=1
      shift 1
      ;;
    --help)
      show_help
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      show_help
      exit 1
      ;;
  esac
done

require_cmd curl
require_cmd jq

check_server
ensure_access_token
execute_sql_allow_exists "CREATE USER '$ADMIN_USER' WITH PASSWORD '$ADMIN_PASSWORD' ROLE dba"
SQL_BUFFER=""
execute_sql_file
verify
generate_env_file

cat <<DONE

Setup complete.

Next:
  1. npm install
  2. npm run start
  3. Update content to trigger summarization (blog_id=$SAMPLE_BLOG_ID):
     curl -sS -X POST "$KALAMDB_URL/v1/api/sql" \\
       -H "Content-Type: application/json" \\
       -H "Authorization: Bearer <token>" \\
       -d '{"sql":"UPDATE blog.blogs SET content = ''KalamDB topics make event-driven agents simple'' WHERE blog_id = $SAMPLE_BLOG_ID"}'

DONE
