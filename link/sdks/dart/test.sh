#!/bin/bash
set -euo pipefail

echo "🧪 Testing KalamDB Dart SDK..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Configuration ──────────────────────────────────────────────────────
export KALAMDB_URL="${KALAMDB_URL:-${KALAM_URL:-http://localhost:8080}}"
export KALAMDB_USER="${KALAMDB_USER:-${KALAM_USER:-admin}}"
export KALAMDB_PASSWORD="${KALAMDB_PASSWORD:-${KALAM_PASS:-kalamdb123}}"
export KALAMDB_ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-${KALAM_ROOT_PASSWORD:-$KALAMDB_PASSWORD}}"
export KALAM_URL="${KALAM_URL:-$KALAMDB_URL}"
export KALAM_USER="${KALAM_USER:-$KALAMDB_USER}"
export KALAM_PASS="${KALAM_PASS:-$KALAMDB_PASSWORD}"
export KALAM_ROOT_PASSWORD="${KALAM_ROOT_PASSWORD:-$KALAMDB_ROOT_PASSWORD}"

warn() {
  echo "⚠️  $*"
}

make_json() {
  python3 - "$@" <<'PY'
import json
import sys

items = sys.argv[1:]
if len(items) % 2 != 0:
    raise SystemExit("expected key/value pairs")

payload = {}
for index in range(0, len(items), 2):
    key = items[index]
    value = items[index + 1]
    if value == "__JSON_NULL__":
        payload[key] = None
    else:
        payload[key] = value

print(json.dumps(payload))
PY
}

extract_json_field() {
  local file_path="$1"
  local field_name="$2"
  python3 - "$file_path" "$field_name" <<'PY'
import json
import pathlib
import sys

file_path = pathlib.Path(sys.argv[1])
field_name = sys.argv[2]

try:
    data = json.loads(file_path.read_text())
except Exception:
    raise SystemExit(1)

value = data.get(field_name)
if value is None:
    raise SystemExit(1)

if isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

post_json() {
  local url="$1"
  local payload="$2"
  local body_file="$3"
  local auth_token="${4:-}"
  local curl_args=(
    -sS
    -o "$body_file"
    -w "%{http_code}"
    -X POST "$url"
    -H "Content-Type: application/json"
    -d "$payload"
  )

  if [[ -n "$auth_token" ]]; then
    curl_args+=( -H "Authorization: Bearer $auth_token" )
  fi

  curl "${curl_args[@]}"
}

try_login() {
  local username="$1"
  local password="$2"
  local body_file="$3"
  local payload
  payload="$(make_json username "$username" password "$password")"
  local http_code
  http_code="$(post_json "$KALAMDB_URL/v1/api/auth/login" "$payload" "$body_file")"
  [[ "$http_code" == "200" ]]
}

ensure_test_auth_ready() {
  if [[ "${KALAMDB_SKIP_AUTH_SETUP:-0}" == "1" || "${KALAMDB_SKIP_AUTH_SETUP:-false}" == "true" ]]; then
    return 0
  fi

  local status_file login_file setup_file root_login_file create_user_file
  status_file="$(mktemp)"
  login_file="$(mktemp)"
  setup_file="$(mktemp)"
  root_login_file="$(mktemp)"
  create_user_file="$(mktemp)"
  trap 'rm -f "$status_file" "$login_file" "$setup_file" "$root_login_file" "$create_user_file"' RETURN

  if curl -fsS "$KALAMDB_URL/v1/api/auth/status" > "$status_file" 2>/dev/null; then
    local needs_setup
    needs_setup="$(extract_json_field "$status_file" needs_setup 2>/dev/null || echo false)"
    if [[ "$needs_setup" == "true" ]]; then
      echo "🔐 Bootstrapping local auth for Dart SDK tests..."
      local setup_payload setup_code
      setup_payload="$(make_json username "$KALAMDB_USER" password "$KALAMDB_PASSWORD" root_password "$KALAMDB_ROOT_PASSWORD" email __JSON_NULL__)"
      setup_code="$(post_json "$KALAMDB_URL/v1/api/auth/setup" "$setup_payload" "$setup_file")"
      if [[ "$setup_code" != "200" ]]; then
        echo "Auth setup failed with HTTP $setup_code" >&2
        cat "$setup_file" >&2
        return 1
      fi
      return 0
    fi
  fi

  if try_login "$KALAMDB_USER" "$KALAMDB_PASSWORD" "$login_file"; then
    return 0
  fi

  if [[ "$KALAMDB_USER" == "root" ]]; then
    echo "Configured root credentials failed for $KALAMDB_URL" >&2
    cat "$login_file" >&2
    return 1
  fi

  warn "Configured test user '$KALAMDB_USER' failed to log in; trying root bootstrap path."
  if ! try_login root "$KALAMDB_ROOT_PASSWORD" "$root_login_file"; then
    echo "Could not authenticate test user '$KALAMDB_USER' or root against $KALAMDB_URL" >&2
    echo "Set KALAMDB_USER/KALAMDB_PASSWORD or KALAMDB_ROOT_PASSWORD to match the running server." >&2
    echo "Test user response:" >&2
    cat "$login_file" >&2
    echo "Root response:" >&2
    cat "$root_login_file" >&2
    return 1
  fi

  local root_token
  root_token="$(extract_json_field "$root_login_file" access_token 2>/dev/null || true)"
  if [[ -z "$root_token" ]]; then
    echo "Root login succeeded but no access token was returned." >&2
    cat "$root_login_file" >&2
    return 1
  fi

  echo "🔐 Ensuring Dart SDK test DBA user '$KALAMDB_USER' exists..."
  local create_sql create_payload create_code
  create_sql="CREATE USER '$KALAMDB_USER' WITH PASSWORD '$KALAMDB_PASSWORD' ROLE dba"
  create_payload="$(make_json sql "$create_sql")"
  create_code="$(post_json "$KALAMDB_URL/v1/api/sql" "$create_payload" "$create_user_file" "$root_token")"

  if [[ "$create_code" != "200" ]]; then
    if ! grep -Eiq 'already exists|duplicate|conflict|idempotent' "$create_user_file"; then
      echo "Failed to create Dart SDK test user '$KALAMDB_USER' with root credentials." >&2
      cat "$create_user_file" >&2
      return 1
    fi
  fi

  if try_login "$KALAMDB_USER" "$KALAMDB_PASSWORD" "$login_file"; then
    return 0
  fi

  warn "Test user '$KALAMDB_USER' still cannot log in after root bootstrap; falling back to root for this run."
  export KALAMDB_USER=root
  export KALAMDB_PASSWORD="$KALAMDB_ROOT_PASSWORD"
  export KALAM_USER="$KALAMDB_USER"
  export KALAM_PASS="$KALAMDB_PASSWORD"

  if ! try_login "$KALAMDB_USER" "$KALAMDB_PASSWORD" "$login_file"; then
    echo "Root fallback also failed for $KALAMDB_URL" >&2
    cat "$login_file" >&2
    return 1
  fi
}

# ── Dependencies ──────────────────────────────────────────────────────
echo "📦 Ensuring dependencies are installed..."
flutter pub get

BRIDGE_DIR="$SCRIPT_DIR/../../kalam-link-dart"
echo "🦀 Building host native library used by flutter test..."
(
  cd "$BRIDGE_DIR"
  CARGO_TARGET_DIR="$BRIDGE_DIR/target" cargo build --release
)

# ── Static analysis ──────────────────────────────────────────────────
echo ""
echo "🧭 Running analyzer checks..."
flutter analyze

# ── Unit tests (offline, no server required) ─────────────────────────
echo ""
echo "🔬 Running unit tests (no server)..."
flutter test test/models_test.dart

# ── E2E tests (require running KalamDB server) ──────────────────────
echo ""
echo "🔗 Checking server at $KALAMDB_URL ..."
if curl -sf "$KALAMDB_URL/health" > /dev/null 2>&1 \
  || curl -sf "$KALAMDB_URL/v1/api/healthcheck" > /dev/null 2>&1; then
  echo "✅ Server is reachable"
  ensure_test_auth_ready
  echo ""
  echo "🧪 Running e2e tests..."
  export KALAM_INTEGRATION_TEST=1
  flutter test test/e2e
  echo ""
  echo "✅ All Dart SDK tests passed!"
else
  echo "⚠️  Server not reachable at $KALAMDB_URL — skipping e2e tests."
  echo "   Start the server: cd backend && cargo run"
  echo "   Then re-run: ./test.sh"
  exit 1
fi
