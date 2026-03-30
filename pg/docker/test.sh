#!/usr/bin/env bash
# ==========================================================================
# KalamDB PostgreSQL Extension — Docker Test Runner
# ==========================================================================
#
# End-to-end test for the pg_kalam FDW extension running in docker-compose.
# Both KalamDB and PostgreSQL run as compose services.
#
# Prerequisites:
#   1. Build the extension image:
#      ./pg/docker/build.sh
#   2. Start compose services:
#      cd pg/docker && docker compose up -d
#
# Environment variables:
#   PGHOST           — PostgreSQL host     (default: localhost)
#   PGPORT           — PostgreSQL port     (default: 5433)
#   PGUSER           — PostgreSQL user     (default: kalamdb)
#   PGPASSWORD       — PostgreSQL password (default: kalamdb123)
#   PGDATABASE       — PostgreSQL database (default: kalamdb)
#   KALAMDB_API_URL  — KalamDB HTTP API    (default: http://localhost:8088)
#   KALAMDB_PASSWORD — Admin password      (default: kalamdb123)
# ==========================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGRX_HOME="$HOME/.pgrx"
PG_MAJOR="${PG_MAJOR:-16}"

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5433}"
export PGUSER="${PGUSER:-kalamdb}"
export PGPASSWORD="${PGPASSWORD:-kalamdb123}"
export PGDATABASE="${PGDATABASE:-kalamdb}"

KALAMDB_API_URL="${KALAMDB_API_URL:-http://localhost:8088}"
KALAMDB_PASSWORD="${KALAMDB_PASSWORD:-kalamdb123}"

find_pgrx_bin() {
    local binary="$1"
    local candidate
    candidate="$(find "$PGRX_HOME" -maxdepth 3 -type f -path "$PGRX_HOME/${PG_MAJOR}.*/pgrx-install/bin/$binary" 2>/dev/null | sort -V | tail -n 1)"
    if [[ -n "$candidate" ]]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    command -v "$binary"
}

PG_ISREADY_BIN="$(find_pgrx_bin pg_isready)"
PSQL_BIN="$(find_pgrx_bin psql)"

echo "========================================"
echo " KalamDB PG Extension — Docker Test"
echo "========================================"
echo " PG Host:     $PGHOST:$PGPORT"
echo " PG Database: $PGDATABASE"
echo " PG User:     $PGUSER"
echo " KalamDB API: $KALAMDB_API_URL"
echo "========================================"
echo ""

# Step 1: Check KalamDB server is reachable
echo "Checking KalamDB server at $KALAMDB_API_URL ..."
for i in $(seq 1 15); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        --connect-timeout 2 \
        --max-time 5 \
        -X POST "$KALAMDB_API_URL/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\"}" \
        || true)
    if [ "$HTTP_CODE" != "000" ]; then
        echo "KalamDB server is reachable."
        break
    fi
    if [ "$i" -eq 15 ]; then
        echo "ERROR: KalamDB server at $KALAMDB_API_URL is not reachable."
        echo "Start it with: cd backend && cargo run"
        exit 1
    fi
    sleep 2
done

echo ""
echo "Bootstrapping KalamDB namespace 'rmtest' and table 'profiles' ..."

LOGIN_RESP=$(curl -sf "$KALAMDB_API_URL/v1/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\"}" \
    2>/dev/null || true)

BEARER_TOKEN=$(printf '%s' "$LOGIN_RESP" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
if [ -z "$BEARER_TOKEN" ]; then
    echo "WARNING: Could not login to KalamDB. Trying setup first..."
    curl -sf "$KALAMDB_API_URL/v1/api/auth/setup" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\",\"root_password\":\"$KALAMDB_PASSWORD\"}" \
        > /dev/null 2>&1 || true
    LOGIN_RESP=$(curl -sf "$KALAMDB_API_URL/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\"}" \
        2>/dev/null || true)
    BEARER_TOKEN=$(printf '%s' "$LOGIN_RESP" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
fi

if [ -z "$BEARER_TOKEN" ]; then
    echo "ERROR: Could not authenticate with KalamDB server."
    exit 1
fi

curl -s "$KALAMDB_API_URL/v1/api/sql" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"sql": "CREATE NAMESPACE IF NOT EXISTS rmtest"}' > /dev/null

curl -s "$KALAMDB_API_URL/v1/api/sql" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"sql": "CREATE USER TABLE IF NOT EXISTS rmtest.profiles (id TEXT PRIMARY KEY, name TEXT, age INTEGER)"}' > /dev/null

curl -s "$KALAMDB_API_URL/v1/api/sql" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"sql": "CREATE SHARED TABLE IF NOT EXISTS rmtest.shared_items (id TEXT PRIMARY KEY, title TEXT, value INTEGER)"}' > /dev/null

echo "KalamDB tables bootstrapped (rmtest.profiles, rmtest.shared_items)."

echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if "$PG_ISREADY_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1; then
        echo "PostgreSQL is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: PostgreSQL did not become ready within 30 seconds."
        echo "Is the container running?  cd pg/docker && docker compose up -d"
        exit 1
    fi
    sleep 1
done

echo ""
echo "Running test.sql ..."
echo ""

PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -f "$SCRIPT_DIR/test.sql"

# ── Cross-Verification: verify FDW writes are visible via KalamDB REST API ──
echo ""
echo "Cross-verifying FDW writes via KalamDB REST API ..."

# Insert a verification row via PG FDW
PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -c "SET kalam.user_id = 'cross-verify-user'; INSERT INTO rmtest.profiles (id, name, age) VALUES ('xv1', 'CrossVerify', 99);"

# Query the same row via KalamDB HTTP API
API_RESULT=$(curl -s "$KALAMDB_API_URL/v1/api/sql" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"sql": "SELECT id, name, age FROM rmtest.profiles WHERE id = '\''xv1'\''"}')

if echo "$API_RESULT" | grep -q '"xv1"'; then
    echo "  PASS: FDW-inserted row 'xv1' is visible via KalamDB REST API."
else
    echo "  FAIL: FDW-inserted row 'xv1' NOT found via KalamDB REST API!"
    echo "  API response: $API_RESULT"
    exit 1
fi

# Clean up the cross-verification row
PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -c "SET kalam.user_id = 'cross-verify-user'; DELETE FROM rmtest.profiles WHERE id = 'xv1';"

# ── Concurrent Clients Test ──
echo ""
echo "Testing concurrent PostgreSQL clients ..."

# Launch 5 concurrent psql sessions, each doing INSERT + SELECT
for i in $(seq 1 5); do
    (
        PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
            -v ON_ERROR_STOP=1 \
            -P pager=off \
            -c "SET kalam.user_id = 'concurrent-user'; INSERT INTO rmtest.profiles (id, name, age) VALUES ('cc$i', 'Concurrent$i', $((20 + i))); SELECT COUNT(*) FROM rmtest.profiles;" \
            > /dev/null 2>&1
    ) &
done

# Wait for all background jobs to finish
FAIL=0
for job in $(jobs -p); do
    wait "$job" || FAIL=1
done

if [ "$FAIL" -eq 0 ]; then
    echo "  PASS: 5 concurrent clients completed successfully."
else
    echo "  FAIL: One or more concurrent clients failed!"
    exit 1
fi

# Verify all concurrent rows exist
CC_COUNT=$( PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -t -A -c "SET kalam.user_id = 'concurrent-user'; SELECT COUNT(*) FROM rmtest.profiles WHERE id IN ('cc1','cc2','cc3','cc4','cc5');" | tail -n 1 | tr -d '[:space:]' )

if [ "$CC_COUNT" -ge 5 ]; then
    echo "  PASS: All 5 concurrent rows visible ($CC_COUNT)."
else
    echo "  FAIL: Expected 5 concurrent rows, got $CC_COUNT"
    exit 1
fi

# Cleanup concurrent rows
PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -c "SET kalam.user_id = 'concurrent-user'; DELETE FROM rmtest.profiles WHERE id IN ('cc1','cc2','cc3','cc4','cc5');" \
    > /dev/null 2>&1

echo ""
echo "========================================"
echo " All tests passed!"
echo "========================================"
