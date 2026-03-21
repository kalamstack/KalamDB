#!/usr/bin/env bash
# ==========================================================================
# KalamDB PostgreSQL Extension — Remote Mode Docker Test Runner
# ==========================================================================
#
# End-to-end test for the remote-mode FDW extension connecting to a
# KalamDB server running on the host machine.
#
# Prerequisites:
#   1. KalamDB server running on the host (cd backend && cargo run)
#   2. Remote extension artifacts built: KALAM_PG_BUILD_MODE=remote ./docker/pg/build.sh
#   3. PostgreSQL container running: cd docker/pg && docker compose -f docker-compose.remote.yml up -d
#
# Usage:
#   ./test-remote.sh
#
# Environment variables:
#   PGHOST           — PostgreSQL host     (default: localhost)
#   PGPORT           — PostgreSQL port     (default: 5434)
#   PGUSER           — PostgreSQL user     (default: kalamdb)
#   PGPASSWORD       — PostgreSQL password (default: kalamdb123)
#   PGDATABASE       — PostgreSQL database (default: kalamdb)
#   KALAMDB_API_URL  — KalamDB HTTP API    (default: http://localhost:8080)
#   KALAMDB_PASSWORD — Admin password      (default: kalamdb123)
# ==========================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5434}"
export PGUSER="${PGUSER:-kalamdb}"
export PGPASSWORD="${PGPASSWORD:-kalamdb123}"
export PGDATABASE="${PGDATABASE:-kalamdb}"

KALAMDB_API_URL="${KALAMDB_API_URL:-http://localhost:8080}"
KALAMDB_PASSWORD="${KALAMDB_PASSWORD:-kalamdb123}"

if [ -x "$HOME/.pgrx/16.13/pgrx-install/bin/pg_isready" ]; then
    PG_ISREADY_BIN="$HOME/.pgrx/16.13/pgrx-install/bin/pg_isready"
else
    PG_ISREADY_BIN="$(command -v pg_isready)"
fi

if [ -x "$HOME/.pgrx/16.13/pgrx-install/bin/psql" ]; then
    PSQL_BIN="$HOME/.pgrx/16.13/pgrx-install/bin/psql"
else
    PSQL_BIN="$(command -v psql)"
fi

echo "========================================"
echo " KalamDB PG Extension — Remote Mode Test"
echo "========================================"
echo " PG Host:     $PGHOST:$PGPORT"
echo " PG Database: $PGDATABASE"
echo " PG User:     $PGUSER"
echo " KalamDB API: $KALAMDB_API_URL"
echo "========================================"
echo ""

# --------------------------------------------------------------------------
# Step 1: Check KalamDB server is reachable
# --------------------------------------------------------------------------
echo "Checking KalamDB server at $KALAMDB_API_URL ..."
for i in $(seq 1 15); do
    if curl -sf "$KALAMDB_API_URL/health" > /dev/null 2>&1; then
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

# --------------------------------------------------------------------------
# Step 2: Create namespace + table on KalamDB server via HTTP API
# --------------------------------------------------------------------------
echo ""
echo "Bootstrapping KalamDB namespace 'rmtest' and table 'profiles' ..."

# Login to get a Bearer token
LOGIN_RESP=$(curl -sf "$KALAMDB_API_URL/v1/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\"}" \
    2>/dev/null || true)

BEARER_TOKEN=$(echo "$LOGIN_RESP" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)
if [ -z "$BEARER_TOKEN" ]; then
    echo "WARNING: Could not login to KalamDB (admin user may not exist)."
    echo "  Trying setup first..."
    curl -sf "$KALAMDB_API_URL/v1/api/auth/setup" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\",\"root_password\":\"$KALAMDB_PASSWORD\"}" \
        > /dev/null 2>&1 || true
    LOGIN_RESP=$(curl -sf "$KALAMDB_API_URL/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"$KALAMDB_PASSWORD\"}" \
        2>/dev/null || true)
    BEARER_TOKEN=$(echo "$LOGIN_RESP" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)
fi

if [ -z "$BEARER_TOKEN" ]; then
    echo "ERROR: Could not authenticate with KalamDB server."
    exit 1
fi
echo "  Authenticated with KalamDB server."

# Create namespace (ignore if already exists)
RESP=$(curl -s "$KALAMDB_API_URL/v1/api/sql" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"sql": "CREATE NAMESPACE IF NOT EXISTS rmtest"}' \
    2>/dev/null)
echo "  CREATE NAMESPACE: $(echo "$RESP" | grep -o '"message":"[^"]*"' | head -1)"

# Create user table (ignore if already exists)
RESP=$(curl -s "$KALAMDB_API_URL/v1/api/sql" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"sql": "CREATE USER TABLE IF NOT EXISTS rmtest.profiles (id TEXT PRIMARY KEY, name TEXT, age INTEGER)"}' \
    2>/dev/null)
echo "  CREATE TABLE: $(echo "$RESP" | grep -o '"message":"[^"]*"' | head -1)"

# --------------------------------------------------------------------------
# Step 3: Wait for PostgreSQL container to be ready
# --------------------------------------------------------------------------
echo ""
echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if "$PG_ISREADY_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1; then
        echo "PostgreSQL is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: PostgreSQL did not become ready within 30 seconds."
        echo "Is the container running?"
        echo "  cd docker/pg && docker compose -f docker-compose.remote.yml up -d"
        exit 1
    fi
    sleep 1
done

# --------------------------------------------------------------------------
# Step 4: Run the remote-mode SQL test
# --------------------------------------------------------------------------
echo ""
echo "Running test-remote.sql ..."
echo ""

PAGER=cat "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -v ON_ERROR_STOP=1 \
    -P pager=off \
    -f "$SCRIPT_DIR/test-remote.sql"

echo ""
echo "========================================"
echo " All remote-mode tests passed!"
echo "========================================"
