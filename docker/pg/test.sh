#!/usr/bin/env bash
# ==========================================================================
# KalamDB PostgreSQL Extension — Docker Test Runner
# ==========================================================================
#
# Runs the end-to-end test suite against the kalamdb-pg Docker container.
#
# Usage:
#   # 1. Generate Linux extension artifacts (from repo root):
#   ./docker/pg/build.sh --artifacts
#
#   # 2. Start the official PostgreSQL container with mounted artifacts:
#   cd docker/pg && docker compose up -d
#
#   # 3. Run this script:
#   ./test.sh
#
# Environment variables:
#   PGHOST     — PostgreSQL host     (default: localhost)
#   PGPORT     — PostgreSQL port     (default: 5433)
#   PGUSER     — PostgreSQL user     (default: kalamdb)
#   PGPASSWORD — PostgreSQL password (default: kalamdb123)
#   PGDATABASE — PostgreSQL database (default: kalamdb)
# ==========================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5433}"
export PGUSER="${PGUSER:-kalamdb}"
export PGPASSWORD="${PGPASSWORD:-kalamdb123}"
export PGDATABASE="${PGDATABASE:-kalamdb}"

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
echo " KalamDB PG Extension — Test Suite"
echo "========================================"
echo " Host:     $PGHOST:$PGPORT"
echo " Database: $PGDATABASE"
echo " User:     $PGUSER"
echo "========================================"
echo ""

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if "$PG_ISREADY_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1; then
        echo "PostgreSQL is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: PostgreSQL did not become ready within 30 seconds."
        echo "Is the container running?  docker compose up -d"
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

echo ""
echo "========================================"
echo " All tests passed!"
echo "========================================"
