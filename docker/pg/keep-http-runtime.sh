#!/usr/bin/env bash
set -euo pipefail

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${POSTGRES_USER:-kalamdb}"
export PGPASSWORD="${POSTGRES_PASSWORD:-kalamdb123}"
export PGDATABASE="${POSTGRES_DB:-kalamdb}"

wait_for_postgres() {
    until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
        sleep 1
    done
}

start_runtime_session() {
    PAGER=cat psql \
        -h "$PGHOST" \
        -p "$PGPORT" \
        -U "$PGUSER" \
        -d "$PGDATABASE" \
        -v ON_ERROR_STOP=1 \
        -P pager=off <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_kalam;
SELECT pg_kalam_embedded_start(NULL);
SELECT pg_sleep(2147483647);
SQL
}

wait_for_postgres

while true; do
    echo "starting persistent embedded KalamDB runtime session"
    if ! start_runtime_session; then
        echo "embedded KalamDB runtime session exited; retrying in 2s" >&2
        sleep 2
    fi
done