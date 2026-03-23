#!/usr/bin/env bash
#
# pgrx-test-setup.sh — Set up the pgrx-managed PostgreSQL 16 for DDL e2e testing.
#
# Prerequisites:
#   1. pgrx PG16 installed: cargo pgrx init --pg16 download
#   2. KalamDB server running locally with cluster mode (gRPC on :9188, HTTP on :8080)
#
# Usage:
#   ./pg/scripts/pgrx-test-setup.sh          # Full setup (install extension + create DB + server)
#   ./pg/scripts/pgrx-test-setup.sh --start  # Just start PG if stopped
#   ./pg/scripts/pgrx-test-setup.sh --stop   # Stop pgrx PG
#   ./pg/scripts/pgrx-test-setup.sh --psql   # Open psql against the test DB
#
# After setup, connect manually:
#   ~/.pgrx/16.13/pgrx-install/bin/psql -h localhost -p 28816 -U $USER -d kalamdb_test
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_DIR="$SCRIPT_DIR/../.."
PG_CRATE="$SCRIPT_DIR/.."

# pgrx PG16 paths
PGRX_HOME="$HOME/.pgrx"
PG_CONFIG="$PGRX_HOME/16.13/pgrx-install/bin/pg_config"
PSQL="$PGRX_HOME/16.13/pgrx-install/bin/psql"
PG_CTL="$PGRX_HOME/16.13/pgrx-install/bin/pg_ctl"
DATA_DIR="$PGRX_HOME/data-16"

# Connection settings (pgrx defaults)
PG_HOST="localhost"
PG_PORT=28816
PG_USER="$USER"
TEST_DB="kalamdb_test"

# KalamDB gRPC (local server)
KALAMDB_GRPC_HOST="127.0.0.1"
KALAMDB_GRPC_PORT="9188"

info()  { echo "  [INFO] $*"; }
error() { echo "  [ERROR] $*" >&2; }

check_pg_config() {
    if [ ! -x "$PG_CONFIG" ]; then
        error "pgrx PG16 not found at $PG_CONFIG"
        error "Run: cargo pgrx init --pg16 download"
        exit 1
    fi
}

start_pg() {
    if pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; then
        info "PostgreSQL already running on port $PG_PORT"
        return 0
    fi
    info "Starting pgrx PostgreSQL 16..."
    "$PG_CTL" -D "$DATA_DIR" -l "$DATA_DIR/pgrx.log" -o "-p $PG_PORT" start
    # Wait for ready
    for i in $(seq 1 15); do
        if pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; then
            info "PostgreSQL ready on port $PG_PORT"
            return 0
        fi
        sleep 1
    done
    error "PostgreSQL did not start within 15s"
    exit 1
}

stop_pg() {
    info "Stopping pgrx PostgreSQL 16..."
    "$PG_CTL" -D "$DATA_DIR" stop -m fast 2>/dev/null || info "Already stopped"
}

install_extension() {
    info "Building and installing pg_kalam extension..."
    cd "$PG_CRATE"
    cargo pgrx install --pg-config="$PG_CONFIG" 2>&1 | tail -3
    info "Extension installed"
}

setup_database() {
    local psql_cmd="$PSQL -h $PG_HOST -p $PG_PORT -U $PG_USER"

    # Create test database
    if $psql_cmd -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$TEST_DB'" | grep -q 1; then
        info "Database '$TEST_DB' already exists"
    else
        info "Creating database '$TEST_DB'..."
        $psql_cmd -d postgres -c "CREATE DATABASE $TEST_DB;"
    fi

    # Install extension (handles CREATE OR REPLACE via IF NOT EXISTS)
    info "Installing pg_kalam extension in '$TEST_DB'..."
    $psql_cmd -d "$TEST_DB" -c "DROP EXTENSION IF EXISTS pg_kalam CASCADE;"
    $psql_cmd -d "$TEST_DB" -c "CREATE EXTENSION pg_kalam;"

    # Create foreign server pointing to local KalamDB gRPC
    info "Creating foreign server 'kalam_server' → $KALAMDB_GRPC_HOST:$KALAMDB_GRPC_PORT..."
    $psql_cmd -d "$TEST_DB" -c "
        CREATE SERVER IF NOT EXISTS kalam_server
            FOREIGN DATA WRAPPER pg_kalam
            OPTIONS (host '$KALAMDB_GRPC_HOST', port '$KALAMDB_GRPC_PORT');
    "

    # Ensure pg_kalam is in shared_preload_libraries so the DDL hook loads at startup
    local conf="$DATA_DIR/postgresql.conf"
    if grep -q "shared_preload_libraries.*pg_kalam" "$conf" 2>/dev/null; then
        info "shared_preload_libraries already includes pg_kalam"
    else
        info "Adding pg_kalam to shared_preload_libraries..."
        echo "shared_preload_libraries = 'pg_kalam'" >> "$conf"
        info "Restarting PostgreSQL to pick up shared_preload_libraries..."
        "$PG_CTL" -D "$DATA_DIR" restart -l "$DATA_DIR/pgrx.log"
        sleep 2
    fi

    info "Setup complete!"
    info ""
    info "Connect with:"
    info "  $PSQL -h $PG_HOST -p $PG_PORT -U $PG_USER -d $TEST_DB"
    info ""
    info "Run e2e DDL tests with:"
    info "  cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_ddl)'"
}

open_psql() {
    exec "$PSQL" -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$TEST_DB"
}

# --- Main ---

check_pg_config

case "${1:-}" in
    --start)
        start_pg
        ;;
    --stop)
        stop_pg
        ;;
    --psql)
        open_psql
        ;;
    --install)
        install_extension
        ;;
    *)
        start_pg
        install_extension
        setup_database
        ;;
esac
