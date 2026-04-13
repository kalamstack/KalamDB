#!/usr/bin/env bash
#
# pgrx-test-setup.sh — Set up a pgrx-managed PostgreSQL instance for pg e2e testing.
#
# Prerequisites:
#   1. pgrx PG installed: cargo pgrx init --pg<major> download
#   2. KalamDB server running locally with cluster mode (gRPC on :9188, HTTP on :8080)
#
# Usage:
#   ./pg/scripts/pgrx-test-setup.sh          # Full setup (start PG + install extension + create DB + server)
#   ./pg/scripts/pgrx-test-setup.sh --start  # Just start PG if stopped
#   ./pg/scripts/pgrx-test-setup.sh --stop   # Stop pgrx PG
#   ./pg/scripts/pgrx-test-setup.sh --psql   # Open psql against the test DB
#
# After setup, connect manually:
#   PG_MAJOR=17 ./pg/scripts/pgrx-test-setup.sh
#   ~/.pgrx/<pg-version>/pgrx-install/bin/psql -h localhost -p 28817 -U $USER -d kalamdb_test
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_DIR="$SCRIPT_DIR/../.."
PG_CRATE="$SCRIPT_DIR/.."

# pgrx-managed PostgreSQL selection
PGRX_HOME="$HOME/.pgrx"
PG_MAJOR="${PG_MAJOR:-16}"
PG_EXTENSION_FLAVOR="${PG_EXTENSION_FLAVOR:-pg${PG_MAJOR}}"

resolve_pgrx_version_dir() {
    local candidate

    if [[ -n "${PGRX_VERSION_DIR:-}" ]]; then
        printf '%s\n' "$PGRX_VERSION_DIR"
        return 0
    fi

    while IFS= read -r candidate; do
        [[ -z "$candidate" ]] && continue
        [[ "$candidate" == *_unpack ]] && continue
        if [[ -x "$candidate/pgrx-install/bin/pg_config" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done < <(find "$PGRX_HOME" -maxdepth 1 -type d -name "${PG_MAJOR}.*" 2>/dev/null | sort -V -r)
}

PGRX_VERSION_DIR="$(resolve_pgrx_version_dir)"
PGRX_INSTALL_BIN_DIR="$PGRX_VERSION_DIR/pgrx-install/bin"
PG_CONFIG="$PGRX_INSTALL_BIN_DIR/pg_config"
PSQL="$PGRX_INSTALL_BIN_DIR/psql"
PG_CTL="$PGRX_INSTALL_BIN_DIR/pg_ctl"
INITDB="$PGRX_INSTALL_BIN_DIR/initdb"
DATA_DIR="$PGRX_HOME/data-${PG_MAJOR}"

# Connection settings (pgrx defaults)
PG_HOST="localhost"
PG_PORT="${PG_PORT:-288${PG_MAJOR}}"
PG_USER="$USER"
TEST_DB="kalamdb_test"

# KalamDB gRPC (local server)
KALAMDB_GRPC_HOST="127.0.0.1"
KALAMDB_GRPC_PORT="9188"

info()  { echo "  [INFO] $*"; }
error() { echo "  [ERROR] $*" >&2; }

check_pg_config() {
    if [[ -z "$PGRX_VERSION_DIR" ]]; then
        error "Could not find a pgrx install for PostgreSQL $PG_MAJOR under $PGRX_HOME"
        error "Run: cargo pgrx init --pg${PG_MAJOR} download"
        exit 1
    fi
    if [ ! -x "$PG_CONFIG" ]; then
        error "pgrx PostgreSQL $PG_MAJOR not found at $PG_CONFIG"
        error "Run: cargo pgrx init --pg${PG_MAJOR} download"
        exit 1
    fi
    if [[ "$PG_EXTENSION_FLAVOR" != "pg${PG_MAJOR}" ]]; then
        error "PG_EXTENSION_FLAVOR must match PG_MAJOR (expected pg${PG_MAJOR}, got $PG_EXTENSION_FLAVOR)"
        exit 1
    fi
}

ensure_data_dir() {
    if [[ -f "$DATA_DIR/PG_VERSION" && -f "$DATA_DIR/postgresql.conf" ]]; then
        return 0
    fi

    info "Initializing pgrx PostgreSQL $PG_MAJOR data directory..."
    rm -rf "$DATA_DIR"
    "$INITDB" -D "$DATA_DIR" >/dev/null
}

start_pg() {
    ensure_data_dir

    if pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; then
        info "PostgreSQL already running on port $PG_PORT"
        return 0
    fi
    info "Starting pgrx PostgreSQL $PG_MAJOR..."
    "$PG_CTL" -D "$DATA_DIR" -l "$DATA_DIR/pgrx.log" -o "-p $PG_PORT" start
    wait_for_pg_ready
}

wait_for_pg_ready() {
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

restart_pg() {
    info "Restarting pgrx PostgreSQL $PG_MAJOR..."
    "$PG_CTL" -D "$DATA_DIR" -l "$DATA_DIR/pgrx.log" -o "-p $PG_PORT" restart
    wait_for_pg_ready
}

stop_pg() {
    info "Stopping pgrx PostgreSQL $PG_MAJOR..."
    "$PG_CTL" -D "$DATA_DIR" stop -m fast 2>/dev/null || info "Already stopped"
}

install_extension() {
    info "Building and installing pg_kalam extension..."
    cd "$PG_CRATE"

    # Ensure config.toml exists (cargo pgrx install requires it even with --pg-config)
    local config_toml="$PGRX_HOME/config.toml"
    if [[ ! -f "$config_toml" ]]; then
        info "Creating pgrx config.toml (pg${PG_MAJOR} → $PG_CONFIG)..."
        printf '[configs]\npg%s = "%s"\n' "$PG_MAJOR" "$PG_CONFIG" > "$config_toml"
    fi

    RUST_BACKTRACE=1 cargo pgrx install \
        --pg-config="$PG_CONFIG" \
        --no-default-features \
        -F "$PG_EXTENSION_FLAVOR"
    info "Extension installed"
}

ensure_shared_preload_libraries() {
    local conf="$DATA_DIR/postgresql.conf"

    if grep -Eq "^[[:space:]]*shared_preload_libraries[[:space:]]*=.*pg_kalam" "$conf" 2>/dev/null; then
        info "shared_preload_libraries already includes pg_kalam"
        return 0
    fi

    if grep -Eq "^[[:space:]]*shared_preload_libraries[[:space:]]*=" "$conf" 2>/dev/null; then
        info "Updating shared_preload_libraries to include pg_kalam..."
        perl -0pi -e "s/^[[:space:]]*shared_preload_libraries[[:space:]]*=.*$/shared_preload_libraries = 'pg_kalam'/m" "$conf"
    else
        info "Adding pg_kalam to shared_preload_libraries..."
        echo "shared_preload_libraries = 'pg_kalam'" >> "$conf"
    fi
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

    info "Setup complete!"
    info ""
    info "Connect with:"
    info "  $PSQL -h $PG_HOST -p $PG_PORT -U $PG_USER -d $TEST_DB"
    info ""
    info "Run e2e DDL tests with:"
    info "  cargo nextest run --no-default-features --features e2e,$PG_EXTENSION_FLAVOR -p kalam-pg-extension -E 'test(e2e_ddl)'"
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
        ensure_shared_preload_libraries
        restart_pg
        setup_database
        ;;
esac
