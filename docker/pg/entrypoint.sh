#!/usr/bin/env bash
set -euo pipefail

EXT_ARTIFACTS_DIR="/kalam-ext"
PG_LIB_DIR="/usr/lib/postgresql/16/lib"
PG_EXT_DIR="/usr/share/postgresql/16/extension"
PG_BOOTSTRAP_SCRIPT="/usr/local/bin/kalam-pg-keep-http-runtime.sh"

if [[ ! -f "$EXT_ARTIFACTS_DIR/pg_kalam.so" ]]; then
    echo "missing $EXT_ARTIFACTS_DIR/pg_kalam.so"
    echo "generate Linux artifacts first with: ./docker/pg/build.sh --artifacts"
    exit 1
fi

if [[ ! -f "$EXT_ARTIFACTS_DIR/pg_kalam.control" ]]; then
    echo "missing $EXT_ARTIFACTS_DIR/pg_kalam.control"
    echo "generate Linux artifacts first with: ./docker/pg/build.sh --artifacts"
    exit 1
fi

if ! compgen -G "$EXT_ARTIFACTS_DIR/pg_kalam--*.sql" > /dev/null; then
    echo "missing versioned pg_kalam SQL files in $EXT_ARTIFACTS_DIR"
    echo "generate Linux artifacts first with: ./docker/pg/build.sh --artifacts"
    exit 1
fi

cp "$EXT_ARTIFACTS_DIR/pg_kalam.so" "$PG_LIB_DIR/pg_kalam.so"
cp "$EXT_ARTIFACTS_DIR/pg_kalam.control" "$PG_EXT_DIR/pg_kalam.control"
cp "$EXT_ARTIFACTS_DIR"/pg_kalam--*.sql "$PG_EXT_DIR/"

docker-entrypoint.sh postgres -c shared_preload_libraries=pg_kalam "$@" &
postgres_pid=$!

cleanup() {
    if [[ -n "${bootstrap_pid:-}" ]]; then
        kill "$bootstrap_pid" 2>/dev/null || true
    fi
    kill "$postgres_pid" 2>/dev/null || true
    wait "$postgres_pid" 2>/dev/null || true
}

trap cleanup INT TERM EXIT

if [[ "${KALAM_PG_HTTP_ENABLED:-false}" == "true" ]] || [[ "${KALAM_PG_HTTP_ENABLED:-0}" == "1" ]]; then
    "$PG_BOOTSTRAP_SCRIPT" &
    bootstrap_pid=$!
fi

wait "$postgres_pid"