#!/usr/bin/env bash
set -euo pipefail

EXT_ARTIFACTS_DIR="/kalam-ext"
PG_LIB_DIR="/usr/lib/postgresql/16/lib"
PG_EXT_DIR="/usr/share/postgresql/16/extension"

if [[ ! -f "$EXT_ARTIFACTS_DIR/pg_kalam.so" ]]; then
    echo "missing $EXT_ARTIFACTS_DIR/pg_kalam.so"
    echo "generate Linux artifacts first with: ./pg/docker/build.sh --artifacts"
    exit 1
fi

if [[ ! -f "$EXT_ARTIFACTS_DIR/pg_kalam.control" ]]; then
    echo "missing $EXT_ARTIFACTS_DIR/pg_kalam.control"
    echo "generate Linux artifacts first with: ./pg/docker/build.sh --artifacts"
    exit 1
fi

if ! compgen -G "$EXT_ARTIFACTS_DIR/pg_kalam--*.sql" > /dev/null; then
    echo "missing versioned pg_kalam SQL files in $EXT_ARTIFACTS_DIR"
    echo "generate Linux artifacts first with: ./pg/docker/build.sh --artifacts"
    exit 1
fi

cp "$EXT_ARTIFACTS_DIR/pg_kalam.so" "$PG_LIB_DIR/pg_kalam.so"
cp "$EXT_ARTIFACTS_DIR/pg_kalam.control" "$PG_EXT_DIR/pg_kalam.control"
cp "$EXT_ARTIFACTS_DIR"/pg_kalam--*.sql "$PG_EXT_DIR/"

docker-entrypoint.sh postgres -c shared_preload_libraries=pg_kalam "$@" &
postgres_pid=$!

cleanup() {
    kill "$postgres_pid" 2>/dev/null || true
    wait "$postgres_pid" 2>/dev/null || true
}

trap cleanup INT TERM EXIT

wait "$postgres_pid"