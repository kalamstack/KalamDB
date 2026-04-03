#!/usr/bin/env bash
# KalamDB Benchmark Runner
# Usage: ./run-benchmarks.sh [--urls URLS] [--user USER] [--password PASS] [--iterations N] [--max-subscribers N]
set -euo pipefail

# Raise file-descriptor limit for the benchmark process (WebSocket connections, etc.)
ulimit -n "$(ulimit -Hn 2>/dev/null || echo 65536)" 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKEND_DIR="$REPO_DIR/backend"
BENCH_SERVER_BIN="$REPO_DIR/target/release/kalamdb-server"
BENCH_SERVER_CONFIG="$SCRIPT_DIR/server.toml"
BENCH_SERVER_LOG="$SCRIPT_DIR/logs/benchmark-server.log"
BENCH_DATA_DIR="$SCRIPT_DIR/data"
cd "$SCRIPT_DIR"

# Default values (can be overridden via args or env vars)
URLS="${KALAMDB_URLS:-${KALAMDB_URL:-http://127.0.0.1:8080}}"
USER="${KALAMDB_USER:-admin}"
PASSWORD="${KALAMDB_PASSWORD:-kalamdb123}"
MAX_SUBSCRIBERS="${KALAMDB_MAX_SUBSCRIBERS:-}"
EXTRA_ARGS=()
BENCH_SERVER_PID=""

cleanup_managed_server() {
    if [[ -n "$BENCH_SERVER_PID" ]] && kill -0 "$BENCH_SERVER_PID" 2>/dev/null; then
        kill "$BENCH_SERVER_PID" 2>/dev/null || true
        wait "$BENCH_SERVER_PID" 2>/dev/null || true
    fi

    BENCH_SERVER_PID=""
}

is_loopback_url() {
    case "$1" in
        http://127.0.0.1:*|http://localhost:*|http://[::1]:*|https://127.0.0.1:*|https://localhost:*|https://[::1]:*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

should_manage_server() {
    local urls="$1"
    local url_array=()
    IFS=',' read -r -a url_array <<< "$urls"
    [[ "${#url_array[@]}" -eq 1 ]] || return 1
    is_loopback_url "${url_array[0]}"
}

extract_url_port() {
    local url="$1"
    local scheme="${url%%://*}"
    local after_scheme="${url#*://}"
    local authority="${after_scheme%%/*}"
    local port=""

    if [[ "$authority" == \[*\]* ]]; then
        if [[ "$authority" == *]:* ]]; then
            port="${authority##*:}"
        fi
    elif [[ "$authority" == *:* ]]; then
        port="${authority##*:}"
    fi

    if [[ -n "$port" ]]; then
        printf '%s\n' "$port"
    elif [[ "$scheme" == "https" ]]; then
        printf '443\n'
    else
        printf '80\n'
    fi
}

extract_url_host() {
    local url="$1"
    local after_scheme="${url#*://}"
    local authority="${after_scheme%%/*}"
    local host="$authority"

    if [[ "$authority" == \[*\]* ]]; then
        host="${authority%%]*}"
        host="${host#[}"
    elif [[ "$authority" == *:* ]]; then
        host="${authority%%:*}"
    fi

    if [[ "$host" == "localhost" ]]; then
        printf '127.0.0.1\n'
    else
        printf '%s\n' "$host"
    fi
}

derive_cluster_rpc_port() {
    local http_port="$1"

    if (( http_port <= 64535 )); then
        printf '%s\n' "$((http_port + 1000))"
    else
        printf '%s\n' "$((http_port - 1000))"
    fi
}

ensure_release_server_bin() {
    if [[ -x "$BENCH_SERVER_BIN" ]]; then
        return
    fi

    echo "🔨 Building release KalamDB server"
    (
        cd "$BACKEND_DIR"
        cargo build --release --bin kalamdb-server
    )
}

wait_for_server_health() {
    local url="$1"
    local health_url="${url%/}/health"

    for _attempt in $(seq 1 60); do
        if curl --silent --fail --output /dev/null "$health_url"; then
            return 0
        fi

        if [[ -n "$BENCH_SERVER_PID" ]] && ! kill -0 "$BENCH_SERVER_PID" 2>/dev/null; then
            echo "❌ Managed benchmark server exited before it became healthy"
            tail -n 40 "$BENCH_SERVER_LOG" 2>/dev/null || true
            return 1
        fi

        sleep 1
    done

    echo "❌ Timed out waiting for managed benchmark server health check"
    tail -n 40 "$BENCH_SERVER_LOG" 2>/dev/null || true
    return 1
}

start_managed_server() {
    local url="$1"
    local host
    local port
    local rpc_port
    host="$(extract_url_host "$url")"
    port="$(extract_url_port "$url")"
    rpc_port="$(derive_cluster_rpc_port "$port")"

    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "❌ Refusing to start benchmark server: port $port is already in use"
        lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
        return 1
    fi

    if [[ ! -f "$BENCH_SERVER_CONFIG" ]]; then
        echo "❌ Benchmark server config not found: $BENCH_SERVER_CONFIG"
        return 1
    fi

    ensure_release_server_bin

    rm -rf "$BENCH_DATA_DIR"
    mkdir -p "$(dirname "$BENCH_SERVER_LOG")"
    : > "$BENCH_SERVER_LOG"

    echo "🧪 Starting managed benchmark server"
    echo "   Config: $BENCH_SERVER_CONFIG"
    echo "   Log:    $BENCH_SERVER_LOG"

    KALAMDB_SERVER_HOST="$host" \
    KALAMDB_SERVER_PORT="$port" \
    KALAMDB_CLUSTER_API_ADDR="$host:$port" \
    KALAMDB_CLUSTER_RPC_ADDR="$host:$rpc_port" \
    "$BENCH_SERVER_BIN" "$BENCH_SERVER_CONFIG" >"$BENCH_SERVER_LOG" 2>&1 &
    BENCH_SERVER_PID=$!

    trap cleanup_managed_server EXIT
    wait_for_server_health "$url"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --urls) URLS="$2"; shift 2;;
        --user) USER="$2"; shift 2;;
        --password) PASSWORD="$2"; shift 2;;
        --max-subscribers) MAX_SUBSCRIBERS="$2"; shift 2;;
        *) EXTRA_ARGS+=("$1"); shift;;
    esac
done

MANAGED_SERVER="no"
export KALAMDB_BENCH_WS_SUBSCRIPTIONS_PER_CONNECTION="${KALAMDB_BENCH_WS_SUBSCRIPTIONS_PER_CONNECTION:-100}"

if should_manage_server "$URLS"; then
    start_managed_server "$URLS"
    MANAGED_SERVER="yes"
    export KALAMDB_BENCH_MANAGED_SERVER=1
    export KALAMDB_BENCH_SERVER_PID="$BENCH_SERVER_PID"
    export KALAMDB_BENCH_HTTP2=1
else
    unset KALAMDB_BENCH_MANAGED_SERVER
    unset KALAMDB_BENCH_SERVER_PID
    unset KALAMDB_BENCH_HTTP2
fi

echo "🚀 KalamDB Benchmark Suite"
echo "   Servers: $URLS"
echo "   Managed server: $MANAGED_SERVER"
if [[ -n "$MAX_SUBSCRIBERS" ]]; then
    echo "   Max subscribers: $MAX_SUBSCRIBERS"
fi
echo ""

# Build and run
CMD=(cargo run --release --
    --urls "$URLS"
    --user "$USER"
    --password "$PASSWORD")

if [[ -n "$MAX_SUBSCRIBERS" ]]; then
    CMD+=(--max-subscribers "$MAX_SUBSCRIBERS")
fi

if (( ${#EXTRA_ARGS[@]} > 0 )); then
    CMD+=("${EXTRA_ARGS[@]}")
fi

"${CMD[@]}"

if [[ "$MANAGED_SERVER" == "yes" ]]; then
    cleanup_managed_server
    BENCH_SERVER_PID=""
    trap - EXIT
fi

echo ""
echo "📊 Results directory: $SCRIPT_DIR/results"
