#!/usr/bin/env bash
# Run the connection_scale benchmark with sensible defaults for macOS.
#
# Usage:
#   ./run-connection-scale.sh              # 50K (default)
#   ./run-connection-scale.sh 200000       # 200K
#   ./run-connection-scale.sh 200000 15    # 200K with 15 loopback IPs
#
# Environment overrides (all optional):
#   KALAMDB_URL                       Server URL (default: http://127.0.0.1:8080)
#   KALAMDB_USER / KALAMDB_PASSWORD   Auth credentials
#   KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_BATCH       (default: 500)
#   KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_WAVE_SIZE   (default: 250)
#   KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_TIMEOUT_SECS (default: 60)
#   KALAMDB_BENCH_CONNECTION_SCALE_DELIVERY_TOLERANCE  (default: 0.995 = 99.5%)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

MAX_SUBSCRIBERS="${1:-50000}"
BIND_IP_COUNT="${2:-0}"

# ── Loopback alias setup ────────────────────────────────────────────────
# When BIND_IP_COUNT > 0, ensure that many 127.0.0.x aliases exist on lo0.
# Existing aliases are left untouched; only missing ones are added (needs sudo).
if (( BIND_IP_COUNT > 0 )); then
    addresses=()
    need_sudo=false
    for i in $(seq 1 "$BIND_IP_COUNT"); do
        ip="127.0.0.$i"
        addresses+=("$ip")
        if ! ifconfig lo0 | grep -q "inet $ip "; then
            need_sudo=true
        fi
    done

    if $need_sudo; then
        echo "▸ Ensuring $BIND_IP_COUNT loopback aliases on lo0 (may prompt for sudo)..."
        for i in $(seq 1 "$BIND_IP_COUNT"); do
            ip="127.0.0.$i"
            if ! ifconfig lo0 | grep -q "inet $ip "; then
                sudo ifconfig lo0 alias "$ip"
            fi
        done
    fi

    export KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES="${addresses[*]}"
    # Replace spaces with commas
    KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES="${KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES// /,}"
    export KALAMDB_ALLOW_SINGLE_WS_TARGET=1
    echo "▸ Bind pool: ${#addresses[@]} address(es) [${addresses[0]}..127.0.0.${BIND_IP_COUNT}]"
fi

# ── Raise file-descriptor soft limit ────────────────────────────────────
ulimit -n "$(ulimit -Hn 2>/dev/null || echo 65536)" 2>/dev/null || true

# Always allow a single WS target — this script is specifically for connection_scale.
export KALAMDB_ALLOW_SINGLE_WS_TARGET=1

# ── Sensible benchmark defaults (override via env) ──────────────────────
export KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_BATCH="${KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_BATCH:-500}"
export KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_WAVE_SIZE="${KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_WAVE_SIZE:-250}"
export KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_TIMEOUT_SECS="${KALAMDB_BENCH_CONNECTION_SCALE_CONNECT_TIMEOUT_SECS:-60}"

echo "▸ Running connection_scale benchmark with max_subscribers=${MAX_SUBSCRIBERS}"
exec ./run-benchmarks.sh --bench connection_scale --max-subscribers "$MAX_SUBSCRIBERS"
