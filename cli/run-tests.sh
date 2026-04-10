#!/usr/bin/env bash
# Helper script to run CLI tests with custom server URL and authentication
#
# Usage:
#   ./run-tests.sh                                    # Run all workspace tests + CLI e2e (default)
#   ./run-tests.sh --url http://localhost:3000        # Custom URL
#   ./run-tests.sh --password mypass                  # Custom password
#   ./run-tests.sh --url http://localhost:3000 --password mypass --test smoke
#
# Examples:
#   ./run-tests.sh --test smoke                       # Run smoke tests only
#   ./run-tests.sh --url http://localhost:3000        # Test on port 3000
#   ./run-tests.sh --test "smoke_test_core" --nocapture # Run specific test with output

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

# Default values
SERVER_URL="${KALAMDB_SERVER_URL:-}"
CLUSTER_URLS="${KALAMDB_CLUSTER_URLS:-}"
SERVER_TYPE="${KALAMDB_SERVER_TYPE:-}"
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD-}"
ROOT_PASSWORD_SET=false
if [ "${KALAMDB_ROOT_PASSWORD+x}" = "x" ]; then
    ROOT_PASSWORD_SET=true
fi
TEST_JOBS="${KALAMDB_TEST_JOBS:-}"
TEST_FILTER=""
TEST_LIST_FILE=""
TEST_TARGET=""
NOCAPTURE=""
SHOW_HELP=false
PACKAGE_FILTERS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--url)
            SERVER_URL="$2"
            shift 2
            ;;
        --cluster-urls|--urls)
            CLUSTER_URLS="$2"
            shift 2
            ;;
        --server-type)
            SERVER_TYPE="$2"
            shift 2
            ;;
        -j|--jobs)
            TEST_JOBS="$2"
            shift 2
            ;;
        -P|--package)
            PACKAGE_FILTERS+=("$2")
            shift 2
            ;;
        -p|--password)
            ROOT_PASSWORD="$2"
            ROOT_PASSWORD_SET=true
            shift 2
            ;;
        -t|--test)
            TEST_FILTER="$2"
            shift 2
            ;;
        --test-target)
            TEST_TARGET="$2"
            shift 2
            ;;
        --test-list)
            TEST_LIST_FILE="$2"
            shift 2
            ;;
        --nocapture)
            NOCAPTURE="--nocapture"
            shift
            ;;
        -h|--help)
            SHOW_HELP=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            SHOW_HELP=true
            shift
            ;;
    esac
done

if [ "$SHOW_HELP" = true ]; then
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Default: runs all workspace tests via cargo nextest, with CLI e2e tests enabled"
    echo "         using feature: kalam-cli/e2e-tests"
    echo ""
    echo "Options:"
    echo "  -u, --url <URL>          Single-node server URL"
    echo "  --cluster-urls <URLS>    Comma-separated cluster node URLs"
    echo "  --server-type <TYPE>     Server mode: fresh | running | cluster"
    echo "  -j, --jobs <N>           Override nextest process concurrency"
    echo "  -P, --package <CRATE>    Limit the run to one package (repeatable)"
    echo "  -p, --password <PASS>    Root/admin password"
    echo "  -t, --test <FILTER>      Test filter (e.g., 'smoke', 'smoke_test_core')"
    echo "  --test-target <TARGET>   nextest test target/binary name (e.g., 'cluster')"
    echo "  --test-list <FILE|- >    Newline-delimited test filters to rerun one by one"
    echo "  --nocapture              Pass through test stdout/stderr (--no-capture)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --test smoke --nocapture"
    echo "  $0 --url http://localhost:3000 --password mypass"
    echo "  $0 --cluster-urls http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 --server-type cluster"
    echo "  $0 --package kalam-cli --test-target cluster"
    echo "  $0 --package kalam-cli --package kalam-link"
    echo "  $0 --test-list failed-tests.txt"
    exit 0
fi

if [ -n "$TEST_FILTER" ] && [ -n "$TEST_LIST_FILE" ]; then
    echo "Error: --test and --test-list cannot be used together."
    exit 1
fi

if [ -n "$TEST_LIST_FILE" ] && [ "$TEST_LIST_FILE" != "-" ] && [ ! -f "$TEST_LIST_FILE" ]; then
    echo "Error: test list file not found: $TEST_LIST_FILE"
    exit 1
fi

if [ -n "$CLUSTER_URLS" ]; then
    SERVER_TYPE="cluster"
fi

if [ -z "$SERVER_URL" ]; then
    if [ "$SERVER_TYPE" = "cluster" ] && [ -n "$CLUSTER_URLS" ]; then
        SERVER_URL="${CLUSTER_URLS%%,*}"
    else
        SERVER_URL="http://127.0.0.1:8080"
    fi
fi

AUTO_DETECTED_CLUSTER=false

detect_cluster_urls_from_health() {
    local base_url="$1"

    if [ -z "$base_url" ] || ! command -v python3 >/dev/null 2>&1; then
        return 1
    fi

    curl -fsS --max-time 2 "${base_url%/}/v1/api/cluster/health" 2>/dev/null | python3 -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)

if not payload.get("is_cluster_mode"):
    raise SystemExit(1)

urls = []
for node in payload.get("nodes") or []:
    api_addr = str(node.get("api_addr") or "").strip()
    if api_addr and api_addr not in urls:
        urls.append(api_addr)

if len(urls) <= 1:
    raise SystemExit(1)

print(",".join(urls))
'
}

autodetect_cluster_mode() {
    local detected_cluster_urls

    if [ "$SERVER_TYPE" = "fresh" ]; then
        return 0
    fi

    detected_cluster_urls="$(detect_cluster_urls_from_health "$SERVER_URL")" || return 0

    if [ -z "$detected_cluster_urls" ]; then
        return 0
    fi

    CLUSTER_URLS="$detected_cluster_urls"
    SERVER_TYPE="cluster"
    AUTO_DETECTED_CLUSTER=true
}

autodetect_cluster_mode

parse_host_port_from_url() {
    local url="$1"

    if ! command -v python3 >/dev/null 2>&1; then
        return 1
    fi

    python3 - "$url" <<'PY'
from urllib.parse import urlparse
import sys

url = sys.argv[1].strip()
if not url:
    raise SystemExit(1)

parsed = urlparse(url)
host = parsed.hostname
port = parsed.port
if not host:
    raise SystemExit(1)
if port is None:
    port = 443 if parsed.scheme == "https" else 80

print(f"{host}\n{port}")
PY
}

is_local_host() {
    case "$1" in
        127.0.0.1|localhost|::1|0.0.0.0)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

validate_single_local_listener() {
    local host_port
    local host
    local port
    local pids
    local count

    if ! command -v lsof >/dev/null 2>&1; then
        return 0
    fi

    host_port="$(parse_host_port_from_url "$SERVER_URL")" || return 0
    host="$(printf '%s\n' "$host_port" | sed -n '1p')"
    port="$(printf '%s\n' "$host_port" | sed -n '2p')"

    if ! is_local_host "$host"; then
        return 0
    fi

    pids="$(lsof -n -P -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | sort -u)"
    count="$(printf '%s\n' "$pids" | sed '/^$/d' | wc -l | tr -d ' ')"

    if [ "$count" -le 1 ]; then
        return 0
    fi

    echo "Error: multiple processes are listening on ${host}:${port}."
    echo "Running-server mode requires a single deterministic target."
    echo ""
    lsof -n -P -iTCP:"$port" -sTCP:LISTEN | head -n 20
    echo ""
    echo "Stop the extra server(s) and rerun ./run-tests.sh."
    exit 1
}

validate_cluster_health() {
    local health_url="$1"
    local summary

    if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
        return 0
    fi

    summary="$(curl -fsS --max-time 3 "${health_url%/}/v1/api/cluster/health" 2>/dev/null | python3 -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)

is_cluster = bool(payload.get("is_cluster_mode"))
groups_leading = payload.get("groups_leading")
total_groups = payload.get("total_groups")

if groups_leading is None or total_groups is None:
    nodes = payload.get("nodes") or []
    if nodes:
        groups_leading = max((node.get("groups_leading") or 0) for node in nodes)
        total_groups = max((node.get("total_groups") or 0) for node in nodes)

if not is_cluster or groups_leading is None or total_groups is None:
    print("ok")
    raise SystemExit(0)

if int(groups_leading) < int(total_groups):
    print(f"degraded {groups_leading} {total_groups}")
else:
    print("ok")
')" || true

    case "$summary" in
        ok|"")
            return 0
            ;;
        degraded\ *)
            set -- $summary
            echo "Error: target server reports incomplete cluster leadership (${2}/${3} groups leading)."
            echo "This usually means stale or mismatched local Raft state, and CLI e2e tests will fail nondeterministically."
            echo ""
            echo "Check: ${health_url%/}/v1/api/cluster/health"
            echo "Fix the running server state, then rerun ./run-tests.sh."
            exit 1
            ;;
    esac
}

preflight_running_server() {
    if [ "$SERVER_TYPE" = "fresh" ]; then
        return 0
    fi

    validate_single_local_listener
    validate_cluster_health "$SERVER_URL"
}

if [ ${#PACKAGE_FILTERS[@]} -gt 1 ]; then
    for package in "${PACKAGE_FILTERS[@]}"; do
        if [ "$package" = "kalam-cli" ]; then
            echo "Error: run kalam-cli separately when using --package because e2e-tests is package-specific."
            exit 1
        fi
    done
fi

FEATURE_MODE="workspace + CLI e2e feature"
if [ ${#PACKAGE_FILTERS[@]} -gt 0 ]; then
    if [ ${#PACKAGE_FILTERS[@]} -eq 1 ] && [ "${PACKAGE_FILTERS[0]}" = "kalam-cli" ]; then
        FEATURE_MODE="package + CLI e2e feature"
    else
        FEATURE_MODE="package only"
    fi
fi

# Display configuration
echo "================================================"
echo "Running KalamDB Tests (cargo nextest)"
echo "================================================"
if [ -f "$ENV_FILE" ]; then
    echo "Env File:        $ENV_FILE"
else
    echo "Env File:        (none)"
fi
echo "Server Type:     ${SERVER_TYPE:-auto}"
if [ "$SERVER_TYPE" = "cluster" ]; then
    echo "Cluster URLs:    ${CLUSTER_URLS:-$SERVER_URL}"
    echo "Primary URL:     $SERVER_URL"
    if [ "$AUTO_DETECTED_CLUSTER" = true ]; then
        echo "Cluster Detect:  /v1/api/cluster/health"
    fi
else
    echo "Server URL:      $SERVER_URL"
fi
if [ ${#PACKAGE_FILTERS[@]} -gt 0 ]; then
    echo "Packages:        ${PACKAGE_FILTERS[*]}"
else
    echo "Packages:        workspace"
fi
echo "Root Password:   $([ -z "$ROOT_PASSWORD" ] && echo '(empty)' || echo '***')"
if [ -n "$TEST_TARGET" ]; then
    echo "Test Target:     $TEST_TARGET"
fi
echo "Test Filter:     $([ -z "$TEST_FILTER" ] && echo '(all tests)' || echo "$TEST_FILTER")"
if [ -n "$TEST_LIST_FILE" ]; then
    echo "Test List:       $TEST_LIST_FILE"
fi
if [ -n "$TEST_JOBS" ]; then
    echo "Jobs:            $TEST_JOBS"
fi
echo "Mode:            $FEATURE_MODE"
echo "================================================"
echo ""

preflight_running_server

# Clear shared JWT caches so a restarted running server/cluster does not reuse
# stale admin/root tokens from a previous test session.
rm -f "${TMPDIR:-/tmp}/kalamdb_test_tokens.json" "${TMPDIR:-/tmp}/kalamdb_test_tokens.lock"

# Export environment variables
export KALAMDB_SERVER_URL="$SERVER_URL"
if [ -n "$CLUSTER_URLS" ]; then
    export KALAMDB_CLUSTER_URLS="$CLUSTER_URLS"
else
    unset KALAMDB_CLUSTER_URLS
fi

if [ -n "$SERVER_TYPE" ]; then
    export KALAMDB_SERVER_TYPE="$SERVER_TYPE"
else
    unset KALAMDB_SERVER_TYPE
fi

if [ "$ROOT_PASSWORD_SET" = true ]; then
    export KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD"
else
    unset KALAMDB_ROOT_PASSWORD
fi

# Ensure nextest is available
if ! cargo nextest --version >/dev/null 2>&1; then
    echo "Error: cargo-nextest is not installed."
    echo "Install it with: cargo install cargo-nextest"
    exit 1
fi

build_test_cmd() {
    local test_filter="$1"
    TEST_CMD=(
        cargo nextest run
        --all-targets
    )

    if [ ${#PACKAGE_FILTERS[@]} -gt 0 ]; then
        local package
        for package in "${PACKAGE_FILTERS[@]}"; do
            TEST_CMD+=(-p "$package")
        done

        if [ "$(single_package_name)" = "kalam-cli" ]; then
            TEST_CMD+=(--features "e2e-tests")
        fi
    else
        TEST_CMD+=(--workspace)
        # The PostgreSQL extension crate is tested via the dedicated pgrx workflow,
        # not through generic cargo test/nextest targets.
        TEST_CMD+=(--exclude "kalam-pg-extension")
        TEST_CMD+=(--features "kalam-cli/e2e-tests")
    fi

    if [ -n "$TEST_TARGET" ]; then
        TEST_CMD+=(--test "$TEST_TARGET")
    fi

    # nextest.toml already serializes the stateful kalam-cli / kalam-link
    # packages. Do not force a global `-j 1` here, otherwise the entire
    # workspace becomes single-file even when only those packages need it.
    if [ -n "$TEST_JOBS" ]; then
        TEST_CMD+=(-j "$TEST_JOBS")
    fi

    if [ -n "$test_filter" ]; then
        if [ -z "$TEST_TARGET" ] && [[ "$test_filter" == smoke* ]]; then
            TEST_CMD+=(--test smoke)
            if [[ "$test_filter" != "smoke" ]]; then
                TEST_CMD+=("$test_filter")
            fi
        else
            TEST_CMD+=("$test_filter")
        fi
    fi

    if [ -n "$NOCAPTURE" ]; then
        TEST_CMD+=(--no-capture)
    fi
}

run_single_test() {
    local test_filter="$1"
    build_test_cmd "$test_filter"
    echo "Executing: ${TEST_CMD[*]}"
    echo ""
    "${TEST_CMD[@]}"
}

run_test_list() {
    local test_file="$1"
    local input_path="$test_file"
    local test_filter=""
    local total=0
    local passed=0
    local exit_code=0

    if [ "$test_file" = "-" ]; then
        input_path="/dev/stdin"
    fi

    while IFS= read -r test_filter || [ -n "$test_filter" ]; do
        test_filter="${test_filter%$'\r'}"
        case "$test_filter" in
            ''|\#*)
                continue
                ;;
        esac

        total=$((total + 1))
        echo ""
        echo "=== RUN $test_filter ==="
        if run_single_test "$test_filter"; then
            passed=$((passed + 1))
        else
            exit_code=$?
            echo ""
            echo "Failed test: $test_filter"
            echo "Summary: $passed passed before first failure ($total attempted)"
            return $exit_code
        fi
    done < "$input_path"

    echo ""
    echo "Summary: $passed/$total tests passed from rerun list"
}

single_package_name() {
    if [ ${#PACKAGE_FILTERS[@]} -eq 1 ]; then
        echo "${PACKAGE_FILTERS[0]}"
    fi
}

# Run tests from workspace root
cd "$REPO_ROOT"

if [ -n "$TEST_LIST_FILE" ]; then
    run_test_list "$TEST_LIST_FILE"
else
    run_single_test "$TEST_FILTER"
fi
