#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/run-tests.sh"
FOLLOWER_LIST="$SCRIPT_DIR/test-lists/cluster-followers.txt"

RUN_FOLLOWERS=false
USER_SUPPLIED_TEST=false
USER_SUPPLIED_TEST_LIST=false
PASSTHROUGH_ARGS=()

show_help() {
    echo "Usage: $0 [--followers] [run-tests.sh options]"
    echo ""
    echo "Runs only the CLI cluster integration target."
    echo ""
    echo "Options:"
    echo "  --followers              Run the curated follower/replication bundle"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Other options are forwarded to run-tests.sh, including:"
    echo "  --url, --cluster-urls, --password, --jobs, --nocapture, --test"
    echo ""
    echo "Examples:"
    echo "  $0"
    echo "  $0 --followers --nocapture"
    echo "  $0 --url http://127.0.0.1:8081 --followers --nocapture"
    echo "  $0 --test cluster_test_ws_follower_receives_leader_changes --nocapture"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --followers)
            RUN_FOLLOWERS=true
            shift
            ;;
        -t|--test)
            USER_SUPPLIED_TEST=true
            PASSTHROUGH_ARGS+=("$1")
            shift
            if [[ $# -eq 0 ]]; then
                echo "Error: $0 requires a value after --test"
                exit 1
            fi
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
        --test-list)
            USER_SUPPLIED_TEST_LIST=true
            PASSTHROUGH_ARGS+=("$1")
            shift
            if [[ $# -eq 0 ]]; then
                echo "Error: $0 requires a value after --test-list"
                exit 1
            fi
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            PASSTHROUGH_ARGS+=("$1")
            shift
            ;;
    esac
done

if [ ! -x "$RUNNER" ]; then
    echo "Error: runner not found or not executable: $RUNNER"
    exit 1
fi

if [ "$RUN_FOLLOWERS" = true ] && { [ "$USER_SUPPLIED_TEST" = true ] || [ "$USER_SUPPLIED_TEST_LIST" = true ]; }; then
    echo "Error: --followers cannot be combined with --test or --test-list"
    exit 1
fi

CMD=(
    "$RUNNER"
    --package kalam-cli
    --server-type cluster
    --test-target cluster
)

if [ "$RUN_FOLLOWERS" = true ]; then
    CMD+=(--test-list "$FOLLOWER_LIST")
fi

CMD+=("${PASSTHROUGH_ARGS[@]}")

echo "Executing: ${CMD[*]}"
echo ""
exec "${CMD[@]}"