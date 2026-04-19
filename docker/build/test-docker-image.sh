#!/usr/bin/env bash
# Docker Image Smoke Test
# Tests that the built Docker image works correctly

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="${1:-jamals86/kalamdb:test}"
CONTAINER_NAME="kalamdb-test-$$"
TEST_PORT="${TEST_PORT:-8081}"
TIMEOUT="${TIMEOUT:-30}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-}"
MAX_IMAGE_SIZE_BYTES="${MAX_IMAGE_SIZE_BYTES:-0}"
EXPECTED_OCI_VERSION="${EXPECTED_OCI_VERSION:-}"
EXPECTED_OCI_SOURCE="${EXPECTED_OCI_SOURCE:-}"
EXPECTED_RUNTIME_UID="${EXPECTED_RUNTIME_UID:-65532}"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

container_get() {
    local path="$1"
    docker exec "$CONTAINER_NAME" /usr/local/bin/busybox wget -qO- "http://127.0.0.1:8080${path}"
}

cleanup() {
    log_info "Cleaning up test container..."
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
}

# Trap cleanup on exit
trap cleanup EXIT INT TERM

main() {
    log_info "Testing Docker image: $IMAGE_NAME"
    
    # Check if image exists
    if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
        log_error "Image $IMAGE_NAME not found!"
        exit 1
    fi

    IMAGE_SIZE_BYTES=$(docker image inspect "$IMAGE_NAME" --format '{{.Size}}')
    IMAGE_SIZE_MIB=$(( (IMAGE_SIZE_BYTES + 1048575) / 1048576 ))
    log_info "Image size: ${IMAGE_SIZE_MIB} MiB (${IMAGE_SIZE_BYTES} bytes)"
    if [[ "$MAX_IMAGE_SIZE_BYTES" -gt 0 && "$IMAGE_SIZE_BYTES" -gt "$MAX_IMAGE_SIZE_BYTES" ]]; then
        log_error "✗ Image exceeds size budget (${IMAGE_SIZE_BYTES} > ${MAX_IMAGE_SIZE_BYTES})"
        exit 1
    fi

    log_info "Test 0: Checking OCI metadata labels..."
    IMAGE_VERSION_LABEL=$(docker image inspect "$IMAGE_NAME" --format '{{ index .Config.Labels "org.opencontainers.image.version" }}')
    IMAGE_SOURCE_LABEL=$(docker image inspect "$IMAGE_NAME" --format '{{ index .Config.Labels "org.opencontainers.image.source" }}')
    if [[ -n "$EXPECTED_OCI_VERSION" && "$IMAGE_VERSION_LABEL" != "$EXPECTED_OCI_VERSION" ]]; then
        log_error "✗ OCI version label mismatch: expected '$EXPECTED_OCI_VERSION', got '$IMAGE_VERSION_LABEL'"
        exit 1
    fi
    if [[ -n "$EXPECTED_OCI_SOURCE" && "$IMAGE_SOURCE_LABEL" != "$EXPECTED_OCI_SOURCE" ]]; then
        log_error "✗ OCI source label mismatch: expected '$EXPECTED_OCI_SOURCE', got '$IMAGE_SOURCE_LABEL'"
        exit 1
    fi
    log_info "✓ OCI metadata labels look correct"
    
    log_info "Starting container..."
    # Set root password for testing
    ROOT_PASSWORD="testpass123"
    DOCKER_RUN_ARGS=(
        -d
        --name "$CONTAINER_NAME"
        -p "$TEST_PORT:8080"
        -e KALAMDB_SERVER_HOST=0.0.0.0
        -e KALAMDB_LOG_LEVEL=info
        -e KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD"
    )

    if [[ -n "$DOCKER_PLATFORM" ]]; then
        DOCKER_RUN_ARGS+=(--platform "$DOCKER_PLATFORM")
    fi

    if [ -n "${KALAMDB_JWT_SECRET:-}" ]; then
        DOCKER_RUN_ARGS+=(-e "KALAMDB_JWT_SECRET=${KALAMDB_JWT_SECRET}")
        docker run "${DOCKER_RUN_ARGS[@]}" "$IMAGE_NAME"
    else
        docker run "${DOCKER_RUN_ARGS[@]}" "$IMAGE_NAME"
    fi
    
    log_info "Waiting for server to start (timeout: ${TIMEOUT}s)..."
    START_TIME=$(date +%s)
    while true; do
        CURRENT_TIME=$(date +%s)
        ELAPSED=$((CURRENT_TIME - START_TIME))
        
        if [ $ELAPSED -ge $TIMEOUT ]; then
            log_error "Server did not start within ${TIMEOUT}s"
            log_info "Container logs:"
            docker logs "$CONTAINER_NAME"
            exit 1
        fi

        if ! docker ps --format '{{.Names}}' | grep -Fx "$CONTAINER_NAME" &>/dev/null; then
            log_error "Container exited before becoming ready"
            log_info "Container logs:"
            docker logs "$CONTAINER_NAME"
            exit 1
        fi
        
        if docker exec "$CONTAINER_NAME" /usr/local/bin/busybox wget -q -O /dev/null "http://127.0.0.1:8080/health" > /dev/null 2>&1; then
            log_info "Server is ready! (took ${ELAPSED}s)"
            break
        fi
        
        sleep 1
    done
    
    # Test 1: Health check
    log_info "Test 1: Health check endpoint..."
    HEALTH_RESPONSE=$(container_get "/health")
    if [ $? -eq 0 ]; then
        log_info "✓ Health check passed: $HEALTH_RESPONSE"
    else
        log_error "✗ Health check failed"
        exit 1
    fi
    
    # Test 2: Version info (healthcheck)
    log_info "Test 2: Version info (healthcheck)..."
    VERSION_RESPONSE=$(container_get "/v1/api/healthcheck" || echo "FAILED")
    if [ "$VERSION_RESPONSE" != "FAILED" ]; then
        log_info "✓ Version info passed: $VERSION_RESPONSE"
    else
        log_error "✗ Version info failed"
        exit 1
    fi
    
    # Test 3: Runtime user and writable paths
    log_info "Test 3: Checking runtime user and writable paths..."
    RUNTIME_UID=$(docker exec "$CONTAINER_NAME" /usr/local/bin/busybox sh -c 'id -u')
    if [[ "$RUNTIME_UID" != "$EXPECTED_RUNTIME_UID" ]]; then
        log_error "✗ Container is running as unexpected uid: $RUNTIME_UID"
        exit 1
    fi

    docker exec "$CONTAINER_NAME" /usr/local/bin/busybox sh -c 'test -f /config/server.toml && test -d /data && test -w /data && mkdir -p /data/.kalam && touch /data/.kalam/write-test && rm /data/.kalam/write-test' &>/dev/null
    if [ $? -eq 0 ]; then
        log_info "✓ Runtime paths are present and writable"
    else
        log_error "✗ Runtime paths are not writable as the runtime user"
        exit 1
    fi

    # Test 4: Check binary existence
    log_info "Test 4: Checking binaries inside container..."
    docker exec "$CONTAINER_NAME" /usr/local/bin/busybox sh -c "test -x /usr/local/bin/kalamdb-server" &>/dev/null
    if [ $? -eq 0 ]; then
        log_info "✓ Server binary exists and is executable"
    else
        log_error "✗ Server binary not found or not executable"
        exit 1
    fi
    
    docker exec "$CONTAINER_NAME" /usr/local/bin/kalam-cli --version &>/dev/null
    if [ $? -eq 0 ]; then
        CLI_VERSION=$(docker exec "$CONTAINER_NAME" /usr/local/bin/kalam-cli --version)
        log_info "✓ CLI binary: $CLI_VERSION"
    else
        log_error "✗ CLI binary not found or not executable"
        exit 1
    fi
    
    # Test 5: Check symlink
    log_info "Test 5: Checking CLI symlink..."
    docker exec "$CONTAINER_NAME" /usr/local/bin/kalam --version &>/dev/null
    if [ $? -eq 0 ]; then
        log_info "✓ CLI symlink works"
    else
        log_error "✗ CLI symlink not working"
        exit 1
    fi
    
    # Test 6: Authentication and SQL query test
    log_info "Test 6: Testing authentication and SQL query execution..."
    
    # First, login to get JWT token
    LOGIN_RESPONSE=$(curl -sf -X POST \
        "http://localhost:$TEST_PORT/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"user\":\"root\",\"password\":\"$ROOT_PASSWORD\"}" 2>&1)
    
    if [ $? -ne 0 ]; then
        log_error "✗ Login failed"
        log_info "Container logs:"
        docker logs "$CONTAINER_NAME" | tail -50
        exit 1
    fi
    
    # Extract token from response (new field: access_token, fallback: token)
    TOKEN=$(echo "$LOGIN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)
    if [ -z "$TOKEN" ]; then
        TOKEN=$(echo "$LOGIN_RESPONSE" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
    fi
    
    if [ -z "$TOKEN" ]; then
        log_error "✗ Failed to extract JWT token from login response"
        log_info "Login response: $LOGIN_RESPONSE"
        exit 1
    fi
    
    log_info "✓ Login successful, token received"
    
    # Now test SQL query with Bearer token
    QUERY_RESPONSE=$(curl -sf -X POST \
        "http://localhost:$TEST_PORT/v1/api/sql" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"sql":"SELECT 1 as test"}' 2>&1 || echo "FAILED")

    if [ "$QUERY_RESPONSE" != "FAILED" ]; then
        log_info "✓ SQL query execution passed: $QUERY_RESPONSE"
    else
        log_error "✗ SQL query test failed"
        log_info "Container logs:"
        docker logs "$CONTAINER_NAME" | tail -50
        exit 1
    fi

    # Test 7: Packaged CLI can talk to the packaged server
    log_info "Test 7: Testing packaged CLI against the running server..."
    CLI_QUERY_OUTPUT=$(docker exec "$CONTAINER_NAME" /usr/local/bin/kalam-cli \
        -u http://127.0.0.1:8080 \
        --token "$TOKEN" \
        --command "SELECT 1 AS packaged_cli_test" 2>&1 || echo "FAILED")

    if [[ "$CLI_QUERY_OUTPUT" != "FAILED" && "$CLI_QUERY_OUTPUT" == *"packaged_cli_test"* ]]; then
        log_info "✓ Packaged CLI query passed"
    else
        log_error "✗ Packaged CLI query failed"
        log_info "CLI output: $CLI_QUERY_OUTPUT"
        log_info "Container logs:"
        docker logs "$CONTAINER_NAME" | tail -50
        exit 1
    fi
    
    # Test 8: Check container resource usage
    log_info "Test 8: Checking container resource usage..."
    STATS=$(docker stats "$CONTAINER_NAME" --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}")
    log_info "Container stats:\n$STATS"
    
    # All tests passed
    log_info ""
    log_info "=================================="
    log_info "✓ All tests passed successfully!"
    log_info "=================================="
    log_info ""
    log_info "Container logs:"
    docker logs "$CONTAINER_NAME" | tail -20
    
    return 0
}

main "$@"
