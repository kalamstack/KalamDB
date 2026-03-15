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
TEST_PORT=8081
TIMEOUT=30

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
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
    
    log_info "Starting container..."
    # Set root password for testing
    ROOT_PASSWORD="testpass123"
    if [ -n "${KALAMDB_JWT_SECRET:-}" ]; then
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "$TEST_PORT:8080" \
            -e KALAMDB_SERVER_HOST=0.0.0.0 \
            -e KALAMDB_LOG_LEVEL=info \
            -e KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
            -e "KALAMDB_JWT_SECRET=${KALAMDB_JWT_SECRET}" \
            "$IMAGE_NAME"
    else
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "$TEST_PORT:8080" \
            -e KALAMDB_SERVER_HOST=0.0.0.0 \
            -e KALAMDB_LOG_LEVEL=info \
            -e KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
            "$IMAGE_NAME"
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
        
        if curl -sf "http://localhost:$TEST_PORT/health" &>/dev/null; then
            log_info "Server is ready! (took ${ELAPSED}s)"
            break
        fi
        
        sleep 1
    done
    
    # Test 1: Health check
    log_info "Test 1: Health check endpoint..."
    HEALTH_RESPONSE=$(curl -sf "http://localhost:$TEST_PORT/health")
    if [ $? -eq 0 ]; then
        log_info "✓ Health check passed: $HEALTH_RESPONSE"
    else
        log_error "✗ Health check failed"
        exit 1
    fi
    
    # Test 2: Version info (healthcheck)
    log_info "Test 2: Version info (healthcheck)..."
    VERSION_RESPONSE=$(curl -sf "http://localhost:$TEST_PORT/v1/api/healthcheck" || echo "FAILED")
    if [ "$VERSION_RESPONSE" != "FAILED" ]; then
        log_info "✓ Version info passed: $VERSION_RESPONSE"
    else
        log_error "✗ Version info failed"
        exit 1
    fi
    
    # Test 3: Check binary existence
    log_info "Test 3: Checking binaries inside container..."
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
    
    # Test 4: Check symlink
    log_info "Test 4: Checking CLI symlink..."
    docker exec "$CONTAINER_NAME" /usr/local/bin/kalam --version &>/dev/null
    if [ $? -eq 0 ]; then
        log_info "✓ CLI symlink works"
    else
        log_error "✗ CLI symlink not working"
        exit 1
    fi
    
    # Test 5: Authentication and SQL query test
    log_info "Test 5: Testing authentication and SQL query execution..."
    
    # First, login to get JWT token
    LOGIN_RESPONSE=$(curl -sf -X POST \
        "http://localhost:$TEST_PORT/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"root\",\"password\":\"$ROOT_PASSWORD\"}" 2>&1)
    
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
    
    # Test 6: Check container resource usage
    log_info "Test 6: Checking container resource usage..."
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
