#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="${1:-jamals86/kalamdb:test}"
CONTAINER_NAME="kalamdb-startup-test-$$"
TEST_PORT="${TEST_PORT:-18080}"
TIMEOUT="${TIMEOUT:-30}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-}"

cleanup() {
    docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "Image not found: $IMAGE_NAME" >&2
    exit 1
fi

JWT_SECRET="${KALAMDB_JWT_SECRET:-}"
if [[ -z "$JWT_SECRET" ]]; then
    JWT_SECRET="$(openssl rand -base64 32)"
fi

docker run -d \
    --name "$CONTAINER_NAME" \
    -e KALAMDB_SERVER_HOST=0.0.0.0 \
    -e KALAMDB_ROOT_PASSWORD=testpass123 \
    -e KALAMDB_LOG_LEVEL=info \
    -e KALAMDB_JWT_SECRET="$JWT_SECRET" \
    ${DOCKER_PLATFORM:+--platform "$DOCKER_PLATFORM"} \
    "$IMAGE_NAME" >/dev/null

START_TIME=$(date +%s)
while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        echo "Docker startup test failed: server did not become healthy within ${TIMEOUT}s" >&2
        docker logs "$CONTAINER_NAME" >&2 || true
        exit 1
    fi

    if docker exec "$CONTAINER_NAME" /usr/local/bin/busybox wget --spider -q http://127.0.0.1:8080/health >/dev/null 2>&1; then
        echo "Docker startup test passed in ${ELAPSED}s"
        break
    fi

    if ! docker ps --format '{{.Names}}' | grep -Fx "$CONTAINER_NAME" >/dev/null 2>&1; then
        echo "Docker startup test failed: container exited early" >&2
        docker logs "$CONTAINER_NAME" >&2 || true
        exit 1
    fi

    sleep 1
done