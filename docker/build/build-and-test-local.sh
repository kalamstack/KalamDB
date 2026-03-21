#!/usr/bin/env bash
# Build and test KalamDB Docker image locally
# This mimics what the release workflow does

set -euo pipefail

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGE_TAG="${1:-kalamdb:local}"

main() {
    log_info "Building and testing KalamDB Docker image locally"
    log_info "Image tag: $IMAGE_TAG"
    log_info "Project root: $PROJECT_ROOT"
    
    cd "$PROJECT_ROOT"
    
    # Step 1: Build release binaries
    log_info "Step 1/4: Building release binaries..."
    if [ ! -f "target/release/kalamdb-server" ] || [ ! -f "target/release/kalam" ]; then
        log_info "Building with cargo (this may take a while)..."
        cargo build --release --features jemalloc -p kalamdb-server -p kalam-cli --bin kalamdb-server --bin kalam
    else
        log_warn "Using existing release binaries (run 'cargo clean' to rebuild)"
    fi
    
    # Step 2: Prepare binaries directory
    log_info "Step 2/4: Preparing binaries directory..."
    mkdir -p binaries-amd64
    cp target/release/kalamdb-server binaries-amd64/
    cp target/release/kalam binaries-amd64/
    chmod +x binaries-amd64/kalamdb-server binaries-amd64/kalam
    log_info "Binaries ready:"
    ls -lh binaries-amd64/
    
    # Step 3: Build Docker image
    log_info "Step 3/4: Building Docker image..."
    docker build \
        --build-context binaries=binaries-amd64 \
        -f docker/build/Dockerfile.prebuilt \
        -t "$IMAGE_TAG" \
        .
    
    log_info "Docker image built: $IMAGE_TAG"
    docker images "$IMAGE_TAG"
    
    # Step 4: Run smoke tests
    log_info "Step 4/4: Running smoke tests..."
    chmod +x docker/build/test-docker-image.sh
    ./docker/build/test-docker-image.sh "$IMAGE_TAG"
    
    # Cleanup
    log_info "Cleaning up binaries directory..."
    rm -rf binaries-amd64
    
    log_info ""
    log_info "========================================="
    log_info "✓ Build and test completed successfully!"
    log_info "========================================="
    log_info ""
    log_info "To run the image:"
    log_info "  docker run -p 8080:8080 $IMAGE_TAG"
    log_info ""
    log_info "To push to registry:"
    log_info "  docker tag $IMAGE_TAG your-registry/$IMAGE_TAG"
    log_info "  docker push your-registry/$IMAGE_TAG"
}

main "$@"
