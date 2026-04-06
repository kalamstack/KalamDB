#!/usr/bin/env bash
# Local test script to simulate GitHub Actions release workflow steps
# Run this to test the build before pushing to GitHub

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

echo "=========================================="
echo "Testing Release Workflow Locally"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

print_step() {
    echo -e "${BLUE}▶${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Step 1: Build UI
print_step "Step 1/5: Building Admin UI (includes SDK build)..."
echo ""

# Install wasm-pack if needed
if ! command -v wasm-pack &> /dev/null; then
    print_error "wasm-pack not found. Installing..."
    curl -fsSL --retry 5 --retry-delay 2 --retry-connrefused \
      https://rustwasm.github.io/wasm-pack/installer/init.sh | sh
fi

# Build SDK
print_step "Building @kalamdb/client SDK..."
cd "$REPO_ROOT/link/sdks/typescript/client"
npm install
npm run build
print_success "SDK built"

# Build UI
print_step "Building Admin UI..."
cd "$REPO_ROOT/ui"
npm install
npm run build

if [[ ! -f "dist/index.html" ]]; then
    print_error "UI build failed - dist/index.html not found"
    exit 1
fi
print_success "UI built successfully"
echo ""

# Step 2: Test Linux build
print_step "Step 2/5: Testing Linux x86_64 build..."
cd "$REPO_ROOT"
export SKIP_UI_BUILD=1

if [[ "$(uname -s)" == "Darwin" ]]; then
    print_step "Running on macOS - using cross for Linux build"
    if ! command -v cross &> /dev/null; then
        print_step "Installing cross..."
        cargo install cross --locked
    fi
    rustup target add x86_64-unknown-linux-gnu
    cross build --profile docker --target x86_64-unknown-linux-gnu --bin kalam --bin kalamdb-server
else
    cargo build --profile release-dist --target x86_64-unknown-linux-gnu --bin kalam --bin kalamdb-server
fi
print_success "Linux build successful"
echo ""

# Step 3: Test macOS build (only on macOS)
if [[ "$(uname -s)" == "Darwin" ]]; then
    print_step "Step 3/5: Testing macOS ARM64 build..."
    rustup target add aarch64-apple-darwin
    
    # Check LLVM
    if ! brew list llvm@16 &> /dev/null; then
        print_step "Installing LLVM 16..."
        brew install llvm@16
    fi
    
    export LIBCLANG_PATH="$(brew --prefix llvm@16)/lib"
    export LLVM_CONFIG_PATH="$(brew --prefix llvm@16)/bin/llvm-config"
    
    cargo build --profile release-dist --target aarch64-apple-darwin --bin kalam --bin kalamdb-server
    print_success "macOS ARM64 build successful"
else
    print_step "Step 3/5: Skipping macOS build (not on macOS)"
fi
echo ""

# Step 4: Test Windows build (cross-compile)
print_step "Step 4/5: Testing Windows x86_64 build..."
if ! command -v cross &> /dev/null; then
    print_step "Installing cross..."
    cargo install cross --locked
fi
rustup target add x86_64-pc-windows-gnu
cross build --profile docker --target x86_64-pc-windows-gnu --bin kalam --bin kalamdb-server
print_success "Windows build successful"
echo ""

# Step 5: Test Docker build (optional)
print_step "Step 5/5: Testing Docker build (optional)..."
read -p "Build Docker image? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker build -f docker/build/Dockerfile -t kalamdb:test .
    print_success "Docker build successful"
else
    print_step "Skipping Docker build"
fi
echo ""

echo "=========================================="
print_success "All tests passed! Workflow should work."
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Commit your changes"
echo "  2. Push to GitHub"
echo "  3. Go to Actions → Release → Run workflow"
echo "  4. Set inputs:"
echo "     - version_tag: v0.1.2-test1"
echo "     - platforms: linux-x86_64,macos-aarch64,windows-x86_64"
echo "     - github_release: false"
echo "     - docker_push: false"
echo ""
