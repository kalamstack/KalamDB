# Multi-stage Docker build for KalamDB backend + CLI
# Feature: 006-docker-wasm-examples
# This Dockerfile builds both kalamdb-server and kalam-cli binaries
# Supports multi-architecture: linux/amd64, linux/arm64
#
# CACHING STRATEGY:
# 1. Install system dependencies (cached unless base image changes)
# 2. Install Rust tools (cached unless Rust version changes)
# 3. Copy Cargo.toml/Cargo.lock and build dependencies only (cached unless deps change)
# 4. Copy source and build final binaries (only this rebuilds on code changes)

# ============================================================================
# Stage 1: Base builder with dependencies
# ============================================================================
FROM rust:1.92-bookworm AS base

# Install build dependencies including Node.js for UI build
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        pkg-config \
        libssl-dev \
        libclang-dev \
        clang \
        cmake \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 LTS for UI build
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g npm@latest && \
    rm -rf /var/lib/apt/lists/*

# Install wasm-pack for building WASM SDK
RUN set -eux; \
        curl -fsSL --retry 5 --retry-delay 2 --retry-connrefused \
            https://rustwasm.github.io/wasm-pack/installer/init.sh | sh; \
        command -v wasm-pack; \
        wasm-pack --version

ENV PATH=/usr/local/cargo/bin:$PATH

# Install wasm32 target for Rust
RUN rustup target add wasm32-unknown-unknown

# Set working directory
WORKDIR /build

# ============================================================================
# Stage 2: Dependency builder (cached layer)
# ============================================================================
FROM base AS deps

# Copy only the files needed to determine dependencies
COPY Cargo.toml Cargo.lock ./

# Copy all Cargo.toml files to establish workspace structure
COPY backend/Cargo.toml ./backend/
COPY cli/Cargo.toml ./cli/
COPY link/Cargo.toml ./link/
COPY benchmark/Cargo.toml ./benchmark/

# Create minimal dummy source files for dependency resolution
# These allow cargo to build/cache all dependencies without actual source
RUN mkdir -p backend/src backend/crates && \
    echo "fn main() {}" > backend/src/main.rs && \
    echo "pub fn lib() {}" > backend/src/lib.rs

# Create dummy crates for backend workspace members
RUN mkdir -p backend/crates/kalamdb-api/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-api/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-auth/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-auth/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-commons/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-commons/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-core/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-core/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-filestore/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-filestore/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-raft/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-raft/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-sql/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-sql/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-store/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-store/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-system/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-system/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-tables/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-tables/src/lib.rs

# Copy crate Cargo.toml files for proper dependency resolution
COPY backend/crates/kalamdb-api/Cargo.toml ./backend/crates/kalamdb-api/
COPY backend/crates/kalamdb-auth/Cargo.toml ./backend/crates/kalamdb-auth/
COPY backend/crates/kalamdb-commons/Cargo.toml ./backend/crates/kalamdb-commons/
COPY backend/crates/kalamdb-core/Cargo.toml ./backend/crates/kalamdb-core/
COPY backend/crates/kalamdb-filestore/Cargo.toml ./backend/crates/kalamdb-filestore/
COPY backend/crates/kalamdb-raft/Cargo.toml ./backend/crates/kalamdb-raft/
COPY backend/crates/kalamdb-sql/Cargo.toml ./backend/crates/kalamdb-sql/
COPY backend/crates/kalamdb-store/Cargo.toml ./backend/crates/kalamdb-store/
COPY backend/crates/kalamdb-system/Cargo.toml ./backend/crates/kalamdb-system/
COPY backend/crates/kalamdb-tables/Cargo.toml ./backend/crates/kalamdb-tables/

# Create dummy CLI
RUN mkdir -p cli/src && \
    echo "fn main() {}" > cli/src/main.rs && \
    touch cli/build.rs

# Create dummy link library
RUN mkdir -p link/src && \
    echo "pub fn lib() {}" > link/src/lib.rs

# Create dummy benchmark
RUN mkdir -p benchmark/src && \
    echo "fn main() {}" > benchmark/src/main.rs

# Create minimal version.toml
RUN echo '# Auto-generated for Docker build' > version.toml && \
    echo '[version]' >> version.toml && \
    echo 'git_commit_hash = "docker"' >> version.toml && \
    echo 'git_branch = "docker"' >> version.toml && \
    echo "build_date = \"$(date -u '+%Y-%m-%d %H:%M:%S UTC')\"" >> version.toml

# Build dependencies only (this layer is cached until Cargo.toml/Cargo.lock changes)
RUN cargo build --profile docker --features jemalloc --bin kalamdb-server --bin kalam 2>/dev/null || true
# The "|| true" handles expected errors from dummy source files
# The important thing is that all dependencies are now compiled and cached

# ============================================================================
# Stage 3: Full builder with actual source
# ============================================================================
FROM deps AS builder

# Remove dummy source files
RUN rm -rf backend/src backend/crates cli/src link/src link/sdks benchmark/src

# Copy actual source code
COPY backend/src ./backend/src
COPY backend/crates ./backend/crates
COPY backend/build.rs ./backend/
COPY cli/src ./cli/src
COPY cli/build.rs ./cli/
COPY link/src ./link/src
COPY link/sdks ./link/sdks

# Copy UI source for embedded admin UI
COPY ui ./ui

# Create dummy benchmark (we don't need to build it)
RUN mkdir -p benchmark/src && echo "fn main() {}" > benchmark/src/main.rs

# Copy or generate version.toml for compile-time version info
COPY version.tom[l] ./
RUN if [ ! -f version.toml ]; then \
    echo '# Auto-generated for Docker build' > version.toml && \
    echo '[version]' >> version.toml && \
    echo 'git_commit_hash = "docker"' >> version.toml && \
    echo 'git_branch = "docker"' >> version.toml && \
    echo "build_date = \"$(date -u '+%Y-%m-%d %H:%M:%S UTC')\"" >> version.toml; \
    fi

# Build final binaries (dependencies are already cached from deps stage)
RUN cargo build --profile docker --features jemalloc --bin kalamdb-server --bin kalam

# ============================================================================
# Stage 4: Runtime (optimized for minimal size)
# ============================================================================
FROM debian:bookworm-slim

# Install runtime dependencies and create user in single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/* && \
    useradd -m -u 1000 kalamdb && \
    mkdir -p /data/rocksdb /data/storage /data/logs /config && \
    chown -R kalamdb:kalamdb /data /config

# Copy binaries from builder with correct permissions
COPY --from=builder --chmod=755 /build/target/docker/kalamdb-server /usr/local/bin/kalamdb-server
COPY --from=builder --chmod=755 /build/target/docker/kalam /usr/local/bin/kalam-cli

# Create symlink so 'kalam' command works
RUN ln -s /usr/local/bin/kalam-cli /usr/local/bin/kalam

# Copy default server configuration (from backend example) and normalize data path
COPY --chown=kalamdb:kalamdb backend/server.example.toml /config/server.toml
RUN sed -i 's|data_path = "\./data"|data_path = "/data"|g' /config/server.toml

# Switch to non-root user
USER kalamdb

# Set working directory
WORKDIR /data

# Expose default port
EXPOSE 8080

# Health check using curl
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# Default command
CMD ["/usr/local/bin/kalamdb-server", "/config/server.toml"]
