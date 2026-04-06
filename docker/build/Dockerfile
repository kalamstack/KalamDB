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

# Remove only the main pg extension crate from workspace members (pgrx requires PostgreSQL headers).
# The pg sub-crates (kalam-pg-client, etc.) are kept because backend crates depend on them.
RUN sed -i '/^    "pg",$/d' Cargo.toml

# Copy all Cargo.toml files to establish workspace structure
COPY backend/Cargo.toml ./backend/
COPY cli/Cargo.toml ./cli/
COPY link/link-common/Cargo.toml ./link/link-common/
COPY link/kalam-client/Cargo.toml ./link/kalam-client/
COPY link/kalam-consumer/Cargo.toml ./link/kalam-consumer/
COPY link/kalam-consumer-wasm/Cargo.toml ./link/kalam-consumer-wasm/
COPY link/kalam-link-dart/Cargo.toml ./link/kalam-link-dart/
COPY pg/crates/kalam-pg-common/Cargo.toml ./pg/crates/kalam-pg-common/
COPY pg/crates/kalam-pg-api/Cargo.toml ./pg/crates/kalam-pg-api/
COPY pg/crates/kalam-pg-fdw/Cargo.toml ./pg/crates/kalam-pg-fdw/
COPY pg/crates/kalam-pg-types/Cargo.toml ./pg/crates/kalam-pg-types/
COPY pg/crates/kalam-pg-client/Cargo.toml ./pg/crates/kalam-pg-client/

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
    mkdir -p backend/crates/kalamdb-configs/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-configs/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-core/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-core/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-dba/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-dba/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-filestore/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-filestore/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-macros/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-macros/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-observability/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-observability/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-oidc/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-oidc/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-pg/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-pg/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-publisher/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-publisher/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-raft/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-raft/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-server-auth/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-server-auth/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-session/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-session/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-sharding/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-sharding/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-sql/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-sql/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-store/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-store/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-streams/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-streams/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-system/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-system/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-tables/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-tables/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-vector/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-vector/src/lib.rs && \
    mkdir -p backend/crates/kalamdb-views/src && \
    echo "pub fn lib() {}" > backend/crates/kalamdb-views/src/lib.rs

# Copy crate Cargo.toml files for proper dependency resolution
COPY backend/crates/kalamdb-api/Cargo.toml ./backend/crates/kalamdb-api/
COPY backend/crates/kalamdb-auth/Cargo.toml ./backend/crates/kalamdb-auth/
COPY backend/crates/kalamdb-commons/Cargo.toml ./backend/crates/kalamdb-commons/
COPY backend/crates/kalamdb-configs/Cargo.toml ./backend/crates/kalamdb-configs/
COPY backend/crates/kalamdb-core/Cargo.toml ./backend/crates/kalamdb-core/
COPY backend/crates/kalamdb-dba/Cargo.toml ./backend/crates/kalamdb-dba/
COPY backend/crates/kalamdb-filestore/Cargo.toml ./backend/crates/kalamdb-filestore/
COPY backend/crates/kalamdb-macros/Cargo.toml ./backend/crates/kalamdb-macros/
COPY backend/crates/kalamdb-observability/Cargo.toml ./backend/crates/kalamdb-observability/
COPY backend/crates/kalamdb-oidc/Cargo.toml ./backend/crates/kalamdb-oidc/
COPY backend/crates/kalamdb-pg/Cargo.toml ./backend/crates/kalamdb-pg/
COPY backend/crates/kalamdb-publisher/Cargo.toml ./backend/crates/kalamdb-publisher/
COPY backend/crates/kalamdb-raft/Cargo.toml ./backend/crates/kalamdb-raft/
COPY backend/crates/kalamdb-server-auth/Cargo.toml ./backend/crates/kalamdb-server-auth/
COPY backend/crates/kalamdb-session/Cargo.toml ./backend/crates/kalamdb-session/
COPY backend/crates/kalamdb-sharding/Cargo.toml ./backend/crates/kalamdb-sharding/
COPY backend/crates/kalamdb-sql/Cargo.toml ./backend/crates/kalamdb-sql/
COPY backend/crates/kalamdb-store/Cargo.toml ./backend/crates/kalamdb-store/
COPY backend/crates/kalamdb-streams/Cargo.toml ./backend/crates/kalamdb-streams/
COPY backend/crates/kalamdb-system/Cargo.toml ./backend/crates/kalamdb-system/
COPY backend/crates/kalamdb-tables/Cargo.toml ./backend/crates/kalamdb-tables/
COPY backend/crates/kalamdb-vector/Cargo.toml ./backend/crates/kalamdb-vector/
COPY backend/crates/kalamdb-views/Cargo.toml ./backend/crates/kalamdb-views/

# Create dummy CLI
RUN mkdir -p cli/src && \
    echo "fn main() {}" > cli/src/main.rs && \
    touch cli/build.rs

# Create dummy link crates
RUN mkdir -p link/link-common/src && \
    echo "pub fn lib() {}" > link/link-common/src/lib.rs && \
    mkdir -p link/kalam-client/src && \
    echo "pub fn lib() {}" > link/kalam-client/src/lib.rs && \
    mkdir -p link/kalam-consumer/src && \
    echo "pub fn lib() {}" > link/kalam-consumer/src/lib.rs && \
    mkdir -p link/kalam-consumer-wasm/src && \
    echo "pub fn lib() {}" > link/kalam-consumer-wasm/src/lib.rs && \
    mkdir -p link/kalam-link-dart/src && \
    echo "pub fn lib() {}" > link/kalam-link-dart/src/lib.rs

# Create dummy pg sub-crates (not the main extension — that needs pgrx)
RUN mkdir -p pg/crates/kalam-pg-common/src && \
    echo "pub fn lib() {}" > pg/crates/kalam-pg-common/src/lib.rs && \
    mkdir -p pg/crates/kalam-pg-api/src && \
    echo "pub fn lib() {}" > pg/crates/kalam-pg-api/src/lib.rs && \
    mkdir -p pg/crates/kalam-pg-fdw/src && \
    echo "pub fn lib() {}" > pg/crates/kalam-pg-fdw/src/lib.rs && \
    mkdir -p pg/crates/kalam-pg-types/src && \
    echo "pub fn lib() {}" > pg/crates/kalam-pg-types/src/lib.rs && \
    mkdir -p pg/crates/kalam-pg-client/src && \
    echo "pub fn lib() {}" > pg/crates/kalam-pg-client/src/lib.rs

# Create minimal version.toml
RUN echo '# Auto-generated for Docker build' > version.toml && \
    echo '[version]' >> version.toml && \
    echo 'git_commit_hash = "docker"' >> version.toml && \
    echo 'git_branch = "docker"' >> version.toml && \
    echo "build_date = \"$(date -u '+%Y-%m-%d %H:%M:%S UTC')\"" >> version.toml

# Build dependencies only (this layer is cached until Cargo.toml/Cargo.lock changes)
RUN SKIP_UI_BUILD=1 cargo build --profile docker --features mimalloc --bin kalamdb-server --bin kalam 2>/dev/null || true
# The "|| true" handles expected errors from dummy source files
# The important thing is that all dependencies are now compiled and cached

# ============================================================================
# Stage 3: Full builder with actual source
# ============================================================================
FROM deps AS builder

# Remove dummy source files
RUN rm -rf backend/src backend/crates cli/src link/link-common/src link/kalam-client/src link/kalam-consumer/src link/kalam-consumer-wasm/src link/kalam-link-dart/src pg/crates

# Copy actual source code
COPY backend/src ./backend/src
COPY backend/crates ./backend/crates
COPY backend/build.rs ./backend/
COPY backend/build ./backend/build
COPY cli/src ./cli/src
COPY cli/build.rs ./cli/
COPY link/link-common/src ./link/link-common/src
COPY link/kalam-client/src ./link/kalam-client/src
COPY link/kalam-consumer/src ./link/kalam-consumer/src
COPY link/kalam-consumer-wasm/src ./link/kalam-consumer-wasm/src
COPY link/kalam-link-dart/src ./link/kalam-link-dart/src
COPY pg/crates ./pg/crates

# Copy UI source for embedded admin UI
COPY ui ./ui

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
# SKIP_UI_BUILD: the UI dist/ was already built on the host and COPY'd above.
RUN SKIP_UI_BUILD=1 cargo build --profile docker --features mimalloc --bin kalamdb-server --bin kalam

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

# mimalloc tuning: eager memory return and reduced arena count.
# MIMALLOC_PURGE_DELAY=100: purge freed segments after 100ms (default 10s)
# MIMALLOC_ARENA_EAGER_COMMIT=1: commit arena pages immediately
ENV MIMALLOC_PURGE_DELAY=100
ENV MIMALLOC_ARENA_EAGER_COMMIT=1

# Set working directory
WORKDIR /data

# Expose default port
EXPOSE 8080

# Health check using curl
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# Default command
CMD ["/usr/local/bin/kalamdb-server", "/config/server.toml"]
