# KalamDB PostgreSQL Extension — Docker Build
#
# Two-stage build:
#   1. Builder  — compiles the pg_kalam pgrx extension for Linux
#   2. Runtime  — postgres:16 with the extension installed
#
# The extension is a thin remote FDW client (~9 workspace crates) that
# connects to a KalamDB server via gRPC.  The full backend is NOT built.
#
# Build from repo root:
#   docker build -f pg/docker/Dockerfile -t kalamdb-pg:latest .

# ---------------------------------------------------------------------------
# Stage 1: Builder — Rust toolchain + PG 16 dev headers → .so + .sql
# ---------------------------------------------------------------------------
FROM rust:1.92-bookworm AS builder

ENV CARGO_BUILD_JOBS=1
ENV CARGO_INCREMENTAL=0
# -Cpanic=unwind is required for pgrx #[pg_guard] to catch panics at FFI boundaries.
# Without it, any panic in async/gRPC code causes SIGABRT (process abort).
ENV RUSTFLAGS="-Cdebuginfo=0 -Cpanic=unwind"

# Install PG 16 server + dev headers (needed by pgrx)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg lsb-release \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
       | gpg --dearmor -o /etc/apt/trusted.gpg.d/pgdg.gpg \
    && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-16 \
        postgresql-server-dev-16 \
        libclang-dev clang pkg-config libssl-dev cmake make git \
    && rm -rf /var/lib/apt/lists/*

# cargo-pgrx must match the workspace pgrx crate version
RUN cargo install cargo-pgrx --version "=0.16.1" --locked
RUN cargo pgrx init --pg16=/usr/bin/pg_config

WORKDIR /build

# --- Workspace manifests (all members must exist for resolver) -------------
COPY Cargo.toml Cargo.lock version.toml ./
COPY pg/Cargo.toml pg/Cargo.toml
COPY pg/build.rs pg/build.rs
COPY pg/crates/kalam-pg-common/Cargo.toml pg/crates/kalam-pg-common/Cargo.toml
COPY pg/crates/kalam-pg-api/Cargo.toml    pg/crates/kalam-pg-api/Cargo.toml
COPY pg/crates/kalam-pg-types/Cargo.toml  pg/crates/kalam-pg-types/Cargo.toml
COPY pg/crates/kalam-pg-fdw/Cargo.toml    pg/crates/kalam-pg-fdw/Cargo.toml
COPY pg/crates/kalam-pg-client/Cargo.toml pg/crates/kalam-pg-client/Cargo.toml

# Backend crates that the extension actually compiles against
COPY backend/crates/kalamdb-commons/Cargo.toml     backend/crates/kalamdb-commons/Cargo.toml
COPY backend/crates/kalamdb-macros/Cargo.toml      backend/crates/kalamdb-macros/Cargo.toml
COPY backend/crates/kalamdb-pg/Cargo.toml          backend/crates/kalamdb-pg/Cargo.toml
COPY backend/crates/kalamdb-server-auth/Cargo.toml backend/crates/kalamdb-server-auth/Cargo.toml

# Remaining workspace members — resolver needs the manifest, not the source
COPY backend/Cargo.toml backend/Cargo.toml
COPY backend/crates/kalamdb-configs/Cargo.toml       backend/crates/kalamdb-configs/Cargo.toml
COPY backend/crates/kalamdb-store/Cargo.toml         backend/crates/kalamdb-store/Cargo.toml
COPY backend/crates/kalamdb-system/Cargo.toml        backend/crates/kalamdb-system/Cargo.toml
COPY backend/crates/kalamdb-filestore/Cargo.toml     backend/crates/kalamdb-filestore/Cargo.toml
COPY backend/crates/kalamdb-core/Cargo.toml          backend/crates/kalamdb-core/Cargo.toml
COPY backend/crates/kalamdb-sql/Cargo.toml           backend/crates/kalamdb-sql/Cargo.toml
COPY backend/crates/kalamdb-tables/Cargo.toml        backend/crates/kalamdb-tables/Cargo.toml
COPY backend/crates/kalamdb-streams/Cargo.toml       backend/crates/kalamdb-streams/Cargo.toml
COPY backend/crates/kalamdb-auth/Cargo.toml          backend/crates/kalamdb-auth/Cargo.toml
COPY backend/crates/kalamdb-api/Cargo.toml           backend/crates/kalamdb-api/Cargo.toml
COPY backend/crates/kalamdb-session/Cargo.toml       backend/crates/kalamdb-session/Cargo.toml
COPY backend/crates/kalamdb-observability/Cargo.toml backend/crates/kalamdb-observability/Cargo.toml
COPY backend/crates/kalamdb-publisher/Cargo.toml     backend/crates/kalamdb-publisher/Cargo.toml
COPY backend/crates/kalamdb-raft/Cargo.toml          backend/crates/kalamdb-raft/Cargo.toml
COPY backend/crates/kalamdb-sharding/Cargo.toml      backend/crates/kalamdb-sharding/Cargo.toml
COPY backend/crates/kalamdb-views/Cargo.toml         backend/crates/kalamdb-views/Cargo.toml
COPY backend/crates/kalamdb-vector/Cargo.toml        backend/crates/kalamdb-vector/Cargo.toml
COPY backend/crates/kalamdb-dba/Cargo.toml           backend/crates/kalamdb-dba/Cargo.toml
COPY backend/crates/kalamdb-oidc/Cargo.toml          backend/crates/kalamdb-oidc/Cargo.toml
COPY link/Cargo.toml              link/Cargo.toml
COPY link/kalam-link-dart/Cargo.toml link/kalam-link-dart/Cargo.toml
COPY cli/Cargo.toml               cli/Cargo.toml

# Empty stubs so the resolver sees every workspace member
RUN set -e \
    && mkdir -p backend/src && echo 'fn main(){}' > backend/src/main.rs \
    && echo 'fn main(){}' > backend/build.rs \
    && mkdir -p cli/src    && echo 'fn main(){}' > cli/src/main.rs \
    && echo 'fn main(){}' > cli/build.rs \
    && mkdir -p link/src && touch link/src/lib.rs \
    && mkdir -p link/kalam-link-dart/src && touch link/kalam-link-dart/src/lib.rs \
    && for c in kalamdb-configs kalamdb-store kalamdb-system kalamdb-filestore \
         kalamdb-core kalamdb-sql kalamdb-tables kalamdb-streams kalamdb-auth \
         kalamdb-api kalamdb-session kalamdb-observability kalamdb-publisher \
         kalamdb-raft kalamdb-sharding kalamdb-views kalamdb-vector kalamdb-dba \
         kalamdb-oidc; do \
       mkdir -p "backend/crates/$c/src" && touch "backend/crates/$c/src/lib.rs"; \
    done

# --- Real source for the 9 crates that actually get compiled ---------------
COPY pg/ pg/
COPY backend/crates/kalamdb-commons/ backend/crates/kalamdb-commons/
COPY backend/crates/kalamdb-macros/  backend/crates/kalamdb-macros/
COPY backend/crates/kalamdb-pg/          backend/crates/kalamdb-pg/
COPY backend/crates/kalamdb-server-auth/ backend/crates/kalamdb-server-auth/

# Build + install the extension into the system PG 16 paths
RUN cargo pgrx install \
    -p kalam-pg-extension \
    -c /usr/bin/pg_config \
    --no-default-features \
    --release \
    -F pg16

# Collect the artifacts into one place for the runtime stage
RUN mkdir -p /pg-artifacts \
    && cp /usr/lib/postgresql/16/lib/pg_kalam.so          /pg-artifacts/ \
    && cp /usr/share/postgresql/16/extension/pg_kalam.control /pg-artifacts/ \
    && cp /usr/share/postgresql/16/extension/pg_kalam--*.sql  /pg-artifacts/

# ---------------------------------------------------------------------------
# Stage 2: Runtime — stock postgres:16 with the extension installed
# ---------------------------------------------------------------------------
FROM postgres:16-bookworm

COPY --from=builder /pg-artifacts/pg_kalam.so          /usr/lib/postgresql/16/lib/
COPY --from=builder /pg-artifacts/pg_kalam.control     /usr/share/postgresql/16/extension/
COPY --from=builder /pg-artifacts/pg_kalam--*.sql      /usr/share/postgresql/16/extension/

