# KalamDB PostgreSQL Extension — Docker Build
#
# Two-stage build:
#   1. Builder  — compiles the pg_kalam pgrx extension for Linux
#   2. Runtime  — postgres:${PG_MAJOR} with the extension installed
#
# The extension is a thin remote FDW client (~9 workspace crates) that
# connects to a KalamDB server via gRPC.  The full backend is NOT built.
#
# Build from repo root:
#   docker build -f pg/docker/Dockerfile -t pg-kalam:latest .

ARG RUST_BASE_IMAGE=public.ecr.aws/docker/library/rust:1.92-bookworm
ARG POSTGRES_BASE_IMAGE=public.ecr.aws/docker/library/postgres:16-bookworm
ARG PG_MAJOR=16
ARG PG_EXTENSION_FLAVOR=pg16
ARG PGRX_VERSION=0.17.0
ARG OCI_IMAGE_DESCRIPTION="PostgreSQL with the pg_kalam extension preinstalled so you can connect PostgreSQL foreign tables to a remote KalamDB server."

# ---------------------------------------------------------------------------
# Stage 1: Builder — Rust toolchain + selected PG dev headers → .so + .sql
# ---------------------------------------------------------------------------
FROM ${RUST_BASE_IMAGE} AS builder

ARG PG_MAJOR
ARG PG_EXTENSION_FLAVOR
ARG PGRX_VERSION

ENV CARGO_BUILD_JOBS=1
ENV CARGO_INCREMENTAL=0
# -Cpanic=unwind is required for pgrx #[pg_guard] to catch panics at FFI boundaries.
# Without it, any panic in async/gRPC code causes SIGABRT (process abort).
ENV RUSTFLAGS="-Cdebuginfo=0 -Cpanic=unwind"

# Install the selected PG server + dev headers needed by pgrx.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg lsb-release \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
       | gpg --dearmor -o /etc/apt/trusted.gpg.d/pgdg.gpg \
    && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-${PG_MAJOR} \
        postgresql-server-dev-${PG_MAJOR} \
        libclang-dev clang pkg-config libssl-dev cmake make git \
    && rm -rf /var/lib/apt/lists/*

# cargo-pgrx must match the workspace pgrx crate version
RUN cargo install cargo-pgrx --version "=${PGRX_VERSION}" --locked
RUN cargo pgrx init "--pg${PG_MAJOR}=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"

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
COPY link/link-common/Cargo.toml       link/link-common/Cargo.toml
COPY link/kalam-client/Cargo.toml      link/kalam-client/Cargo.toml
COPY link/kalam-consumer/Cargo.toml    link/kalam-consumer/Cargo.toml
COPY link/kalam-consumer-wasm/Cargo.toml link/kalam-consumer-wasm/Cargo.toml
COPY link/kalam-link-dart/Cargo.toml link/kalam-link-dart/Cargo.toml
COPY cli/Cargo.toml               cli/Cargo.toml

# Empty stubs so the resolver sees every workspace member
RUN set -e \
    && mkdir -p backend/src && echo 'fn main(){}' > backend/src/main.rs \
    && echo 'fn main(){}' > backend/build.rs \
    && mkdir -p cli/src    && echo 'fn main(){}' > cli/src/main.rs \
    && echo 'fn main(){}' > cli/build.rs \
    && mkdir -p link/link-common/src && touch link/link-common/src/lib.rs \
    && mkdir -p link/kalam-client/src && touch link/kalam-client/src/lib.rs \
    && mkdir -p link/kalam-consumer/src && touch link/kalam-consumer/src/lib.rs \
    && mkdir -p link/kalam-consumer-wasm/src && touch link/kalam-consumer-wasm/src/lib.rs \
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

# Build + install the extension into the selected system PG paths.
RUN cargo pgrx install \
    -p kalam-pg-extension \
    -c /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
    --no-default-features \
    --release \
    -F ${PG_EXTENSION_FLAVOR}

# Collect the artifacts into one place for the runtime stage
RUN mkdir -p /pg-artifacts \
    && cp /usr/lib/postgresql/${PG_MAJOR}/lib/pg_kalam.so          /pg-artifacts/ \
    && cp /usr/share/postgresql/${PG_MAJOR}/extension/pg_kalam.control /pg-artifacts/ \
    && cp /usr/share/postgresql/${PG_MAJOR}/extension/pg_kalam--*.sql  /pg-artifacts/

# ---------------------------------------------------------------------------
# Stage 2: Runtime — stock postgres:${PG_MAJOR} with the extension installed
# ---------------------------------------------------------------------------
FROM ${POSTGRES_BASE_IMAGE}

ARG PG_MAJOR
ARG OCI_IMAGE_DESCRIPTION

LABEL org.opencontainers.image.title="pg-kalam" \
    org.opencontainers.image.description="${OCI_IMAGE_DESCRIPTION}"

COPY --from=builder /pg-artifacts/ /pg-artifacts/

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-${PG_MAJOR}-pgvector \
    && rm -rf /var/lib/apt/lists/*

RUN install -m 755 /pg-artifacts/pg_kalam.so "/usr/lib/postgresql/${PG_MAJOR}/lib/pg_kalam.so" \
    && install -m 644 /pg-artifacts/pg_kalam.control "/usr/share/postgresql/${PG_MAJOR}/extension/pg_kalam.control" \
    && install -m 644 /pg-artifacts/pg_kalam--*.sql "/usr/share/postgresql/${PG_MAJOR}/extension/" \
    && rm -rf /pg-artifacts

