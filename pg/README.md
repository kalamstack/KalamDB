# KalamDB PostgreSQL Extension

This directory contains the `pg_kalam` PostgreSQL extension.

The extension is built with `pgrx` and supports PostgreSQL `pg13` through `pg18`. The default build target is `pg16`.

## What this gives you

- `CREATE EXTENSION pg_kalam;`
- A `pg_kalam` foreign data wrapper registered by the extension install SQL
- A PostgreSQL-side bridge to a running KalamDB server over gRPC

## Choose the right workflow

- Testing and development on the same machine: build and install natively with `cargo pgrx install`
- Building a Linux image or using macOS with Dockerized PostgreSQL: compile with `./pg/docker/build-fast.sh` or `./pg/docker/build.sh`
- Production install into a Linux PostgreSQL server: build Linux artifacts in Docker, then copy `pg_kalam.so`, `pg_kalam.control`, and `pg_kalam--*.sql` into that server's PostgreSQL directories
- Running both KalamDB and PostgreSQL together: use `cd pg/docker && docker compose up -d`

## Prerequisites

### Native source build

- Rust toolchain compatible with this workspace
- `cargo-pgrx` version `0.17.0`
- PostgreSQL server and development headers for the major version you want to target
- A working `pg_config` for the PostgreSQL instance you want to install into

### Docker build and Docker runtime

- Docker Desktop on macOS, or Docker Engine on Linux
- Enough memory for a Rust build inside Docker
- The provided Dockerfiles default to public ECR mirrors for Rust and PostgreSQL base images to avoid Docker Hub auth and rate-limit failures
- Override those defaults with `RUST_BASE_IMAGE` or `POSTGRES_BASE_IMAGE` if your environment requires a different registry mirror
- Select the PostgreSQL major with `PG_MAJOR` and matching Cargo feature with `PG_EXTENSION_FLAVOR=pg${PG_MAJOR}`

## Compile from source and install for testing or development

Run these commands from the repository root.

```bash
PG_MAJOR=16
PG_FEATURE="pg${PG_MAJOR}"
PG_CONFIG="$(command -v pg_config)"

cargo install cargo-pgrx --version "=0.17.0" --locked
cargo pgrx init "--pg${PG_MAJOR}=${PG_CONFIG}"

cargo pgrx install \
  -p kalam-pg-extension \
  -c "${PG_CONFIG}" \
  --no-default-features \
  --profile release-pg \
  -F "${PG_FEATURE}"
```

For a different PostgreSQL major, change `PG_MAJOR` and point `PG_CONFIG` at that server's `pg_config`.

What `cargo pgrx install` does:

- builds the extension as `pg_kalam`
- copies `pg_kalam.so` into PostgreSQL's library directory
- copies `pg_kalam.control` and the generated upgrade SQL into PostgreSQL's extension directory

After changing extension code, rerun the same `cargo pgrx install` command.

## Start KalamDB from source for local development

The PostgreSQL extension talks to a running KalamDB server. Start one before creating the foreign server.

```bash
cd backend
cp server.example.toml server.toml
KALAMDB_SERVER_HOST=0.0.0.0 \
KALAMDB_JWT_SECRET="replace-with-a-32-char-secret" \
cargo run --bin kalamdb-server
```

Default endpoints:

- HTTP API: `http://127.0.0.1:8080`
- PostgreSQL extension gRPC target: configure the host and port you expose for KalamDB

If you need a containerized KalamDB server instead of a local Rust process, use one of the Docker options below.

## Install the extension into PostgreSQL for testing and development

Once the extension artifacts are installed into the target PostgreSQL instance, connect as a superuser and run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_kalam;
```

Then create the remote server definition:

```sql
CREATE SERVER kalam_server
  FOREIGN DATA WRAPPER pg_kalam
  OPTIONS (
    host '127.0.0.1',
    port '9188',
    auth_header 'Bearer <your-kalamdb-token-or-shared-secret>'
  );
```

Verify the extension loaded correctly:

```sql
SELECT kalam_version(), kalam_compiled_mode();
```

If you need to reinstall during development:

```sql
DROP EXTENSION IF EXISTS pg_kalam CASCADE;
CREATE EXTENSION pg_kalam;
```

## Compile the extension using Docker

Use this flow when:

- PostgreSQL runs in Docker
- you are on macOS but need Linux extension artifacts
- you want a repeatable Linux build for testing or packaging

### Fast iterative Docker build

From the repository root:

```bash
./pg/docker/build-fast.sh
```

Useful variants:

```bash
./pg/docker/build-fast.sh --compile
./pg/docker/build-fast.sh --runtime
./pg/docker/build-fast.sh --rebuild-base
PG_MAJOR=17 PG_EXTENSION_FLAVOR=pg17 ./pg/docker/build-fast.sh --compile
RUST_BASE_IMAGE=public.ecr.aws/docker/library/rust:1.92-bookworm \
POSTGRES_BASE_IMAGE=public.ecr.aws/docker/library/postgres:17-bookworm \
PG_MAJOR=17 PG_EXTENSION_FLAVOR=pg17 \
./pg/docker/build-fast.sh
```

What this does:

- builds the extension inside a Linux builder container
- caches Cargo registry, git, and target directories in Docker volumes
- writes installable artifacts to `pg/docker/artifacts/`
- builds the `pg-kalam:latest` runtime image

### Clean Docker build

If you want a clean multi-stage build without the cached builder flow:

```bash
./pg/docker/build.sh
```

Equivalent raw Docker command:

```bash
docker build \
  --build-arg PG_MAJOR=17 \
  --build-arg PG_EXTENSION_FLAVOR=pg17 \
  --build-arg PGRX_VERSION=0.17.0 \
  --build-arg RUST_BASE_IMAGE=public.ecr.aws/docker/library/rust:1.92-bookworm \
  --build-arg POSTGRES_BASE_IMAGE=public.ecr.aws/docker/library/postgres:17-bookworm \
  -f pg/docker/Dockerfile \
  -t pg-kalam:latest .
```

## Run PostgreSQL and KalamDB together with Docker Compose

After building the image, start the full stack:

```bash
cd pg/docker
docker compose up -d
```

This compose stack starts:

- `kalamdb` on host ports `8088` and `9188`
- `postgres` with the `pg_kalam` extension image on host port `5433`

On first startup, `pg/docker/init.sql` automatically runs:

- `CREATE EXTENSION IF NOT EXISTS pg_kalam;`
- `CREATE SERVER IF NOT EXISTS kalam_server ...`

Connect with:

```bash
psql "postgresql://kalamdb:kalamdb123@127.0.0.1:5433/kalamdb"
```

Then verify:

```sql
SELECT kalam_version(), kalam_compiled_mode();
\des+
```

## Production install

For production, treat the extension artifacts as platform-specific binaries.

Important constraints:

- the extension must match the PostgreSQL major version of the target server
- the extension must match the target OS and CPU architecture
- a `.so` built on macOS cannot be loaded into Linux PostgreSQL

### Recommended production path

Build Linux artifacts in Docker and install those into the target Linux PostgreSQL instance.

From the repository root:

```bash
PG_MAJOR=17 PG_EXTENSION_FLAVOR=pg17 ./pg/docker/build-fast.sh --compile
```

This produces:

- `pg/docker/artifacts/pg_kalam.so`
- `pg/docker/artifacts/pg_kalam.control`
- `pg/docker/artifacts/pg_kalam--*.sql`

Install them into the target PostgreSQL server:

```bash
install -m 755 pg/docker/artifacts/pg_kalam.so "$(pg_config --pkglibdir)/pg_kalam.so"
install -m 644 pg/docker/artifacts/pg_kalam.control "$(pg_config --sharedir)/extension/pg_kalam.control"
install -m 644 pg/docker/artifacts/pg_kalam--*.sql "$(pg_config --sharedir)/extension/"
```

Then restart PostgreSQL if required by your environment and run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_kalam;
```

### Production image option

If you want PostgreSQL with the extension already installed in a container, build and run the provided runtime image:

```bash
./pg/docker/build-fast.sh
docker run --name pg-kalam \
  -e POSTGRES_USER=kalamdb \
  -e POSTGRES_PASSWORD=kalamdb123 \
  -e POSTGRES_DB=kalamdb \
  -p 5433:5432 \
  -d pg-kalam:latest
```

Then connect and install the extension or let your own init SQL handle it.

## Run only the KalamDB server with Docker

If PostgreSQL is running elsewhere and you only need a KalamDB server for the extension to connect to, you can run the server container by itself.

### Build the KalamDB server image locally

From the repository root:

```bash
docker build -f docker/build/Dockerfile -t kalamdb:local .
```

### Run the server container

```bash
docker run -d \
  --name kalamdb \
  -p 8080:8080 \
  -p 9188:9188 \
  -e KALAMDB_SERVER_HOST=0.0.0.0 \
  -e KALAMDB_JWT_SECRET="replace-with-a-32-char-secret" \
  -e KALAMDB_ALLOW_REMOTE_SETUP=true \
  -e KALAMDB_SECURITY_TRUSTED_PROXY_RANGES="10.0.0.0/8,172.16.0.0/12,192.168.0.0/16" \
  -e KALAMDB_NODE_ID=1 \
  -e KALAMDB_CLUSTER_RPC_ADDR=0.0.0.0:9188 \
  -e KALAMDB_CLUSTER_API_ADDR=http://127.0.0.1:8080 \
  -v kalamdb_data:/data \
  kalamdb:local
```

Verify the container is healthy:

```bash
curl http://127.0.0.1:8080/v1/api/healthcheck
```

If PostgreSQL runs in Docker on the same host, use `host.docker.internal` as the FDW `host` value when the PostgreSQL container connects back to a KalamDB server running on macOS.

## Run only the PostgreSQL container with the extension image

If you already have a KalamDB server running elsewhere, you can run just the PostgreSQL container:

```bash
docker run --name pg-kalam \
  -e POSTGRES_USER=kalamdb \
  -e POSTGRES_PASSWORD=kalamdb123 \
  -e POSTGRES_DB=kalamdb \
  -p 5433:5432 \
  -d pg-kalam:latest
```

Then connect:

```bash
psql "postgresql://kalamdb:kalamdb123@127.0.0.1:5433/kalamdb"
```

And install the extension manually:

```sql
CREATE EXTENSION IF NOT EXISTS pg_kalam;

CREATE SERVER kalam_server
  FOREIGN DATA WRAPPER pg_kalam
  OPTIONS (
    host 'host.docker.internal',
    port '9188',
    auth_header 'Bearer <your-kalamdb-token-or-shared-secret>'
  );
```

## Troubleshooting

- `could not access file "$libdir/pg_kalam"`: the extension was not installed into the same PostgreSQL instance you are using
- `extension "pg_kalam" is not available`: the `.control` and SQL files are missing from PostgreSQL's extension directory
- `incompatible library`: the extension was built for the wrong OS, CPU architecture, or PostgreSQL major version
- `KalamDB server is not running or unreachable ...`: verify the KalamDB container or process is up and that the FDW `host` and `port` are correct
- gRPC connection problems in Docker: for a PostgreSQL container connecting to a host process on macOS, use `host.docker.internal` instead of `127.0.0.1`

## Useful files

- [docker/Dockerfile](docker/Dockerfile)
- [docker/build.sh](docker/build.sh)
- [docker/build-fast.sh](docker/build-fast.sh)
- [docker/docker-compose.yml](docker/docker-compose.yml)
- [docker/init.sql](docker/init.sql)
- [../docker/build/Dockerfile](../docker/build/Dockerfile)
- [../docker/run/single/docker-compose.yml](../docker/run/single/docker-compose.yml)
- [local_test.sql](local_test.sql)