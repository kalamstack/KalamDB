# KalamDB Contribution Guide

This file is the quickest way to get a local KalamDB development environment into a working state.
It focuses on the commands contributors actually need for the server, CLI, PostgreSQL extension, Admin UI, npm projects, and Dart SDK.

## Prerequisites

- Rust stable with the workspace toolchain requirements from [AGENTS.md](./AGENTS.md)
- `cargo-nextest` for Rust test runs
- Node.js 18+ and npm 9+ for the UI, TypeScript SDKs, and npm examples
- Flutter 3.19+ and Dart 3.3+ for the Dart SDK
- LLVM/Clang and a working C/C++ toolchain for RocksDB and native builds

Install the common Rust helpers once:

```bash
cargo install cargo-nextest --locked
cargo install wasm-pack
```

`wasm-opt` from Binaryen is optional but recommended for smaller WASM bundles:

```bash
brew install binaryen
```

## 1. Compile and Run the Backend Server

The backend server lives in `backend/`.

Use `cargo run` when you want both compile and run in one step:

```bash
cd backend
cp server.example.toml server.toml
cargo run
```

Use `cargo build` when you only want to compile:

```bash
cd backend
cargo build
```

Useful variants:

```bash
cd backend
cargo run --bin kalamdb-server

KALAMDB_SERVER_PORT=9000 \
KALAMDB_LOG_LEVEL=debug \
cargo run
```

Default local server URL:

```text
http://127.0.0.1:8080
```

## 2. Make Sure CLI Tests Pass

There is no top-level `cli/tests.sh` in this repo. The supported test wrapper is `cli/run-tests.sh`.

Start the backend first in a separate shell:

```bash
cd backend
cargo run
```

Then run the CLI test wrapper:

```bash
cd cli
./run-tests.sh
```

Common CLI test runs:

```bash
cd cli

# Smoke tests only
./run-tests.sh --test smoke --nocapture

# Full CLI package nextest run with e2e features
cargo nextest run --features e2e-tests

# Point the CLI suite at a custom running server
KALAMDB_SERVER_URL=http://127.0.0.1:8080 \
KALAMDB_ROOT_PASSWORD=kalamdb123 \
./run-tests.sh --test smoke
```

Notes:

- `cli/run-tests.sh` loads `cli/.env` automatically if it exists.
- The wrapper is the best entrypoint when you want the repo's expected environment loading and server autodetection behavior.
- For cluster-only validation, use `./run-cluster-tests.sh` from `cli/`.

## 3. Compile and Test the PostgreSQL Extension

The PostgreSQL extension lives in `pg/` and is built with `pgrx`.

### Local native build and install

Install `cargo-pgrx` once:

```bash
cargo install cargo-pgrx --version "=0.18.0" --locked
```

Initialize `pgrx` for your local PostgreSQL toolchain:

```bash
PG_MAJOR=16
PG_CONFIG="$(command -v pg_config)"

cargo pgrx init "--pg${PG_MAJOR}=${PG_CONFIG}"
```

Compile and install the extension into that PostgreSQL instance:

```bash
PG_MAJOR=16
PG_FEATURE="pg${PG_MAJOR}"
PG_CONFIG="$(command -v pg_config)"

cargo pgrx install \
  --manifest-path pg/Cargo.toml \
  -p kalam-pg-extension \
  -c "${PG_CONFIG}" \
  --no-default-features \
  --profile release-pg \
  -F "${PG_FEATURE}"
```

After code changes, rerun the same `cargo pgrx install` command.

### Run PostgreSQL extension tests

`pg/test.sh` expects:

- a running KalamDB server
- local `pgrx` PostgreSQL available
- `cargo-nextest` installed

Start KalamDB first:

```bash
cd backend
cargo run
```

Then run the PG test wrapper from the repo root:

```bash
./pg/test.sh
```

Useful variants:

```bash
# Skip the DDL slice and run the shorter DML/perf/scenario subset
./pg/test.sh --no-ddl

# Run a narrower nextest filter
./pg/test.sh --filter 'test(e2e_dml)'
```

### Docker build path for Linux artifacts or Dockerized PostgreSQL

If you want Linux artifacts or a Docker-oriented PG workflow, use one of these from the repo root:

```bash
./pg/docker/build-fast.sh
./pg/docker/build.sh
```

See `pg/README.md` for the full Docker workflow.

## 4. Compile the Admin UI

The Admin UI lives in `ui/`.

Build it with:

```bash
cd ui
npm install
npm run build
```

Important detail: `ui/package.json` defines a `prebuild` step, so `npm run build` also rebuilds the local TypeScript SDK and ORM package first.

Common UI commands:

```bash
cd ui

# Local development server
npm run dev

# Build for production
npm run build

# Full UI test suite
npm test

# Narrow auth and SQL Studio flow tests
npm run test:flows
```

## 5. Find and Compile the npm Projects

There is no single npm workspace command at the repo root. Build each npm project from its own directory.

Primary npm project locations:

| Path | Purpose | Build/Test Command |
|------|---------|--------------------|
| `ui/` | Admin UI | `npm install && npm run build` |
| `link/sdks/typescript/client/` | `@kalamdb/client` SDK | `npm install && npm run build` |
| `link/sdks/typescript/orm/` | `@kalamdb/orm` | `npm install && npm run build` |
| `link/sdks/typescript/consumer/` | `@kalamdb/consumer` | `npm install && npm run build` |
| `examples/simple-typescript/` | browser example | `npm install && npm run build` |
| `examples/chat-with-ai/` | chat example | `npm install && npm run build` |
| `examples/summarizer-agent/` | worker example | `npm install && npm test` |
| `link/sdks/typescript/client/example/` | SDK browser demo | `npm install && npm run serve` |

Recommended TypeScript SDK checks:

```bash
cd link/sdks/typescript/client
npm install
npm run build
./test.sh
```

Additional package-specific test commands:

```bash
cd link/sdks/typescript/orm
npm install
npm test

cd ../consumer
npm install
npm test
```

If you need to discover all npm package roots quickly:

```bash
find . -name package.json -not -path '*/node_modules/*'
```

## 6. Compile and Test the Dart SDK

The Dart and Flutter SDK lives in `link/sdks/dart/`.

Do not edit `link/sdks/dart/lib/src/generated/` by hand. Those files are generated.

### Build the SDK and native artifacts

From `link/sdks/dart/`:

```bash
./build.sh
```

That script does the following:

- runs `flutter pub get`
- optionally regenerates `flutter_rust_bridge` bindings
- builds native artifacts for the host-default platform set
- builds the host Rust bridge library in `link/kalam-link-dart`
- runs `flutter analyze`

Useful variants:

```bash
# Build all supported platforms if your machine has the toolchains
./build.sh all

# Build a specific set of platforms
./build.sh android ios web

# Force flutter_rust_bridge regeneration
FRB_GENERATE=always ./build.sh
```

### Run Dart SDK tests

The supported wrapper is:

```bash
cd link/sdks/dart
./test.sh
```

`test.sh` will:

- run `flutter pub get`
- build the host Rust bridge library with `cargo build --release`
- run `flutter analyze`
- run offline unit coverage with `flutter test test/models_test.dart`
- if KalamDB is reachable, run live integration tests with `flutter test test/e2e`

Start the backend first when you want the e2e part to run:

```bash
cd backend
cargo run

cd ../link/sdks/dart
./test.sh
```

Environment variables for live Dart SDK tests:

```bash
KALAMDB_URL=http://localhost:8080
KALAMDB_USER=admin
KALAMDB_PASSWORD=kalamdb123
KALAMDB_ROOT_PASSWORD=kalamdb123
```

## Recommended Contributor Validation Flow

For a change limited to one surface, validate only that surface first.

Examples:

- Backend-only change: `cd backend && cargo build`
- CLI behavior change: `cd cli && ./run-tests.sh --test smoke --nocapture`
- PG extension change: `./pg/test.sh`
- Admin UI change: `cd ui && npm run build && npm test`
- TypeScript SDK change: `cd link/sdks/typescript/client && ./test.sh`
- Dart SDK change: `cd link/sdks/dart && ./test.sh`

For cross-cutting changes, use the repo-wide sweep after starting the backend server:

```bash
./scripts/test-all.sh
```

## Documentation Expectations

- If you change architecture or execution flow, update the relevant docs in `docs/architecture/` or `docs/architecture/decisions/`.
- If you change anything under `link/sdks/**`, update the matching SDK docs in the `KalamSite` repo as well.
- Keep commands in this file aligned with the actual scripts in `backend/`, `cli/`, `pg/`, `ui/`, and `link/sdks/`.