# kalam-link — Developer Notes

This document is for contributors and developers working on the `kalam-link` TypeScript/JavaScript SDK itself.
For usage documentation, see [README.md](README.md).

## Architecture

The TypeScript SDK is a thin JavaScript wrapper over a Rust core compiled to WebAssembly (WASM).

```
Application (TypeScript / JavaScript)
  └─ kalam-link (this package)           ← high-level API, auth, reconnect logic
      └─ dist/wasm/                       ← bundled WASM + JS bindings (auto-generated)
          └─ link/src/ (Rust)             ← core kalam-link library
              └─ link/src/wasm/           ← WASM entry points (wasm-pack)
```

`createClient(...)` auto-loads the bundled WASM bytes and applies runtime shims for both
Node.js and browser environments — no manual WASM bootstrap required for normal use.

## Prerequisites

| Tool | Version |
|------|---------|
| Node.js | >= 18 |
| npm | >= 9 |
| Rust toolchain | stable (via `rustup`) |
| `wasm-pack` | latest |
| `wasm-opt` (Binaryen) | recommended |

Install `wasm-pack`:

```bash
cargo install wasm-pack
```

Install `wasm-opt` on macOS:

```bash
brew install binaryen
```

## Build From Source

```bash
cd link/sdks/typescript
npm install
npm run build
```

The full `build` script runs these steps in order:

| Step | Command | Description |
|------|---------|-------------|
| 1 | `build:wasm` | Compile Rust → WASM with `wasm-pack --profile release-dist`, then run `wasm-opt -Oz --all-features` when available |
| 2 | `build:fix-types` | Patch `JsonValue` type into generated `.d.ts` |
| 3 | `build:ts` | Compile TypeScript → `dist/` |
| 4 | `build:copy-wasm` | Copy WASM artifacts into `dist/wasm/` |

Intermediate WASM output lands in `wasm/` (gitignored from publish via `files`). Compiled output is in `dist/`.

## Running Tests

### Unit / offline tests

Does not require a running server:

```bash
npm test
```

Runs a full local build first, then executes the offline Node test suite with
`NO_SERVER=true`:

- `tests/basic.test.mjs`
- `tests/normalize.test.mjs`
- `tests/auth-provider-retry.test.mjs`
- `tests/agent-runtime.test.mjs`
- `tests/cell-value.test.mjs`
- `tests/single-socket-subscriptions.test.mjs`
- `tests/readme-examples.test.mjs`
- `tests/sdk-runtime-coverage.test.mjs`

### Agent runtime tests only

```bash
npm run test:agent-runtime
```

### Full SDK test run

Requires a running KalamDB instance. This is the same path used by the release
workflow:

```bash
./test.sh
```

`test.sh` does three things in order:

1. Builds the SDK.
2. Runs the offline suite above.
3. Runs the live e2e suite serially with `node --test --test-concurrency=1`.

The e2e files are:

- `tests/e2e/auth/auth.test.mjs`
- `tests/e2e/query/query.test.mjs`
- `tests/e2e/query/dml-helpers.test.mjs`
- `tests/e2e/ddl/ddl.test.mjs`
- `tests/e2e/lifecycle/lifecycle.test.mjs`
- `tests/e2e/subscription/subscription.test.mjs`
- `tests/e2e/reconnect/reconnect.test.mjs`

Serial execution is intentional in CI because the suite creates many eager
WebSocket clients; running the files one-by-one avoids spurious auth rate-limit
failures on shared runners.

### Running individual live test files

Requires a running KalamDB instance. Set connection env vars before running:

```bash
KALAMDB_URL=http://localhost:8080 \
KALAMDB_USER=admin \
KALAMDB_PASSWORD=kalamdb123 \
node --test --test-concurrency=1 tests/e2e/reconnect/reconnect.test.mjs
```

## Low-Level WASM Entrypoint

For cases where you need direct access to the raw WASM API (e.g. custom timestamp formatting,
low-level client construction):

```ts
import init, {
  KalamClient,
  WasmTimestampFormatter,
  parseIso8601,
  timestampNow,
  initSync,
} from 'kalam-link/wasm';
```

Most applications should use the high-level `kalam-link` exports instead.

## Publishing

Update `version` in `package.json`, add a `CHANGELOG` entry, then:

```bash
cd link/sdks/typescript
npm run build
npm publish
```

For a dry run:

```bash
npm publish --dry-run
```

## Crate Layout

| Path | Purpose |
|------|---------|
| `link/src/` | Core Rust client library (HTTP, WebSocket, auth, reconnect) |
| `link/src/wasm/` | WASM entry points (`#[wasm_bindgen]` annotations) |
| `link/sdks/typescript/src/` | TypeScript API layer (`client.ts`, `auth.ts`, etc.) |
| `link/sdks/typescript/dist/` | Compiled output (gitignored) |
| `link/sdks/typescript/wasm/` | wasm-pack output |
