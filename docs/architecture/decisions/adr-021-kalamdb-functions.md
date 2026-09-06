# ADR-021: kalamdb-functions Crate and Activation Ownership

**Status**: Accepted  
**Date**: 2026-09-05  
**Related**: docs/plans/2026-09-01-kalamdb-0.7.md, docs/plans/functions-v1-implementation.md, docs/plans/2026-02-14-flatbuffers-flexbuffers-vortex-migration-plan.md

## Context

Functions V1 needs a runtime (V8 adapter, revision cache, sandbox ABI) plus SQL contracts (`CREATE TYPE`, `CREATE PROCEDURE`, nested `CALL`). Persistence of nested `STRUCT`/`List` and catalog rows is already owned by the 0.7 serialization track. Putting codecs or scalar indexes inside a functions crate would fork those tracks.

## Decision

Add `backend/crates/kalamdb-functions` when Task 5 (runtime/V8) starts. Wave 1 freezes dialect/AST/IDs only; the crate is not required until the runtime ABI exists.

Ownership:

| Crate | Owns | Must not own |
| --- | --- | --- |
| `kalamdb-dialect` | `CREATE TYPE` / `ALTER TYPE` / `CREATE PROCEDURE` / `CREATE SCHEMA` / `SET search_path` ASTs, classification, later contract compiler | Storage bytes, V8 |
| `kalamdb-commons` | `TypeId`, `RoutineId`, contract value/type models | FlatBuffers/FlexBuffers, `encode`/`decode` on models |
| `kalamdb-serialization` | Persisted nested `STRUCT`/`List` and catalog object bytes | SQL parsing, V8 |
| `kalamdb-system` | `system.types` / `system.routines` providers | A functions-only serializer |
| `kalamdb-functions` | Runtime ABI, V8 adapter, revision cache, sandbox host | Persistence codec, scalar index implementation |
| CLI / `kalam-schema-diff` | Local compile, generate, deploy | Live-server-only contract discovery |

Activation (implemented in Functions Task 6, recorded here so Wave 1 does not invent a second protocol):

1. Local generate compiles SQL to a `ContractSnapshot` without a running server.
2. `kalam deploy` uploads hashed artifacts to filestore.
3. Activation points `system.function_revisions` at an immutable artifact id.
4. Runtime loads the active revision; nested in-process calls pass Arrow values, not bytes.

`UNION` / `INTERFACE` stay reserved errors in V1. Schedules / extra runtimes are out of 0.7.

## Runtime spike (Task 5 / Checkpoint B)

V1 executes **TypeScript bundled to JavaScript** in a sandboxed isolate. WASM is a reserved `FunctionRuntime` and is not loaded.

- Crate: workspace-pinned [`v8`](https://crates.io/crates/v8) (denoland rusty_v8) **150.3.0**; Cargo.lock resolved **150.4.0**.
- ABI: `ABI_VERSION = 1`. Host values cross as Arrow/`ScalarValue` (scalar, STRUCT, List, JSONB) without JSON stringify.
- Artifacts: `{storage}/functions/artifacts/{artifact_id}/module.js` (SHA-256 content address). Activation CAS-swaps `system.function_modules.active_revision_id` after writing artifact + revision rows. Interruption before the pointer swap leaves the previous revision active.
- Spike timings (dev profile, `echo` fixture, 2026-09-05): **cold_start = 0.0012s**, **warm_invoke = 0.0053s**.
- Limits: timeout watchdog, cancellation token, near-heap-limit callback mapped to `MemoryLimit`.

`kalamdb-server` depends on `kalamdb-functions` as of Task 7 (`CALL` / REST / PGWire). Host callbacks live in `kalamdb-core`.

## Consequences

- Nested procedure types persist only through `kalamdb-serialization`.
- Functions catalog rows use `encode_object`; they do not extend `kalamdb-commons` codecs.
- `kalamdb-functions` exists as of Task 5. `kalamdb-server` may depend on it as of Task 7.
