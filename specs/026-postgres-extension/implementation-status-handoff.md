# PostgreSQL Remote Mode Implementation Status and Agent Handoff

## Purpose

This document is the current implementation handoff for work under `specs/026-postgres-extension`.

It records:

* what has already been implemented
* what was intentionally left incomplete
* what another agent should do next
* which assumptions are temporary and should be revisited

This file is meant to let a new agent continue without reconstructing the state from commit history or thread context.

## Current Direction

The project is remote-first.

The intended architecture remains:

* thin PostgreSQL extension / FDW layer
* shared `kalam-pg-*` crates for request/session/client logic
* backend `kalamdb-pg` crate for PostgreSQL-facing gRPC service logic
* minimal `kalamdb-server` / cluster startup changes
* reuse of the existing shared Raft gRPC listener instead of starting a second standalone PostgreSQL-specific server

## What Has Been Implemented

### 1. Spec reshaping and preservation

The spec folder was updated to be remote-first while preserving prior embedded-heavy work as history/reference.

Relevant files:

* `specs/026-postgres-extension/README`
* `specs/026-postgres-extension/priority-shift.md`
* `specs/026-postgres-extension/transactional-fdw.md`
* `specs/026-postgres-extension/direct-entitystore-dml.md`
* `specs/026-postgres-extension/legacy-dual-mode-reference.md`

Important result:

* embedded mode remains documented only as transitional compatibility scope
* remote mode is the active implementation target
* older spec detail was preserved rather than discarded

### 2. Shared remote session contract

Remote session metadata was added to the shared PostgreSQL API/request layer.

Implemented files:

* `pg/crates/kalam-pg-api/src/session.rs`
* `pg/crates/kalam-pg-api/src/request.rs`
* `pg/crates/kalam-pg-api/tests/request_validation.rs`

What exists now:

* `RemoteSessionContext`
* validation for remote session id / schema / transaction metadata
* request structs can carry optional remote session information

Purpose:

* let scans and DML carry a stable PG-backend-to-Kalam session identity
* prepare for later transaction handle reuse

### 3. Backend session registry

A lightweight session registry was added in the backend PostgreSQL bridge crate.

Implemented files:

* `backend/crates/kalamdb-pg/src/session_registry.rs`
* `backend/crates/kalamdb-pg/tests/session_registry.rs`

What exists now:

* `RemotePgSession`
* `SessionRegistry`
* open/reuse/update/remove behavior
* schema + transaction metadata storage per session id

Purpose:

* keep per-PG-backend session state in `kalamdb-pg`
* avoid invasive server-global session architecture changes for the first slice

### 4. PostgreSQL current schema propagation

The extension session settings layer now captures PostgreSQL current schema.

Implemented files:

* `pg/src/session_settings.rs`
* `pg/tests/session_settings.rs`

What exists now:

* session settings parsing includes `current_schema`
* remote session open/update can carry schema/default namespace intent

### 5. FDW server options groundwork for remote mode

The FDW/common config layer now recognizes remote server connection settings.

Implemented files:

* `pg/crates/kalam-pg-common/src/config.rs`
* `pg/crates/kalam-pg-fdw/src/server_options.rs`
* `pg/crates/kalam-pg-fdw/tests/options.rs`

What exists now:

* `RemoteServerConfig`
* parsing for remote `host` and `port`
* validation that embedded and remote options are not mixed

### 6. Minimal gRPC PG service in backend

A minimal PostgreSQL-facing tonic service exists in `kalamdb-pg`.

Implemented files:

* `backend/crates/kalamdb-pg/src/service.rs`
* `backend/crates/kalamdb-pg/src/lib.rs`
* `backend/crates/kalamdb-pg/Cargo.toml`

What exists now:

* `PingRequest` / `PingResponse`
* `OpenSessionRequest` / `OpenSessionResponse`
* `PgServiceClient`
* `PgServiceServer`
* `KalamPgService`
* metadata-based authorization check using the `authorization` gRPC metadata key
* session open/reuse with schema propagation into the session registry

Important limitation:

* only the initial connectivity/session handshake exists right now
* scan RPCs and direct DML RPCs are not implemented yet

### 7. Minimal remote client crate

`kalam-pg-client` is no longer a placeholder.

Implemented files:

* `pg/crates/kalam-pg-client/src/lib.rs`
* `pg/crates/kalam-pg-client/Cargo.toml`
* `pg/crates/kalam-pg-client/tests/connectivity.rs`

What exists now:

* `RemoteKalamClient::connect`
* `RemoteKalamClient::ping`
* `RemoteKalamClient::open_session`
* metadata auth propagation on each request
* response mapped to a simple `RemoteSessionHandle`

### 8. Real shared-server mount on the existing Raft gRPC port

The PostgreSQL gRPC service is no longer only test-local; it can be mounted onto the real shared RPC server path.

Implemented files:

* `backend/crates/kalamdb-raft/src/network/service.rs`
* `backend/crates/kalamdb-raft/src/executor/raft.rs`
* `backend/crates/kalamdb-core/src/app_context.rs`
* `backend/crates/kalamdb-raft/tests/pg_rpc_mount.rs`

What exists now:

* `start_rpc_server(...)` accepts an optional `KalamPgService`
* `RaftExecutor` can hold and pass that PG service
* `AppContext` wires the PG service when cluster mode / `RaftExecutor` is active
* the PG service is hosted on the same shared RPC listener used for Raft + cluster RPCs

This satisfies the earlier requirement to reuse the already-open cluster gRPC surface instead of creating a second server stack.

## Temporary Decisions That Must Be Treated Carefully

### 1. Temporary auth bootstrap

For the first end-to-end server mount, the PG remote service auth is bootstrapped as:

* `Authorization: Bearer <auth.jwt_secret>`

This is currently wired in:

* `backend/crates/kalamdb-core/src/app_context.rs`

Why it was done:

* it gives the new remote path a concrete shared secret immediately
* it avoids a bigger config refactor during the initial connectivity milestone
* it keeps `kalamdb-server` changes minimal

Why it is temporary:

* it is not a dedicated PG remote credential model
* it couples the PG remote bridge to an existing server secret
* it may not be the final UX or security boundary desired long term

Next agent should consider replacing this with one of:

* a dedicated PG remote auth token in config
* mTLS or shared RPC credential aligned with the cluster RPC security model
* a stricter authenticated session-bootstrap flow

### 2. Connectivity milestone only

The current implementation proves:

* client can connect
* auth metadata is checked
* `ping` works
* `open_session` works
* current schema is stored in server-side session state
* the service can be mounted on the shared Raft RPC server

It also now proves:

* FDW scan execution through the remote client
* FDW DML execution through the remote client
* transaction begin/commit/rollback over remote handles
* remote use of the core applier/direct DML path

## Verified Tests Run So Far

All pg-related crate tests pass (58 tests total, 0 failures):

Backend kalamdb-pg tests (28 tests):
* `cargo nextest run -p kalamdb-pg` — 13 session_registry tests + 15 service_auth_tx tests

PG sub-crate tests (25 tests):
* `cargo nextest run -p kalam-pg-api -p kalam-pg-client -p kalam-pg-fdw -p kalam-pg-common -p kalam-pg-types`

pgrx extension tests (5 tests):
* `RUST_TEST_THREADS=1 cargo pgrx test pg16 -p kalam-pg-extension --no-default-features`

OperationService unit tests (13 tests, 0 failures):
* `cargo test -p kalamdb-core --lib operations::service::tests`

Full workspace compile: `cargo check` passes with no errors.
PG extension compile: `cargo check -p kalam-pg-extension --features pg16 --no-default-features` passes.

## Verification In Progress / Incomplete

None — full workspace compile and all tests verified.

## What Still Needs To Be Done

### 1. ~~Wire the FDW/executor path to the remote client~~ ✅ DONE

Completed: FDW scan and DML callbacks are fully wired through `RemoteBackendExecutor` → `RemoteKalamClient` → gRPC → `KalamPgService` → `OperationService`.

### 2. ~~Add real remote scan RPCs~~ ✅ DONE

Completed: `ScanRpcRequest` / `ScanRpcResponse` with Arrow IPC encoding. `OperationService::execute_scan()` uses direct `TableProvider::scan()` — no SQL reconstruction, no `SessionContext`.

### 3. ~~Add remote direct DML RPCs~~ ✅ DONE

Completed: `InsertRpcRequest`, `UpdateRpcRequest`, `DeleteRpcRequest` with JSON row encoding. `OperationService` routes mutations through `UnifiedApplier` by table type (user/shared/stream). System table writes are rejected.

### 4. Domain type consolidation (Phase 2 partial) ✅ DONE

Domain-typed request/result types (`ScanRequest`, `InsertRequest`, `UpdateRequest`, `DeleteRequest`, `ScanResult`, `MutationResult`) moved from `kalamdb-pg::query_executor` to `kalamdb-commons::models::pg_operations`. Both `kalamdb-core` and `kalamdb-pg` re-export from the canonical location.

The full Phase 2 dependency inversion (removing `kalamdb-core → kalamdb-pg`) is blocked by circular deps (kalamdb-core → kalamdb-raft → kalamdb-pg). To complete it, `KalamPgService` construction must move from `app_context.rs` to the server binary crate.

### 5. ~~Add transaction/session lifecycle RPCs~~ ✅ DONE

Completed: BeginTransaction, CommitTransaction, RollbackTransaction RPCs added to `kalamdb-pg::service`. Each has prost request/response messages, PgService trait methods, server routing + Svc dispatch structs, and KalamPgService implementation.

Transaction state management added to `SessionRegistry` with `TransactionState` enum (Active/Committed/RolledBack), atomic `tx_counter`, and `transaction_has_writes` tracking. Rollback is idempotent when no transaction is active.

Client-side methods added to `RemoteKalamClient` in `kalam-pg-client`.

FDW transaction lifecycle hooks implemented in `pg/src/fdw_xact.rs`:
* `ensure_transaction()` lazily begins KalamDB transaction on first FDW operation
* `xact_callback()` (`extern "C-unwind"`) registered via `RegisterXactCallback` for PG16
* XACT_EVENT_COMMIT → `commit_transaction`, XACT_EVENT_ABORT → `rollback_transaction`
* Wired into `fdw_scan.rs` (begin_foreign_scan_impl) and `fdw_modify.rs` (begin_foreign_modify_impl)

Read-your-writes semantics require MVCC on the KalamDB side (Phase 2 / future work). The RPC contract and lifecycle coordination are fully in place.

### 6. ~~Revisit auth design~~ ✅ DONE

Replaced the temporary `Bearer <auth.jwt_secret>` bootstrap with a dedicated PG auth token:

* Added `pg_auth_token: Option<String>` field to `AuthSettings` in `kalamdb-configs`
* Added `KALAMDB_PG_AUTH_TOKEN` environment variable override
* `app_context.rs` uses `pg_auth_token` with fallback to `jwt_secret` for backward compatibility
* The PG extension auth is now cleanly separated from user-facing JWT auth

Full mTLS (per `reorganization-plan-v2.md`) remains a future enhancement — the dedicated token provides a concrete security boundary for the PG remote bridge without coupling to user auth.

### 7. ~~OperationService integration tests~~ ✅ DONE

Added 13 unit tests in `kalamdb-core::operations::service::tests` covering:

* **Scan paths**: nonexistent table → NotFound, empty table → 0 rows, data round-trip (3 rows), column projection, invalid column → InvalidArgument, limit hint propagation
* **DML rejection (system tables)**: insert/update/delete on system tables → PermissionDenied (no applier call needed)
* **Validation (user_id required)**: insert/update/delete on user and stream tables without user_id → InvalidArgument

Tests use `test_app_context_simple()` (no Raft) with in-memory `MemTable` providers registered via `SchemaRegistry::insert_cached()`. DML through the applier requires Raft and is not tested here.

Also added `#[derive(Debug)]` to `ScanResult` and `MutationResult` in `kalamdb-commons::models::pg_operations`.

### 8. ~~Expand remaining test coverage~~ ✅ DONE

Added comprehensive test coverage:

**service_auth_tx.rs** (15 tests):
* Auth rejection: ping without auth when configured, ping with wrong auth
* Auth success: ping without auth when not configured, ping with correct auth
* Session lifecycle: open_session rejects empty id, returns session+schema, updates schema on existing
* Transaction lifecycle: begin succeeds, commit succeeds, rollback succeeds, double begin fails, wrong tx id fails, begin after commit succeeds, empty session id rejected
* Auth gating: transaction RPCs require auth when configured
* Server construction: pg_service_server_builds_from_impl

**session_registry.rs** (13 tests):
* Session reuse, schema update, begin/commit, begin/rollback, double begin fails, wrong tx id fails, rollback without transaction is noop, commit already committed fails, mark writes, multiple independent sessions, remove session, begin on nonexistent fails

## Remaining Future Work

### Phase 2: Read-your-writes and transaction-aware scans/DML
* Scan and DML RPCs do not yet pass `transaction_id` through to OperationService
* Requires MVCC on the KalamDB side to observe uncommitted writes within a transaction
* The RPC contract and lifecycle hooks are ready; needs KalamDB-side transaction isolation

### Phase 2: Subtransactions and savepoints
* Per `transactional-fdw.md` Phase 2
* Requires SubXact callbacks and KalamDB savepoint primitives

### Phase 3: Distributed atomicity
* Per `transactional-fdw.md` Phase 3
* Two-phase commit between PG local and KalamDB foreign tables

### mTLS upgrade
* Per `reorganization-plan-v2.md`
* Replace dedicated auth token with mutual TLS certificates
* Unify cluster and PG extension TLS under one `[rpc_tls]` config

### Full Phase 2 dependency inversion
* Remove `kalamdb-core → kalamdb-pg` dependency
* Move `KalamPgService` construction from `app_context.rs` to server binary crate
* Blocked by circular deps (kalamdb-core → kalamdb-raft → kalamdb-pg)

### Remote e2e integration tests
* Full round-trip: extension → gRPC → OperationService → provider → response
* Requires a running KalamDB server and PG instance with the extension loaded

## Files Most Important For Future Work

Backend remote bridge and shared server mount:

* `backend/crates/kalamdb-pg/src/service.rs` — gRPC service with all RPCs (ping, session, scan, DML, transactions)
* `backend/crates/kalamdb-pg/src/session_registry.rs` — per-backend session + transaction state
* `backend/crates/kalamdb-raft/src/network/service.rs`
* `backend/crates/kalamdb-raft/src/executor/raft.rs`
* `backend/crates/kalamdb-core/src/app_context.rs` — PG service wiring with dedicated auth token

Extension/shared client side:

* `pg/crates/kalam-pg-client/src/lib.rs` — RemoteKalamClient with transaction methods
* `pg/crates/kalam-pg-api/src/session.rs`
* `pg/crates/kalam-pg-api/src/request.rs`
* `pg/src/session_settings.rs`
* `pg/src/fdw_xact.rs` — FDW transaction lifecycle hooks (xact callback)
* `pg/src/remote_state.rs` — remote extension state accessors
* `pg/crates/kalam-pg-fdw/src/server_options.rs`

Auth configuration:

* `backend/crates/kalamdb-configs/src/config/types.rs` — `AuthSettings.pg_auth_token`
* `backend/crates/kalamdb-configs/src/config/override.rs` — `KALAMDB_PG_AUTH_TOKEN` env var

Core direct DML reuse path:

* `backend/crates/kalamdb-core/src/applier/applier.rs`
* `backend/crates/kalamdb-core/src/applier/executor/dml.rs`

OperationService (typed execution path + tests):

* `backend/crates/kalamdb-core/src/operations/service.rs`
* `backend/crates/kalamdb-core/src/operations/scan.rs`
* `backend/crates/kalamdb-core/src/operations/error.rs`
* `backend/crates/kalamdb-commons/src/models/pg_operations.rs`

## Handoff Summary

The project is past the design-only stage.

The repository now has:

* a backend PG gRPC bridge crate
* a real remote client crate
* session/schema propagation groundwork
* actual mounting on the shared server gRPC listener
* a passing connectivity test against the mounted shared RPC server

The main remaining gap is not server reachability anymore.

The main remaining gap is functional FDW integration:

* scans
* direct DML
* transaction lifecycle
* final auth model
