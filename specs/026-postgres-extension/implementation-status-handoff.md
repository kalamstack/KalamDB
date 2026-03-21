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

* `kalam-pg/crates/kalam-pg-api/src/session.rs`
* `kalam-pg/crates/kalam-pg-api/src/request.rs`
* `kalam-pg/crates/kalam-pg-api/tests/request_validation.rs`

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

* `kalam-pg/crates/kalam-pg-extension/src/session_settings.rs`
* `kalam-pg/crates/kalam-pg-extension/tests/session_settings.rs`

What exists now:

* session settings parsing includes `current_schema`
* remote session open/update can carry schema/default namespace intent

### 5. FDW server options groundwork for remote mode

The FDW/common config layer now recognizes remote server connection settings.

Implemented files:

* `kalam-pg/crates/kalam-pg-common/src/config.rs`
* `kalam-pg/crates/kalam-pg-fdw/src/server_options.rs`
* `kalam-pg/crates/kalam-pg-fdw/tests/options.rs`

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

* `kalam-pg/crates/kalam-pg-client/src/lib.rs`
* `kalam-pg/crates/kalam-pg-client/Cargo.toml`
* `kalam-pg/crates/kalam-pg-client/tests/connectivity.rs`

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

It does **not** yet prove:

* FDW scan execution through the remote client
* FDW DML execution through the remote client
* transaction begin/commit/rollback over remote handles
* remote use of the core applier/direct DML path

## Verified Tests Run So Far

The following tests were run and passed during this work:

* `cargo test -p kalam-pg-api --test request_validation -- --nocapture`
* `cargo test -p kalamdb-pg --test session_registry -- --nocapture`
* `cargo test -p kalam-pg-extension --test session_settings -- --nocapture`
* `cargo test -p kalam-pg-fdw --test request_planner -- --nocapture`
* `cargo test -p kalam-pg-fdw --test options -- --nocapture`
* `cargo test -p kalam-pg-client --test connectivity -- --nocapture`
* `cargo test -p kalamdb-raft --test pg_rpc_mount -- --nocapture`
* `cargo nextest run -p kalamdb-pg -p kalam-pg-client`

## Verification In Progress / Incomplete

A broader batched compile check was started for:

* `cargo check -p kalamdb-core -p kalamdb-raft -p kalamdb-pg -p kalam-pg-client`

Observed status before handoff:

* compilation reached `Checking kalamdb-core`
* no final success/failure result was captured in the handoff thread before control was transferred

Another agent should rerun this cleanly and capture the final result.

## What Still Needs To Be Done

### 1. Wire the FDW/executor path to the remote client

This is the most important remaining implementation gap.

The FDW/common extension side still needs to:

* instantiate `RemoteKalamClient` from parsed remote server options
* use the mounted shared RPC server instead of a placeholder path
* perform session open/reuse from the real executor path

Likely files to inspect next:

* `kalam-pg/crates/kalam-pg-extension/src/executor_factory.rs`
* `kalam-pg/crates/kalam-pg-fdw/src/*`
* any mode-selection / execution wiring in `kalam-pg-extension`

### 2. Add real remote scan RPCs

Needed next:

* define typed scan request/response RPCs
* map FDW pushdown-safe scan state into the remote client
* execute through KalamDB-side typed scan services

Requirements:

* do not fall back to generic SQL forwarding for standard scans
* keep request/response typed
* preserve schema/session context

### 3. Add remote direct DML RPCs

Needed next:

* define typed insert/update/delete RPCs
* route them through backend direct DML handling
* reuse existing applier/direct DML path where practical

Important constraint:

* do not scatter DML behavior into many unrelated modules
* keep the direct DML path centralized and careful
* avoid new SQL rewrite / reparse passes in hot paths

Likely backend paths to inspect:

* `backend/crates/kalamdb-core/src/applier/applier.rs`
* `backend/crates/kalamdb-core/src/applier/executor/dml.rs`
* any typed mutation handler layers used by the applier

### 4. Add transaction/session lifecycle RPCs

The current session work is only the foundation.

Still needed:

* begin transaction
* commit transaction
* rollback transaction
* transaction handle attached to the existing remote session
* read-your-writes semantics through that handle

Relevant spec:

* `specs/026-postgres-extension/transactional-fdw.md`

### 5. Revisit auth design

The temporary bootstrap credential should be replaced or confirmed.

Another agent should decide whether to keep:

* temporary `Bearer <auth.jwt_secret>`

or move to:

* dedicated config token
* better shared-RPC auth model
* stronger separation between user-auth JWT and PG remote bridge auth

### 6. Expand test coverage

Tests still needed:

* FDW-side executor integration tests using `RemoteKalamClient`
* remote scan success path
* remote DML success path
* auth rejection path for the PG remote service
* session reuse across multiple requests
* schema update on an already-open session
* transaction lifecycle tests when transaction RPCs exist

## Recommended Next Execution Order

Another agent should continue in this order:

1. rerun the broad compile verification and cleanly capture the result
2. wire the extension executor path to create/use `RemoteKalamClient`
3. implement remote scan RPCs
4. implement remote direct DML RPCs via the core direct DML path
5. add auth-failure and session-reuse tests
6. add transaction lifecycle RPCs and tests
7. replace or formalize the temporary auth bootstrap

## Files Most Important For The Next Agent

Backend remote bridge and shared server mount:

* `backend/crates/kalamdb-pg/src/service.rs`
* `backend/crates/kalamdb-pg/src/session_registry.rs`
* `backend/crates/kalamdb-raft/src/network/service.rs`
* `backend/crates/kalamdb-raft/src/executor/raft.rs`
* `backend/crates/kalamdb-core/src/app_context.rs`

Extension/shared client side:

* `kalam-pg/crates/kalam-pg-client/src/lib.rs`
* `kalam-pg/crates/kalam-pg-api/src/session.rs`
* `kalam-pg/crates/kalam-pg-api/src/request.rs`
* `kalam-pg/crates/kalam-pg-extension/src/session_settings.rs`
* `kalam-pg/crates/kalam-pg-fdw/src/server_options.rs`

Core direct DML reuse path:

* `backend/crates/kalamdb-core/src/applier/applier.rs`
* `backend/crates/kalamdb-core/src/applier/executor/dml.rs`

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
