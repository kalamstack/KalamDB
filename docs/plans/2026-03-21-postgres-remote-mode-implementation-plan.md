# PostgreSQL Remote Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the first remote PostgreSQL FDW execution path by introducing a dedicated backend `kalamdb-pg` crate, wiring it minimally into `kalamdb-server`, and connecting the `kalam-pg` extension to that remote path with explicit session/schema propagation.

**Architecture:** Keep `kalamdb-server` as a thin host. Put PostgreSQL-specific remote transport/server logic into `backend/crates/kalamdb-pg`, reusing existing `AppContext`, `ExecutionContext`, fast DML/provider paths, and tonic/prost infrastructure already present in the backend. Keep the PostgreSQL extension thin by routing all remote execution through `kalam-pg-client` and shared `kalam-pg-api` request/session types.

**Tech Stack:** Rust 2021, tonic/prost, pgrx, DataFusion, existing `AppContext`/`ExecutionContext`, `kalamdb-session`, `kalamdb-tables`, `kalamdb-raft` transport patterns.

---

### Task 1: Lock remote-mode contract in specs

**Files:**
- Modify: `specs/026-postgres-extension/README`
- Modify: `specs/026-postgres-extension/transactional-fdw.md`
- Modify: `specs/026-postgres-extension/direct-entitystore-dml.md`
- Modify: `specs/026-postgres-extension/priority-shift.md`

**Step 1: Confirm the spec now covers the new constraints**

Run: `sed -n '1,260p' specs/026-postgres-extension/README`
Expected: remote-mode constraints mention `kalamdb-pg`, minimal `kalamdb-server`, session/schema propagation, and centralized direct DML.

**Step 2: Verify the companion specs mention session-aware transactions and cautious DML centralization**

Run: `rg -n "kalamdb-pg|session|schema|centralized|lightweight" specs/026-postgres-extension`
Expected: matches in the updated remote-first spec docs.

### Task 2: Add remote-mode shared request/session fields in `kalam-pg-api`

**Files:**
- Modify: `pg/crates/kalam-pg-api/src/request.rs`
- Modify: `pg/crates/kalam-pg-api/src/session.rs`
- Modify: `pg/crates/kalam-pg-api/src/traits.rs`
- Test: `pg/crates/kalam-pg-api/tests/request_validation.rs`

**Step 1: Write failing tests for remote session metadata**

Add tests covering:
- session-aware request validation
- schema/default-namespace propagation fields
- conflicting tenant/session identity handling still working

**Step 2: Run the failing API tests**

Run: `cargo test -p kalam-pg-api request_validation -- --nocapture`
Expected: FAIL because remote session/schema fields do not exist or are not validated yet.

**Step 3: Add minimal request/session metadata**

Add fields for:
- remote session identifier
- current schema / namespace override
- optional transaction identifier

Keep the API transport-friendly and avoid introducing Postgres-specific heavy types.

**Step 4: Re-run the API tests**

Run: `cargo test -p kalam-pg-api request_validation -- --nocapture`
Expected: PASS.

### Task 3: Create backend `kalamdb-pg` crate skeleton

**Files:**
- Create: `backend/crates/kalamdb-pg/Cargo.toml`
- Create: `backend/crates/kalamdb-pg/src/lib.rs`
- Create: `backend/crates/kalamdb-pg/src/service.rs`
- Create: `backend/crates/kalamdb-pg/src/session_registry.rs`
- Create: `backend/crates/kalamdb-pg/src/proto.rs`
- Modify: `Cargo.toml`

**Step 1: Write a failing crate-level smoke test**

Create a test that expects:
- the new crate can build
- a session registry can open/reuse a session key
- the service type can be constructed from `Arc<AppContext>`

**Step 2: Run the failing test/build**

Run: `cargo test -p kalamdb-pg -- --nocapture`
Expected: FAIL because the crate does not exist yet.

**Step 3: Add the minimal new crate**

Implement:
- `Arc<AppContext>`-based service constructor
- in-memory session registry keyed by remote session id / backend identity
- placeholder service methods for open/update/close session and first request handling

Keep filesystem/storage logic out of this crate.

**Step 4: Re-run the new crate tests**

Run: `cargo test -p kalamdb-pg -- --nocapture`
Expected: PASS.

### Task 4: Add a real remote RPC surface for PG sessions and scans

**Files:**
- Modify: `backend/crates/kalamdb-pg/src/proto.rs`
- Modify: `backend/crates/kalamdb-pg/src/service.rs`
- Create: `backend/crates/kalamdb-pg/tests/service.rs`
- Modify: `backend/Cargo.toml`
- Modify: `backend/src/lifecycle.rs`

**Step 1: Write failing tests for the first remote service operations**

Cover:
- open session
- update current schema on session
- execute a scan using that session

Use the smallest vertical slice first, even if mutations are added in a later task.

**Step 2: Run the failing backend tests**

Run: `cargo test -p kalamdb-pg service -- --nocapture`
Expected: FAIL because the RPC/service methods are not implemented yet.

**Step 3: Implement the remote service**

Implement:
- tonic/prost message mapping in the new crate
- service methods that build `ExecutionContext` from `AuthSession` + current schema
- minimal `kalamdb-server` lifecycle wiring to register/start the service

Do not move business logic into `kalamdb-server`; keep it as startup wiring only.

**Step 4: Re-run the backend tests**

Run: `cargo test -p kalamdb-pg service -- --nocapture`
Expected: PASS.

### Task 5: Implement `kalam-pg-client` remote executor

**Files:**
- Modify: `pg/crates/kalam-pg-client/Cargo.toml`
- Modify: `pg/crates/kalam-pg-client/src/lib.rs`
- Create: `pg/crates/kalam-pg-client/src/client.rs`
- Create: `pg/crates/kalam-pg-client/src/executor.rs`
- Create: `pg/crates/kalam-pg-client/src/proto.rs`
- Test: `pg/crates/kalam-pg-client/tests/client.rs`

**Step 1: Write failing tests for remote client behavior**

Cover:
- open/reuse remote session
- propagate current schema/default namespace
- invoke scan through remote executor

**Step 2: Run the failing client tests**

Run: `cargo test -p kalam-pg-client client -- --nocapture`
Expected: FAIL because the crate is still a placeholder.

**Step 3: Implement the minimal remote executor**

Implement:
- tonic client wrapper
- session bootstrap/reuse
- scan execution
- error mapping into `KalamPgError`

Keep the extension unaware of tonic details.

**Step 4: Re-run the client tests**

Run: `cargo test -p kalam-pg-client client -- --nocapture`
Expected: PASS.

### Task 6: Wire remote executor into the PostgreSQL extension

**Files:**
- Modify: `pg/src/remote_executor.rs`
- Modify: `pg/src/lib.rs`
- Modify: `pg/src/session_settings.rs`
- Modify: `pg/crates/kalam-pg-fdw/src/server_options.rs`
- Modify: `pg/crates/kalam-pg-fdw/src/request_planner.rs`
- Test: `pg/tests/session_settings.rs`
- Test: `pg/crates/kalam-pg-fdw/tests/request_planner.rs`

**Step 1: Write failing tests for remote configuration/session propagation**

Cover:
- remote executor factory creation from FDW/server options
- current schema propagation
- request planner preserving tenant/session information for remote execution

**Step 2: Run the failing extension/fdw tests**

Run: `cargo test -p kalam-pg-fdw request_planner -- --nocapture`
Run: `cargo test -p kalam-pg-extension session_settings -- --nocapture`
Expected: FAIL because remote factory/schema propagation are not implemented.

**Step 3: Implement the minimal extension wiring**

Implement:
- remote executor factory
- server options for remote address/session behavior
- current schema extraction from PostgreSQL/session settings into request/session metadata

Keep heavy transport code out of the extension crate.

**Step 4: Re-run the targeted tests**

Run: `cargo test -p kalam-pg-fdw request_planner -- --nocapture`
Run: `cargo test -p kalam-pg-extension session_settings -- --nocapture`
Expected: PASS.

### Task 7: Add direct remote DML on the server path

**Files:**
- Modify: `backend/crates/kalamdb-pg/src/service.rs`
- Create: `backend/crates/kalamdb-pg/src/mutation_service.rs`
- Create: `backend/crates/kalamdb-pg/tests/mutation_service.rs`
- Modify: `pg/crates/kalam-pg-client/src/executor.rs`
- Test: `pg/crates/kalam-pg-client/tests/client.rs`

**Step 1: Write failing tests for insert/update/delete without SQL reparse**

Cover:
- insert request reaches provider direct path
- update request uses primary-key targeted direct path
- delete request uses direct delete path

**Step 2: Run the failing mutation tests**

Run: `cargo test -p kalamdb-pg mutation_service -- --nocapture`
Expected: FAIL because mutation RPC/service is not implemented.

**Step 3: Implement the centralized mutation path**

Implement one shared remote mutation service that:
- maps requests to existing fast/provider DML functions
- preserves auth/session/schema context
- keeps subscription publication behavior intact

Do not duplicate mutation logic across handlers.

**Step 4: Re-run mutation tests**

Run: `cargo test -p kalamdb-pg mutation_service -- --nocapture`
Expected: PASS.

### Task 8: Run focused verification

**Files:**
- Modify: `docs/plans/2026-03-21-postgres-remote-mode-implementation-plan.md`

**Step 1: Run a single batched compile check**

Run: `cd /Users/jamal/git/KalamDB && cargo check > batch_compile_output.txt 2>&1`
Expected: exit code 0, or one output file listing all remaining compile errors to fix in one pass.

**Step 2: Run targeted tests for touched crates**

Run: `cargo test -p kalam-pg-api -- --nocapture`
Run: `cargo test -p kalamdb-pg -- --nocapture`
Run: `cargo test -p kalam-pg-client -- --nocapture`
Run: `cargo test -p kalam-pg-fdw -- --nocapture`

**Step 3: Record any deliberate gaps**

If full extension integration or live server round-trip is not complete yet, document exactly which pieces are stubbed and what the next vertical slice should implement.
