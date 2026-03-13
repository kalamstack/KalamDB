# DBA Namespace Bootstrap Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a first-start `dba` namespace with bootstrap-managed `dashboard` and `notifications` tables plus typed internal repositories in `kalamdb-dba`.

**Architecture:** Define the `dba` tables as normal `TableDefinition`s in a new crate, then register them through `SchemaRegistry` during startup so they become ordinary DataFusion/shared-user providers. Expose internal CRUD via typed repositories that map models to `Row` values and delegate writes through the unified applier/DML path.

**Tech Stack:** Rust 2021, KalamDB core/app context, SchemaRegistry, unified applier/DML executor, DataFusion table providers.

---

### Task 1: Prove bootstrap surface with failing tests

**Files:**
- Modify: `backend/tests/misc/system/mod.rs`
- Create: `backend/tests/misc/system/test_dba_init.rs`

**Step 1: Write the failing bootstrap visibility test**

Add a test that boots the shared test server, queries `system.namespaces` for `dba`, and queries `system.schemas` for `dba.dashboard` and `dba.notifications`.

**Step 2: Write the failing internal repository test**

Add a test that imports `kalamdb_dba`, constructs typed dashboard/notification rows, writes them through the new repository API, and verifies they are readable through SQL.

**Step 3: Run targeted test compile**

Run: `cd backend && cargo nextest run test_dba_init > ../batch_compile_output.txt 2>&1`
Expected: compile/test failure because `kalamdb-dba` and bootstrap wiring do not exist yet.

### Task 2: Create the `kalamdb-dba` crate

**Files:**
- Create: `backend/crates/kalamdb-dba/Cargo.toml`
- Create: `backend/crates/kalamdb-dba/src/lib.rs`
- Create: `backend/crates/kalamdb-dba/src/bootstrap.rs`
- Create: `backend/crates/kalamdb-dba/src/mapping.rs`
- Create: `backend/crates/kalamdb-dba/src/repository/mod.rs`
- Create: `backend/crates/kalamdb-dba/src/models/mod.rs`
- Create: `backend/crates/kalamdb-dba/src/models/dashboard.rs`
- Create: `backend/crates/kalamdb-dba/src/models/notification.rs`

**Step 1: Define typed models**

Create one-file-per-model structs for `DashboardRow` and `NotificationRow` with explicit `TableDefinition` builders under namespace `dba`.

**Step 2: Add row mapping helpers**

Implement serde + column-aware `model_to_row` / `row_to_model` helpers for non-system typed tables.

**Step 3: Add repositories**

Implement typed repositories that accept `Arc<AppContext>`, map models to `Row`, and call `app_context.applier().insert_shared_data/update_shared_data/delete_shared_data` for dashboard and `insert_user_data/update_user_data/delete_user_data` for notifications.

**Step 4: Add bootstrap helper**

Implement a bootstrap function that idempotently ensures namespace `dba` exists and registers the two table definitions through `SchemaRegistry`.

### Task 3: Wire startup and app context

**Files:**
- Modify: `Cargo.toml`
- Modify: `backend/Cargo.toml`
- Modify: `backend/crates/kalamdb-core/Cargo.toml`
- Modify: `backend/src/lifecycle.rs`
- Modify: `backend/crates/kalamdb-core/src/app_context.rs`

**Step 1: Register workspace crate**

Add `kalamdb-dba` to the workspace and crate dependencies.

**Step 2: Run bootstrap on startup**

Invoke the new bootstrap helper during server startup after existing table loading and before the server is considered ready.

**Step 3: Expose repository access**

Store a singleton `DbaRegistry`/repository bundle on `AppContext` so internal code and tests can use it without rebuilding state.

### Task 4: Verify and fix in one batch

**Files:**
- Modify: any compile/test failures reported by the batch run

**Step 1: Run batched compile/test pass**

Run: `cd backend && cargo check > ../batch_compile_output.txt 2>&1`

**Step 2: Fix all reported issues in one edit batch**

Resolve compile errors, imports, trait bounds, and startup/test mismatches.

**Step 3: Run targeted tests**

Run: `cd backend && cargo nextest run test_dba_init`

**Step 4: Run smoke test if server is available**

Run: `cd cli && cargo test --test smoke`
Expected: pass if the backend server is already running; otherwise document that smoke verification was not possible in this turn.
