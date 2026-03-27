# PG Extension Local E2E Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Cover the PostgreSQL extension's local `pgrx` integration path for mirrored table creation, DML propagation into KalamDB, scan/filter and join behavior, and user-facing error reporting.

**Architecture:** Reuse the existing local `pgrx` PostgreSQL workflow already used by the DDL suite, then move the DML e2e helpers off Docker so both DDL and DML tests exercise the same native extension install talking to a locally running KalamDB server. Add focused tests for each requested behavior and validate them with batched `cargo nextest` runs plus targeted reruns for failures.

**Tech Stack:** Rust, `pgrx`, `tokio-postgres`, `reqwest`, KalamDB HTTP API, `cargo nextest`

---

### Task 1: Unify PG e2e environment around local pgrx

**Files:**
- Modify: `pg/tests/e2e_common/mod.rs`
- Modify: `pg/tests/e2e_ddl_common/mod.rs`
- Test: `pg/tests/e2e_dml.rs`
- Reference: `pg/scripts/pgrx-test-setup.sh`

**Step 1: Write the failing tests**

Add DML tests that rely on the local `pgrx` setup instead of Docker-backed helpers.

**Step 2: Run test to verify it fails**

Run: `cd pg && cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_dml)'`

Expected: FAIL because the helper still assumes Docker ports/bootstrap.

**Step 3: Write minimal implementation**

Refactor the shared e2e helper to target local PostgreSQL on port `28816` and local KalamDB on ports `8080` and `9188`, including extension/server bootstrap assertions.

**Step 4: Run test to verify it passes**

Run the same DML suite again.

**Step 5: Commit**

Commit after the local helper is stable.

### Task 2: Add mirror + DML + error coverage

**Files:**
- Modify: `pg/tests/e2e_ddl.rs`
- Modify: `pg/tests/e2e_dml.rs`
- Modify: `pg/tests/e2e_common/mod.rs`

**Step 1: Write the failing tests**

Add tests for:
- `CREATE FOREIGN TABLE` mirrored schema parity between PostgreSQL and KalamDB
- INSERT/UPDATE/DELETE visibility in KalamDB after FDW writes
- SELECT with filters on KalamDB data
- JOIN between PostgreSQL-local data and KalamDB foreign tables
- user-table scans without `kalam.user_id`
- offline KalamDB server / bad server endpoint reporting

**Step 2: Run test to verify it fails**

Run targeted nextest commands for the new test names.

**Step 3: Write minimal implementation**

Update helpers and, only if required by failures, extension code so the new scenarios pass with clear errors.

**Step 4: Run test to verify it passes**

Re-run each targeted test, then the full PG e2e selection.

**Step 5: Commit**

Commit once the full local e2e coverage is green.

### Task 3: Verification and timeout calibration

**Files:**
- Modify: `pg/tests/e2e_ddl.rs`
- Modify: `pg/tests/e2e_dml.rs`

**Step 1: Measure observed runtimes**

Run the targeted and full PG extension suites and record durations.

**Step 2: Add/update async test timeouts**

Set `#[ntest::timeout(...)]` on async tests using observed runtime × 1.5, rounded reasonably.

**Step 3: Final verification**

Run:
- `cd pg && cargo check > /tmp/pg_extension_check.txt 2>&1`
- `cd pg && cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_ddl) | test(e2e_dml)'`

Expected: all targeted PG extension tests pass locally against `pgrx` PostgreSQL and a running local KalamDB server.
