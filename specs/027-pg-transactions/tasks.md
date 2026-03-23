# Tasks: PostgreSQL-Style Transactions for KalamDB

**Input**: Design documents from `/specs/027-pg-transactions/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/pg-transaction-rpc.md, contracts/sql-transaction-batch.md, quickstart.md

**Tests**: Included — the spec (FR-016) and contracts explicitly require automated test coverage.

**Organization**: Tasks grouped by user story to enable independent implementation and testing. US1 and US2 (both P1) share a single phase because write staging, overlay reads, and atomic commit are inseparable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks in same phase)
- **[Story]**: Which user story this task belongs to (US1–US6)
- Exact file paths included in each task description

## Path Conventions

- Backend crates: `backend/crates/kalamdb-{crate}/src/`
- pg extension: `pg/src/`
- CLI tests: `cli/tests/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Define transaction types, configuration, and feature scaffolding before any behavioral changes.

- [ ] T001 [P] Add transaction configuration fields (timeout_secs default 300, max_buffer_bytes default 104857600) to `ServerConfig` in `backend/crates/kalamdb-configs/src/lib.rs`
- [ ] T002 [P] Define `TransactionId` newtype wrapper, `TransactionState` enum (Active, Committed, RolledBack, TimedOut, Aborted), and `TransactionOrigin` enum (PgRpc, SqlBatch) in `backend/crates/kalamdb-commons/src/models/transaction.rs` and re-export from `backend/crates/kalamdb-commons/src/models/mod.rs`
- [ ] T003 [P] Define `OperationKind` enum (Insert, Update, Delete) in `backend/crates/kalamdb-commons/src/models/transaction.rs` for staged mutation classification

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build the core transaction coordinator, staged mutation buffer, and overlay data structures in `kalamdb-core`. All user-story work depends on this phase.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T004 Create `TransactionHandle` struct (transaction_id, session_id, origin, state, snapshot_seq, started_at, last_activity_at, write_count, write_bytes, touched_tables) in `backend/crates/kalamdb-core/src/transactions/handle.rs`
- [ ] T005 [P] Create `StagedMutation` struct (transaction_id, mutation_order, table_id, table_type, user_id, operation_kind, primary_key, payload, tombstone) in `backend/crates/kalamdb-core/src/transactions/staged_mutation.rs`
- [ ] T006 [P] Create `TransactionOverlay` struct with per-table primary-key-to-latest-state map, inserted_keys, deleted_keys, updated_keys, and overlay resolution logic in `backend/crates/kalamdb-core/src/transactions/overlay.rs`
- [ ] T007 [P] Create `TransactionCommitResult` struct (transaction_id, outcome, affected_rows, committed_at, failure_reason) in `backend/crates/kalamdb-core/src/transactions/commit_result.rs`
- [ ] T008 Create `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` with methods: `begin(session_id, origin) -> TransactionId`, `stage(transaction_id, StagedMutation)`, `commit(transaction_id) -> TransactionCommitResult`, `rollback(transaction_id)`, `get_overlay(transaction_id) -> TransactionOverlay`, `get_handle(transaction_id) -> TransactionHandle`. Use DashMap for concurrent transaction registry. Depend on `Arc<dyn StorageBackend>` for the commit path, not RocksDB types.
- [ ] T009 Create `backend/crates/kalamdb-core/src/transactions/mod.rs` re-exporting all transaction types and the coordinator
- [ ] T010 Wire `TransactionCoordinator` into `AppContext` in `backend/crates/kalamdb-core/src/app_context.rs` — add field, accessor method, and initialize in constructor
- [ ] T011 Add stream-table rejection guard in `TransactionCoordinator::stage()` — return an error if `table_type == TableType::Stream`, per FR-002A

**Checkpoint**: Transaction coordinator is available via AppContext. No behavioral changes yet — existing autocommit path is untouched.

---

## Phase 3: User Story 1 + User Story 2 — Atomic Multi-Statement Writes + Read-Your-Writes (Priority: P1) 🎯 MVP

**Goal**: pg_kalam transactions stage writes, overlay reads return uncommitted data within the transaction, and COMMIT atomically applies all staged mutations using the storage layer's batch write primitive. ROLLBACK discards all staged writes.

**Independent Test**: Begin a pg transaction, insert rows into two foreign tables, verify reads within the transaction return the rows, then ROLLBACK and verify the rows are gone. Separately, COMMIT and verify both rows are visible.

### Tests for US1 + US2

- [ ] T012 [P] [US1] Write integration test for pg RPC commit across two tables in `backend/crates/kalamdb-pg/tests/transaction_commit.rs`
- [ ] T013 [P] [US1] Write integration test for pg RPC rollback discarding all writes in `backend/crates/kalamdb-pg/tests/transaction_rollback.rs`
- [ ] T014 [P] [US2] Write integration test for read-your-writes (insert + select within transaction) in `backend/crates/kalamdb-pg/tests/transaction_read_your_writes.rs`

### Implementation for US1 + US2

- [ ] T015 [US1] Modify `OperationService::execute_insert`, `execute_update`, `execute_delete` in `backend/crates/kalamdb-core/src/operations/service.rs` to check session for an active transaction — if active, delegate to `TransactionCoordinator::stage()` instead of immediate `applier` calls
- [ ] T016 [US1] Implement atomic commit in `TransactionCoordinator::commit()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — collect all staged mutations, build a storage-backend batch write, apply atomically, emit side effects (notifications, publisher) only after durable commit succeeds
- [ ] T017 [US2] Integrate `TransactionOverlay` into `SharedTableProvider` scan path in `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs` — when session has an active transaction, merge overlay entries with committed MVCC rows before returning results
- [ ] T018 [US2] Integrate `TransactionOverlay` into `UserTableProvider` scan path in `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs` — same overlay merge logic for user-scoped rows
- [ ] T019 [US1] Wire `SessionRegistry::begin_transaction` / `commit_transaction` / `rollback_transaction` to `TransactionCoordinator` methods in `backend/crates/kalamdb-pg/src/service.rs` (BeginTransaction, CommitTransaction, RollbackTransaction RPC handlers)
- [ ] T020 [US1] Ensure `OperationService::execute_scan` in `backend/crates/kalamdb-core/src/operations/scan.rs` passes transaction context through to the provider when a transaction is active, so the overlay is applied during `TableProvider::scan()`
- [ ] T021 [US1] Verify that the canonical `transaction_id` from `BeginTransaction` is preserved through staged mutations and the commit path — add assertion in commit tests

**Checkpoint**: pg_kalam multi-statement transactions work end-to-end: write staging, read-your-writes, atomic commit, and rollback. Autocommit path remains untouched for non-transaction requests.

---

## Phase 4: User Story 3 — Transaction Isolation Between Concurrent Sessions (Priority: P2)

**Goal**: Uncommitted writes from one session are invisible to other sessions. Each transaction reads from its snapshot plus its own overlay only.

**Independent Test**: Open two sessions, begin transactions in each, insert a row in session A, verify session B does not see it, commit session A, verify session B still does not see it within its own transaction (snapshot isolation), start a new query outside session B's transaction and verify it is visible.

### Tests for US3

- [ ] T022 [P] [US3] Write integration test for concurrent-session isolation in `backend/crates/kalamdb-pg/tests/transaction_isolation.rs`
- [ ] T023 [P] [US3] Write integration test verifying snapshot isolation — session B does not see rows committed by session A after B's transaction began in `backend/crates/kalamdb-core/tests/snapshot_isolation.rs`

### Implementation for US3

- [ ] T024 [US3] Capture `snapshot_seq` at `TransactionCoordinator::begin()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` and store it in `TransactionHandle`
- [ ] T025 [US3] Enforce snapshot boundary in `SharedTableProvider` and `UserTableProvider` scan paths — filter out committed rows with `_seq > snapshot_seq` when a transaction overlay is active, in `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs` and `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`
- [ ] T026 [US3] Ensure overlay-only rows (inserted in the current transaction) bypass the snapshot_seq filter so read-your-writes still works when isolation is enforced

**Checkpoint**: Multiple concurrent transactions are fully isolated. Snapshot boundary prevents cross-transaction visibility.

---

## Phase 5: User Story 4 — Direct Transaction Management via KalamDB SQL Statements (Priority: P2)

**Goal**: KalamDB's SQL execution path recognizes `BEGIN`, `COMMIT`, and `ROLLBACK` statements. The `/v1/api/sql` endpoint supports zero, one, or multiple sequential transaction blocks per request, with automatic rollback of any unclosed blocks at request end.

**Independent Test**: Send `BEGIN; INSERT ...; INSERT ...; COMMIT;` to `/v1/api/sql` and verify both rows are committed. Send `BEGIN; INSERT ...; ROLLBACK;` and verify the row is absent. Send two sequential blocks and verify independent commit/rollback. Send an unclosed block and verify automatic rollback.

### Tests for US4

- [ ] T027 [P] [US4] Write integration test for `BEGIN ... COMMIT` through `/v1/api/sql` in `backend/crates/kalamdb-core/tests/sql_transaction_commit.rs`
- [ ] T028 [P] [US4] Write integration test for `BEGIN ... ROLLBACK` through `/v1/api/sql` in `backend/crates/kalamdb-core/tests/sql_transaction_rollback.rs`
- [ ] T029 [P] [US4] Write integration test for multiple sequential transaction blocks in one `/v1/api/sql` request in `backend/crates/kalamdb-core/tests/sql_transaction_multi_block.rs`
- [ ] T030 [P] [US4] Write integration test for request-end rollback of unclosed SQL transaction in `backend/crates/kalamdb-core/tests/sql_transaction_unclosed.rs`
- [ ] T031 [P] [US4] Write integration test for nested `BEGIN` rejection in `backend/crates/kalamdb-core/tests/sql_transaction_nested_begin.rs`
- [ ] T032 [P] [US4] Write regression test verifying autocommit SQL still works when no transaction statements are present in `backend/crates/kalamdb-core/tests/sql_autocommit_regression.rs`

### Implementation for US4

- [ ] T033 [US4] Add `BEGIN`, `START TRANSACTION`, `COMMIT`, and `ROLLBACK` recognition to the SQL statement classifier in `backend/crates/kalamdb-sql/src/statement_classifier.rs` — classify as new `SqlStatementKind::TransactionControl` variants
- [ ] T034 [US4] Implement request-scoped transaction state tracking in the SQL executor — add a `RequestTransactionState` struct that tracks the active transaction for the current multi-statement batch in `backend/crates/kalamdb-core/src/sql/executor/sql_executor.rs`
- [ ] T035 [US4] Implement `BEGIN` handler in the SQL executor that calls `TransactionCoordinator::begin()` with origin `SqlBatch` and stores the transaction handle in `RequestTransactionState` in `backend/crates/kalamdb-core/src/sql/executor/handlers/`
- [ ] T036 [US4] Implement `COMMIT` handler in the SQL executor that calls `TransactionCoordinator::commit()` and clears `RequestTransactionState` in `backend/crates/kalamdb-core/src/sql/executor/handlers/`
- [ ] T037 [US4] Implement `ROLLBACK` handler in the SQL executor that calls `TransactionCoordinator::rollback()` and clears `RequestTransactionState` in `backend/crates/kalamdb-core/src/sql/executor/handlers/`
- [ ] T038 [US4] Route DML statements in the SQL executor through `TransactionCoordinator::stage()` when `RequestTransactionState` has an active transaction, instead of the immediate write path, in `backend/crates/kalamdb-core/src/sql/executor/sql_executor.rs`
- [ ] T039 [US4] Add request-end cleanup in `execute_sql_v1` handler in `backend/crates/kalamdb-api/src/handlers/sql/execute.rs` — after all statements are processed, roll back any still-open request-scoped transactions and return an error if any were unclosed
- [ ] T040 [US4] Reject nested `BEGIN` — if `RequestTransactionState` already has an active transaction when `BEGIN` is encountered, return a clear error

**Checkpoint**: KalamDB SQL path supports explicit transactions. REST endpoint handles sequential blocks and auto-cleanup. Autocommit regression tests pass.

---

## Phase 6: User Story 5 — Automatic Rollback on Connection Drop (Priority: P3)

**Goal**: If a pg_kalam session disconnects or the server cleans up a session, any active transaction is automatically rolled back and all staged writes are discarded.

**Independent Test**: Begin a pg transaction, insert rows, forcibly close the session, verify from a new session that the rows are absent and no transaction state remains.

### Tests for US5

- [ ] T041 [P] [US5] Write integration test for `CloseSession` rollback in `backend/crates/kalamdb-pg/tests/transaction_session_close.rs`
- [ ] T042 [P] [US5] Write integration test for orphaned transaction cleanup (stale session) in `backend/crates/kalamdb-pg/tests/transaction_disconnect.rs`

### Implementation for US5

- [ ] T043 [US5] In `SessionRegistry::close_session` in `backend/crates/kalamdb-pg/src/session_registry.rs`, call `TransactionCoordinator::rollback()` for the canonical transaction ID before removing session state
- [ ] T044 [US5] In the pg RPC `CloseSession` handler in `backend/crates/kalamdb-pg/src/service.rs`, ensure cleanup invokes the transaction coordinator rollback before dropping session
- [ ] T045 [US5] Update `pg/src/fdw_xact.rs` to align with the ParadeDB PreCommit/Abort pattern — ensure `register_xact_callback(Abort, ...)` clears CURRENT_TX and issues rollback RPC, and `register_xact_callback(PreCommit, ...)` issues commit RPC

**Checkpoint**: Session disconnect or PostgreSQL Abort automatically rolls back the canonical transaction. No orphaned staged writes remain.

---

## Phase 7: User Story 6 — Transaction Timeout Protection (Priority: P3)

**Goal**: Transactions that exceed the configured timeout are automatically aborted. Transactions exceeding the write buffer size limit are rejected.

**Independent Test**: Begin a transaction, wait beyond the timeout, attempt another operation, verify it fails with a timeout error and the transaction is aborted.

### Tests for US6

- [ ] T046 [P] [US6] Write integration test for transaction timeout enforcement in `backend/crates/kalamdb-core/tests/transaction_timeout.rs`
- [ ] T047 [P] [US6] Write integration test for write buffer size limit enforcement in `backend/crates/kalamdb-core/tests/transaction_buffer_limit.rs`

### Implementation for US6

- [ ] T048 [US6] Add a periodic timeout sweep in `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — spawn a background task that checks `started_at` / `last_activity_at` against the configured timeout and aborts expired transactions
- [ ] T049 [US6] Enforce write buffer size limit in `TransactionCoordinator::stage()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — if `write_bytes` exceeds the configured limit, abort the transaction and return an error
- [ ] T050 [US6] Log timeout and buffer-limit events using tracing spans with `transaction_id` field per AGENTS.md convention in `backend/crates/kalamdb-core/src/transactions/coordinator.rs`

**Checkpoint**: Runaway transactions are automatically terminated. Memory exhaustion from unbounded write buffers is prevented.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Observability, performance validation, stream-table rejection tests, and canonical transaction ID end-to-end verification.

- [ ] T051 [P] Create `ActiveTransactionMetric` projection for system observability (transaction_id, session_id, state, age_ms, write_count, write_bytes, caller_kind) in `backend/crates/kalamdb-core/src/transactions/metrics.rs` and expose through the existing system stats or metrics endpoint
- [ ] T052 [P] Write autocommit performance regression test verifying non-transaction reads and writes regress by no more than 5% in `backend/crates/kalamdb-core/tests/autocommit_perf_regression.rs`
- [ ] T053 [P] Write integration test verifying stream tables are rejected inside explicit transactions in `backend/crates/kalamdb-core/tests/transaction_stream_table_rejection.rs`
- [ ] T054 [P] Write end-to-end test verifying canonical transaction ID propagation from `BeginTransaction` through staged mutations and commit result in `backend/crates/kalamdb-pg/tests/transaction_canonical_id.rs`
- [ ] T055 Verify that the non-transaction autocommit fast path in `OperationService` adds only a cheap presence check (no allocation, no overlay construction) in `backend/crates/kalamdb-core/src/operations/service.rs` — review and add inline comments documenting the fast-path guarantee

---

## Dependencies

```
Phase 1 (T001–T003) ─── no deps, all parallel
       │
       ▼
Phase 2 (T004–T011) ─── depends on Phase 1 types
       │
       ├──────────────────────┬──────────────────────┐
       ▼                      ▼                      ▼
Phase 3 (US1+US2)       Phase 4 (US3)          Phase 5 (US4)
  T012–T021              T022–T026              T027–T040
       │                      │                      │
       └──────────┬───────────┘                      │
                  │                                   │
                  ▼                                   │
           Phase 6 (US5)                              │
            T041–T045                                 │
                  │                                   │
                  ├───────────────────────────────────┘
                  ▼
           Phase 7 (US6)
            T046–T050
                  │
                  ▼
           Phase 8 (Polish)
            T051–T055
```

**Key dependency notes**:
- Phase 3 (US1+US2) and Phase 5 (US4) can start in parallel after Phase 2 — they touch different crates (`kalamdb-pg`/`kalamdb-tables` vs `kalamdb-sql`/`kalamdb-api`)
- Phase 4 (US3) can start after Phase 3 because isolation builds on the overlay infrastructure
- Phase 6 (US5) depends on Phase 3 (transaction coordinator must be wired to pg sessions)
- Phase 7 (US6) depends on Phase 6 (timeout cleanup shares the coordinator rollback path)

## Parallel Execution Examples

**Within Phase 1**: T001 (configs), T002 (types), T003 (enums) — three crates, zero overlap.

**Within Phase 2**: T005, T006, T007 can all run in parallel (separate files in `transactions/` module). T004 should go first (T005 references TransactionHandle fields). T008 depends on T004–T007.

**Within Phase 3**: T012, T013, T014 (test files) can run in parallel. T017 and T018 (overlay in shared vs user providers) can run in parallel.

**Phases 3 + 5 in parallel**: Phase 3 modifies `kalamdb-pg`, `kalamdb-core/operations`, `kalamdb-tables`. Phase 5 modifies `kalamdb-sql`, `kalamdb-api`. No file overlap.

## Implementation Strategy

1. **MVP (Phase 1 + 2 + 3)**: Delivers pg_kalam atomic multi-statement transactions with read-your-writes — the primary motivation for this feature. Deployable and testable independently.
2. **Isolation (Phase 4)**: Adds snapshot isolation for production multi-user safety.
3. **SQL REST (Phase 5)**: Extends transactions to KalamDB's own SQL surface. Can be developed in parallel with Phase 3.
4. **Safety nets (Phase 6 + 7)**: Disconnect cleanup and timeout protection for production reliability.
5. **Polish (Phase 8)**: Observability, performance validation, and edge-case coverage.
