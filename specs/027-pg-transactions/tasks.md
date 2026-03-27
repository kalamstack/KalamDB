# Tasks: PostgreSQL-Style Transactions for KalamDB

**Input**: Design documents from `/specs/027-pg-transactions/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/pg-transaction-rpc.md, contracts/sql-transaction-batch.md, quickstart.md

**Tests**: Included — the spec (FR-016) and contracts explicitly require automated test coverage.

**Organization**: Tasks grouped by user story to enable independent implementation and testing. US1 and US2 (both P1) share a single phase because write staging, overlay reads, and atomic commit are inseparable in the write path.

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

**Purpose**: Define transaction types, configuration, and feature scaffolding before any behavioral changes. All tasks touch different crates and can run fully in parallel.

- [ ] T001 [P] Add transaction configuration fields (`transaction_timeout_secs: u64` default 300, `max_transaction_buffer_bytes: usize` default 104857600) to `ServerConfig` in `backend/crates/kalamdb-configs/src/config/types.rs` and wire defaults into the `Default` impl
- [ ] T002 [P] Define `TransactionId` newtype wrapper following the established ID pattern in `backend/crates/kalamdb-commons/src/models/ids/transaction_id.rs` — implement `StorageKey`, `Display`, `FromStr`, `Serialize`/`Deserialize`, `From<String>`/`From<&str>`, UUID v7 validation. Re-export from `backend/crates/kalamdb-commons/src/models/ids/mod.rs` (Research Decision 13)
- [ ] T003 [P] Define `TransactionState` enum (`Active`, `Committing`, `Committed`, `RolledBack`, `TimedOut`, `Aborted`), `TransactionOrigin` enum (`PgRpc`, `SqlBatch`, `Internal`), and `OperationKind` enum (`Insert`, `Update`, `Delete`) in `backend/crates/kalamdb-commons/src/models/transaction.rs`. Re-export from `backend/crates/kalamdb-commons/src/models/mod.rs` (Research Decisions 14, 17)
- [ ] T004 [P] Replace the existing `TransactionState` enum in `backend/crates/kalamdb-pg/src/session_registry.rs` with an import of the new commons `TransactionState` from T003 — update all references in the pg crate. The existing enum has 3 variants (`Active`, `Committed`, `RolledBack`); the new one has 6. Update match arms accordingly

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build the core transaction coordinator, staged mutation buffer, overlay data structures, and snapshot capture in `kalamdb-core`. All user-story work depends on this phase.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T005 Create `TransactionHandle` struct in `backend/crates/kalamdb-core/src/transactions/handle.rs` with fields: `transaction_id: TransactionId`, `session_id: String`, `origin: TransactionOrigin`, `state: TransactionState`, `snapshot_commit_seq: u64`, `started_at: Instant`, `last_activity_at: Instant`, `write_count: usize`, `write_bytes: usize`, `touched_tables: HashSet<TableId>`. Note: `snapshot_commit_seq` uses the global `commit_seq` counter (Research Decision 1), NOT the per-row `_seq` Snowflake ID
- [ ] T006 [P] Create `StagedMutation` struct in `backend/crates/kalamdb-core/src/transactions/staged_mutation.rs` with fields: `transaction_id: TransactionId`, `mutation_order: u64`, `table_id: TableId`, `table_type: TableType`, `user_id: Option<UserId>`, `operation_kind: OperationKind`, `primary_key: String`, `payload: serde_json::Value`, `tombstone: bool`
- [ ] T007 [P] Create `TransactionOverlay` struct in `backend/crates/kalamdb-core/src/transactions/overlay.rs` with per-table primary-key-to-latest-state map (`entries_by_table: HashMap<TableId, BTreeMap<String, StagedMutation>>`), `inserted_keys`, `deleted_keys`, `updated_keys` sets, and overlay resolution logic that returns the latest visible state per primary key
- [ ] T008 [P] Create `TransactionCommitResult` struct and `TransactionSideEffects` struct in `backend/crates/kalamdb-core/src/transactions/commit_result.rs` with fields: `transaction_id: TransactionId`, `outcome`, `affected_rows: usize`, `committed_at: Option<Instant>`, `failure_reason: Option<KalamDbError>`, `emitted_side_effects: TransactionSideEffects` where `TransactionSideEffects` has `notifications_sent: usize`, `manifest_updates: usize`, `publisher_events: usize`
- [ ] T009 Create `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` with methods: `begin(session_id, origin) -> TransactionId`, `stage(transaction_id, StagedMutation) -> Result`, `commit(transaction_id) -> TransactionCommitResult`, `rollback(transaction_id)`, `get_overlay(transaction_id) -> Option<TransactionOverlay>`, `get_handle(transaction_id) -> Option<TransactionHandle>`. Use `DashMap<TransactionId, TransactionHandle>` for concurrent transaction registry. Depend on `Arc<AppContext>` (not `Arc<dyn StorageBackend>`) because the commit path must go through `DmlExecutor` / the Raft applier layer (Research Decision 7A). Maintain a global `commit_seq: AtomicU64` counter for snapshot boundaries (Research Decision 1). Capture `snapshot_commit_seq` at `begin()` by reading current `commit_seq` value
- [ ] T010 Create `backend/crates/kalamdb-core/src/transactions/mod.rs` re-exporting `TransactionHandle`, `StagedMutation`, `TransactionOverlay`, `TransactionCommitResult`, `TransactionSideEffects`, and `TransactionCoordinator`
- [ ] T011 Wire `TransactionCoordinator` into `AppContext` in `backend/crates/kalamdb-core/src/app_context.rs` — add field `transaction_coordinator: OnceCell<Arc<TransactionCoordinator>>`, accessor method, and initialize during server startup
- [ ] T012 [P] Add stream-table rejection guard in `TransactionCoordinator::stage()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — return `KalamDbError` if `table_type == TableType::Stream` per FR-002A
- [ ] T013 [P] Add DDL rejection guard in `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — provide a public `reject_ddl_in_transaction(transaction_id)` check that returns a clear error, callable from both pg RPC and SQL batch paths before executing any DDL statement (Research Decision 16)

**Checkpoint**: Transaction coordinator is available via `AppContext` with snapshot capture. No behavioral changes yet — existing autocommit path is untouched.

---

## Phase 3: User Story 1 + User Story 2 — Atomic Multi-Statement Writes + Read-Your-Writes (Priority: P1) 🎯 MVP

**Goal**: pg_kalam transactions stage writes, overlay reads return uncommitted data within the transaction, and COMMIT atomically applies all staged mutations through a single Raft `TransactionCommit` command via `DmlExecutor`. ROLLBACK discards all staged writes.

**Independent Test**: Begin a pg transaction, insert rows into two foreign tables, verify reads within the transaction return the rows, then ROLLBACK and verify the rows are gone. Separately, COMMIT and verify both rows are visible.

### Tests for US1 + US2

- [ ] T014 [P] [US1] Write integration test for pg RPC commit across two tables in `backend/crates/kalamdb-pg/tests/transaction_commit.rs` — BEGIN, INSERT into table A, INSERT into table B, COMMIT, verify both rows visible (SC-001)
- [ ] T015 [P] [US1] Write integration test for pg RPC rollback discarding all writes in `backend/crates/kalamdb-pg/tests/transaction_rollback.rs` — BEGIN, INSERT, INSERT, ROLLBACK, verify zero rows (SC-002)
- [ ] T016 [P] [US2] Write integration test for read-your-writes in `backend/crates/kalamdb-pg/tests/transaction_read_your_writes.rs` — BEGIN, INSERT, SELECT within same transaction confirms row visible, ROLLBACK, SELECT confirms row gone (SC-003)

### Implementation for US1 + US2

- [ ] T017 [P] [US1] Add `TransactionCommit { transaction_id: TransactionId, mutations: Vec<StagedMutation> }` variant to `RaftCommand` enum in `backend/crates/kalamdb-raft/src/commands/mod.rs` and implement the state machine handler that replays all mutations through the applier atomically in one apply cycle (Research Decision 7B). The state machine handler should iterate `mutations`, dispatching each to the existing user/shared data applier methods
- [ ] T018 [P] [US1] Add `Option<TransactionId>` field to `UserDataCommand` and `SharedDataCommand` Raft command enums in `backend/crates/kalamdb-raft/src/commands/user_data.rs` and `backend/crates/kalamdb-raft/src/commands/shared_data.rs`, defaulting to `None` for all existing variants. Ensure serialization compatibility — `None` serializes as a single byte (Research Decision 7C)
- [ ] T019 [US1] Implement atomic commit in `TransactionCoordinator::commit()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — collect all staged mutations, propose a single `TransactionCommit` command through `self.app_context.applier()`. Increment the global `commit_seq` counter atomically after successful Raft apply and tag commit result with the new `commit_seq` value. Emit side effects count in `TransactionSideEffects`. **Do NOT commit through raw `StorageBackend::batch()`** (Research Decision 7)
- [ ] T020 [US1] Modify `OperationService::execute_insert`, `execute_update`, `execute_delete` in `backend/crates/kalamdb-core/src/operations/service.rs` to check session for an active transaction — if active, build a `StagedMutation` and delegate to `TransactionCoordinator::stage()` instead of calling `self.app_context.applier()` immediately. The transaction presence check must be near-zero cost for non-transactional path (single `DashMap::get` on session_id)
- [ ] T021 [P] [US2] Create `TransactionOverlayExec` as a custom DataFusion `ExecutionPlan` implementation in `backend/crates/kalamdb-core/src/transactions/overlay_exec.rs` — wraps a base `ExecutionPlan` and merges staged inserts/updates while filtering staged deletes from the `RecordBatch` stream. Must implement `ExecutionPlan::execute()`, `schema()`, `children()`, and participate in DataFusion's projection pushdown by forwarding required columns (Research Decision 15)
- [ ] T022 [US2] Integrate `TransactionOverlayExec` into `SharedTableProvider::scan()` in `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs` — when session has an active transaction (detected via `SessionUserContext` extension), wrap the base scan plan with `TransactionOverlayExec` to merge overlay entries with committed MVCC rows
- [ ] T023 [US2] Integrate `TransactionOverlayExec` into `UserTableProvider::scan()` in `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs` — same overlay merge logic for user-scoped rows
- [ ] T024 [US1] Wire `PgService` RPC handlers `begin_transaction`, `commit_transaction`, `rollback_transaction` in `backend/crates/kalamdb-pg/src/service.rs` to call `TransactionCoordinator::begin()`, `commit()`, `rollback()` respectively, passing the session's canonical transaction ID. Currently these are client stubs — implement the server-side trait handlers
- [ ] T025 [US1] Ensure `OperationService::execute_scan()` in `backend/crates/kalamdb-core/src/operations/service.rs` passes transaction context (active `TransactionId` and `TransactionOverlay` reference) to the `SessionContext`/`SessionUserContext` extension so providers can detect and apply the overlay during `TableProvider::scan()`
- [ ] T026 [US1] Verify canonical `transaction_id` identity — add assertion in commit integration test that `BeginTransactionResponse.transaction_id` matches the ID in `TransactionCommitResult` and staged mutation records (FR-018)

**Checkpoint**: pg_kalam multi-statement transactions work end-to-end: write staging, read-your-writes overlay, atomic commit via Raft, and rollback. Autocommit path remains untouched for non-transaction requests.

---

## Phase 4: User Story 3 — Transaction Isolation Between Concurrent Sessions (Priority: P2)

**Goal**: Uncommitted writes from one session are invisible to other sessions. Each transaction reads from its snapshot boundary (`commit_seq <= snapshot_commit_seq`) plus its own overlay only.

**Independent Test**: Open two sessions, begin transactions in each, insert a row in session A, verify session B does not see it, commit session A, verify session B still does not see it within its own transaction (snapshot isolation per Acceptance Scenario 3), start a new query outside session B's transaction and verify it is finally visible.

### Tests for US3

- [ ] T027 [P] [US3] Write integration test for concurrent-session isolation in `backend/crates/kalamdb-pg/tests/transaction_isolation.rs` — session A inserts row, session B does not see it until A commits (SC-004)
- [ ] T028 [P] [US3] Write integration test for snapshot isolation in `backend/crates/kalamdb-core/tests/snapshot_isolation.rs` — session B starts transaction, session A commits new row, session B's transaction still does not see it because `commit_seq > B's snapshot_commit_seq`

### Implementation for US3

- [ ] T029 [US3] Enforce snapshot boundary in `SharedTableProvider::scan()` and `UserTableProvider::scan()` — when a transaction overlay is active, filter out committed rows with `commit_seq > snapshot_commit_seq` in `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs` and `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`. The filter is on `commit_seq` (global commit counter from Research Decision 1), NOT `_seq` (Snowflake ID)
- [ ] T030 [US3] Ensure overlay-only rows (inserted in the current transaction but not yet committed) bypass the `commit_seq` snapshot filter so read-your-writes still works when isolation is enforced — the `TransactionOverlayExec` produces these rows independently of the committed scan

**Checkpoint**: Multiple concurrent transactions are fully isolated. Snapshot boundary prevents cross-transaction visibility. Read-your-writes continues to work within each transaction.

---

## Phase 5: User Story 4 — Direct Transaction Management via KalamDB SQL Statements (Priority: P2)

**Goal**: KalamDB's SQL execution path recognizes `BEGIN`, `COMMIT`, and `ROLLBACK` statements. The `/v1/api/sql` endpoint supports zero, one, or multiple sequential transaction blocks per request, with automatic rollback of any unclosed blocks at request end. The SQL classifier already has `BeginTransaction`, `CommitTransaction`, `RollbackTransaction` variants — this phase implements their handlers.

**Independent Test**: Send `BEGIN; INSERT ...; INSERT ...; COMMIT;` to `/v1/api/sql` and verify both rows are committed. Send `BEGIN; INSERT ...; ROLLBACK;` and verify the row is absent. Send two sequential blocks and verify independent commit/rollback. Send an unclosed block and verify automatic rollback.

### Tests for US4

- [ ] T031 [P] [US4] Write integration test for `BEGIN ... COMMIT` through `/v1/api/sql` in `backend/crates/kalamdb-core/tests/sql_transaction_commit.rs` (SC-009)
- [ ] T032 [P] [US4] Write integration test for `BEGIN ... ROLLBACK` through `/v1/api/sql` in `backend/crates/kalamdb-core/tests/sql_transaction_rollback.rs`
- [ ] T033 [P] [US4] Write integration test for multiple sequential transaction blocks in one `/v1/api/sql` request in `backend/crates/kalamdb-core/tests/sql_transaction_multi_block.rs` (SC-012)
- [ ] T034 [P] [US4] Write integration test for request-end rollback of unclosed SQL transaction in `backend/crates/kalamdb-core/tests/sql_transaction_unclosed.rs` (SC-012)
- [ ] T035 [P] [US4] Write integration test for nested `BEGIN` rejection in `backend/crates/kalamdb-core/tests/sql_transaction_nested_begin.rs`
- [ ] T036 [P] [US4] Write regression test verifying autocommit SQL still works when no transaction statements are present in `backend/crates/kalamdb-core/tests/sql_autocommit_regression.rs` (SC-007, SC-010)

### Implementation for US4

- [ ] T037 [US4] Create `RequestTransactionState` struct in `backend/crates/kalamdb-core/src/sql/executor/request_transaction_state.rs` that tracks the active transaction for the current multi-statement batch — holds `Option<TransactionId>`, provides `begin()`, `commit()`, `rollback()`, and `is_active()` methods
- [ ] T038 [US4] Implement `BEGIN` handler in the SQL executor handler chain in `backend/crates/kalamdb-core/src/sql/executor/handlers/` — match on `SqlStatementKind::BeginTransaction`, call `TransactionCoordinator::begin()` with origin `SqlBatch`, store handle in `RequestTransactionState`. Reject if `RequestTransactionState` already has an active transaction (nested `BEGIN`)
- [ ] T039 [US4] Implement `COMMIT` handler in the SQL executor handler chain in `backend/crates/kalamdb-core/src/sql/executor/handlers/` — match on `SqlStatementKind::CommitTransaction`, call `TransactionCoordinator::commit()`, clear `RequestTransactionState`
- [ ] T040 [US4] Implement `ROLLBACK` handler in the SQL executor handler chain in `backend/crates/kalamdb-core/src/sql/executor/handlers/` — match on `SqlStatementKind::RollbackTransaction`, call `TransactionCoordinator::rollback()`, clear `RequestTransactionState`
- [ ] T041 [US4] Route DML statements in the SQL executor through `TransactionCoordinator::stage()` when `RequestTransactionState` has an active transaction, instead of the immediate write path, in `backend/crates/kalamdb-core/src/sql/executor/sql_executor.rs` — the existing `DmlKind::Insert/Update/Delete` dispatch should check request transaction state
- [ ] T042 [US4] Add request-end cleanup in `execute_sql_v1` handler in `backend/crates/kalamdb-api/src/handlers/sql/execute.rs` — after all statements are processed, check if `RequestTransactionState` has an open transaction. If so, roll it back and return an error response per FR-020
- [ ] T043 [US4] Reject DDL statements inside SQL batch transactions — in the SQL executor's statement dispatch, check `RequestTransactionState.is_active()` before executing any DDL (`CreateTable`, `DropTable`, `AlterTable`, etc.) and return error before execution (Research Decision 16)

**Checkpoint**: KalamDB SQL path supports explicit transactions. REST endpoint handles sequential blocks and auto-cleanup. Existing autocommit SQL regression tests pass.

---

## Phase 6: User Story 5 — Automatic Rollback on Connection Drop (Priority: P3)

**Goal**: If a pg_kalam session disconnects or the server cleans up a session, any active transaction is automatically rolled back and all staged writes are discarded.

**Independent Test**: Begin a pg transaction, insert rows, forcibly close the session, verify from a new session that the rows are absent and no transaction state remains.

### Tests for US5

- [ ] T044 [P] [US5] Write integration test for `CloseSession` rollback in `backend/crates/kalamdb-pg/tests/transaction_session_close.rs` — BEGIN, INSERT, close session, verify row absent (SC-005)
- [ ] T045 [P] [US5] Write integration test for orphaned transaction cleanup of stale session in `backend/crates/kalamdb-pg/tests/transaction_disconnect.rs`

### Implementation for US5

- [ ] T046 [US5] In `SessionRegistry::close_session` in `backend/crates/kalamdb-pg/src/session_registry.rs`, call `TransactionCoordinator::rollback()` for the session's canonical transaction ID before removing session state (FR-019)
- [ ] T047 [US5] In the pg RPC `CloseSession` handler in `backend/crates/kalamdb-pg/src/service.rs`, ensure cleanup invokes the transaction coordinator's rollback before dropping session — the canonical `transaction_id` from `RemotePgSession.transaction_id()` is used
- [ ] T048 [US5] Verify `pg/src/fdw_xact.rs` alignment — the existing implementation already fires remote commit at `XACT_EVENT_COMMIT` (correct per Research Decision 8). Ensure `register_xact_callback(Abort, ...)` clears `CURRENT_TX` and issues rollback RPC to the server. Review the existing `Abort` path and add rollback RPC call if missing

**Checkpoint**: Session disconnect or PostgreSQL `Abort` automatically rolls back the canonical transaction. No orphaned staged writes remain.

---

## Phase 7: User Story 6 — Transaction Timeout Protection (Priority: P3)

**Goal**: Transactions that exceed the configured timeout are automatically aborted. Transactions exceeding the write buffer size limit are rejected.

**Independent Test**: Begin a transaction, wait beyond the timeout, attempt another operation, verify it fails with a timeout error and the transaction is aborted.

### Tests for US6

- [ ] T049 [P] [US6] Write integration test for transaction timeout enforcement in `backend/crates/kalamdb-core/tests/transaction_timeout.rs` — begin transaction, advance time or use short timeout, verify operation fails
- [ ] T050 [P] [US6] Write integration test for write buffer size limit enforcement in `backend/crates/kalamdb-core/tests/transaction_buffer_limit.rs` — stage writes exceeding buffer, verify rejection

### Implementation for US6

- [ ] T051 [US6] Add a periodic timeout sweep in `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — spawn a `tokio::spawn` background task that checks `started_at` / `last_activity_at` against the configured `transaction_timeout_secs` from `ServerConfig` and aborts expired transactions by calling internal `rollback()` (FR-008)
- [ ] T052 [US6] Enforce write buffer size limit in `TransactionCoordinator::stage()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — if `write_bytes` exceeds the configured `max_transaction_buffer_bytes`, abort the transaction and return a clear `KalamDbError`
- [ ] T053 [US6] Log timeout and buffer-limit events using tracing spans with `transaction_id` field per AGENTS.md convention in `backend/crates/kalamdb-core/src/transactions/coordinator.rs`

**Checkpoint**: Runaway transactions are automatically terminated. Memory exhaustion from unbounded write buffers is prevented.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Observability, performance validation, stream-table rejection tests, and canonical transaction ID end-to-end verification.

- [ ] T054 [P] Create `ActiveTransactionMetric` struct in `backend/crates/kalamdb-core/src/transactions/metrics.rs` with fields: `transaction_id`, `session_id`, `state`, `age_ms`, `idle_ms`, `write_count`, `write_bytes`, `origin: TransactionOrigin`. Add a `TransactionCoordinator::active_metrics() -> Vec<ActiveTransactionMetric>` method and expose through the existing system stats or metrics endpoint (FR-012)
- [ ] T055 [P] Write autocommit performance regression test verifying non-transaction reads and writes regress by no more than 5% in `backend/crates/kalamdb-core/tests/autocommit_perf_regression.rs` (SC-008, SC-010)
- [ ] T056 [P] Write integration test verifying stream tables are rejected inside explicit transactions in `backend/crates/kalamdb-core/tests/transaction_stream_table_rejection.rs` (SC-013)
- [ ] T057 [P] Write integration test verifying DDL is rejected inside explicit transactions in `backend/crates/kalamdb-core/tests/transaction_ddl_rejection.rs`
- [ ] T058 [P] Write end-to-end test verifying canonical transaction ID propagation from `BeginTransaction` through staged mutations and commit result in `backend/crates/kalamdb-pg/tests/transaction_canonical_id.rs` (SC-011)
- [ ] T059 Verify non-transaction autocommit fast path adds only a cheap presence check (no allocation, no overlay construction) in `OperationService` in `backend/crates/kalamdb-core/src/operations/service.rs` — review, benchmark, and add inline comment documenting the fast-path guarantee (SC-010)
- [ ] T060 Run quickstart.md validation scenarios 1–8 end-to-end and confirm all pass

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 1 (T001–T004) ─── no deps, all parallel
       │
       ▼
Phase 2 (T005–T013) ─── depends on Phase 1 types
       │
       ├──────────────────────┬──────────────────────┐
       ▼                      ▼                      ▼
Phase 3 (US1+US2)       Phase 4 (US3)          Phase 5 (US4)
  T014–T026              T027–T030              T031–T043
       │                      │                      │
       └──────────┬───────────┘                      │
                  │                                   │
                  ▼                                   │
           Phase 6 (US5)                              │
            T044–T048                                 │
                  │                                   │
                  ├───────────────────────────────────┘
                  ▼
           Phase 7 (US6)
            T049–T053
                  │
                  ▼
           Phase 8 (Polish)
            T054–T060
```

### Key Dependency Notes

- **Phase 3 (US1+US2) and Phase 5 (US4) can start in parallel** after Phase 2 — they touch different crates (`kalamdb-pg`/`kalamdb-tables`/`kalamdb-raft` vs `kalamdb-sql`/`kalamdb-api`)
- **Phase 4 (US3)** depends on Phase 3 because isolation builds on the overlay infrastructure from US2
- **Phase 6 (US5)** depends on Phase 3 because transaction coordinator must be wired to pg sessions first
- **Phase 7 (US6)** depends on Phase 6 because timeout cleanup shares the coordinator rollback path and session cleanup semantics

### Within Each Phase

- Tests (marked [P]) can run in parallel within the same phase
- Models/structs before coordinator logic
- Coordinator logic before wiring into providers and RPC handlers
- Core implementation before integration with external surfaces (API, pg)

### Parallel Opportunities

**Within Phase 1**: T001 (configs), T002 (TransactionId), T003 (enums), T004 (pg migration) — four crates, zero overlap.

**Within Phase 2**: T005 first (TransactionHandle), then T006, T007, T008 in parallel (separate files in `transactions/`). T009 (coordinator) depends on T005–T008. T010 (mod.rs) depends on all. T011 (AppContext) depends on T009–T010. T012 and T013 can run in parallel after T009.

**Within Phase 3**: T014, T015, T016 (test files) all parallel. T017 and T018 (Raft command changes) parallel. T022 and T023 (overlay in shared vs user providers) parallel.

**Phases 3 + 5 in parallel**: Phase 3 modifies `kalamdb-pg`, `kalamdb-core/operations`, `kalamdb-core/transactions`, `kalamdb-tables`, `kalamdb-raft`. Phase 5 modifies `kalamdb-sql`, `kalamdb-api`, `kalamdb-core/sql/executor`. File overlap is limited to `kalamdb-core` but in different subdirectories.

---

## Parallel Example: Phase 3 (US1+US2)

```text
# All test files in parallel:
T014: backend/crates/kalamdb-pg/tests/transaction_commit.rs
T015: backend/crates/kalamdb-pg/tests/transaction_rollback.rs
T016: backend/crates/kalamdb-pg/tests/transaction_read_your_writes.rs

# Then Raft changes in parallel:
T017: backend/crates/kalamdb-raft/src/commands/mod.rs (TransactionCommit variant)
T018: backend/crates/kalamdb-raft/src/commands/{user_data,shared_data}.rs (Option<TransactionId>)

# Then coordinator commit + overlay in parallel:
T019: backend/crates/kalamdb-core/src/transactions/coordinator.rs (commit path)
T021: backend/crates/kalamdb-core/src/transactions/overlay_exec.rs (ExecutionPlan)

# Then provider wiring in parallel:
T022: backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs
T023: backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs
```

---

## Implementation Strategy

### MVP First (Phase 1 + 2 + 3)

1. Complete Phase 1: Setup — type definitions and configuration
2. Complete Phase 2: Foundational — transaction coordinator engine
3. Complete Phase 3: US1+US2 — pg_kalam atomic transactions with read-your-writes
4. **STOP and VALIDATE**: Run quickstart scenarios 1–3, verify pg_kalam e2e tests pass
5. Deploy/demo — this is the primary motivation for the feature

### Incremental Delivery

1. **Setup + Foundational** → Transaction infrastructure ready
2. **US1+US2 (Phase 3)** → pg_kalam transactions work → **MVP Deploy** (SC-001 through SC-003)
3. **US3 (Phase 4)** → Snapshot isolation → Production multi-user safety (SC-004)
4. **US4 (Phase 5)** → SQL REST transactions → Full SQL surface coverage (SC-009, SC-012)
5. **US5 (Phase 6)** → Disconnect cleanup → Production reliability (SC-005)
6. **US6 (Phase 7)** → Timeout protection → Defensive stability (SC-006)
7. **Polish (Phase 8)** → Observability, perf regression, edge cases → Production readiness

### Parallel Team Strategy

With two developers after Phase 2:
- **Developer A**: Phase 3 (US1+US2 — pg path, Raft, overlay, providers)
- **Developer B**: Phase 5 (US4 — SQL executor, API handler)
- Both converge for Phase 4, 6, 7, 8

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks in same phase
- [Story] label maps task to specific user story for traceability
- US1+US2 share Phase 3 because atomic commit and read-your-writes are inseparable
- Exact crate/file paths verified against the codebase as of the plan date
- SQL classifier already has `BeginTransaction`/`CommitTransaction`/`RollbackTransaction` variants — no new parsing needed
- `commit_seq` is a new concept; `_seq` is the existing Snowflake per-row ID — they serve different purposes
- Commit after each task or logical group; stop at any checkpoint to validate independently