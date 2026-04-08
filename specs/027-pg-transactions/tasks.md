# Tasks: PostgreSQL-Style Transactions for KalamDB

**Input**: Design documents from `/specs/027-pg-transactions/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/pg-transaction-rpc.md, contracts/sql-transaction-batch.md, quickstart.md
**Last Updated**: 2026-04-07 (Phase 0 completed, session foundation in place, hardening/fanout + Raft-alignment delta added)

**Tests**: Included — the spec (FR-016) and contracts explicitly require automated test coverage.

**Organization**: Tasks grouped by user story to enable independent implementation and testing. US1 and US2 (both P1) share a single phase because write staging, overlay reads, and atomic commit are inseparable in the write path.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks in same phase)
- **[Story]**: Which user story this task belongs to (US1–US6)
- Exact file paths included in each task description

[P]: #
[Story]: #
[US1]: #
[US2]: #
[US3]: #
[US4]: #
[US5]: #
[US6]: #

## Path Conventions

- Backend crates: `backend/crates/kalamdb-{crate}/src/`
- pg extension: `pg/src/`
- CLI tests: `cli/tests/`

---

## Phase 0: Session Foundation (COMPLETED ✅)

**Purpose**: Establish sessions as the central management entity for pg-origin transactions. Pg sessions are the anchor for the FDW path, and their session_id is the correlation key across pg transport operations.

**Status**: All tasks completed and validated (65 tests passing across kalamdb-views, kalam-pg-extension, kalam-pg-client).

- [x] T-S01 [P] Add `Hash` derive to `RemoteServerConfig` in `pg/crates/kalam-pg-common/src/config.rs` to enable config-keyed session registry
- [x] T-S02 Refactor `pg/src/remote_state.rs` from single `OnceLock<RemoteExtensionState>` to `RemoteStateRegistry<T>` keyed by `RemoteServerConfig` with dual indexing (by_config + by_session_id)
- [x] T-S03 [P] Implement `session_id_for_config()` generating unique `pg-<pid>-<config_hash_hex>` session IDs per config
- [x] T-S04 Refactor `pg/src/fdw_xact.rs` from `Mutex<Option<ActiveTransaction>>` to `LazyLock<Mutex<HashMap<String, ActiveTransaction>>>` keyed by session_id for per-session transaction tracking (Research Decision 19)
- [x] T-S05 [P] Update `fdw_xact::xact_callback()` to iterate all active transactions at COMMIT/ABORT, resolving remote state per-session via `get_remote_extension_state_for_session()`
- [x] T-S06 [P] Update `pg/src/fdw_ddl.rs` to always resolve config before looking up state (remove stale `get_remote_extension_state` import)
- [x] T-S07 [P] Update `pg/src/pgrx_entrypoint.rs` (`kalam_exec`) to always resolve config rather than trying parameterless lookup first
- [x] T-S08 [P] Update `on_proc_exit_close_sessions()` to iterate all registered states on PG backend exit
- [x] T-S09 [P] Update `backend/crates/kalamdb-views/src/sessions.rs` `parse_backend_pid()` to handle new `pg-<pid>-<hash>` session ID format
- [x] T-S10 [P] Add integration test `same_config_reuses_remote_state_and_session` proving gRPC session reuse with real mock server
- [x] T-S11 [P] Add integration test `different_configs_open_distinct_remote_sessions` proving config isolation
- [x] T-S12 [P] Add test `parse_backend_pid_accepts_config_scoped_session_ids` for new session ID format parsing

**Checkpoint**: Session infrastructure is in place. Each PG backend × config pair has a unique session with independent transaction tracking. The `system.sessions` view correctly shows session + transaction metadata. gRPC connections are reused per-config.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Define transaction types, configuration, and feature scaffolding before any behavioral changes. All tasks touch different crates and can run fully in parallel.

- [x] T001 [P] Add transaction configuration fields (`transaction_timeout_secs: u64` default 300, `max_transaction_buffer_bytes: usize` default 104857600) to `ServerConfig` in `backend/crates/kalamdb-configs/src/config/types.rs` and wire defaults into the `Default` impl
- [x] T002 [P] Define `TransactionId` newtype wrapper following the established ID pattern in `backend/crates/kalamdb-commons/src/models/ids/transaction_id.rs` — implement `StorageKey`, `Display`, `FromStr`, `Serialize`/`Deserialize`, `From<String>`/`From<&str>`, UUID v7 validation. Re-export from `backend/crates/kalamdb-commons/src/models/ids/mod.rs` (Research Decision 13)
- [x] T003 [P] Define `TransactionState` enum (`OpenRead`, `OpenWrite`, `Committing`, `Committed`, `RollingBack`, `RolledBack`, `TimedOut`, `Aborted`), `TransactionOrigin` enum (`PgRpc`, `SqlBatch`, `Internal`), and `OperationKind` enum (`Insert`, `Update`, `Delete`) in `backend/crates/kalamdb-commons/src/models/transaction.rs`. Re-export from `backend/crates/kalamdb-commons/src/models/mod.rs` (Research Decisions 14, 17, 25)
- [x] T004 [P] Replace the existing `TransactionState` enum in `backend/crates/kalamdb-pg/src/session_registry.rs` with an import of the new commons `TransactionState` from T003 — update all references in the pg crate. The existing enum has 3 variants (`Active`, `Committed`, `RolledBack`); the new one maps onto the expanded lifecycle and must still expose stable pg/session observability strings
- [x] T004A [P] Create a shared `backend/crates/kalamdb-transactions/` crate for query-facing transaction types (`TransactionOverlay`, `TransactionOverlayExec`, transaction-specific DataFusion extension/traits) so `kalamdb-core`, `kalamdb-tables`, and transport crates can share transaction query context without introducing a `kalamdb-tables -> kalamdb-core` dependency (Research Decision 36)
- [x] T004B [P] Add hidden `_commit_seq` system-column support for committed user/shared row versions in `backend/crates/kalamdb-commons/src/constants.rs`, row models, Arrow schema helpers, and `backend/crates/kalamdb-commons/src/serialization/row_codec.rs` so hot and cold storage carry snapshot-visibility metadata (Research Decision 37)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build the core transaction coordinator, staged mutation buffer, overlay data structures, and snapshot capture in `kalamdb-core`. All user-story work depends on this phase.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T005 Create `ExecutionOwnerKey` in `backend/crates/kalamdb-core/src/transactions/owner.rs` plus a hot `TransactionHandle` in `backend/crates/kalamdb-core/src/transactions/handle.rs` with fields: `transaction_id: TransactionId`, `owner_key: ExecutionOwnerKey`, `owner_id: Arc<str>`, `origin: TransactionOrigin`, `state: TransactionState`, `snapshot_commit_seq: u64`, `started_at: Instant`, `last_activity_at: Instant`, `write_count: usize`, `write_bytes: usize`, `touched_tables: HashSet<TableId>`, `has_write_set: bool`. Note: `snapshot_commit_seq` uses the global `commit_seq` counter (Research Decision 1), NOT the per-row `_seq` Snowflake ID
- [x] T006 [P] Create `StagedMutation` in `backend/crates/kalamdb-core/src/transactions/staged_mutation.rs` and `TransactionWriteSet` in `backend/crates/kalamdb-core/src/transactions/write_set.rs` with fields for ordered mutations, latest-by-key lookup, overlay cache, and tracked buffer bytes. The write set must be allocated lazily on first staged write and remain the single open-transaction source of truth for staged insert/update/delete changes rather than duplicating live/publisher queues during the transaction (Research Decisions 24, 25, 27)
- [x] T007 [P] Create `TransactionOverlay` in `backend/crates/kalamdb-transactions/src/overlay.rs` with per-table primary-key-to-latest-state map (`entries_by_table: HashMap<TableId, BTreeMap<String, StagedMutation>>`), `inserted_keys`, `deleted_keys`, `updated_keys` sets, and overlay resolution logic that returns the latest visible state per primary key
- [x] T008 [P] Create `TransactionCommitResult`, `TransactionSideEffects`, and `CommitSideEffectPlan` in `backend/crates/kalamdb-core/src/transactions/commit_result.rs` with fields for durable outcome plus deferred live query notifications, publisher events, and manifest/file work that execute after commit is sealed. The plan should be derived once from the committed `TransactionWriteSet`, remain easy to inspect by `transaction_id`, and contain only user/shared-table changes (Research Decision 27)
- [x] T009 Create `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` with methods: `begin(owner_key, owner_id, origin) -> TransactionId`, `stage(transaction_id, StagedMutation) -> Result`, `commit(transaction_id) -> TransactionCommitResult`, `rollback(transaction_id)`, `get_overlay(transaction_id) -> Option<TransactionOverlay>`, `get_handle(transaction_id) -> Option<TransactionHandle>`. Use separate maps for `active_by_owner`, `active_by_id`, and `write_sets`. Depend on `Arc<AppContext>` (not `Arc<dyn StorageBackend>`) because the commit path must go through `DmlExecutor` / the Raft applier layer (Research Decision 7A). Read snapshot boundaries from a shared `CommitSequenceTracker` instead of owning a private authoritative `commit_seq` counter, and ensure all shared guards are dropped before awaited work begins
- [x] T010 Create `backend/crates/kalamdb-core/src/transactions/mod.rs` re-exporting `TransactionHandle`, `StagedMutation`, `TransactionOverlay`, `TransactionCommitResult`, `TransactionSideEffects`, and `TransactionCoordinator`
- [x] T011 Wire `TransactionCoordinator` and the shared `CommitSequenceTracker` into `AppContext` in `backend/crates/kalamdb-core/src/app_context.rs` — add fields/accessors and initialize them during server startup so BEGIN can read the latest committed snapshot and the write/apply path can stamp `_commit_seq`
- [x] T012 [P] Add stream-table rejection guard in `TransactionCoordinator::stage()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — return `KalamDbError` if `table_type == TableType::Stream` per FR-002A, before any mutation enters the write set, commit batch, or post-commit side-effect plan
- [x] T013 [P] Add DDL rejection guard in `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — provide a public `reject_ddl_in_transaction(transaction_id)` check that returns a clear error, callable from both pg RPC and SQL batch paths before executing any DDL statement (Research Decision 16)

**Checkpoint**: Transaction coordinator is available via `AppContext` with snapshot capture. No behavioral changes yet — existing autocommit path is untouched.

---

## Phase 3: User Story 1 + User Story 2 — Atomic Multi-Statement Writes + Read-Your-Writes (Priority: P1) 🎯 MVP

**Goal**: pg_kalam transactions stage writes, overlay reads return uncommitted data within the transaction, and COMMIT atomically applies all staged mutations through a single Raft `TransactionCommit` command via `DmlExecutor`. ROLLBACK discards all staged writes.

**Independent Test**: Begin a pg transaction, insert rows into two foreign tables, verify reads within the transaction return the rows, then ROLLBACK and verify the rows are gone. Separately, COMMIT and verify both rows are visible.

### Tests for US1 + US2

- [x] T014 [P] [US1] Write integration test for pg RPC commit across two tables in `backend/crates/kalamdb-pg/tests/transaction_commit.rs` — BEGIN, INSERT into table A, INSERT into table B, COMMIT, verify both rows visible (SC-001)
- [x] T015 [P] [US1] Write integration test for pg RPC rollback discarding all writes in `backend/crates/kalamdb-pg/tests/transaction_rollback.rs` — BEGIN, INSERT, INSERT, ROLLBACK, verify zero rows (SC-002)
- [x] T016 [P] [US2] Write integration test for read-your-writes in `backend/crates/kalamdb-pg/tests/transaction_read_your_writes.rs` — BEGIN, INSERT, SELECT within same transaction confirms row visible, ROLLBACK, SELECT confirms row gone (SC-003)

### Implementation for US1 + US2

- [x] T017 [P] [US1] Add `TransactionCommit { transaction_id: TransactionId, mutations: Vec<StagedMutation> }` variant to `RaftCommand` enum in `backend/crates/kalamdb-raft/src/commands/mod.rs` and implement the state machine handler that replays all mutations through the applier atomically in one apply cycle (Research Decision 7B). The apply cycle must allocate one committed `commit_seq`, stamp it onto every committed row version in the batch, and return that committed value to the coordinator
- [x] T018 [P] [US1] Add `Option<TransactionId>` field to `UserDataCommand` and `SharedDataCommand` Raft command enums in `backend/crates/kalamdb-raft/src/commands/user_data.rs` and `backend/crates/kalamdb-raft/src/commands/shared_data.rs`, defaulting to `None` for all existing variants. Ensure serialization compatibility — `None` serializes as a single byte (Research Decision 7C)
- [x] T018A [P] [US1] Thread the shared `CommitSequenceTracker` through the write/apply path (`backend/crates/kalamdb-core/src/applier/**`, `backend/crates/kalamdb-tables/**`) so autocommit writes and `TransactionCommit` both obtain `_commit_seq` from the same durable source and record the committed value needed for snapshot tracking (Research Decision 39)
- [x] T019 [US1] Implement atomic commit in `TransactionCoordinator::commit()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` using ordered commit phases: move the handle to `Committing`, collect staged mutations, propose a single `TransactionCommit` command through `self.app_context.applier()`, await a commit result carrying the committed `commit_seq`, derive one `CommitSideEffectPlan` from the committed write set, detach the owner from `active_by_owner`, and execute the plan outside the coordinator lock. Shared guards MUST be dropped before awaiting Raft apply or side-effect work. **Do NOT commit through raw `StorageBackend::batch()`** (Research Decisions 7, 26, 27, 40)
- [x] T020 [US1] Modify `OperationService::execute_insert`, `execute_update`, `execute_delete` in `backend/crates/kalamdb-core/src/operations/service.rs` to check for an active transaction via the fast owner-key lookup — if active, build a `StagedMutation` and delegate to `TransactionCoordinator::stage()` instead of calling `self.app_context.applier()` immediately. The transaction presence check must stay near-zero cost for the non-transactional path and must allocate the cold write set only on the first staged write
- [x] T021 [P] [US2] Create `TransactionOverlayExec` as a custom DataFusion `ExecutionPlan` implementation in `backend/crates/kalamdb-transactions/src/overlay_exec.rs` — wraps a base `ExecutionPlan` and merges staged inserts/updates while filtering staged deletes from the `RecordBatch` stream. Must implement `ExecutionPlan::execute()`, `schema()`, `children()`, and participate in DataFusion's projection pushdown by forwarding required columns (Research Decision 15)
- [x] T022 [US2] Integrate `TransactionOverlayExec` into `SharedTableProvider::scan()` in `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs` — when session has an active transaction (detected via the dedicated transaction query extension, not by overloading `SessionUserContext`), wrap the base scan plan with `TransactionOverlayExec` to merge overlay entries with committed MVCC rows
- [x] T023 [US2] Integrate `TransactionOverlayExec` into `UserTableProvider::scan()` in `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs` — same overlay merge logic for user-scoped rows
- [x] T024 [US1] Wire `PgService` RPC handlers `begin_transaction`, `commit_transaction`, `rollback_transaction` in `backend/crates/kalamdb-pg/src/service.rs` to call `TransactionCoordinator::begin()`, `commit()`, `rollback()` respectively, passing the session's canonical transaction ID. Currently these are client stubs — implement the server-side trait handlers
- [x] T025 [US1] Ensure `OperationService::execute_scan()` in `backend/crates/kalamdb-core/src/operations/service.rs` passes transaction query context (active `TransactionId`, `snapshot_commit_seq`, and a lightweight overlay handle) through the dedicated transaction extension from `kalamdb-transactions`, not by stuffing the full overlay into `SessionUserContext`, so providers can detect and apply the overlay during `TableProvider::scan()`
- [x] T026 [US1] Verify canonical `transaction_id` identity — add assertion in commit integration test that `BeginTransactionResponse.transaction_id` matches the ID in `TransactionCommitResult` and staged mutation records (FR-018)

**Checkpoint**: pg_kalam multi-statement transactions work end-to-end: write staging, read-your-writes overlay, atomic commit via Raft, and rollback. Autocommit path remains untouched for non-transaction requests.

---

## Phase 4: User Story 3 — Transaction Isolation Between Concurrent Sessions (Priority: P2)

**Goal**: Uncommitted writes from one session are invisible to other sessions. Each transaction reads from its snapshot boundary (`commit_seq <= snapshot_commit_seq`) plus its own overlay only.

**Independent Test**: Open two sessions, begin transactions in each, insert a row in session A, verify session B does not see it, commit session A, verify session B still does not see it within its own transaction (snapshot isolation per Acceptance Scenario 3), start a new query outside session B's transaction and verify it is finally visible.

### Tests for US3

- [x] T027 [P] [US3] Write integration test for concurrent-session isolation in `backend/crates/kalamdb-pg/tests/transaction_isolation.rs` — session A inserts row, session B does not see it until A commits (SC-004)
- [x] T028 [P] [US3] Write integration test for snapshot isolation in `backend/crates/kalamdb-core/tests/snapshot_isolation.rs` — session B starts transaction, session A commits new row, session B's transaction still does not see it because `commit_seq > B's snapshot_commit_seq`

### Implementation for US3

- [x] T029 [US3] Enforce snapshot boundary in `SharedTableProvider::scan()`, `UserTableProvider::scan()`, and `backend/crates/kalamdb-tables/src/utils/version_resolution.rs` — when a transaction overlay is active, resolve the newest committed row version with `_commit_seq <= snapshot_commit_seq`, using `_seq` only as an intra-commit tie-break. This MUST happen before, or as part of, version resolution; do not compute `MAX(_seq)` first and post-filter later
- [x] T030 [US3] Ensure overlay-only rows (inserted in the current transaction but not yet committed) bypass the `commit_seq` snapshot filter so read-your-writes still works when isolation is enforced — the `TransactionOverlayExec` produces these rows independently of the committed scan

**Checkpoint**: Multiple concurrent transactions are fully isolated. Snapshot boundary prevents cross-transaction visibility. Read-your-writes continues to work within each transaction.

---

## Phase 5: User Story 4 — Direct Transaction Management via KalamDB SQL Statements (Priority: P2)

**Goal**: KalamDB's SQL execution path recognizes `BEGIN`, `COMMIT`, and `ROLLBACK` statements. The `/v1/api/sql` endpoint supports zero, one, or multiple sequential transaction blocks per request, with automatic rollback of any unclosed blocks at request end. The SQL classifier already has `BeginTransaction`, `CommitTransaction`, `RollbackTransaction` variants — this phase implements their handlers.

**Independent Test**: Send `BEGIN; INSERT ...; INSERT ...; COMMIT;` to `/v1/api/sql` and verify both rows are committed. Send `BEGIN; INSERT ...; ROLLBACK;` and verify the row is absent. Send two sequential blocks and verify independent commit/rollback. Send an unclosed block and verify automatic rollback.

### Tests for US4

- [x] T031 [P] [US4] Write integration test for `BEGIN ... COMMIT` through `/v1/api/sql` in `backend/crates/kalamdb-core/tests/sql_transaction_commit.rs` (SC-009)
- [x] T032 [P] [US4] Write integration test for `BEGIN ... ROLLBACK` through `/v1/api/sql` in `backend/crates/kalamdb-core/tests/sql_transaction_rollback.rs`
- [x] T033 [P] [US4] Write integration test for multiple sequential transaction blocks in one `/v1/api/sql` request in `backend/crates/kalamdb-core/tests/sql_transaction_multi_block.rs` (SC-012)
- [x] T034 [P] [US4] Write integration test for request-end rollback of unclosed SQL transaction in `backend/crates/kalamdb-core/tests/sql_transaction_unclosed.rs` (SC-012)
- [x] T035 [P] [US4] Write integration test for nested `BEGIN` rejection in `backend/crates/kalamdb-core/tests/sql_transaction_nested_begin.rs`
- [x] T036 [P] [US4] Write regression test verifying autocommit SQL still works when no transaction statements are present in `backend/crates/kalamdb-core/tests/sql_autocommit_regression.rs` (SC-007, SC-010)

### Implementation for US4

- [x] T037 [US4] Create `RequestTransactionState` struct in `backend/crates/kalamdb-core/src/sql/executor/request_transaction_state.rs` that tracks the active transaction for the current multi-statement batch — holds a stable `request_owner_id` (for example `sql-req-<request_id>`) plus `Option<TransactionId>`, and provides `begin()`, `commit()`, `rollback()`, and `is_active()` methods
- [x] T038 [US4] Implement `BEGIN` handler in the SQL executor handler chain in `backend/crates/kalamdb-core/src/sql/executor/handlers/` — match on `SqlStatementKind::BeginTransaction`, call `TransactionCoordinator::begin(request_owner_id, SqlBatch)`, store handle in `RequestTransactionState`, and reject nested `BEGIN`. This owner ID must make the active transaction visible in `system.transactions` without creating a `system.sessions` row
- [x] T039 [US4] Implement `COMMIT` handler in the SQL executor handler chain in `backend/crates/kalamdb-core/src/sql/executor/handlers/` — match on `SqlStatementKind::CommitTransaction`, call `TransactionCoordinator::commit()`, clear `RequestTransactionState`
- [x] T040 [US4] Implement `ROLLBACK` handler in the SQL executor handler chain in `backend/crates/kalamdb-core/src/sql/executor/handlers/` — match on `SqlStatementKind::RollbackTransaction`, call `TransactionCoordinator::rollback()`, clear `RequestTransactionState`
- [x] T041 [US4] Route DML statements in the SQL executor through `TransactionCoordinator::stage()` when `RequestTransactionState` has an active transaction, instead of the immediate write path, in `backend/crates/kalamdb-core/src/sql/executor/sql_executor.rs`, `backend/crates/kalamdb-core/src/sql/executor/fast_insert.rs`, and `backend/crates/kalamdb-core/src/sql/executor/fast_point_dml.rs` — neither DataFusion DML nor SQL fast-path helpers may bypass staging
- [x] T042 [US4] Add request-end cleanup in `execute_sql_v1` handler in `backend/crates/kalamdb-api/src/http/sql/execute.rs` — after all statements are processed, check if `RequestTransactionState` has an open transaction. If so, roll it back and return an error response per FR-020
- [x] T043 [US4] Reject DDL statements inside SQL batch transactions — in the SQL executor's statement dispatch, check `RequestTransactionState.is_active()` before executing any DDL (`CreateTable`, `DropTable`, `AlterTable`, etc.) and return error before execution (Research Decision 16)

**Checkpoint**: KalamDB SQL path supports explicit transactions. REST endpoint handles sequential blocks and auto-cleanup. Active SQL transactions appear in `system.transactions` while active, and existing autocommit SQL regression tests pass.

---

## Phase 6: User Story 5 — Automatic Rollback on Connection Drop (Priority: P3)

**Goal**: If a pg_kalam session disconnects or the server cleans up a session, any active transaction is automatically rolled back and all staged writes are discarded.

**Independent Test**: Begin a pg transaction, insert rows, forcibly close the session, verify from a new session that the rows are absent and no transaction state remains.

### Tests for US5

- [x] T044 [P] [US5] Write integration test for `CloseSession` rollback in `backend/crates/kalamdb-pg/tests/transaction_session_close.rs` — BEGIN, INSERT, close session, verify row absent (SC-005)
- [x] T045 [P] [US5] Write integration test for orphaned transaction cleanup of stale session in `backend/crates/kalamdb-pg/tests/transaction_disconnect.rs`

### Implementation for US5

- [x] T046 [US5] In `SessionRegistry::close_session` in `backend/crates/kalamdb-pg/src/session_registry.rs`, call `TransactionCoordinator::rollback()` for the session's canonical transaction ID before removing session state (FR-019). The rollback path must be idempotent if the transaction is already `RollingBack`, `TimedOut`, or terminal
- [x] T047 [US5] In the pg RPC `CloseSession` handler in `backend/crates/kalamdb-pg/src/service.rs`, ensure cleanup invokes the transaction coordinator's rollback before dropping session — the canonical `transaction_id` from `RemotePgSession.transaction_id()` is used, and a concurrent `Committing` state is handled without double cleanup
- [x] T048 [US5] Verify `pg/src/fdw_xact.rs` alignment — the existing implementation already fires remote commit at `XACT_EVENT_COMMIT` (correct per Research Decision 8). Ensure `register_xact_callback(Abort, ...)` clears `CURRENT_TX` and issues rollback RPC to the server. Review the existing `Abort` path and add rollback RPC call if missing

**Checkpoint**: Session disconnect or PostgreSQL `Abort` automatically rolls back the canonical transaction. No orphaned staged writes remain.

---

## Phase 7: User Story 6 — Transaction Timeout Protection (Priority: P3)

**Goal**: Transactions that exceed the configured timeout are automatically aborted. Transactions exceeding the write buffer size limit are rejected.

**Independent Test**: Begin a transaction, wait beyond the timeout, attempt another operation, verify it fails with a timeout error and the transaction is aborted.

### Tests for US6

- [x] T049 [P] [US6] Write integration test for transaction timeout enforcement in `backend/crates/kalamdb-core/tests/transaction_timeout.rs` — begin transaction, advance time or use short timeout, verify operation fails
- [x] T050 [P] [US6] Write integration test for write buffer size limit enforcement in `backend/crates/kalamdb-core/tests/transaction_buffer_limit.rs` — stage writes exceeding buffer, verify rejection

### Implementation for US6

- [x] T051 [US6] Add a bounded periodic timeout sweep in `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — spawn one shared background task that checks `started_at` / `last_activity_at` against the configured `transaction_timeout_secs` from `ServerConfig`, moves expired transactions to `TimedOut`, and reuses the ordered rollback path instead of ad hoc deletion. Do NOT spawn one timer task per transaction (FR-008, Research Decision 40)
- [x] T052 [US6] Enforce write buffer size limit in `TransactionCoordinator::stage()` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs` — if `write_bytes` exceeds the configured `max_transaction_buffer_bytes`, abort the transaction and return a clear `KalamDbError`
- [x] T053 [US6] Log timeout and buffer-limit events using tracing spans with `transaction_id` field per AGENTS.md convention in `backend/crates/kalamdb-core/src/transactions/coordinator.rs`

**Checkpoint**: Runaway transactions are automatically terminated. Memory exhaustion from unbounded write buffers is prevented.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: `system.transactions` observability, performance validation, stream-table rejection tests, and canonical transaction ID end-to-end verification.

- [x] T054 [P] Create `ActiveTransactionMetric` struct in `backend/crates/kalamdb-core/src/transactions/metrics.rs` with fields: `transaction_id`, `owner_id`, `state`, `age_ms`, `idle_ms`, `write_count`, `write_bytes`, `touched_tables_count`, `snapshot_commit_seq`, `origin: TransactionOrigin`. Add a `TransactionCoordinator::active_metrics() -> Vec<ActiveTransactionMetric>` method sourced directly from the in-memory active handle map (FR-012, FR-029)
- [x] T055 [P] Write autocommit performance regression test verifying non-transaction reads and writes regress by no more than 5% in `backend/crates/kalamdb-core/tests/autocommit_perf_regression.rs` (SC-008, SC-010)
- [x] T056 [P] Write integration test verifying stream tables are rejected inside explicit transactions in `backend/crates/kalamdb-core/tests/transaction_stream_table_rejection.rs` (SC-013)
- [x] T057 [P] Write integration test verifying DDL is rejected inside explicit transactions in `backend/crates/kalamdb-core/tests/transaction_ddl_rejection.rs`
- [x] T058 [P] Write end-to-end test verifying canonical transaction ID propagation from `BeginTransaction` through staged mutations and commit result in `backend/crates/kalamdb-pg/tests/transaction_canonical_id.rs` (SC-011)
- [x] T059 Verify non-transaction autocommit fast path adds only a cheap presence check (no allocation, no overlay construction) in `OperationService` in `backend/crates/kalamdb-core/src/operations/service.rs` — review, benchmark, and add inline comment documenting the fast-path guarantee (SC-010)
- [x] T060 [P] Add `SystemTable::Transactions` in `backend/crates/kalamdb-commons/src/system_tables.rs`, create `backend/crates/kalamdb-views/src/transactions.rs`, and wire `TransactionsView` through `backend/crates/kalamdb-core/src/views/system_schema_provider.rs` using a query-time callback from `TransactionCoordinator` (FR-027, FR-029)
- [x] T061 [P] Write integration tests proving `system.transactions` shows active pg and SQL batch transactions while active and that `system.sessions` remains pg-only in `backend/crates/kalamdb-core/tests/system_transactions_view.rs` (SC-015, SC-016)
- [x] T062 Run quickstart.md validation scenarios 1–11 end-to-end and confirm all pass
- [x] T063 [P] Add post-commit fanout plan plumbing in `backend/crates/kalamdb-core/src/transactions/commit_result.rs` and `backend/crates/kalamdb-core/src/live/notification.rs` so committed transaction notifications are released after durability from a single commit-local plan derived from the staged write set, using existing table-scoped candidate sets rather than a full connection scan or incremental pre-commit fanout state (FR-034, FR-035)
- [x] T064 [P] Extend `backend/crates/kalamdb-core/src/live/notification.rs` caching/tests so payload work is shared across projection and serialization groups where possible, matching the production hardening target from Research Decision 29 (FR-037)
- [x] T065 [P] Add hot-table fanout and slow-subscriber stress coverage in `backend/crates/kalamdb-core/src/live/manager/tests.rs` verifying preserved per-table order, bounded buffers, and no leaked subscription state under commit bursts (SC-019)
- [x] T066 [P] Add repeated race/fault-injection tests in `backend/crates/kalamdb-core/tests/transaction_races.rs` and `backend/crates/kalamdb-pg/tests/transaction_races.rs` covering `COMMIT` vs `ROLLBACK`, timeout, session close, and request-end cleanup (SC-020)

---

## Phase 9: Raft Cluster Alignment (Pre-Cluster Enablement)

**Purpose**: Align explicit transactions with KalamDB's existing leader-forwarded, follower-lagging Raft semantics before enabling transaction support beyond single-node deployments.

- [x] T067 [P] Add `TransactionRaftBinding` (or equivalent `LocalSingleNode` / `UnboundCluster` / `BoundCluster`) to the transaction model in `backend/crates/kalamdb-core/src/transactions/handle.rs` and related coordinator helpers so cluster-mode transactions can bind to exactly one data Raft group and leader
- [x] T068 [P] Reject cross-group explicit transactions in cluster mode in `backend/crates/kalamdb-core/src/operations/service.rs`, the SQL executor transaction path, and the pg service transaction path by comparing each transactional table access against the bound `GroupId`; return a clear not-supported error naming the existing and requested groups (FR-038, FR-039)
- [x] T069 Wire cluster leader-affinity handling for active transactions in `backend/crates/kalamdb-api/src/http/sql/forward.rs`, `backend/crates/kalamdb-pg/src/service.rs`, and `backend/crates/kalamdb-core/src/transactions/coordinator.rs` so the first table access binds an unbound transaction to one data-group leader and later operations either execute on that leader or abort cleanly after leader change (FR-040, FR-041)
- [x] T070 [P] Add cluster integration coverage proving a single-group explicit transaction opened through a follower/frontdoor is forwarded or pinned to the correct leader and preserves read-your-writes plus atomic commit semantics without duplicate staging (SC-021)
- [x] T071 [P] Add failover coverage proving leader change during an open explicit transaction aborts the transaction with zero partial durable writes and zero leaked coordinator state in `backend/crates/kalamdb-core/tests/transaction_cluster_failover.rs` and corresponding pg integration coverage (SC-022)

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 0 (T-S01–T-S12) ─── ✅ COMPLETED (session foundation)
       │
       ▼
Phase 1 (T001–T004B) ─── no deps on incomplete work, all parallel
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
              T054–T066
                     │
                     ▼
            Phase 9 (Raft Alignment)
              T067–T071
```

### Session Foundation → Transaction Coordinator Bridge

Phase 0 established the session infrastructure. Phase 2's `TransactionCoordinator` must integrate with sessions:

- `TransactionCoordinator::begin(session_id, origin)` — the session_id comes from `SessionRegistry` (Phase 0)
- `TransactionCoordinator::commit(transaction_id)` — the transaction_id was generated by `SessionRegistry.begin_transaction()` (Phase 0)
- `TransactionCoordinator::rollback(transaction_id)` — triggered by `fdw_xact::xact_callback()` (Phase 0) via the RPC handler
- Phase 6 (T046/T047) fixes the known `close_session` gap by calling coordinator rollback before session removal

### Key Dependency Notes

- **Phase 3 (US1+US2) and Phase 5 (US4) can start in parallel** after Phase 2 — they touch different crates (`kalamdb-pg`/`kalamdb-tables`/`kalamdb-raft` vs `kalamdb-sql`/`kalamdb-api`)
- **Phase 4 (US3)** depends on Phase 3 because isolation builds on the overlay infrastructure from US2
- **Phase 6 (US5)** depends on Phase 3 because transaction coordinator must be wired to pg sessions first
- **Phase 7 (US6)** depends on Phase 6 because timeout cleanup shares the coordinator rollback path and session cleanup semantics
- **Phase 9 (Raft Alignment)** depends on Phases 3–8 because it assumes the coordinator, overlay, commit path, and observability already exist before cluster-specific leader affinity and failover rules are added

### Within Each Phase

- Tests (marked [P]) can run in parallel within the same phase
- Models/structs before coordinator logic
- Coordinator logic before wiring into providers and RPC handlers
- Core implementation before integration with external surfaces (API, pg)

### Parallel Opportunities

**Within Phase 1**: T001 (configs), T002 (TransactionId), T003 (enums), T004 (pg migration), T004A (shared tx crate), and T004B (`_commit_seq` system-column support) — multiple crates, low overlap.

**Within Phase 2**: T005 first (TransactionHandle), then T006, T007, T008 in parallel (separate files in `transactions/` / `kalamdb-transactions`). T009 (coordinator) depends on T005–T008. T010 (mod.rs) depends on all. T011 (AppContext + commit-sequence tracker) depends on T009–T010. T012 and T013 can run in parallel after T009.

**Within Phase 3**: T014, T015, T016 (test files) all parallel. T017, T018, and T018A (Raft/apply + commit-sequence changes) can be batched together. T022 and T023 (overlay in shared vs user providers) parallel.

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
T018A: backend/crates/kalamdb-core/src/applier/** + backend/crates/kalamdb-tables/** (`_commit_seq` allocation/stamping)

# Then coordinator commit + overlay in parallel:
T019: backend/crates/kalamdb-core/src/transactions/coordinator.rs (commit path)
T021: backend/crates/kalamdb-transactions/src/overlay_exec.rs (ExecutionPlan)

# Then provider wiring in parallel:
T022: backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs
T023: backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs
```

---

## Implementation Strategy

### Session Foundation Already Done (Phase 0 ✅)

The prerequisite session infrastructure is complete. All remaining phases can reference the established session model:
- Session IDs: `pg-<pid>-<config_hash>` — unique per backend × config
- Transaction IDs: `tx-{owner_id}-{counter}` — unique per transaction owner (`session_id` for pg, request owner ID for SQL batch)
- Session registry with transaction state fields
- Per-session fdw_xact tracking
- gRPC session lifecycle (open, close, exit handler)

### MVP Next (Phase 1 + 2 + 3)

1. Complete Phase 1: Setup — type definitions and configuration
2. Complete Phase 2: Foundational — transaction coordinator engine
3. Complete Phase 3: US1+US2 — pg_kalam atomic transactions with read-your-writes
4. **STOP and VALIDATE**: Run quickstart scenarios 1–3, verify pg_kalam e2e tests pass
5. Deploy/demo — this is the primary motivation for the feature

### Incremental Delivery

0. **Session Foundation (Phase 0)** → ✅ Complete (sessions, session IDs, tx lifecycle RPCs, xact callbacks)
1. **Setup + Foundational** → Transaction infrastructure ready (TransactionCoordinator wired to sessions)
2. **US1+US2 (Phase 3)** → pg_kalam transactions work → **MVP Deploy** (SC-001 through SC-003)
3. **US3 (Phase 4)** → Snapshot isolation → Production multi-user safety (SC-004)
4. **US4 (Phase 5)** → SQL REST transactions → Full SQL surface coverage (SC-009, SC-012)
5. **US5 (Phase 6)** → Disconnect cleanup → Production reliability (SC-005) — **fixes close_session gap**
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