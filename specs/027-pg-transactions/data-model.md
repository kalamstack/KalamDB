# Data Model: PostgreSQL-Style Transactions for KalamDB

## Entity: TransactionHandle

- Purpose: Represents the single active explicit transaction owned by a session.
- Fields:
  - `transaction_id`: unique typed identifier for the transaction and the canonical end-to-end transaction ID.
  - `session_id`: owning session identifier.
  - `origin`: `PgRpc` or `SqlBatch` source of the transaction.
  - `state`: `Active`, `Committed`, `RolledBack`, `TimedOut`, or `Aborted`.
  - `snapshot_commit_seq: u64`: the global `commit_seq` counter value captured at `BEGIN`. All reads see committed data with `commit_seq <= snapshot_commit_seq`. This is NOT the per-row `_seq` Snowflake ID — see Research Decision 1.
  - `started_at: Instant`: server timestamp when the transaction began.
  - `last_activity_at: Instant`: timestamp of the most recent read or write in the transaction.
  - `write_count: usize`: number of staged mutations.
  - `write_bytes: usize`: approximate in-memory size of the staged write set, measured as the sum of serialized payload byte sizes across all `StagedMutation` entries.
  - `touched_tables: HashSet<TableId>`: set of `TableId` values referenced by staged mutations.
- Validation rules:
  - A session may own at most one `Active` transaction at a time.
  - `transaction_id` must remain stable for the full lifecycle of the transaction.
  - `snapshot_commit_seq` is immutable after `BEGIN`.
  - `write_bytes` must stay below the configured transaction memory limit.
  - `state` transitions are monotonic and terminal once committed or aborted.

## Entity: StagedMutation

- Purpose: Stores one logical DML operation buffered inside a transaction.
- Fields:
  - `transaction_id`: owning transaction and canonical correlation ID copied from `TransactionHandle`.
  - `mutation_order`: monotonically increasing order inside the transaction.
  - `table_id`: target table.
  - `table_type`: `Shared` or `User` in this phase. `Stream` is reserved for future support but is not valid for explicit transactions now.
  - `user_id`: optional subject user for user-scoped tables.
  - `operation_kind`: `Insert`, `Update`, or `Delete`.
  - `primary_key: String`: canonical primary key value (serialized as string) for overlay and conflict lookup.
  - `payload`: row payload or update payload after coercion/validation.
  - `tombstone`: whether the visible result for this key is deleted.
- Validation rules:
  - Every staged mutation must reference a valid `TableId` and a resolved primary key.
  - `table_type` must be `Shared` or `User` for Phase 1 explicit transactions.
  - Mutations are ordered per transaction and later mutations on the same key override earlier visible state.
  - Delete mutations must behave like tombstones in the overlay model.

## Entity: TransactionOverlay

- Purpose: Query-time projection of staged mutations used to implement read-your-writes.
- Fields:
  - `transaction_id`: owning transaction.
  - `entries_by_table`: per-table map of primary-key to latest visible staged state.
  - `inserted_keys`: keys first created in the transaction.
  - `deleted_keys`: keys hidden by transaction-local deletes.
  - `updated_keys`: keys whose visible row differs from the committed snapshot.
- Validation rules:
  - Overlay resolution must always prefer the latest staged mutation for a key.
  - Deleted keys suppress both committed snapshot rows and earlier inserted rows in the same transaction.
  - Overlay data is derived from `StagedMutation` and must be discarded on rollback.

## Entity: TransactionCommitResult

- Purpose: Captures the outcome of applying a transaction.
- Fields:
  - `transaction_id`: committed or aborted transaction.
  - `outcome`: `Committed` or `RolledBack`.
  - `affected_rows`: total rows changed across all staged mutations.
  - `committed_at`: timestamp for successful commits.
  - `failure_reason`: optional structured error for aborted commits.
  - `emitted_side_effects: TransactionSideEffects`: typed struct containing `notifications_sent: usize` (live query notification count), `manifest_updates: usize` (manifests written), `publisher_events: usize` (topic events emitted). Provides an auditable summary of side effects released after commit.
- Validation rules:
  - `committed_at` is present only for successful commits.
  - Side effects are emitted only after durable commit succeeds.
  - `transaction_id` matches the originating `TransactionHandle` and all included `StagedMutation` records.

## Entity: ActiveTransactionMetric

- Purpose: Observability projection for system tables and metrics.
- Fields:
  - `transaction_id`
  - `session_id`
  - `state`
  - `age_ms`
  - `idle_ms`
  - `write_count`
  - `write_bytes`
  - `origin: TransactionOrigin` — `PgRpc`, `SqlBatch`, or `Internal` (see Research Decision 17).
- Validation rules:
  - Only active transactions are exposed in live metrics.
  - Metric rows disappear immediately after commit or rollback.

## Relationships

- One `Session` owns zero or one active `TransactionHandle`.
- One `TransactionHandle` owns zero or many `StagedMutation` records.
- One `TransactionHandle` materializes one `TransactionOverlay` for reads.
- One finished `TransactionHandle` yields one `TransactionCommitResult`.
- The same `transaction_id` is preserved across `TransactionHandle`, `StagedMutation`, observability rows, and `TransactionCommitResult`.

## State Transitions

### TransactionHandle

- `Active -> Committed`
  - Trigger: successful `COMMIT` after validation and durable apply.
- `Active -> RolledBack`
  - Trigger: explicit `ROLLBACK`.
- `Active -> TimedOut`
  - Trigger: transaction timeout enforcement.
- `Active -> Aborted`
  - Trigger: session close, server cleanup, or commit failure before durable apply finishes.
- `TimedOut -> RolledBack`
  - Trigger: internal cleanup path that discards the write set.

### Staged key visibility inside one transaction

- `Absent -> Inserted`
- `Inserted -> Updated`
- `Inserted -> Deleted`
- `CommittedSnapshotRow -> Updated`
- `CommittedSnapshotRow -> Deleted`

The final visible transaction-local state for each primary key is the last staged mutation in `mutation_order`.

## Entity: CommitSequenceCounter

- Purpose: Global monotonic counter providing commit-time snapshot boundaries for MVCC transaction isolation.
- Fields:
  - `current_value: AtomicU64`: the current commit sequence number. Incremented atomically on each successful transaction commit.
- Behavior:
  - Lives in `TransactionCoordinator` (or a dedicated `CommitSequence` service in `kalamdb-core`).
  - On `BEGIN`: the current `commit_seq` value is captured as the transaction's `snapshot_commit_seq`.
  - On `COMMIT`: `commit_seq` is atomically incremented. The new value is tagged on all rows written by this transaction.
  - Non-transactional autocommit writes also increment `commit_seq` so that transaction snapshots correctly exclude them.
  - Must be persisted or recoverable on server restart (e.g., derived from the highest committed `commit_seq` in storage).
- Validation rules:
  - `commit_seq` is strictly monotonically increasing.
  - The counter MUST NOT be confused with the per-row `_seq` Snowflake ID, which is assigned at row write time and serves a different purpose.

## Entity: TransactionSideEffects

- Purpose: Typed struct summarizing side effects released after a successful transaction commit.
- Fields:
  - `notifications_sent: usize`: count of live query notifications emitted.
  - `manifest_updates: usize`: count of manifest files updated.
  - `publisher_events: usize`: count of topic publisher events emitted.
- Validation rules:
  - Side effects are emitted only after durable commit succeeds.
  - This struct is informational for observability; it does not need to be persisted.

## Enum: TransactionOrigin

- Purpose: Type-safe identification of how a transaction was initiated.
- Variants:
  - `PgRpc`: Transaction initiated via pg gRPC session (PostgreSQL FDW).
  - `SqlBatch`: Transaction initiated via `/v1/api/sql` HTTP endpoint.
  - `Internal`: Transaction initiated internally by the server (e.g., system operations).
- Usage: Used in `TransactionHandle.origin` and `ActiveTransactionMetric.origin`. Replaces the previously inconsistent `caller_kind` naming.
