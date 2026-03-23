# Data Model: PostgreSQL-Style Transactions for KalamDB

## Entity: TransactionHandle

- Purpose: Represents the single active explicit transaction owned by a session.
- Fields:
  - `transaction_id`: unique typed identifier for the transaction and the canonical end-to-end transaction ID.
  - `session_id`: owning session identifier.
  - `origin`: `PgRpc` or `SqlBatch` source of the transaction.
  - `state`: `Active`, `Committed`, `RolledBack`, `TimedOut`, or `Aborted`.
  - `snapshot_seq`: committed MVCC sequence boundary captured at `BEGIN`.
  - `started_at`: server timestamp when the transaction began.
  - `last_activity_at`: timestamp of the most recent read or write in the transaction.
  - `write_count`: number of staged mutations.
  - `write_bytes`: approximate in-memory size of the staged write set.
  - `touched_tables`: set of `TableId` values referenced by staged mutations.
  - `caller_kind`: `PgRpc` or `SqlBatch`.
- Validation rules:
  - A session may own at most one `Active` transaction at a time.
  - `transaction_id` must remain stable for the full lifecycle of the transaction.
  - `snapshot_seq` is immutable after `BEGIN`.
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
  - `primary_key`: canonical primary key value for overlay and conflict lookup.
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
  - `emitted_side_effects`: summary of notifications, manifest updates, and publisher work released after commit.
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
  - `caller_kind`
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
