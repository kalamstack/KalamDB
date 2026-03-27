# Contract: pg Transaction RPC

## Purpose

Define the externally visible behavior of the existing pg RPC transaction lifecycle once server-side transactional semantics are implemented.

## Transport

- Service: `kalamdb.pg.PgService`
- Existing message shapes remain unchanged.
- Session continuity is keyed by `session_id`.
- The `transaction_id` returned by `BeginTransaction` is the canonical transaction identifier used through staging, observability, cleanup, and durable commit.

## Lifecycle Methods

### `BeginTransaction(session_id)`

- Input:
  - `session_id`: non-empty session identifier.
- Success result:
  - returns a non-empty `transaction_id`.
  - captures a transaction snapshot for that session.
  - marks the session as having one active explicit transaction.
  - establishes the canonical transaction ID that all later staged mutations for that session are associated with until commit or rollback.
- Failure cases:
  - empty `session_id` -> `INVALID_ARGUMENT`
  - session unavailable or server cannot allocate transaction state -> `FAILED_PRECONDITION` or `INTERNAL`
  - nested `BEGIN` on an already active transaction -> `FAILED_PRECONDITION`

### `CommitTransaction(session_id, transaction_id)`

- Input:
  - `session_id`: non-empty session identifier.
  - `transaction_id`: must match the active transaction.
- Success result:
  - applies all staged writes atomically.
  - makes committed rows visible to other sessions.
  - returns the committed `transaction_id`.
- Failure cases:
  - empty identifiers -> `INVALID_ARGUMENT`
  - no active transaction or ID mismatch -> `FAILED_PRECONDITION`
  - validation/storage failure before durable commit -> request fails and transaction is rolled back

### `RollbackTransaction(session_id, transaction_id)`

- Input:
  - `session_id`: non-empty session identifier.
  - `transaction_id`: active transaction identifier.
- Success result:
  - discards all staged writes.
  - clears the active transaction from the session.
  - returns the rolled back `transaction_id` or empty string for idempotent no-op rollback where no active transaction exists.
- Failure cases:
  - empty `session_id` -> `INVALID_ARGUMENT`
  - mismatched `transaction_id` -> `FAILED_PRECONDITION`

## DML and Scan Semantics While a Transaction Is Active

### `Insert`, `Update`, `Delete`

- Requests continue to use the existing fields: table identity, `session_id`, optional `user_id`, and payload.
- If the referenced session has an active transaction:
  - the server resolves the active canonical `transaction_id` from session state.
  - DML is staged in that transaction instead of being applied immediately.
  - the mutation participates in the eventual `COMMIT` or `ROLLBACK` under that same transaction ID.
- If the session has no active transaction:
  - behavior remains autocommit and matches current pre-transaction semantics.

### `Scan`

- If the referenced session has an active transaction:
  - results are read from the transaction snapshot plus that transaction's staged writes.
  - uncommitted writes from other sessions are never visible.
- If the session has no active transaction:
  - behavior remains unchanged.

## Session Cleanup Semantics

- `CloseSession(session_id)` automatically rolls back any active transaction before removing session state.
- PostgreSQL backend exit and equivalent connection teardown paths must trigger the same rollback behavior.
- Transaction timeout cleanup also clears session-bound transaction state.
- Cleanup must target the same canonical `transaction_id` that was returned by `BeginTransaction`.

## PostgreSQL Lifecycle Alignment

- pg_kalam continues to bind remote transaction finalization to PostgreSQL transaction callbacks.
- Remote durable commit occurs at PostgreSQL `XACT_EVENT_COMMIT` (the existing hook point in `fdw_xact.rs`), NOT at `PRE_COMMIT`. This means the PostgreSQL WAL commit record has already been flushed before the KalamDB remote commit fires. If the remote commit fails at this point, PostgreSQL has already committed — this is the standard FDW best-effort finalization pattern. See Research Decision 8.
- PostgreSQL `Abort` or backend/session close must discard pending state for the canonical transaction ID without durable apply.
- DDL statements (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`) are rejected inside explicit transactions on the pg RPC path, matching the KalamDB SQL batch behavior (see Research Decision 16).

## Compatibility Requirements

- pg_kalam does not need new request fields.
- Existing `session_id`-scoped behavior is preserved.
- Existing autocommit DML callers continue to work without explicit `BEGIN`.
