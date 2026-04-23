# Transaction Architecture

## Overview

KalamDB has one explicit transaction model centered on `TransactionCoordinator` in `backend/crates/kalamdb-core/src/transactions/coordinator.rs`.

Two different caller families feed that model today:

- KalamDB-native callers such as HTTP SQL batches and typed internal services
- PostgreSQL FDW callers routed through the gRPC `PgService`

The important design rule is that explicit transactions do not create a separate write path. Both caller families eventually use the same server-side transaction coordinator and the same durable commit path into the RocksDB-backed table providers.

Autocommit requests are different: they bypass transaction staging and go directly to the unified applier/provider path.

## Core Components

### Server-side transaction authority

- `TransactionCoordinator`
  File: `backend/crates/kalamdb-core/src/transactions/coordinator.rs`
  Role: source of truth for active explicit transactions, staged write sets, transaction state, ownership, and commit / rollback.

- `TransactionWriteSet`, `TransactionOverlay`, `TransactionQueryContext`
  Files under: `backend/crates/kalamdb-transactions/src/`
  Role: hold staged mutations and provide read-your-writes visibility before commit.

- `RequestTransactionState`
  File: `backend/crates/kalamdb-core/src/sql/executor/request_transaction_state.rs`
  Role: binds SQL batch request ownership to `TransactionCoordinator` for explicit SQL transactions.

### Typed execution path

- `OperationService`
  File: `backend/crates/kalamdb-core/src/operations/service.rs`
  Role: typed executor used by the PostgreSQL gRPC bridge and future transports. It stages writes when a live explicit transaction exists and uses the autocommit fast path when one does not.

- `KalamPgService`
  File: `backend/crates/kalamdb-pg/src/service.rs`
  Role: gRPC server surface for the PostgreSQL extension. It translates PG RPCs into typed domain requests and delegates transaction begin / commit / rollback to `OperationService` through `OperationExecutor`.

- `SessionRegistry`
  File: `backend/crates/kalamdb-pg/src/session_registry.rs`
  Role: tracks remote PostgreSQL sessions and pinned transaction ids for observability and cleanup. It is not the authoritative source for durable transaction state; the coordinator is.

### Durable commit path

- `UnifiedApplier` / `RaftApplier`
  File: `backend/crates/kalamdb-core/src/applier/applier.rs`
  Role: single command-application interface. All committed writes go through the Raft-backed path, even in single-node mode.

- Raft state machines
  Files: `backend/crates/kalamdb-raft/src/state_machine/user_data.rs`, `backend/crates/kalamdb-raft/src/state_machine/shared_data.rs`
  Role: apply committed transaction batches to the correct data group.

- Provider appliers and DML executor
  Files: `backend/crates/kalamdb-core/src/applier/raft/provider_user_data_applier.rs`, `backend/crates/kalamdb-core/src/applier/raft/provider_shared_data_applier.rs`, `backend/crates/kalamdb-core/src/applier/executor/dml.rs`
  Role: persist committed rows into RocksDB-backed providers, stamp `commit_seq`, and emit side effects.

### PostgreSQL bridge components

- `fdw_xact.rs`
  File: `pg/src/fdw_xact.rs`
  Role: PostgreSQL transaction hook integration. Lazily opens a remote KalamDB transaction when an explicit PostgreSQL transaction first touches a Kalam-backed foreign table, and finalizes it on PostgreSQL transaction end.

- `remote_state.rs`
  File: `pg/src/remote_state.rs`
  Role: long-lived remote gRPC session cache per PostgreSQL backend / foreign-server config.

- `fdw_modify.rs`, `fdw_scan.rs`, `write_buffer.rs`
  Files: `pg/src/fdw_modify.rs`, `pg/src/fdw_scan.rs`, `pg/src/write_buffer.rs`
  Role: statement-level bridge logic for foreign writes and reads. In autocommit mode these do not open a remote transaction. In explicit PostgreSQL transactions they reuse the remote transaction created by `fdw_xact.rs`.

## KalamDB-Native Explicit Transaction Flow

### 1. Begin

An explicit SQL batch transaction begins through `RequestTransactionState::begin()`.

That method:

1. derives an `ExecutionOwnerKey` from the request id
2. calls `TransactionCoordinator::begin()`
3. stores the returned typed `TransactionId` as the canonical transaction id for that request owner

The coordinator creates a `TransactionHandle`, captures the current committed snapshot sequence, and starts an empty in-memory write set.

### 2. Stage writes instead of applying them

During an explicit transaction, typed DML in `OperationService` does not go straight to RocksDB.

Instead:

1. `OperationService` checks whether an active transaction exists for the caller
2. `stage_insert`, `stage_update`, or `stage_delete` converts the request into `StagedMutation` values
3. `TransactionCoordinator::stage()` or `stage_batch()` stores them in the transaction write set

No durable provider write happens yet.

### 3. Read-your-writes visibility

Reads inside the same explicit transaction use `TransactionQueryContext` and `CoordinatorOverlayView`.

That means a transaction can see its own staged inserts, updates, and deletes before commit, while other callers still see only committed data.

### 4. Commit

`TransactionCoordinator::commit()` is the single server-side seal point for explicit transactions.

Commit steps:

1. validate that the transaction is still open
2. remove the staged write set from coordinator memory
3. call `app_context.applier().commit_transaction(transaction_id, mutations)`
4. route the batch through the Raft-backed apply path
5. persist committed rows through RocksDB-backed providers
6. stamp the durable `commit_seq`
7. emit notifications / publisher side effects
8. remove active transaction bookkeeping from the coordinator

The durable apply path is:

`TransactionCoordinator::commit()`

-> `UnifiedApplier::commit_transaction()`

-> Raft proposal / state machine apply

-> `ProviderUserDataApplier` or `ProviderSharedDataApplier`

-> `DmlExecutor::apply_user_transaction_batch()` or `apply_shared_transaction_batch()`

-> `UserTableProvider` / `SharedTableProvider`

-> RocksDB-backed table storage

Parquet is not the commit target for explicit transactions. RocksDB-backed hot storage is the durable write path, and Parquet remains the later flush / cold-storage path.

### 5. Rollback

`TransactionCoordinator::rollback()` removes the in-memory staged write set and clears active state.

Because explicit writes are staged until commit, rollback does not need a compensating write to RocksDB. Uncommitted changes were never durably applied.

## Constraints in the Current KalamDB Model

### Single data-group scope

Explicit transaction commit is currently constrained to one data Raft group.

`RaftApplier::transaction_group_id()` rejects a batch that spans multiple data groups. This keeps the durable apply path single-group and avoids multi-group coordination in the current design.

### Unsupported table classes

Explicit transactions reject:

- `TableType::System`
- `TableType::Stream`

This is enforced during staging and commit path validation.

## PostgreSQL Integration Model

## Session vs transaction

The PostgreSQL bridge has two different concepts:

1. Remote session
   Managed in `pg/src/remote_state.rs`.
   Opened with `OpenSession` and reused across statements for the same PostgreSQL backend / foreign-server config.

2. Remote KalamDB transaction
   Managed through `pg/src/fdw_xact.rs` plus server-side `TransactionCoordinator` state.
   Opened only when an explicit PostgreSQL transaction actually touches a Kalam-backed foreign table.

This distinction is important:

- a PostgreSQL backend can have a long-lived remote session without any active KalamDB transaction
- bare `BEGIN` does not automatically create a KalamDB transaction

## Lazy opening rule

KalamDB is only involved when PostgreSQL actually involves a Kalam-backed foreign table inside an explicit transaction block.

Current behavior:

- `BEGIN` + only native PostgreSQL statements -> no remote KalamDB transaction
- `BEGIN` + first foreign scan or foreign modify -> `fdw_xact::ensure_transaction()` opens one remote KalamDB transaction
- later foreign statements in the same PostgreSQL transaction reuse that same remote transaction id

This keeps native-only PostgreSQL transactions from paying KalamDB transaction overhead.

## PostgreSQL explicit transaction flow

### 1. PostgreSQL enters explicit block

`fdw_xact.rs` tracks whether the backend is inside a PostgreSQL transaction block.

No remote transaction is opened here.

### 2. First foreign operation touches KalamDB

In `fdw_modify.rs` and `fdw_scan.rs`, when the backend is inside an explicit PostgreSQL transaction block, the bridge calls `fdw_xact::ensure_transaction()`.

That method:

1. checks whether this PostgreSQL backend already has an active remote transaction for the current session
2. if not, calls remote `BeginTransaction`
3. stores the returned transaction id as the canonical remote transaction id for that PostgreSQL backend transaction

### 3. Remote server side

The gRPC `KalamPgService` forwards begin / commit / rollback to `OperationService`.

`OperationService` then delegates to `TransactionCoordinator`, so PostgreSQL explicit transactions and native explicit transactions converge on the same server-side authority.

### 4. Mixed native PostgreSQL + foreign KalamDB work

The supported mixed flow is:

1. `BEGIN`
2. native PostgreSQL reads / writes
3. first Kalam foreign read or write -> lazy remote `BeginTransaction`
4. more native PostgreSQL reads / writes
5. more Kalam foreign reads / writes -> reuse same remote transaction id
6. `COMMIT` or `ROLLBACK`

The PostgreSQL extension tests in `pg/tests/e2e_dml/transactional.rs` cover this mixed flow.

### 5. Commit ordering

The PostgreSQL bridge finalizes the remote KalamDB transaction in PostgreSQL `XACT_EVENT_PRE_COMMIT`.

That means:

1. pending foreign writes are flushed / staged work is ready
2. remote `CommitTransaction` is sent to KalamDB
3. only if that succeeds does PostgreSQL proceed to its final COMMIT record

If remote commit fails in `PRE_COMMIT`, the extension raises an error and PostgreSQL aborts the local transaction, which rolls back native PostgreSQL rows as well.

### 6. Rollback ordering

On PostgreSQL abort, or after a remote `PRE_COMMIT` failure, the extension discards buffered foreign writes and sends remote `RollbackTransaction`.

Because KalamDB explicit writes are staged until commit, rollback does not need an undo write on the KalamDB side.

## What is guaranteed today

- one canonical `TransactionId` per active explicit KalamDB transaction
- one server-side transaction authority: `TransactionCoordinator`
- one durable apply path for committed explicit transactions: `UnifiedApplier` -> Raft -> provider applier -> RocksDB-backed providers
- read-your-writes within one explicit transaction
- PostgreSQL lazy remote transaction opening only when a foreign table is involved
- PostgreSQL `PRE_COMMIT` remote commit before PostgreSQL final commit record
- PostgreSQL abort when remote commit fails before final PostgreSQL commit

## What is not guaranteed today

- crash-safe distributed atomicity across native PostgreSQL storage and KalamDB storage
- XA / prepared-transaction interoperability
- multi-data-group explicit transactions inside KalamDB
- savepoint / subtransaction equivalence between PostgreSQL and KalamDB

The current PostgreSQL bridge is transaction-aware, but it is not a full two-phase commit coordinator.

## Observability

Useful runtime views:

- `system.sessions`
  Shows remote PostgreSQL session state, client identity, and pinned transaction state.

- `system.transactions`
  Shows active KalamDB transaction coordinator state such as owner, origin, state, and write count.

These are the best places to verify that a PostgreSQL backend is reusing one remote transaction id across multiple foreign statements.

## Code Anchors

- `backend/crates/kalamdb-core/src/transactions/coordinator.rs`
- `backend/crates/kalamdb-core/src/sql/executor/request_transaction_state.rs`
- `backend/crates/kalamdb-core/src/operations/service.rs`
- `backend/crates/kalamdb-core/src/applier/applier.rs`
- `backend/crates/kalamdb-core/src/applier/executor/dml.rs`
- `backend/crates/kalamdb-pg/src/service.rs`
- `backend/crates/kalamdb-pg/src/session_registry.rs`
- `pg/src/fdw_xact.rs`
- `pg/src/fdw_modify.rs`
- `pg/src/fdw_scan.rs`
- `pg/src/remote_state.rs`
- `pg/tests/e2e_dml/transactional.rs`