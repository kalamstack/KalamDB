# ADR-018: Transaction Architecture and PostgreSQL Bridge Semantics

**Status**: Accepted  
**Date**: 2026-04-22  
**Related**: ADR-009 (Three-Layer Architecture), docs/architecture/transactions.md, specs/026-postgres-extension/transactional-fdw.md

## Context

KalamDB now has two important transaction entry points:

1. native KalamDB explicit transactions driven by request-scoped SQL execution and typed services
2. PostgreSQL FDW explicit transactions driven remotely through gRPC `PgService`

The design needed to answer four questions clearly:

1. What is the authoritative source of transaction state?
2. Does PostgreSQL `BEGIN` always force KalamDB to open a transaction?
3. Which transaction id is canonical once a PostgreSQL transaction does involve KalamDB?
4. What commits first when PostgreSQL and KalamDB are mixed in one explicit transaction?

## Decision

### 1. `TransactionCoordinator` is the single server-side transaction authority

All explicit KalamDB transactions, regardless of caller, are owned by `TransactionCoordinator`.

`SessionRegistry` and PostgreSQL backend-local extension state may cache or pin transaction ids for cleanup and observability, but they are not the source of truth for durable transaction state.

### 2. PostgreSQL does not open a KalamDB transaction on bare `BEGIN`

The PostgreSQL extension must open a remote KalamDB transaction lazily, only when an explicit PostgreSQL transaction first touches a Kalam-backed foreign table.

This avoids paying transaction overhead for native-only PostgreSQL transactions.

### 3. The canonical transaction id is the one returned by KalamDB `BeginTransaction`

Once the PostgreSQL bridge opens a remote KalamDB transaction, the returned `TransactionId` is preserved across:

- PostgreSQL FDW state
- gRPC service state
- coordinator state
- staged mutations
- observability (`system.sessions`, `system.transactions`)
- commit / rollback cleanup

We do not synthesize a parallel PostgreSQL-side transaction identifier for KalamDB durability.

### 4. Explicit commits use the same durable path for all callers

Committed explicit transactions always flow through:

`TransactionCoordinator`

-> `UnifiedApplier`

-> Raft state machine apply

-> provider applier

-> RocksDB-backed table providers

Explicit transactions do not use a second direct-write path.

### 5. PostgreSQL remote commit happens at `PRE_COMMIT`

For mixed native PostgreSQL + KalamDB transactions, the extension finalizes KalamDB during PostgreSQL `XACT_EVENT_PRE_COMMIT`.

That means:

- remote KalamDB commit happens before PostgreSQL reaches its final COMMIT record
- if remote commit fails at `PRE_COMMIT`, PostgreSQL aborts and local PostgreSQL writes roll back

### 6. Full distributed atomicity remains out of scope

The current model is transaction-aware but not crash-safe distributed 2PC.

We explicitly do not claim:

- XA semantics
- prepared-transaction interoperability
- crash-safe atomicity across PostgreSQL local storage and KalamDB storage

## Rationale

### Why keep one coordinator?

Duplicating transaction state across PG bridge state, request state, and commit logic would create split-brain failure modes. Keeping `TransactionCoordinator` authoritative ensures one state machine for begin / stage / commit / rollback.

### Why lazy-open for PostgreSQL?

Many PostgreSQL transactions never touch a Kalam-backed foreign table. Opening a remote KalamDB transaction on every `BEGIN` would add unnecessary RPCs, state, and failure surface for no benefit.

### Why preserve the KalamDB transaction id?

The KalamDB transaction id is already the durable identity used by coordinator state, staged mutations, metrics, and cleanup. Preserving that id end-to-end avoids glue code that maps one transaction namespace into another.

### Why commit at `PRE_COMMIT`?

If remote commit waited until PostgreSQL had already fully committed, a remote failure could no longer abort native PostgreSQL rows. `PRE_COMMIT` is the last point where PostgreSQL can still abort the local transaction.

## Consequences

### Positive

- One explicit transaction authority across SQL/API and PostgreSQL callers
- No remote KalamDB transaction for native-only PostgreSQL `BEGIN` / `COMMIT` blocks
- No separate explicit-transaction write path or ad-hoc glue layer
- PostgreSQL can reuse one remote transaction id across multiple foreign statements inside one explicit block
- Remote commit failure still aborts the PostgreSQL transaction before final local commit

### Negative

- The system still does not offer crash-safe PG + KalamDB distributed atomicity
- Explicit transactions are still limited to one data Raft group
- PostgreSQL savepoints / subtransactions are not fully modeled in KalamDB yet

### Operational implications

- `system.sessions` and `system.transactions` are the primary runtime views for verifying PG bridge transaction behavior
- Architecture changes touching transaction boundaries must update both the overview doc and this ADR

## Implementation Notes

Relevant code anchors:

- `backend/crates/kalamdb-core/src/transactions/coordinator.rs`
- `backend/crates/kalamdb-core/src/operations/service.rs`
- `backend/crates/kalamdb-core/src/applier/applier.rs`
- `backend/crates/kalamdb-core/src/applier/executor/dml.rs`
- `backend/crates/kalamdb-pg/src/service.rs`
- `backend/crates/kalamdb-pg/src/session_registry.rs`
- `pg/src/fdw_xact.rs`
- `pg/src/fdw_modify.rs`
- `pg/src/fdw_scan.rs`
- `pg/src/remote_state.rs`

## Follow-up Guidance

If future work adds:

- savepoints / subtransactions
- multi-group explicit transactions
- prepared transactions
- crash-safe PG + KalamDB atomicity

then this ADR must be revisited and either amended or superseded by a new ADR.