# Transactional FDW Semantics for KalamDB PostgreSQL Extension

## Status Note

This document now applies to the **remote-first** architecture.

The earlier version of the design described transaction semantics around an embedded executor. The same intent remains valid, but the primary execution model is now:

* PostgreSQL transaction hooks in the FDW
* a remote KalamDB transaction handle
* a remote KalamDB session associated with the PostgreSQL backend connection
* commit/rollback coordination between PostgreSQL and the remote KalamDB server

Embedded mode, while temporarily retained, should reuse the same logical transaction contract in-process where practical.

## Objective

Define how `kalam-pg` should participate in PostgreSQL transaction semantics so that foreign-table DML against KalamDB can be committed or rolled back together with the surrounding PostgreSQL transaction.

The immediate goal is application-visible correctness:

* `BEGIN`
* multiple `INSERT` / `UPDATE` / `DELETE` statements against Kalam foreign tables
* error or explicit `ROLLBACK`
* no durable KalamDB changes remain visible

This document does **not** require full distributed two-phase commit in the first phase.

## Historical Current State vs Updated Target

Earlier direction:

1. PostgreSQL parses the SQL statement.
2. `kalam-pg` FDW callbacks convert the row change into a typed request.
3. The embedded executor calls direct provider methods such as:
   * `insert_rows`
   * `update_row_by_pk`
   * `delete_row_by_pk`
4. The mutation is applied immediately.

As a result, PostgreSQL rollback would not imply KalamDB rollback.

Updated target direction:

1. PostgreSQL enters a transaction.
2. The first Kalam foreign operation opens or reuses a remote KalamDB session for that PostgreSQL backend connection.
3. Lazy transaction state creation happens inside that established remote session.
4. The FDW opens or reuses a remote KalamDB transaction handle.
5. All subsequent scans/mutations use that same session and transaction context.
6. PostgreSQL commit/abort hooks finalize the remote transaction exactly once.

The correctness goal is unchanged; only the primary execution target has shifted from embedded-first to remote-first.

## Requirement Levels

### Phase 1: Transaction-aware foreign DML

Required behavior:

* all foreign-table DML in one PostgreSQL top-level transaction must use one KalamDB transaction context
* if PostgreSQL aborts, KalamDB must roll back that transaction
* if PostgreSQL commits, KalamDB must commit that transaction
* reads within the same transaction should observe prior uncommitted foreign writes from the same transaction

Out of scope for Phase 1:

* prepared transactions
* crash-safe distributed atomicity across local PostgreSQL tables and KalamDB tables
* XA / 2PC / external transaction coordinator support

### Phase 2: Subtransactions and savepoints

Required behavior:

* `SAVEPOINT`
* nested PL/pgSQL exception blocks
* subtransaction aborts

This requires explicit mapping between PostgreSQL subtransactions and KalamDB nested transaction state or savepoint equivalents.

### Phase 3: Distributed atomicity

Optional future work:

* atomic behavior across PostgreSQL local tables and KalamDB foreign tables under crash conditions
* likely requires prepared transactions and explicit two-phase commit support on the KalamDB side

## FDW Requirements

## 1. PostgreSQL transaction callback integration

The extension must register transaction lifecycle hooks:

* top-level commit callback
* top-level abort callback
* subtransaction commit callback
* subtransaction abort callback

The extension must maintain backend-local transaction state keyed to the current PostgreSQL backend/session.

## 2. Lazy foreign transaction start

The extension must **not** start a KalamDB transaction for every PostgreSQL transaction automatically.

Instead:

* start a KalamDB transaction lazily on the first foreign read/write that requires transactional participation
* reuse that transaction for subsequent foreign operations in the same PostgreSQL transaction

This avoids unnecessary overhead for PostgreSQL transactions that never touch Kalam foreign tables.

In the remote-first model, this means lazily opening a remote transaction handle. In temporary embedded mode, it means lazily allocating the equivalent in-process transaction context.

The transaction handle should be session-scoped rather than free-floating, so current schema and execution identity stay aligned across requests in the same PostgreSQL backend.

Before transaction hooks are layered in, the remote path must first prove a stable authenticated session handshake:

* `ping` verifies transport reachability
* `open_session` verifies auth and backend-local session reuse
* session open/update carries PostgreSQL current schema so subsequent transaction state is attached to the correct namespace context

## 3. Per-backend transaction state

The extension must store per-backend FDW transaction state containing at least:

* PostgreSQL top-level transaction identifier or equivalent backend-local marker
* optional KalamDB transaction handle
* optional remote session identifier
* nested/savepoint bookkeeping for subtransactions
* whether the transaction has performed writes
* whether the transaction has already been finalized

## 4. Read-your-writes semantics

Foreign scans executed after a foreign mutation in the same PostgreSQL transaction must use the same KalamDB transaction context or snapshot.

Without this, application behavior will be inconsistent.

## 5. Statement failure behavior

If a PostgreSQL statement affecting a Kalam foreign table fails before overall transaction commit, the extension must ensure KalamDB state associated with that failed PostgreSQL transaction does not become durable.

## KalamDB Requirements

KalamDB must expose a transaction-capable API suitable for FDW integration.

Minimum required primitives:

* `begin_transaction()`
* `commit_transaction(tx)`
* `rollback_transaction(tx)`
* transaction-aware `scan`
* transaction-aware `insert`
* transaction-aware `update`
* transaction-aware `delete`

Strongly recommended:

* nested savepoints or subtransaction markers
* transaction-local visibility / read-your-writes
* idempotent rollback on already-aborted handles
* explicit transaction timeout / cleanup for abandoned handles
* session-aware transaction binding so schema/default namespace does not drift during a transaction

## Execution Model

### Proposed flow

1. PostgreSQL enters transaction.
2. First Kalam foreign operation occurs.
3. FDW allocates backend-local session + transaction state.
4. FDW opens or reuses a remote KalamDB session.
5. FDW opens a KalamDB transaction lazily within that session.
6. Every subsequent scan/mutation uses the same KalamDB session and transaction handle.
7. PostgreSQL ends transaction.
8. FDW callback maps outcome:
   * commit -> KalamDB commit
   * abort -> KalamDB rollback

In the remote-first target, step 4 is a remote `begin_transaction` call and step 7 is remote `commit_transaction` / `rollback_transaction` coordination.

### Savepoint flow

If PostgreSQL enters a subtransaction:

* FDW must record the nesting level
* if KalamDB supports savepoints, create one
* on subtransaction abort, roll back to that savepoint
* on subtransaction release, collapse the nested state into the parent

If KalamDB cannot support savepoints, then PostgreSQL savepoint semantics are only partially implementable and must be documented as unsupported.

## Consistency Guarantees

### Supported in Phase 1

* rollback-on-error for foreign-only DML in one PostgreSQL transaction
* explicit `COMMIT` / `ROLLBACK` behavior for Kalam foreign tables
* read-your-writes for foreign reads and writes in one PostgreSQL transaction

### Not guaranteed in Phase 1

* crash-safe atomicity across PostgreSQL local storage and KalamDB foreign storage
* full distributed transaction semantics
* prepared transaction interoperability

This limitation must be documented clearly in user-facing docs.

## API Shape Recommendation

To minimize future rework, the FDW request layer should add optional transaction metadata now, even if the initial implementation is a no-op.

Suggested request metadata:

* `transaction_id`
* `subtransaction_depth`
* `is_read_only`
* `statement_id` or mutation sequence

This keeps the transport and embedded APIs aligned across future remote and embedded implementations.

## Temporary Embedded Compatibility

If embedded mode remains temporarily, it should reuse the same logical transaction model:

* lazy transaction creation
* shared transaction metadata fields
* identical commit/rollback semantics from the FDW point of view

The difference should be transport, not behavior.

## Non-Goals

This document does not require:

* SQL string forwarding for DML
* PostgreSQL becoming KalamDB's storage engine
* embedding PostgreSQL transaction manager logic inside KalamDB

The intended model remains:

* PostgreSQL parses SQL
* FDW converts operations into typed requests
* KalamDB executes them under a transaction handle

## Acceptance Criteria

This requirement is considered satisfied when all of the following are true:

1. `BEGIN; INSERT ...; INSERT ...; ROLLBACK;` leaves no visible KalamDB changes.
2. `BEGIN; UPDATE ...; DELETE ...; COMMIT;` persists both changes exactly once.
3. reads inside the same PostgreSQL transaction can see earlier uncommitted foreign writes from that same transaction.
4. a PostgreSQL transaction callback finalizes every opened KalamDB transaction exactly once.
5. if savepoints are claimed as supported, nested rollback behavior is covered by integration tests.
