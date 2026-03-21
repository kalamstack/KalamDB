# Direct EntityStore DML Path for PostgreSQL FDW

## Status Note

This requirement remains active, but it should now be read as **remote-first**:

* the FDW builds typed mutation requests
* the remote KalamDB server executes those requests through the lowest practical typed write path
* embedded mode, while temporarily retained, should reuse the same logical mutation service in-process
* the direct write implementation should stay centralized and cautious so it does not destabilize existing server behavior

Nothing from the earlier design intent is discarded. This document preserves the previous reasoning and acceptance criteria while shifting the primary target from embedded execution to the remote server path.

## Objective

Define a lower-overhead DML path for PostgreSQL FDW mutations so that `INSERT`, `UPDATE`, and `DELETE` against Kalam foreign tables bypass unnecessary SQL-layer translation and write directly through the typed table/provider path.

The goal is to reduce write latency and allocation overhead while preserving:

* correctness
* access control
* live subscriptions / live query notifications
* future transactional compatibility

## Current State and Target State

The earlier design direction assumed an embedded executor path:

1. PostgreSQL parses the SQL statement.
2. FDW callback extracts row values from tuple slots.
3. FDW builds typed requests:
   * `InsertRequest`
   * `UpdateRequest`
   * `DeleteRequest`
4. Embedded executor dispatches to provider methods.

That structure was already better than sending SQL strings back into KalamDB.

The updated target state keeps the same typed request construction, but the main path is now:

1. PostgreSQL parses the SQL statement.
2. FDW callback extracts row values from tuple slots.
3. FDW builds typed mutation requests.
4. Remote transport sends those requests to KalamDB.
5. KalamDB executes through direct provider / `EntityStore` / `IndexedEntityStore` style mutation paths.

The preferred implementation location for the remote path is a dedicated backend crate such as `kalamdb-pg`, not broad PostgreSQL-specific changes scattered through `kalamdb-server`.

Where practical, that remote mutation service should delegate into existing core write primitives such as the applier / DML executor path rather than creating a separate PostgreSQL-only mutation engine.

The design rule stays the same:

FDW DML must use the lowest practical typed write path and must **not** re-enter a general SQL execution layer for routine mutations.

## Requirement

For PostgreSQL FDW DML, the implementation must use direct typed mutation paths backed by provider / `EntityStore`-style operations rather than SQL text execution.

This means:

* `INSERT` must write through direct typed row insertion
* `UPDATE` must write through direct typed primary-key update or equivalent targeted mutation path
* `DELETE` must write through direct typed primary-key delete or soft-delete path

The FDW must not take a mutation like:

* `INSERT INTO ...`
* `UPDATE ...`
* `DELETE ...`

and repackage it as a generic SQL string for execution through the main SQL parser unless a specific operation is unsupported by the direct path.

In remote mode this prohibition applies to the server-side execution layer. In temporary embedded mode it applies to the in-process executor path as well.

## Why This Requirement Exists

Routing FDW DML through generic SQL execution adds avoidable overhead:

* SQL text generation
* SQL parsing
* planning / normalization work
* extra translation layers
* harder transactional mapping

FDW DML already has structured information available from PostgreSQL execution state, so the extension should exploit that structure directly.

## Remote-First Architectural Rule

The intended primary path is:

1. PostgreSQL tuple extraction
2. typed mutation request construction in the FDW
3. transport of that typed mutation request to KalamDB
4. `kalamdb-pg` remote service receives the request
5. authorization / tenant validation on the server
6. direct provider / `EntityStore` / `IndexedEntityStore` mutation
7. shared live-subscription notification pipeline
8. transaction bookkeeping if a foreign transaction is active

The following path should be avoided for standard FDW DML:

1. generate SQL text
2. send SQL text into KalamDB SQL engine
3. parse and plan it again
4. then mutate storage

## Live Subscription Requirement

Direct writes must still participate in live subscriptions and live-query invalidation.

This is mandatory.

The performance optimization is only valid if change propagation semantics remain unchanged from the user's perspective.

### Required behavior

Before or during the direct mutation path, the implementation must pass through the same subscription / notification pipeline that SQL-driven writes would use.

That means:

* affected live subscriptions must be detectable
* row changes must be published to the live query system
* WebSocket subscribers must observe the mutation
* subscription filtering and row visibility rules must still be respected

### Design rule

The direct `EntityStore` / provider mutation path must invoke a shared mutation publication layer rather than bypassing it.

This publication layer should be the single place responsible for:

* pre-checking whether subscriptions exist for the affected table / namespace
* materializing change events in the expected shape
* notifying live-query and WebSocket systems

## Pre-Check Requirement

To avoid unnecessary work on hot write paths, the implementation should support a fast pre-check for live subscription relevance.

Expected behavior:

1. inspect whether any live subscriptions are registered for the affected table / namespace
2. if none exist, avoid expensive change materialization work
3. if subscriptions exist, run the full notification path

This pre-check must be lightweight and must not change correctness.

## Authorization Requirement

Before mutating data, the execution path must validate:

* authenticated caller/session context
* table access permissions
* tenant/user scoping
* mutation permissions for the target table family

This validation must happen on the KalamDB side even if PostgreSQL already supplied session context.

## EntityStore Scope

This document uses "EntityStore" broadly to mean the lowest-level typed write path available in KalamDB's runtime architecture.

The exact implementation may be:

* table provider mutation methods
* store-backed typed mutation helpers
* a shared write service built on `EntityStore` / `IndexedEntityStore` primitives

The requirement is about **bypassing generic SQL execution**, not about forcing one exact internal type name.

## Interaction With Transactions

This optimization must remain compatible with future transaction support.

Therefore the direct DML path must be designed so that it can accept an optional transaction handle or execution context.

When transactional FDW semantics are added:

* direct mutations must use the active KalamDB transaction
* notification dispatch may need commit-aware behavior depending on final transaction design

The direct path must not assume immediate autonomous commit forever.

## Temporary Embedded Compatibility

If embedded mode remains for a transition period, it should:

* call the same logical mutation service in-process
* preserve the same authorization, notification, and transaction semantics
* avoid introducing a separate embedded-only mutation contract

The difference should be transport, not semantics.

## Implementation Caution

Because this path touches stable write behavior, implementation should prefer:

* one shared remote mutation service entrypoint
* reuse of existing direct provider methods already used by the fast DML path
* minimal changes to `kalamdb-server` startup/wiring
* no duplication of DML logic across HTTP handlers, SQL handlers, and PG transport handlers

The goal is to add a new access path, not to refactor stable write-path internals more than necessary for correctness.

## Acceptance Criteria

This requirement is satisfied when all of the following are true:

1. FDW `INSERT` / `UPDATE` / `DELETE` use direct typed mutation calls, not SQL text execution.
2. In remote mode, the KalamDB server executes those requests through direct typed mutation calls, not SQL text execution.
3. live subscriptions still receive updates from FDW-driven mutations.
4. a fast subscription pre-check avoids unnecessary event-materialization work when no subscribers exist.
5. the direct mutation path remains compatible with future transaction-handle propagation.
