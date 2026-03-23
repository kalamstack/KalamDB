# Research: PostgreSQL-Style Transactions for KalamDB

## Decision 1: Use snapshot isolation for explicit transactions

- Decision: Explicit `BEGIN` transactions capture a snapshot boundary at transaction start and read from that committed snapshot plus the transaction's own staged writes.
- Rationale: The feature spec requires read-your-writes and requires that a transaction not observe rows committed by another session after the first transaction has already started. Snapshot isolation satisfies both while fitting the existing MVCC `_seq` version model.
- Alternatives considered:
  - Read committed: simpler, but it fails the spec's concurrent-session acceptance scenario where a started transaction must not see later commits from another session.
  - Serializable: stronger, but materially more complex and out of scope for the first single-node implementation.

## Decision 2: Put transaction orchestration in `kalamdb-core`, not in the pg RPC layer

- Decision: Add a transaction coordinator in `backend/crates/kalamdb-core` and keep `backend/crates/kalamdb-pg` limited to session/RPC lifecycle and transport validation.
- Rationale: The pg service already forwards scans and DML to `OperationService`, which currently writes immediately through `AppContext::applier()`. True transactions must coordinate reads and writes across the shared server write path, not just pg bookkeeping. This also allows the SQL REST path to reuse the same engine.
- Alternatives considered:
  - Keep all transaction logic in `kalamdb-pg`: rejected because REST SQL would need a second transaction implementation and core write/query paths would remain unaware of transaction visibility.
  - Push transactions fully into the storage crate: rejected because the MVCC skill and repo guidance prefer visibility filtering in the query/provider path, not as a leaky storage-engine-only abstraction.

## Decision 3: Stage typed mutations in memory until commit

- Decision: Buffer typed insert/update/delete operations as a per-transaction write set, then apply them on `COMMIT` through a dedicated commit routine.
- Rationale: The existing hot path applies writes immediately and emits side effects synchronously. To support rollback and all-or-nothing commit, the server must avoid mutating shared state before commit. A staged write set keeps rollback cheap and lets commit control when downstream side effects fire.
- Alternatives considered:
  - Immediate writes plus compensating rollback: rejected because it cannot guarantee atomicity across multiple tables or prevent leaked side effects.
  - Provider-local buffering only: rejected because transactions span tables and need one authoritative commit point.

## Decision 4: Implement read-your-writes in the provider scan path

- Decision: Extend request/session context with transaction metadata and merge staged writes with committed MVCC rows in the table provider scan path and direct DML lookup helpers.
- Rationale: Existing scans already resolve MVCC versions in `kalamdb-tables` providers and `base_scan`/`scan_rows`. The MVCC guidance says visibility belongs in the query path. Overlaying the transaction write set there preserves DataFusion planning and avoids SQL rewrites.
- Alternatives considered:
  - Rewrite SQL queries with transaction filters: rejected by repo guidance and would add hot-path parsing overhead.
  - Hide visibility entirely in RocksDB access methods: rejected because reads also combine Parquet plus RocksDB and already rely on provider-level resolution.

## Decision 5: Keep the pg RPC wire contract unchanged while making the returned transaction ID canonical end-to-end

- Decision: Preserve the existing `BeginTransaction`, `CommitTransaction`, and `RollbackTransaction` message shapes and continue binding DML/scan behavior to `session_id` on the server, but treat the `transaction_id` returned by `BeginTransaction` as the canonical correlation ID across staged mutations, observability, cleanup, and durable commit.
- Rationale: pg_kalam already issues the needed RPCs and tracks `session_id` plus `transaction_id`. Making that same `transaction_id` canonical end-to-end satisfies the requirement to track one transaction identity from pg_kalam until the transaction reaches RocksDB without forcing new wire fields on every DML request.
- Alternatives considered:
  - Add `transaction_id` to every scan/DML request: rejected for the first phase because it would force extension, client, and service API changes even though the session registry already tracks the active transaction.
  - Generate a second internal transaction identifier in the server: rejected because it would make observability and cleanup harder to reason about and would break the user's requirement for one stable transaction identity.

## Decision 6: Support KalamDB server SQL transaction statements through the existing SQL execution path

- Decision: KalamDB's SQL execution path itself supports `BEGIN`/`COMMIT`/`ROLLBACK`, exposed first through the existing `/v1/api/sql` multi-statement request flow. One request may contain multiple sequential transaction blocks, but no transaction created by `/v1/api/sql` may survive past the end of that request.
- Rationale: The user requirement is that transaction SQL statements work in KalamDB server itself, not only through pg RPC. The current SQL API is stateless HTTP with no transaction session token in `QueryRequest`, so request-scoped SQL batches are the lowest-risk way to make the server transaction-aware now while reusing the same core transaction engine as pg. Because the endpoint is stateless, all request-scoped transactions must be shut down automatically when the request finishes.
- Alternatives considered:
  - Add persistent HTTP transaction sessions: rejected for this phase because it introduces new transport/session contracts and is not required for pg_kalam.
  - Exclude SQL API transaction support entirely: rejected because the spec requires direct transaction control outside pg_kalam and the existing batch endpoint can satisfy that requirement.

## Decision 6A: Separate the `/v1/api/sql` transaction model from the pg gRPC transaction model

- Decision: `/v1/api/sql` transactions are request-scoped and may appear multiple times sequentially within one request, while pg gRPC transactions are session-scoped and intentionally span multiple network calls until explicit commit/rollback or session close.
- Rationale: This matches the current transport designs. The SQL HTTP endpoint has a single request body containing multiple statements and no transaction token for future requests. The pg gRPC path already has `session_id` and `transaction_id`, making it appropriate for multi-call transaction lifecycles.
- Alternatives considered:
  - Make `/v1/api/sql` mimic pg session-spanning transactions: rejected because the current REST contract has no persistent transaction session handle.
  - Force pg gRPC transactions to fit in one call: rejected because it would break the intended PostgreSQL FDW transaction lifecycle.

## Decision 7: Use the storage layer's atomic commit primitive for durable commit while keeping transaction orchestration storage-engine portable

- Decision: Stage all transaction writes outside shared state, then apply the final commit with the storage layer's atomic commit primitive so either the whole durable write succeeds or none of it is applied. The current implementation can use RocksDB-backed atomic batch semantics behind that abstraction, but transaction orchestration itself must not depend on RocksDB-specific public APIs.
- Rationale: The current storage layer already relies on RocksDB atomic batching for entity and index updates, which is the right primitive today. However, repository guidance requires keeping storage-engine construction and specifics behind the storage boundary so future migration to a different engine remains possible without rewriting transaction semantics.
- Alternatives considered:
  - Apply writes eagerly and depend on compensating undo: rejected because side effects and cross-table atomicity become fragile.
  - Introduce RocksDB `TransactionDB` as a first step: rejected for now because the existing stack already depends on atomic batch operations and the feature only needs one atomic durable apply step per commit.
  - Build transaction orchestration directly on RocksDB types in `kalamdb-core`: rejected because it would make future storage-engine migration materially harder.

## Decision 8: Mirror the PostgreSQL extension pattern used by ParadeDB for pending state finalization and abort cleanup

- Decision: Follow the same lifecycle pattern used by ParadeDB's PostgreSQL extension internals: keep pending extension-owned work in transaction-scoped state, finalize that work only at PostgreSQL `PreCommit`, and drop pending state on PostgreSQL `Abort`.
- Rationale: ParadeDB uses `pgrx::register_xact_callback` to defer finalization of pending insert state to `PreCommit` and to clear logical worker state on `Abort`. That is the right model for pg_kalam too: the FDW should continue to drive the lifecycle from PostgreSQL transaction callbacks, while the KalamDB server uses the same canonical transaction ID to stage work and to discard it when the PostgreSQL session aborts or closes before commit.
- Alternatives considered:
  - Finalize remote work before PostgreSQL commit: rejected because it can expose committed remote state before PostgreSQL decides the transaction outcome.
  - Depend only on request boundaries instead of transaction callbacks: rejected because it does not mirror PostgreSQL transaction semantics closely enough.

## Decision 9: Roll back active transactions on session close, timeout, and incomplete SQL batches

- Decision: Active transactions are aborted on `CloseSession`, pg backend disconnect, configured transaction timeout, or when a SQL batch ends with an open transaction.
- Rationale: The current pg session lifecycle already closes sessions on PostgreSQL backend exit. Extending cleanup to abort staged writes prevents orphaned state and aligns with the feature's safety requirements.
- Alternatives considered:
  - Leave orphaned transactions for manual cleanup: rejected because it leaks memory and violates rollback guarantees.
  - Auto-commit on disconnect: rejected because it is incompatible with PostgreSQL semantics.

## Decision 10: Preserve a near-zero-cost path for non-transaction requests

- Decision: Requests that do not use explicit transactions continue through the existing autocommit path with only a cheap active-transaction presence check and no transaction overlay construction.
- Rationale: The repository guidance is performance-first, and the feature must not materially slow normal writes or reads when transactions are unused. The transaction coordinator should therefore be opt-in and bypassed quickly for ordinary requests.
- Alternatives considered:
  - Route every request through transaction machinery: rejected because it would add avoidable allocations and branching to the hot path.

## Decision 11: Limit Phase 1 to single-node transactional durability

- Decision: The initial plan guarantees explicit transactions only within one KalamDB server node and one local storage domain.
- Rationale: The spec already scopes distributed and Raft-spanning transactions out of the first implementation. This keeps the design aligned with the existing single-node `OperationService` and provider architecture.
- Alternatives considered:
  - Cross-Raft-group transactions: rejected as a separate consensus and recovery problem.
  - Savepoints and nested transactions: rejected as out of scope and unnecessary for the pg_kalam driver goal.

## Decision 12: Limit explicit transaction support to user and shared tables in Phase 1

- Decision: Explicit transactions in this phase apply only to user tables and shared tables. Stream tables are rejected when used inside an explicit transaction.
- Rationale: Stream tables currently do not support transactions, and their delivery/ordering semantics need a separate design. Making the scope explicit avoids overpromising and keeps the first implementation aligned with current capabilities.
- Alternatives considered:
  - Attempt partial stream-table transaction support now: rejected because it would introduce undefined behavior around stream semantics and expand the feature surface too far.
