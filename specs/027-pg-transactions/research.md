# Research: PostgreSQL-Style Transactions for KalamDB

## Decision 1: Use snapshot isolation for explicit transactions with a global commit sequence counter

- Decision: Explicit `BEGIN` transactions capture a snapshot boundary at transaction start using a global monotonic `commit_seq` counter (not the per-row `_seq` Snowflake ID). The transaction reads from committed data with `commit_seq <= snapshot_commit_seq` plus the transaction's own staged writes.
- Rationale: The feature spec requires read-your-writes and requires that a transaction not observe rows committed by another session after the first transaction has already started. Snapshot isolation satisfies both. However, the per-row `_seq` column (Snowflake ID from `SystemColumnsService`) is assigned at write time, not commit time — a concurrent transaction may receive a higher `_seq` but commit earlier. Using `_seq` as the snapshot boundary would produce incorrect visibility. A dedicated `commit_seq` counter, incremented by `TransactionCoordinator` on each successful commit, is the correct MVCC boundary. Each committed write set is tagged with its `commit_seq`, and snapshots read "all data with `commit_seq <= my_snapshot`".
- Alternatives considered:
  - Read committed: simpler, but it fails the spec's concurrent-session acceptance scenario where a started transaction must not see later commits from another session.
  - Serializable: stronger, but materially more complex and out of scope for the first single-node implementation.
  - Use `_seq` (Snowflake ID) as snapshot boundary: rejected because `_seq` is assigned at row write time, not transaction commit time. Two concurrent transactions can interleave `_seq` assignments in ways that break snapshot visibility. A global `commit_seq` provides a clean, monotonic commit ordering.
  - Use wallclock timestamps: rejected because clock skew and non-monotonicity make this unreliable even on a single node.

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

## Decision 7: Commit staged mutations through DmlExecutor (post-Raft apply layer), not through raw StorageBackend::batch()

- Decision: On transaction commit, replay all staged mutations through `DmlExecutor` (the post-Raft apply layer in `kalamdb-core`), which handles `_seq` generation, index updates, notification emission, publisher events, live query notifications, manifest updates, and file reference tracking. Do NOT commit through raw `StorageBackend::batch()`.
- Rationale: All writes in KalamDB currently flow through `UnifiedApplier` → `RaftApplier` → Raft consensus → state machine → `CommandExecutorImpl::dml()` → `DmlExecutor` → providers. The `DmlExecutor`/provider layer is where `_seq` is assigned, secondary indexes are updated, live query notifications fire, and topic publishing happens. Writing directly to `StorageBackend::batch()` would bypass all of that, producing rows without `_seq`, without index entries, without notifications, and without Raft log entries. For Phase 1 single-node, the commit path should propose a `TransactionCommit` command through Raft so the entire staged write set is applied atomically in one Raft apply cycle by `DmlExecutor`.
- Alternatives considered:
  - `StorageBackend::batch()` directly: rejected because it bypasses Raft consensus, MVCC `_seq` generation, secondary indexes, notifications, publisher events, and the entire provider layer.
  - Replay staged mutations as individual Raft proposals: rejected because if the server crashes after proposal N of M, you get partial commit. The entire transaction must be a single Raft proposal for atomicity.
  - Apply writes eagerly and depend on compensating undo: rejected because side effects and cross-table atomicity become fragile.
  - Introduce RocksDB `TransactionDB` as a first step: rejected for now because the existing stack already depends on atomic batch operations and the feature only needs one atomic durable apply step per commit.

## Decision 7A: TransactionCoordinator depends on Arc<AppContext>, not Arc<dyn StorageBackend>

- Decision: The `TransactionCoordinator` takes `Arc<AppContext>` as its dependency, not `Arc<dyn StorageBackend>`.
- Rationale: `StorageBackend` is a raw key-value interface (`put`/`get`/`batch`). The transaction coordinator needs to commit through typed table providers via `DmlExecutor`, which handles MVCC `_seq` generation, index updates, notification emission, publisher events, live query notifications, manifest updates, and file reference tracking. It also needs access to `SchemaRegistry` for table validation. All of these are available through `AppContext`. Committing through `StorageBackend` would skip the entire provider layer.
- Alternatives considered:
  - `Arc<dyn StorageBackend>`: rejected because it's the wrong abstraction level — transaction commit needs typed provider operations, not raw K/V writes.
  - Passing individual dependencies: rejected because `AppContext` is the established pattern for dependency injection in `kalamdb-core` and adding 5+ individual Arc parameters would be unwieldy.

## Decision 7B: Add TransactionCommit Raft command variant for atomic cross-table commit

- Decision: Add a `TransactionCommit(TransactionId, Vec<StagedMutation>)` variant to the Raft data command types. The Raft state machine applies the entire batch atomically in one apply cycle through `DmlExecutor`.
- Rationale: For true atomicity (all-or-nothing across tables), the commit must be a single Raft proposal. Currently, each INSERT/UPDATE/DELETE is a separate Raft proposal. If staged mutations were replayed as N separate Raft proposals and the server crashed after proposal K, you'd have partial commit — violating transaction atomicity. A single `TransactionCommit` command ensures the state machine processes all mutations together.
- Alternatives considered:
  - N separate Raft proposals per staged mutation: rejected because partial application on crash breaks atomicity.
  - Skip Raft entirely for transaction commit: rejected because it breaks replication consistency in cluster mode and loses durability guarantees.

## Decision 7C: Prepare Raft commands for future distributed transactions by adding Option<TransactionId>

- Decision: Add `Option<TransactionId>` to `UserDataCommand` and `SharedDataCommand` Raft enums now, defaulting to `None` for non-transactional operations.
- Rationale: If this field is not added now, any future distributed transaction support will require rewriting the Raft protocol and breaking the serialized log format. Adding an optional field now is zero-cost for non-transactional operations (serialized as a single byte) and prevents a breaking protocol change later. The `TransactionCommit` batch command uses this field; individual autocommit mutations leave it as `None`.
- Alternatives considered:
  - Add field later when distributed transactions are needed: rejected because it would require a Raft log migration, which is complex and disruptive.
  - Use a separate Raft group for transactions: rejected as unnecessary complexity for Phase 1.

## Decision 8: Mirror the PostgreSQL extension pattern used by ParadeDB for pending state finalization and abort cleanup

- Decision: Follow the same lifecycle pattern used by ParadeDB's PostgreSQL extension internals: keep pending extension-owned work in transaction-scoped state, finalize that work at PostgreSQL `XACT_EVENT_COMMIT` (the point at which the transaction is durably committed), and drop pending state on PostgreSQL `Abort`.
- Rationale: ParadeDB uses `pgrx::register_xact_callback` to manage transaction-scoped state. The existing pg_kalam code in `fdw_xact.rs` already fires its commit action at `XACT_EVENT_COMMIT` (not `PRE_COMMIT`). This is the correct hook point because at `XACT_EVENT_COMMIT` the PostgreSQL WAL commit record has been flushed, so if the KalamDB remote commit fails, PostgreSQL has already committed — but that risk is acceptable for FDW semantics and matches the architectural pattern of "best-effort remote finalization after local commit." The plan must align with the existing `fdw_xact.rs` implementation rather than incorrectly specifying `PRE_COMMIT`.
- Alternatives considered:
  - Use `PRE_COMMIT` hook: rejected because the existing pg_kalam `fdw_xact.rs` code uses `XACT_EVENT_COMMIT`, and PRE_COMMIT would require the remote server to be available during the commit critical path — any network failure would abort the PostgreSQL transaction.
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

## Decision 13: TransactionId must follow the established type-safe newtype ID pattern

- Decision: Define `TransactionId` in `kalamdb-commons/src/models/ids/` following the established newtype pattern with `StorageKey`, `Display`, `FromStr`, `Serialize`/`Deserialize`, `From<String>`/`From<&str>`, and validation logic. Use UUID v7 (time-ordered) as the underlying format.
- Rationale: The codebase has an established pattern for type-safe IDs (`NamespaceId`, `TableId`, `UserId`, `StorageId`) in `kalamdb-commons/src/models/ids/`. Using a raw `String` for transaction identifiers (as the current pg session registry does) loses type safety and violates the project's coding guidelines. The newtype pattern prevents accidental mixing of transaction IDs with other string values, enables StorageKey derivation for RocksDB persistence, and provides validated construction. UUID v7 gives time-ordering for debugging.
- Alternatives considered:
  - Raw `String`: rejected because it violates type-safety guidelines and enables accidental misuse.
  - Numeric auto-increment: rejected because it doesn't provide enough entropy for concurrent transaction creation and is harder to correlate across pg sessions.
  - UUID v4: rejected in favor of UUID v7 because time-ordering aids debugging and log analysis.

## Decision 14: Replace the existing pg TransactionState enum with a unified version in kalamdb-commons

- Decision: Move `TransactionState` from `kalamdb-pg/src/session_registry.rs` (currently has 3 variants: `Active`, `Committed`, `RolledBack`) to `kalamdb-commons` and extend it with the additional states needed by the transaction coordinator (5 variants: `Active`, `Committing`, `Committed`, `RolledBack`, `TimedOut`). Update the pg crate to use the commons version.
- Rationale: The pg crate currently defines its own `TransactionState` with 3 variants. The transaction coordinator needs additional states (`Committing` for the in-flight commit phase, `TimedOut` for transaction timeout). Having two `TransactionState` enums in different crates creates confusion and type collision. A single enum in commons, used by both `kalamdb-core` and `kalamdb-pg`, prevents variant drift and simplifies state machine reasoning.
- Alternatives considered:
  - Keep separate enums: rejected because it creates maintenance burden and potential semantic drift.
  - Add new states only to pg crate: rejected because the coordinator in `kalamdb-core` is the authoritative state owner, not the pg transport layer.

## Decision 15: Use a TransactionOverlayExec DataFusion execution plan for scan merging

- Decision: Implement overlay scan merging via a custom `TransactionOverlayExec` DataFusion execution plan node that wraps the provider's base scan and merges staged writes. Do not attempt to merge at the `RecordBatch` level outside DataFusion's execution framework.
- Rationale: DataFusion expects execution plans to produce `RecordBatch` streams through `ExecutionPlan::execute()`. The overlay must merge staged inserts/updates and filter staged deletes with the base committed scan. Implementing this as a proper `ExecutionPlan` node means it participates in DataFusion's query planning, optimization, and execution framework — including projection pushdown and predicate pushdown from the base scan. Merging outside the execution framework would bypass query optimization and create a parallel code path.
- Alternatives considered:
  - In-memory `RecordBatch` merge after scan completes: rejected because it breaks streaming execution, loads entire result sets into memory, and bypasses DataFusion's framework.
  - Modify existing provider scan methods directly: rejected because it couples transaction awareness into the non-transactional hot path and violates the "near-zero-cost for non-transaction requests" goal (Decision 10).

## Decision 16: Reject DDL statements inside explicit transactions

- Decision: `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, and other DDL statements are rejected with an error when issued inside an explicit `BEGIN`/`COMMIT` block, for both pg RPC and SQL batch paths.
- Rationale: DDL operations in KalamDB modify system tables, schema registry, and metadata — these have their own atomic semantics separate from user data transactions. Mixing DDL and DML in one transaction creates complex rollback requirements (e.g., rolling back a `CREATE TABLE` after the schema registry already has the entry). PostgreSQL itself handles DDL in transactions through its catalog versioning, but KalamDB's architecture doesn't support catalog rollback. Rejecting DDL keeps the transaction scope simple and correct.
- Alternatives considered:
  - Allow DDL and attempt rollback: rejected because the schema registry, system tables, and metadata paths don't support undo/rollback semantics.
  - Allow DDL but auto-commit: rejected because it would silently break atomicity expectations — users expect everything inside BEGIN/COMMIT to be atomic.
  - Log a warning but allow: rejected because it leads to undefined behavior on rollback.

## Decision 17: Standardize on `origin: TransactionOrigin` for caller identification

- Decision: Use a single `origin: TransactionOrigin` enum field (variants: `PgRpc`, `SqlBatch`, `Internal`) throughout the transaction data model. Do not use `caller_kind` as an alternative name.
- Rationale: The data model had `caller_kind` in `ActiveTransactionMetric` and `origin` in `TransactionHandle` — two names for the same concept. Using one canonical name (`origin`) with one canonical type (`TransactionOrigin`) prevents confusion, simplifies grep/search, and aligns with the pattern of using descriptive enum types rather than raw strings.
- Alternatives considered:
  - Use `caller_kind` everywhere: rejected because `origin` is more semantic and self-describing.
  - Use raw strings: rejected because type-safe enums are a core coding principle.
