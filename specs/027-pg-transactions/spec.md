# Feature Specification: PostgreSQL-Style Transactions for KalamDB

**Feature Branch**: `027-pg-transactions`  
**Created**: 2026-03-23  
**Status**: Draft  
**Input**: User description: "I want to add transactions to kalamdb it should be similar to postgres transaction, the main cause is to be able to run transactions from pg_kalam to the kalamdb-server"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Atomic Multi-Statement Writes via pg_kalam (Priority: P1)

A developer using PostgreSQL with the pg_kalam foreign data wrapper needs to execute multiple write operations (INSERT, UPDATE, DELETE) against KalamDB tables within a single PostgreSQL transaction. All writes within the transaction must either fully succeed on COMMIT or be completely discarded on ROLLBACK, ensuring the KalamDB data is never left in a partial state.

**Why this priority**: This is the primary motivation for the feature. Today, pg_kalam already sends BEGIN/COMMIT/ROLLBACK RPCs to the server, but the server does not actually group writes into an atomic unit — writes are applied immediately and cannot be rolled back. Without server-side transaction support, pg_kalam transactions are unreliable for any multi-statement workflow.

**Independent Test**: Can be tested by opening a PostgreSQL transaction, inserting rows into two different KalamDB foreign tables, issuing ROLLBACK, and verifying that neither table contains the inserted rows. Conversely, issuing COMMIT and verifying both tables contain the expected rows.

**Acceptance Scenarios**:

1. **Given** two KalamDB foreign tables exist in PostgreSQL, **When** a user begins a transaction, inserts a row into each table, and commits, **Then** both rows are visible in subsequent queries.
2. **Given** a user begins a transaction and inserts rows into a KalamDB foreign table, **When** the user issues ROLLBACK, **Then** none of the inserted rows are visible in subsequent queries.
3. **Given** a user begins a transaction and inserts a row, then updates it within the same transaction, **When** the user commits, **Then** the final updated state is persisted.
4. **Given** a user begins a transaction and deletes a row, **When** the user issues ROLLBACK, **Then** the deleted row is still present.
5. **Given** pg_kalam receives a transaction ID from the server at BEGIN, **When** rows are staged and later committed or rolled back, **Then** that same transaction ID remains the canonical identifier for the work from pg_kalam through the server's durable apply path.
6. **Given** a transaction targets user or shared tables, **When** the user commits or rolls back, **Then** transactional semantics are enforced for those tables without requiring the caller to know which storage engine is backing the server internally.

---

### User Story 2 - Read-Your-Writes Within a Transaction (Priority: P1)

A developer performing multiple operations within a transaction expects to read back their own uncommitted changes. For example, inserting a row and then querying the table within the same transaction should return the newly inserted row, even though it has not yet been committed.

**Why this priority**: Read-your-writes consistency is a fundamental expectation of PostgreSQL transactions. Without it, application logic that depends on sequential operations within a transaction will produce incorrect results, making the transaction feature unreliable.

**Independent Test**: Can be tested by beginning a transaction, inserting a row, then selecting from the same table within the transaction and verifying the row is returned. After rollback, verify the row is gone.

**Acceptance Scenarios**:

1. **Given** a user begins a transaction and inserts a row, **When** the user queries the same table within the transaction, **Then** the inserted row is included in the results.
2. **Given** a user begins a transaction, inserts a row, and then deletes it, **When** the user queries the table within the transaction, **Then** the row is not returned.
3. **Given** a user begins a transaction and updates a row, **When** the user queries the row within the transaction, **Then** the updated values are returned.

---

### User Story 3 - Transaction Isolation Between Concurrent Sessions (Priority: P2)

Two PostgreSQL sessions connected via pg_kalam are operating concurrently. Each session's uncommitted changes must be invisible to the other session. Only committed data is visible to other sessions.

**Why this priority**: Isolation prevents data corruption and race conditions in multi-user environments. While single-session correctness (P1) is the foundation, multi-session isolation is critical for any production deployment.

**Independent Test**: Can be tested by opening two concurrent PostgreSQL connections, beginning a transaction in each, inserting a row in session A, and verifying session B does not see the row until session A commits.

**Acceptance Scenarios**:

1. **Given** session A begins a transaction and inserts a row, **When** session B queries the same table, **Then** session B does not see the uncommitted row.
2. **Given** session A commits its transaction, **When** session B queries the table, **Then** session B sees the newly committed row.
3. **Given** session A begins a transaction, **When** session B commits a new row, **Then** session A does not see session B's new row within its already-started transaction (snapshot isolation).

---

### User Story 4 - Direct Transaction Management via KalamDB SQL Statements (Priority: P2)

A developer using the KalamDB server directly, without pg_kalam, needs the server's SQL execution path itself to understand and execute BEGIN, COMMIT, and ROLLBACK statements. This enables batching multiple DML statements atomically through KalamDB's native SQL surface, including the existing HTTP SQL endpoint.

**Why this priority**: While pg_kalam is the primary driver, the transaction mechanism must also exist in KalamDB's own SQL execution path so the server is transaction-aware independently of PostgreSQL. This ensures one transaction model across all access paths.

**Independent Test**: Can be tested by sending a multi-statement SQL request containing BEGIN, DML statements, and COMMIT to the KalamDB server, then verifying the committed data. A second test should replace COMMIT with ROLLBACK and verify the data is discarded.

**Acceptance Scenarios**:

1. **Given** a user sends BEGIN through KalamDB's SQL execution path, executes two INSERT statements, and sends COMMIT, **Then** both rows are persisted and visible.
2. **Given** a user sends BEGIN through KalamDB's SQL execution path and executes an INSERT, **When** the user sends ROLLBACK, **Then** the row is not persisted.
3. **Given** a user sends a single `/v1/api/sql` request containing more than one transaction block, **When** each block is explicitly committed or rolled back in order, **Then** each block is executed independently within that same request.
4. **Given** a `/v1/api/sql` request terminates while one or more explicit transactions are still open, **When** the request ends, **Then** all still-open request-scoped transactions are automatically shut down and rolled back.

---

### User Story 5 - Automatic Rollback on Connection Drop (Priority: P3)

If a client session disconnects (network failure, crash, timeout) while a transaction is in progress, the server must automatically roll back any uncommitted changes to prevent data from being left in a partial state.

**Why this priority**: Safety net for production reliability. Without automatic cleanup, crashed clients would leave phantom uncommitted data in the system.

**Independent Test**: Can be tested by beginning a transaction, inserting rows, forcibly closing the connection, then verifying from a new session that the rows were not persisted.

**Acceptance Scenarios**:

1. **Given** a session has an active transaction with uncommitted writes, **When** the session disconnects unexpectedly, **Then** all uncommitted writes are discarded.
2. **Given** a session has an active transaction, **When** the session idle timeout expires, **Then** the transaction is rolled back and resources are freed.
3. **Given** a pg_kalam session is closed while a transaction is active, **When** the server cleans up the session, **Then** that transaction's canonical transaction ID is marked aborted and all staged writes for that transaction are discarded.

---

### User Story 6 - Transaction Timeout Protection (Priority: P3)

Long-running transactions that exceed a configurable time limit are automatically aborted to prevent resource exhaustion and lock contention.

**Why this priority**: Defensive measure for production stability. Runaway or forgotten transactions should not hold resources indefinitely.

**Independent Test**: Can be tested by beginning a transaction, waiting beyond the configured timeout, and verifying the transaction is automatically aborted and subsequent operations within it fail.

**Acceptance Scenarios**:

1. **Given** a transaction has been idle beyond the configured timeout, **When** the user attempts another operation within the transaction, **Then** the operation fails with a timeout error and the transaction is aborted.
2. **Given** the server is configured with a transaction timeout, **When** a transaction exceeds this limit, **Then** the server logs the timeout event and frees associated resources.

---

### Edge Cases

- What happens when a transaction spans writes to both shared tables and user-scoped tables?
- What happens when a transaction attempts to touch a stream table, which does not currently support transactions?
- How does the system handle a COMMIT that partially fails (e.g., one table write succeeds but another encounters a constraint violation)?
- What happens when a client sends DML statements outside of an explicit transaction (autocommit mode)?
- What happens when a client sends `BEGIN`, `COMMIT`, or `ROLLBACK` through KalamDB SQL without using pg_kalam?
- What happens when a single `/v1/api/sql` request contains multiple sequential `BEGIN ... COMMIT/ROLLBACK` blocks?
- What happens when a `/v1/api/sql` request ends while one or more transaction blocks remain open?
- How is the same transaction ID preserved from `BeginTransaction` through staged mutations, observability, and the final RocksDB commit batch?
- What happens if a client issues BEGIN while a transaction is already active (nested transaction / savepoint)?
- What happens when the server restarts while transactions are in progress?
- How does the system behave when a transaction contains only read operations (no writes)?
- What happens when a transaction buffer grows very large (e.g., millions of rows before COMMIT)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The server MUST support explicit transaction lifecycle commands: BEGIN (or START TRANSACTION), COMMIT, and ROLLBACK, within an established session.
- **FR-001A**: The KalamDB server's own SQL execution path MUST recognize and execute BEGIN (or START TRANSACTION), COMMIT, and ROLLBACK statements, not only the pg RPC transaction calls.
- **FR-001B**: A single `/v1/api/sql` request MUST be allowed to contain zero, one, or more sequential explicit transaction blocks.
- **FR-002**: All DML operations (INSERT, UPDATE, DELETE) executed within an active transaction MUST be buffered and only made durable and visible to other sessions upon COMMIT.
- **FR-002A**: For this phase, explicit transaction support MUST apply to user tables and shared tables only. Stream tables are out of scope and MUST be rejected with a clear error if used inside an explicit transaction.
- **FR-003**: On ROLLBACK (explicit or implicit via disconnect/timeout), the server MUST discard all buffered writes for that transaction with no side effects.
- **FR-004**: Within an active transaction, the originating session MUST see its own uncommitted writes when querying (read-your-writes consistency).
- **FR-005**: Uncommitted writes from one session's transaction MUST NOT be visible to other sessions (session-level isolation).
- **FR-006**: DML operations executed outside of an explicit transaction MUST continue to behave as autocommit (each statement is its own implicit transaction, applied immediately).
- **FR-007**: The server MUST automatically rollback any in-progress transaction when a session disconnects or is terminated.
- **FR-008**: The server MUST enforce a configurable transaction timeout. Transactions exceeding this limit MUST be automatically aborted.
- **FR-009**: The server MUST reject nested BEGIN commands within an active transaction with a clear error message (savepoints are out of scope for this feature).
- **FR-010**: A COMMIT on an empty transaction (no DML was executed) MUST succeed as a no-op.
- **FR-011**: A ROLLBACK on an empty or non-existent transaction MUST succeed as a no-op.
- **FR-012**: The server MUST track active transactions and expose their count and age via system observability (e.g., system table or metrics endpoint).
- **FR-013**: Writes within a transaction MUST apply atomically on COMMIT — either all writes across all affected tables succeed, or none do.
- **FR-014**: The pg_kalam extension's existing BEGIN/COMMIT/ROLLBACK RPC calls MUST work with the new server-side transaction implementation without changes to the pg_kalam wire protocol.
- **FR-015**: The server MUST handle concurrent transactions from multiple sessions without data corruption or lost writes.
- **FR-016**: The implementation MUST include automated tests covering pg RPC transaction lifecycle, KalamDB SQL BEGIN/COMMIT/ROLLBACK execution, rollback-on-error, disconnect/timeout cleanup, isolation, and autocommit regression behavior.
- **FR-017**: The implementation MUST preserve the current non-transaction execution path so requests that do not use explicit transactions do not pay more than minimal overhead.
- **FR-018**: The transaction ID returned by `BeginTransaction` MUST remain the canonical identifier for that transaction across pg_kalam, server-side staged mutations, observability, cleanup, and the final durable commit path.
- **FR-019**: When a pg_kalam session closes with an active transaction, the server MUST automatically abort that same canonical transaction ID and discard all uncommitted staged writes for it.
- **FR-020**: When a `/v1/api/sql` request ends, the server MUST automatically shut down and roll back every still-open request-scoped transaction created by that request, because request-scoped SQL transactions are not allowed to survive across API calls.
- **FR-021**: The transaction orchestration layer MUST depend on the server's storage abstraction rather than on RocksDB-specific types, so the implementation can migrate to a different storage engine in the future without changing transaction semantics.

### Key Entities

- **Transaction**: Represents an active unit of work within a session. Has a lifecycle (active → committed or aborted), a unique identifier, a start timestamp, and an association with a session. Contains a buffer of uncommitted write operations.
- **Canonical Transaction ID**: The stable transaction identifier created at BEGIN and preserved across pg_kalam, server session state, staged mutations, observability, and the final durable commit batch.
- **Session**: An authenticated client connection (HTTP session, WebSocket, or pg_kalam backend). Holds at most one active transaction at a time. Responsible for transaction lifecycle management.
- **Write Operation**: An individual DML operation (insert, update, or delete) captured within a transaction's buffer. Includes the target table, operation type, and row data.
- **Transaction Snapshot**: A point-in-time view of committed data established when a transaction begins. Used to determine which committed rows are visible to the transaction.

## Assumptions

- **Isolation Level**: The initial implementation will target Read Committed or Snapshot Isolation semantics (each query within a transaction sees the latest committed data plus its own writes). Serializable isolation is out of scope.
- **Atomic Commit Primitive**: Transaction commits will rely on the storage layer's atomic commit primitive for the final durable apply step. Rollback before commit is achieved by discarding the staged write set rather than undoing partially applied writes.
- **Single-Node Focus**: This feature targets single-node KalamDB deployments. Distributed transactions across Raft groups are a separate future feature.
- **Table Scope**: Transactional support in this phase is limited to user and shared tables. Stream tables remain non-transactional until a future design defines their transactional model.
- **No Savepoints**: Nested transactions and savepoints (SAVEPOINT / RELEASE / ROLLBACK TO) are explicitly out of scope for this feature.
- **No DDL in Transactions**: DDL statements (CREATE TABLE, ALTER TABLE, DROP TABLE) within a transaction are out of scope. DDL continues to execute with autocommit semantics.
- **Transaction Timeout Default**: The default transaction timeout will be 5 minutes, configurable via server settings. This is a reasonable default for interactive workloads while preventing resource leaks.
- **Write Buffer Size Limit**: The server will enforce a maximum write buffer size per transaction to prevent memory exhaustion. A reasonable default is 100MB. Transactions exceeding this limit are aborted with an error.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A multi-statement transaction (INSERT into two tables + COMMIT) via pg_kalam completes successfully and both rows are visible, with the total roundtrip taking less than 50ms for small payloads on localhost.
- **SC-002**: A ROLLBACK after multiple writes discards 100% of uncommitted changes — zero stale rows remain.
- **SC-003**: Read-your-writes within a transaction returns all uncommitted changes from the current session with 100% accuracy.
- **SC-004**: Uncommitted writes are invisible to concurrent sessions in 100% of test cases.
- **SC-005**: An unexpected client disconnect with an active transaction results in automatic rollback within the session cleanup window (configurable, default 30 seconds).
- **SC-006**: The system supports at least 100 concurrent active transactions without performance degradation.
- **SC-007**: Autocommit mode (no explicit BEGIN) continues to work identically to pre-transaction behavior — zero regressions in existing tests.
- **SC-008**: Existing pg_kalam e2e performance tests (sequential insert 100, 1k) continue to pass with no throughput regression greater than 5%.
- **SC-009**: Automated tests validate `BEGIN`, `COMMIT`, and `ROLLBACK` through KalamDB's native SQL execution path with 100% pass rate.
- **SC-010**: Autocommit read and write performance without explicit transactions regresses by no more than 5% in targeted baseline benchmarks.
- **SC-011**: In pg_kalam transaction tests, the same transaction ID is observable from `BeginTransaction` through commit or rollback handling with 100% consistency.
- **SC-012**: Automated tests validate that a single `/v1/api/sql` request can contain multiple sequential transaction blocks and that any still-open blocks are rolled back automatically when the request terminates.
- **SC-013**: Automated tests verify that explicit transactions succeed for user/shared tables and are rejected cleanly for stream tables.
