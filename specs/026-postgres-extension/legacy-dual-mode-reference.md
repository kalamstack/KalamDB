# Legacy Dual-Mode Detailed Reference

## Preservation Note

This document preserves the earlier detailed PostgreSQL FDW design work as historical/reference material.

Current priority is defined by [README](/Users/jamal/git/KalamDB/specs/026-postgres-extension/README) and [priority-shift.md](/Users/jamal/git/KalamDB/specs/026-postgres-extension/priority-shift.md):

* remote mode is the active priority
* embedded mode remains temporary compatibility scope

Where this legacy document conflicts with newer remote-first guidance, prefer the newer guidance. Otherwise, retain this document as design inventory and history so previously explored ideas are not lost.

---

# KalamDB PostgreSQL FDW Integration – Detailed Design Specification

## 1. Objectives

The goal of this design is to allow PostgreSQL users to access **KalamDB** tables directly from PostgreSQL and join them with native PostgreSQL tables using a **Foreign Data Wrapper (FDW)** built with pgrx.

The baseline table families are:

* user tables
* shared tables
* stream tables

However, the design also allows an **embedded-mode experiment** where shared tables are excluded and PostgreSQL native tables are used instead for shared/global data.

The FDW supports **two compile-time execution modes**:

* **Remote mode** -> communicates with an external KalamDB server over gRPC
* **Embedded mode** -> executes KalamDB logic directly inside the extension (no gRPC)

The design preserves KalamDB's core features (MVCC, TTL streams, embeddings) while exposing them through PostgreSQL.

In embedded mode, the implementation may optionally run in a **user+stream only** profile, where:

* Kalam handles user tables and stream tables
* PostgreSQL native tables replace the need for Kalam shared tables

Key principle:

Do not forward SQL. Operate at scan level (projection, filter, limit).

This allows:

* cross-database joins in PostgreSQL
* pushdown into KalamDB (via DataFusion TableProviders)

---

## 2. High-level Architecture

### 2.1 Remote mode

```text
           +-------------------+       gRPC       +--------------------+
           |   PostgreSQL FDW  | <--------------> |   KalamDB Server   |
           | (pgrx extension)  |                 | (DataFusion engine |
           +-------------------+                 |  + storage + RLS)  |
                     |                           +--------------------+
                     |
      Postgres planner splits plan
                     |
    ┌────────────────┴─────────────┐
    │        local Postgres        │
    └───────────────────────────────┘
```

### 2.2 Embedded mode

```text
           +---------------------------------------------+
           | PostgreSQL + Kalam FDW (embedded runtime)   |
           |                                             |
           | FDW -> backend trait -> DataFusion engine   |
           +-------------------------+-------------------+
                                     |
                     [storage backend selected by feature]
                                     |
                        Postgres KV or RocksDB + Parquet
```

#### Embedded storage backend note

Embedded mode must support **pluggable storage backends**, with a strict rule:

**Both backends must flush to Parquet cold storage**

Supported embedded storage strategies:

1. **Embedded + RocksDB (default parity mode)**

   * RocksDB used as hot KV store
   * closer to normal KalamDB runtime behavior
   * best for consistency with standalone server

2. **Embedded + PostgreSQL KV backend (lightweight mode)**

   * PostgreSQL tables used as KV hot store
   * no RocksDB dependency
   * smaller extension footprint and simpler deployment

### Common rule for both backends

Regardless of backend choice:

* Hot data is stored in:

  * RocksDB (rocksdb mode)
  * PostgreSQL tables (pg-kv mode)

* Cold data MUST be flushed to: **Parquet storage (local or object store)**

This keeps:

* consistent long-term storage format
* alignment with KalamDB architecture (hot + cold separation)
* ability to reuse analytics and historical queries across modes

### Flush behavior

* Flush policy should be shared across both backends:

  * size-based flush
  * time-based flush
  * TTL-based for streams

* Flush implementation should live in shared Kalam logic, not in backend-specific code

### Design requirement

* Storage backend must be abstracted behind a trait (e.g., `HotStore`)
* Flush-to-Parquet pipeline must be **independent of hot store implementation**
* FDW and execution layer must not depend on RocksDB or Postgres KV directly

This ensures:

* same query behavior across modes
* only storage implementation differs

The preferred design is to keep this decision behind a storage abstraction so the FDW and embedded executor do not depend directly on RocksDB.

#### Embedded table-model note

Embedded mode should also support an additional experiment:

* allow a **user+stream only** embedded profile
* exclude Kalam shared tables from the embedded build/runtime
* use normal PostgreSQL tables for shared/global data instead

Why this can make sense:

* shared tables overlap heavily with what PostgreSQL already gives natively
* embedded mode is mainly interesting for tenant-isolated data and realtime streams
* excluding shared tables can reduce embedded complexity and narrow the runtime surface

This means embedded mode may support two table-model strategies:

1. **Embedded full table model**

   * user + shared + stream
   * closer to normal Kalam behavior

2. **Embedded user+stream profile**

   * user + stream only
   * shared/global data stays in PostgreSQL native tables
   * lighter embedded runtime and simpler mental model

### 2.3 Execution flow (same for both modes)

1. PostgreSQL planner calls FDW
2. FDW builds **ScanRequest / MutationRequest**
3. Backend executes:

   * remote -> gRPC
   * embedded -> direct Rust call
4. DataFusion TableProvider executes scan
5. Arrow -> Postgres rows

---

## 3. FDW Implementation Using pgrx

### 3.1 Creating the extension

* Use `pgrx` to scaffold a PostgreSQL extension in Rust. `pgrx` provides safe wrappers around the C FDW API. The extension defines a `ForeignDataWrapper` with the required callback functions (`GetForeignRelSize`, `GetForeignPaths`, `GetForeignPlan`, `BeginForeignScan`, `IterateForeignScan`, `EndForeignScan`, and modification callbacks for `INSERT`, `UPDATE`, `DELETE`).
* The extension will be packaged as `pg_kalam` and installed into PostgreSQL.
* The extension must compile in exactly one mode:

  * `remote`
  * `embedded`
* In `remote` mode, foreign server options specify the gRPC endpoint (for example `kalamdb_host`, `kalamdb_port`) and authentication options.
* In `embedded` mode, foreign server options configure embedded behavior only; no transport endpoint is needed.

### 3.2 Foreign table definition

A KalamDB table is declared in PostgreSQL as a foreign table:

```sql
CREATE EXTENSION pg_kalam;

-- Example: user table "messages" in namespace "app"
CREATE SERVER kalam_server FOREIGN DATA WRAPPER pg_kalam
  OPTIONS (host 'localhost', port '50051');

CREATE FOREIGN TABLE kalam_messages (
  id        BIGINT,
  content   TEXT,
  created_at TIMESTAMP,
  embedding vector(384)  -- extension type via pgvector (optional)
) SERVER kalam_server
  OPTIONS (namespace 'app', table 'messages', table_type 'user');
```

For shared or stream tables the `table_type` option is `shared` or `stream`, and the FDW will not require a `user_id` to scope the data. A `user_id` option may be passed per session (e.g., via `SET kalam.user_id = 'user123';`) so the FDW can enforce row-level security for user/stream tables.

### 3.3 Scan logic

The FDW builds a **transport-agnostic ScanRequest**.

Steps:

1. Extract `user_id`
2. Build request:

   * namespace
   * table
   * table_type
   * projection
   * filters
   * limit

3. Execute against backend:

   * **remote mode** -> gRPC call
   * **embedded mode** -> direct Rust method

4. Backend executes using DataFusion TableProvider:

   * merge hot/cold storage
   * apply MVCC
   * apply TTL

5. Convert Arrow -> Postgres

FDW does NOT know if backend is remote or embedded.

---

### 3.4 Write operations

Same pattern as scan:

1. Build request
2. Execute:

   * remote -> gRPC
   * embedded -> direct call

Operations:

* insert
* update
* delete

---

## 3.5 SQL Compatibility Layer (User-Scoped Native Queries)

To make queries feel **native to PostgreSQL developers**, the FDW introduces a transparent compatibility layer based on implicit system columns:

* `_userid`
* `_seq`
* `_deleted`

### Goal

Allow developers to write standard SQL without needing Kalam-specific commands like:

```sql
EXECUTE AS USER 'user123'
```

Instead, they can write:

```sql
SELECT * FROM chat.messages WHERE _userid = 'user123';
```

### Behavior

The FDW intercepts queries and rewrites them internally:

#### SELECT

```sql
SELECT * FROM chat.messages WHERE _userid = 'user123' AND id = 10;
```

Translated internally to:

```text
Execute as user 'user123'
Scan chat.messages WHERE id = 10
```

* `_userid` is **not treated as a real column** in storage
* It is extracted and converted into execution context
* Remaining filters are pushed down normally

#### INSERT

```sql
INSERT INTO chat.messages (id, content, _userid)
VALUES (1, 'hello', 'user123');
```

Translated to:

```text
Execute as user 'user123'
Insert into chat.messages (id, content)
```

#### UPDATE

```sql
UPDATE chat.messages
SET content = 'hi'
WHERE id = 1 AND _userid = 'user123';
```

Translated to:

```text
Execute as user 'user123'
Update chat.messages WHERE id = 1
```

#### DELETE

```sql
DELETE FROM chat.messages
WHERE id = 1 AND _userid = 'user123';
```

Translated to:

```text
Execute as user 'user123'
Delete from chat.messages WHERE id = 1
```

### Notes

* `_userid` is **mandatory for user/stream tables** unless already provided via session (`SET kalam.user_id`).
* If both `_userid` and session user are provided, they must match.
* `_seq` and `_deleted` are exposed as **system metadata columns**:

  * `_seq` -> internal sequence/version (used for ordering / subscriptions)
  * `_deleted` -> soft-delete flag (MVCC visibility)

These columns:

* may be selectable in queries
* are **not writable directly** (except internal engine usage)
* are populated from KalamDB internal metadata

### Why this matters

* Queries feel **100% PostgreSQL-native**
* No need to learn Kalam-specific syntax
* Compatible with ORMs and existing SQL tooling
* Keeps execution model aligned with KalamDB's user isolation

### Implementation requirements

* FDW planner must:

  * detect `_userid` filters in WHERE clause
  * remove them from filter pushdown
  * convert them into execution context (`user_id` in ScanRequest)

* FDW modify handlers must:

  * extract `_userid` from INSERT/UPDATE target lists
  * validate consistency with session user

* `_userid`, `_seq`, `_deleted` must be:

  * injected into foreign table schema as **virtual columns**
  * excluded from physical storage mapping

This ensures PostgreSQL queries remain natural while still leveraging KalamDB's execution model.

---

## 4. Remote transport interface (gRPC mode only)

Used only in **remote mode**.

In embedded mode, same structs exist but are passed directly.

ScanRequest contains:

* namespace
* table
* table_type
* user_id
* projection
* filters
* limit

Response:

* Arrow RecordBatch

Mutations:

* insert / update / delete

Important: transport layer is optional and fully isolated.

---

## 5. Row-level Security (RLS) and User Context

User and stream tables are **tenant-isolated**. During query planning and scanning, the FDW must pass the current `user_id` to the KalamDB server. PostgreSQL sessions can set a variable (`SET kalam.user_id = '...';`) which the FDW reads; for cross-database joins the FDW ensures that both sides use the same `user_id`. If the `user_id` is missing for a user table, the FDW returns an error.

Shared tables ignore the user_id and are scanned globally. Stream tables use user_id to scope event streams; the FDW will apply TTL in KalamDB.

## 6. Datatype Conversions

KalamDB defines a data type system enumerated in `KalamDataType`. A virtual view `system.datatypes` provides a mapping from Arrow types to Kalam types and human-readable SQL names. The FDW maps PostgreSQL types to Kalam types via this table. The following table summarises the conversion:

| PostgreSQL typeKalam type & representationEvidence |                                                                                     |                       |
| -------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------- |
| `BOOLEAN`                                          | `KalamDataType::Boolean` -> Arrow `Boolean`                                         | `Boolean` mapping     |
| `SMALLINT` (INT2)                                  | `KalamDataType::SmallInt` -> Arrow `Int16`                                          | integer types mapping |
| `INTEGER` (INT4)                                   | `KalamDataType::Int` -> Arrow `Int32`                                               | integer mapping       |
| `BIGINT` (INT8)                                    | `KalamDataType::BigInt` -> Arrow `Int64`                                            | integer mapping       |
| `REAL` (FLOAT4)                                    | `KalamDataType::Float` -> Arrow `Float32`                                           | floating mapping      |
| `DOUBLE PRECISION` (FLOAT8)                        | `KalamDataType::Double` -> Arrow `Float64`                                          | floating mapping      |
| `TEXT`, `VARCHAR`, `CHAR`                          | `KalamDataType::Text` -> Arrow `Utf8`                                               | string mapping        |
| `BYTEA`                                            | `KalamDataType::Bytes` -> Arrow `Binary`                                            | binary mapping        |
| `DATE`                                             | `KalamDataType::Date` -> Arrow `Date32` or `Date64`                                 | date mapping          |
| `TIME` (no timezone)                               | `KalamDataType::Time` -> Arrow `Time32` or `Time64`                                 | time mapping          |
| `TIMESTAMP` (no timezone)                          | `KalamDataType::Timestamp` -> Arrow `Timestamp`                                     | timestamp mapping     |
| `TIMESTAMPTZ` (with timezone)                      | `KalamDataType::DateTime` -> Arrow `Timestamp` with timezone                        | datetime mapping      |
| `UUID`                                             | `KalamDataType::Uuid` -> Arrow `FixedSizeBinary(16)`                                | UUID mapping          |
| `NUMERIC(p,s)`                                     | `KalamDataType::Decimal {precision, scale}` -> Arrow `Decimal128`                   | decimal mapping       |
| `JSON`, `JSONB`                                    | `KalamDataType::Json` -> Arrow `Utf8` containing JSON text (round-trip)             | KalamDataType::Json   |
| `VECTOR(n)` via pgvector (optional)                | `KalamDataType::Embedding(n)` -> Arrow `FixedSizeList` of Float32                   | embedding mapping     |
| `FILE` (custom)                                    | `KalamDataType::File` - stored as JSON object or binary depending on representation | file mapping          |

Notes:

* Numeric values are transmitted using JSON numbers in gRPC and converted to Arrow's integer/float/decimal types.
* `TIMESTAMP` values are encoded as ISO-8601 strings or microsecond epoch and converted to Arrow `Timestamp` columns. `TIMESTAMPTZ` maps to `DateTime` and is always stored in UTC inside KalamDB.
* Embeddings use a fixed-size list of `f32` of the requested dimension. The FDW may store them in PostgreSQL using the `vector` type from the `pgvector` extension; otherwise they can be stored in `BYTEA` or `ARRAY` of FLOAT.
* Unsupported types fall back to JSON strings or are not allowed in the initial version; the FDW will return an error when registering such columns.

## 7. Query Planning and Join Strategy

### 7.1 Local vs remote execution

The FDW distinguishes two modes:

1. **Scan Mode (default)** - The FDW receives a partial scan plan (projection, filters, limit) and retrieves rows via gRPC. This supports cross-database joins because PostgreSQL retains control over the join order. Filters are pushed down **inexactly**: DataFusion applies them but PostgreSQL also re-applies them to guarantee correctness. This is the mode used when a query contains at least one table that is not a KalamDB table or when multiple KalamDB servers are used.
2. **Passthrough Mode** - If the query uses only KalamDB tables on the same server, the FDW can send the entire SQL statement to KalamDB's DataFusion engine. This is optional and may be added later. In this mode cross-table joins are executed by KalamDB, which may push them down to storage and avoid transferring intermediate results.

### 7.2 Cross-database joins

Consider the query:

```sql
SELECT u.name, m.content
FROM users u
JOIN kalam_messages m ON u.id = m.user_id
WHERE u.country = 'US'
ORDER BY m.created_at DESC
LIMIT 20;
```

* PostgreSQL decides it needs rows from its local `users` table and the foreign `kalam_messages` table.
* The FDW sees that `kalam_messages` is a user table; it extracts the user_id either from `SET kalam.user_id` or from the join condition `ON u.id = m.user_id`. It constructs a `ScanRequest` where the filter includes `user_id = ?` (if available) and optionally additional conditions (`m.created_at` is used only by PostgreSQL after the join). Because `u.country = 'US'` applies only to the local table, it is not pushed down.
* The FDW returns Arrow batches of `id`, `content`, `created_at`, `user_id` to PostgreSQL. PostgreSQL performs the join using the local `users` table and the returned rows. The result is sorted and limited.

If both `users` and `messages` were KalamDB tables, the FDW could recognise that the join is between two remote tables and forward the entire SQL to KalamDB's DataFusion for a more efficient join.

## 8. Error Handling and Transactions

KalamDB uses optimistic concurrency via MVCC. Each DML operation returns a new version; thus the FDW does not participate in PostgreSQL 2-phase commit. PostgreSQL transactions that involve both local and KalamDB tables can still rely on **read-committed** semantics: changes in KalamDB are visible only after the operation completes, and rollbacks in PostgreSQL do not roll back the remote changes. For consistency, the FDW can support a best-effort rollback by deleting inserted versions or re-inserting previous versions when the local transaction aborts, but this is outside the scope of the minimal design.

Any error returned by KalamDB (e.g., primary key violation, type mismatch, missing user_id) is translated into a PostgreSQL error with a relevant SQLSTATE (`23505` for unique violation, `42703` for undefined column, etc.). For example, attempts to insert a duplicate primary key will return `KalamDbError::AlreadyExists` and map to PostgreSQL `unique_violation`.

## 9. Code Organization, Crate Layout, Runtime Modes, and Feature Flags

This extension should be implemented as a **small workspace**, not a single large crate. That will make the code easier to maintain, test, document, and compile in different modes.

### 9.1 Design goals for the codebase

The implementation must follow these rules:

* Keep each crate focused on one responsibility.
* Keep PostgreSQL-facing code separate from transport, conversion, and execution code.
* Prefer small files with clear names over large "god files".
* Every public type, trait, enum, and function must have Rust doc comments.
* Every non-trivial module must include a short module-level overview.
* Add inline comments only where the logic is subtle or safety-critical.
* Avoid hiding important behavior in macros unless the abstraction clearly reduces duplication.
* Prefer simple and scalable architecture over overly clever abstractions.
* Remote mode and embedded mode must share the same FDW core, request models, and type conversion layer.
* Mode-specific concerns such as gRPC transport, embedded runtime bootstrapping, server config loading, OpenTelemetry startup, cluster metadata, and node discovery must be isolated behind features.
* In embedded mode, storage backend selection must also be isolated behind features so the build can choose a lighter backend when RocksDB is not desired.
* In embedded mode, table-family selection should also be isolated behind features so the build can choose a lighter profile that excludes Kalam shared tables when PostgreSQL native tables are enough.

### 9.2 Recommended workspace layout

Use a workspace such as:

```text
kalam-pg/
├── Cargo.toml
├── crates/
│   ├── kalam-pg-extension/      # pgrx entrypoint, SQL bindings, FDW callbacks
│   ├── kalam-pg-fdw/            # FDW planning/scanning/modification logic
│   ├── kalam-pg-api/            # transport-agnostic backend traits + request/response models
│   ├── kalam-pg-types/          # datatype mapping: Postgres <-> Arrow <-> Kalam
│   ├── kalam-pg-common/         # shared config, errors, constants, helpers
│   ├── kalam-pg-client/         # remote-mode only client adapter (gRPC)
│   ├── kalam-pg-embedded/       # embedded-mode only runtime adapter
│   └── kalam-pg-testing/        # shared test utilities, fixtures, integration helpers
└── docs/
    ├── architecture.md
    ├── crate-graph.md
    └── development.md
```

This split is intentional:

* `kalam-pg-client` is compiled only for **remote mode**
* `kalam-pg-embedded` is compiled only for **embedded mode**
* `kalam-pg-common` and `kalam-pg-api` hold the shared code used by both modes

That keeps the codebase in one workspace while still excluding unnecessary pieces from the final build depending on which mode is enabled.

### 9.3 Responsibilities of each crate

#### `kalam-pg-extension`

This is the only crate directly exposed as the PostgreSQL extension.

Responsibilities:

* `pgrx` setup and extension entrypoints
* SQL objects exposed to PostgreSQL
* foreign data wrapper registration
* GUC/session variable registration such as `kalam.user_id`
* feature-gated backend wiring
* bridge layer calling into `kalam-pg-fdw`
* extension-visible config surface for both modes

This crate should stay thin.

Suggested files:

```text
src/
├── lib.rs              # extension entrypoint
├── guc.rs              # session settings
├── server.rs           # FDW handler registration
├── backend.rs          # feature-gated backend construction
├── settings.rs         # extension-visible config surface
├── sql_api.rs          # SQL-facing helper functions if needed
└── error.rs            # Postgres-facing error translation
```

#### `kalam-pg-fdw`

This crate contains the actual FDW lifecycle implementation.

Responsibilities:

* relation option parsing
* planning hooks
* scan state lifecycle
* insert/update/delete state lifecycle
* pushdown decisions
* join/passthrough policy

Suggested files:

```text
src/
├── lib.rs
├── options.rs          # table/server/user mapping options
├── planner.rs          # rel size, paths, pushdown decisions
├── scan.rs             # begin/iterate/end foreign scan
├── modify.rs           # insert/update/delete callbacks
├── import_schema.rs    # IMPORT FOREIGN SCHEMA support
├── state.rs            # scan/modify state structs
└── capability.rs       # what can/cannot be pushed down
```

#### `kalam-pg-api`

Defines shared internal interfaces used by FDW code regardless of backend mode.

Responsibilities:

* traits for scan/insert/update/delete operations
* request models independent from transport or embedded mode
* execution traits used by both remote and embedded backends
* backend-neutral session and tenant context

Suggested files:

```text
src/
├── lib.rs
├── traits.rs           # backend executor traits
├── request.rs          # ScanRequest, InsertRequest, etc.
├── response.rs         # ScanResponse, mutation responses
└── session.rs          # tenant/session context
```

#### `kalam-pg-types`

This crate owns all type conversion logic.

Responsibilities:

* Postgres OID/type mapping
* Arrow array conversion
* Kalam scalar/value conversion
* pgvector integration behind an optional feature

Suggested files:

```text
src/
├── lib.rs
├── postgres.rs         # Postgres type descriptors
├── arrow.rs            # Arrow conversions
├── kalam.rs            # Kalam type conversions
├── scalar.rs           # scalar value conversion helpers
├── row.rs              # row/tuple conversion helpers
└── vector.rs           # pgvector integration (feature-gated)
```

#### `kalam-pg-client`

This crate is compiled only in **remote mode**.

Responsibilities:

* gRPC client
* transport retries/timeouts
* authentication headers/tokens
* Arrow IPC batch transport
* conversion between internal request/response models and protobuf/gRPC messages
* reuse of the already implemented cluster / Raft gRPC contracts where appropriate

Suggested files:

```text
src/
├── lib.rs
├── client.rs           # client construction and connection management
├── scan.rs             # remote scan calls
├── modify.rs           # remote mutation calls
├── auth.rs             # auth propagation
├── config.rs           # connection settings
└── proto.rs            # generated or wrapped protobuf types
```

#### `kalam-pg-embedded`

This crate is only compiled when the embedded mode is enabled.

Responsibilities:

* start or access an in-process KalamDB runtime
* call the same internal APIs as the transport crate but without gRPC
* adapt DataFusion/TableProvider execution into the FDW API
* support embedded storage backend selection
* allow an embedded build that uses PostgreSQL as the KV hot-store backend instead of RocksDB

Suggested files:

```text
src/
├── lib.rs
├── runtime.rs          # embedded runtime bootstrapping
├── executor.rs         # implementation of backend executor traits
├── catalog.rs          # session/catalog registration helpers
├── config.rs           # embedded runtime configuration
├── storage.rs          # embedded storage backend selection
├── pg_kv.rs            # postgres-backed KV implementation or adapter
└── rocksdb.rs          # rocksdb-backed embedded adapter (feature-gated)
```

#### `kalam-pg-common`

Shared low-level pieces used by both modes.

Responsibilities:

* common error types
* option parsing helpers
* feature flag guards
* constants
* logging/tracing utility types shared by both modes
* shared config model definitions
* system table visibility policy enums and helpers
* small shared builders and utility functions that should not live in either mode-specific crate

Suggested files:

```text
src/
├── lib.rs
├── error.rs
├── constants.rs
├── config.rs
├── feature.rs
├── tracing.rs
├── mode.rs
└── system_policy.rs
```

### 9.4 Feature flags

```toml
[features]
default = ["remote"]

remote = ["dep:kalam-pg-client"]
embedded = [
  "dep:kalam-pg-embedded",
  "dep:kalamdb-core",
  "dep:kalamdb-filestore"
]

embedded-rocksdb = [
  "embedded",
  "dep:kalamdb-store"
]

embedded-pg-kv = ["embedded"]
embedded-no-shared = ["embedded"]

pgvector = []
embedded-config = []
embedded-otel = []
```

Enforce one mode:

```rust
#[cfg(all(feature = "remote", feature = "embedded"))]
compile_error!("Enable only one mode");

#[cfg(not(any(feature = "remote", feature = "embedded")))]
compile_error!("Enable one mode");
```

---

### 9.5 Feature flag rules

To keep the code clean:

* The FDW crate must depend only on `kalam-pg-api` traits, not directly on client or embedded code.
* `kalam-pg-extension` chooses the backend executor using feature flags.
* The request/response models must be shared and transport-agnostic.
* `kalam-pg-client` must only be compiled in `remote` mode.
* `kalam-pg-embedded` must only be compiled in `embedded` mode.
* `kalam-pg-common` and `kalam-pg-api` remain shared across both modes.
* Logging, config loading, and OpenTelemetry initialization must be mode-aware and feature-gated.
* In `remote` mode, do not compile embedded runtime bootstrapping, local `server.toml` loading, cluster membership management, or embedded-only system tables.
* In `embedded` mode, do not compile gRPC client code or remote transport wrappers.
* In `embedded-pg-kv` builds, do not compile RocksDB-backed embedded storage.
* In `embedded-rocksdb` builds, keep RocksDB isolated to the embedded storage path only.
* In `embedded-no-shared` builds, do not register or expose Kalam shared tables in the embedded runtime.
* Avoid scattering `#[cfg(feature = ...)]` everywhere; isolate them in constructor and wiring modules.
* If both `remote` and `embedded` are enabled together, compilation should fail unless there is a deliberate hybrid mode.

Example:

```rust
#[cfg(all(feature = "remote", feature = "embedded"))]
compile_error!("Enable only one backend mode: 'remote' or 'embedded'.");
```

### 9.6 Backend selection pattern

```rust
pub trait KalamBackendExecutor: Send + Sync {
    fn scan(&self, req: ScanRequest) -> Result<ScanResponse, KalamPgError>;
    fn insert(&self, req: InsertRequest) -> Result<InsertResponse, KalamPgError>;
    fn update(&self, req: UpdateRequest) -> Result<UpdateResponse, KalamPgError>;
    fn delete(&self, req: DeleteRequest) -> Result<DeleteResponse, KalamPgError>;
}
```

Implementations:

* remote -> `GrpcKalamBackendExecutor` from `kalam-pg-client`
* embedded -> `EmbeddedKalamBackendExecutor` from `kalam-pg-embedded`

FDW depends ONLY on this trait.

Execution difference:

* remote -> serialize -> gRPC -> deserialize
* embedded -> direct call

### 9.6.1 Config, logging, and telemetry policy

#### Remote mode

* Do not load the full KalamDB `server.toml` in the extension.
* Use only extension-local client configuration and connection options.
* Logging should be limited to FDW/client concerns such as request IDs, retries, latency, and planning/debug output.
* OpenTelemetry should be optional and limited to outgoing gRPC spans and trace propagation.
* Cluster and node membership remain the responsibility of the external KalamDB server.

#### Embedded mode

* The embedded runtime may load a dedicated embedded configuration file or a validated subset of the normal server config.
* Do not blindly load the full server configuration unless the embedded runtime truly needs it.
* Logging setup should live in `kalam-pg-embedded`, not in FDW glue code.
* OpenTelemetry should be optional behind a dedicated feature such as `embedded-otel`.
* Embedded mode should prefer a smaller, local runtime profile over full server behavior by default.
* Embedded mode should also allow a lighter storage option where PostgreSQL is used as the KV hot-store backend instead of RocksDB.
* The default embedded profile should favor minimal footprint where possible.

### 9.6.2 System tables policy by mode

Always meaningful in both modes:

* schema and catalog metadata
* tables / columns / datatypes
* local stats relevant to the active mode
* jobs that actually exist in the selected mode

Conditionally meaningful in embedded mode:

* shared-table metadata only if the embedded profile actually includes Kalam shared tables

Primarily remote-only / server-owned:

* cluster membership
* nodes list
* Raft-specific metadata
* distributed ownership / leader state
* remote server operational tables

Embedded mode guidance:

* cluster and node tables should usually be hidden, unsupported, or replaced by simplified local-only virtual tables
* do not expose distributed system tables in embedded mode unless there is a strong use case
* when `embedded-no-shared` is enabled, do not expose Kalam shared-table system metadata as if that table family exists locally
* make the active embedded profile explicit in metadata so users are not confused about which table families are supported

---

### 9.7 Documentation standards for the code

Every crate must include:

* a crate-level `//!` overview
* module-level documentation for major modules
* rustdoc comments for all public items
* examples for any public builder/config type
* notes for unsafe or Postgres-lifetime-sensitive logic

Recommended standards:

#### Public trait example

```rust
/// Executes a table scan against a KalamDB backend.
///
/// Implementations may talk to a remote server over gRPC or execute
/// directly against an embedded runtime, depending on enabled features.
pub trait KalamBackendExecutor {
    /// Performs a scan using projection, filter, and limit pushdown.
    fn scan(&self, req: ScanRequest) -> Result<ScanResponse, KalamPgError>;
}
```

#### Module-level example

```rust
//! FDW scan lifecycle.
//!
//! This module owns the PostgreSQL foreign scan callbacks and maps them
//! into transport-agnostic `ScanRequest` calls executed by the selected
//! Kalam backend.
```

### 9.8 Testing strategy by crate

#### Unit tests

* `kalam-pg-types`: datatype conversions
* `kalam-pg-common`: option parsing, config validation, error mapping, and system table policy
* `kalam-pg-api`: request validation and trait contract tests

#### Integration tests

* `kalam-pg-fdw`: planner/scan/modify behavior with mocked backend executor
* `kalam-pg-client`: gRPC integration against a test server
* `kalam-pg-embedded`: embedded runtime smoke tests, embedded config loading, local logging/tracing setup, embedded system table visibility, storage backend selection, and embedded table-family profile selection
* `kalam-pg-extension`: end-to-end pgrx/Postgres tests

#### Required test matrix

Test both feature modes:

* `cargo test --features remote`
* `cargo test --features embedded`

Also test optional vector support:

* `cargo test --features "remote pgvector"`
* `cargo test --features "embedded pgvector"`
* `cargo test --features "embedded embedded-config"`
* `cargo test --features "embedded embedded-otel"`
* `cargo test --features "embedded-pg-kv"`
* `cargo test --features "embedded-rocksdb"`
* `cargo test --features "embedded-no-shared"`

### 9.9 Why multiple crates are worth it here

Multiple crates are justified because this project has four very different concerns:

1. PostgreSQL extension integration
2. backend execution mode selection
3. datatype and row conversion
4. mode-specific runtime code (remote client vs embedded runtime)

If kept in one crate, the code will become harder to document, harder to test, and much harder to compile cleanly with different runtime modes.

So the recommended decision is:

* **yes**, divide the project into multiple crates
* keep the extension crate thin
* keep backend mode behind traits and feature flags
* keep conversion logic isolated in its own crate
* put remote-only logic into `kalam-pg-client`
* put embedded-only logic into `kalam-pg-embedded`
* keep shared pieces in `kalam-pg-common` and `kalam-pg-api`

## 10. Mode-specific Operational Guidance

### 10.1 Logging

* Keep a shared logging facade/types layer in `kalam-pg-common`.
* Remote mode should log client-side concerns only.
* Embedded mode may initialize a local runtime logger, but that setup must live in `kalam-pg-embedded`.
* **All logs emitted from the extension should be routed to the PostgreSQL logger (elog/ereport) when running inside Postgres**, so they appear in standard Postgres logs and respect its log levels.
* Provide a small adapter (e.g., `PgLogger`) that maps Rust log levels (trace/debug/info/warn/error) to PostgreSQL levels.
* Avoid initializing global loggers that conflict with Postgres; use a scoped adapter instead.
* Do not let logging initialization leak across the workspace in an uncontrolled way.

### 10.2 Config loading

* Remote mode should use connection/client config only and avoid reading the full `server.toml`.
* Embedded mode may load a dedicated embedded config file or a validated subset of the normal server config.
* Keep config parsing/types shared where useful, but keep config loading ownership mode-specific.
* Storage backend selection for embedded mode should also be configurable here, for example:

  * embedded + RocksDB
  * embedded + PostgreSQL KV backend

### 10.3 OpenTelemetry

* OTEL should be optional in both modes.
* Remote mode: use it mainly for outgoing gRPC spans and correlation.
* Embedded mode: use it only when explicitly enabled, because many embedded deployments will want a lighter runtime.

### 10.4 System tables by mode

* Schema/catalog tables remain useful in both modes.
* Cluster/nodes/Raft tables are primarily meaningful in remote mode.
* In embedded mode, prefer not to expose distributed system tables unless a simplified local-only version is intentionally designed.

### 10.5 Embedded storage backend experiment

The spec should explicitly allow an experiment where embedded mode uses PostgreSQL as the KV hot-store backend instead of RocksDB.

Why this is useful:

* lighter embedded extension build
* fewer native dependencies inside the PostgreSQL process
* simpler packaging for embedded deployments

Trade-off:

* behavior may differ from the standalone KalamDB server that normally uses RocksDB
* this should therefore be treated as an embedded-mode backend option, not as the default for remote/server mode

Recommended rule:

* remote mode remains aligned with the normal KalamDB server architecture
* embedded mode may choose a lighter local storage backend when footprint matters more than exact storage parity

### 10.6 Embedded shared-table exclusion experiment

The spec should also explicitly allow an experiment where embedded mode excludes Kalam shared tables entirely.

Why this can make sense:

* PostgreSQL already provides strong native table support for shared/global data
* the special value of embedded Kalam is stronger around user isolation and realtime streams
* dropping shared tables from embedded mode can reduce runtime complexity and make the embedded story cleaner

Recommended rule:

* remote mode keeps the full Kalam table model
* embedded mode may support an optional **user+stream only** profile
* in that profile, shared/global data should live in PostgreSQL native tables instead of Kalam shared tables

Important caveat:

* this should be clearly surfaced in config, metadata, and docs so users know embedded mode is running a reduced table-family profile

## 11. Future Enhancements

* **PostgreSQL wire protocol** - Instead of using FDW, KalamDB could expose the PostgreSQL wire protocol directly. That would remove the need for an extension and make KalamDB a drop-in replacement for Postgres. Implementing the wire protocol is non-trivial and requires a full SQL parser, planner and executor; DataFusion provides some of this functionality, but query routing and error handling must still be built.
* **Advanced pushdown** - The FDW can push down not only filters but also projections, ORDER BY and LIMIT. For example, a query that orders by a KalamDB column could be pushed down to DataFusion so that sorting is done server-side.
* **Cross-remote joins** - For queries that join two tables within KalamDB, the FDW could send a combined plan to KalamDB so that DataFusion performs the join, reducing data transfer.
* **Schema discovery** - The FDW can automatically create foreign tables by querying KalamDB's system tables (`system.tables`, `system.columns` and `system.datatypes`). This will allow `IMPORT FOREIGN SCHEMA` to import all KalamDB tables into PostgreSQL.

## 12. Conclusion

This specification outlines a **minimal yet powerful** FDW integration for KalamDB.

The crate layout is intentionally split so the build can exclude unnecessary code:

* `kalam-pg-client` only in remote builds
* `kalam-pg-embedded` only in embedded builds
* `kalam-pg-common` and `kalam-pg-api` shared by both

By using pgrx and a mode-specific backend path (gRPC in remote mode, direct Rust calls in embedded mode), and by reusing KalamDB's DataFusion TableProvider abstraction, the design avoids code duplication and preserves important features like per-user isolation, MVCC, TTL for streams, and support for embeddings.

The key is to **scan tables, not forward SQL**, enabling cross-database joins without losing the ability to push down filters and projections.

A final architectural rule should guide the implementation:

* embedded mode should stay **local-first and lightweight** and may use a lighter PostgreSQL-backed KV hot store instead of RocksDB when that gives a better embedded footprint
* embedded mode may also run in a **user+stream only** profile, leaving shared/global tables to PostgreSQL native tables
* remote mode should stay **client-only and transport-focused**
* shared crates should contain only truly shared contracts, types, and helpers
