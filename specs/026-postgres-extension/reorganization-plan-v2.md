# PostgreSQL Extension — Backend Reorganization Plan

## Date

2026-03-21 (revised)

## Purpose

Reorganize the backend layers that the PostgreSQL extension depends on so that:

1. PG remote typed requests execute without SQL reconstruction overhead
2. the PG backend trait uses domain types, not gRPC wire types
3. mutation/scan logic is reusable by any future transport — not PG-specific
4. gRPC service boilerplate is consolidated (optional, lower priority)

This is a plan document. Embedded mode has been fully removed by a separate effort and is not discussed here. The only execution mode is **remote**: PostgreSQL FDW → gRPC → KalamDB server.

## Constraints

These constraints come from the current implementation and must not be violated:

* **No extra network hop** inside the server — the PG gRPC handler and the applier run in-process
* **No extra async queue** on the mutation ack path
* **No second command model** — reuse `MetaCommand`, `UserDataCommand`, `SharedDataCommand` for all server-side writes
* **No SQL reconstruction** for PG typed scans in the final design
* **Notifications stay async and post-commit** — `NotificationService` is not on the write ack path
* **Topic publishing stays synchronous** in provider writes — `TopicPublisherService` durability semantics are unchanged

## Current Problems

### 1. PG scans reconstruct SQL from typed requests

`CorePgQueryExecutor::execute_scan` builds a `SELECT` string from the typed `ScanRpcRequest` (columns, namespace, table, limit), then feeds it back through `SqlExecutor` which re-parses and re-plans it. This wastes CPU on parse/plan overhead that the typed request already eliminates.

PG mutations (`execute_insert`, `execute_update`, `execute_delete`) are fine — they call `UnifiedApplier` directly.

### 2. `PgQueryExecutor` trait uses gRPC wire types

The `PgQueryExecutor` trait in `kalamdb-pg` is defined in terms of `ScanRpcRequest`, `InsertRpcRequest`, etc. — prost-generated structs. This means:

* the backend execution logic (`CorePgQueryExecutor`) depends on transport types
* any new caller that wants typed scan/mutation must go through gRPC types even in-process
* the trait cannot be reused for cluster typed operations without importing PG wire types

### 3. gRPC service boilerplate is repeated

Three hand-rolled tonic services (`kalamdb-raft` Raft + cluster, `kalamdb-pg`) follow the same `Service<http::Request<Body>>` pattern with `~200-300` lines of routing boilerplate each. They handle different payloads but the wiring is copy-pasted.

This is a housekeeping issue, not a correctness or performance problem.

## What Does NOT Need Changing

These components are already well-structured:

* **`UnifiedApplier` + `RaftApplier`** — canonical write port, all mutations go through Raft consensus. No bypass paths remain.
* **`CommandExecutorImpl`** with `DdlExecutor`, `DmlExecutor`, `NamespaceExecutor`, `StorageExecutor`, `UserExecutor` — clean sub-executor split, thread-safe, stateless.
* **`NotificationService`** — async post-commit delivery with cluster follower broadcasting. `CoreClusterHandler::handle_notify_followers()` dispatches to it directly. No router abstraction needed.
* **`TopicPublisherService`** — synchronous in-provider publishing, already isolated in `kalamdb-publisher`.
* **`LiveQueryManager`** — subscription lifecycle owner, already backed by `UserDataCommand` Raft variants.
* **Cluster SQL forwarding** (`CoreClusterHandler::handle_forward_sql`) — receives raw SQL from followers and executes via `SqlExecutor`. This IS SQL, so going through the SQL execution path is correct. No typed bridge needed here.
* **HTTP SQL endpoint** — accepts SQL text, executes via `SqlExecutor`. Correct as-is.

## Target Architecture

Two execution tiers:

```
Tier 1 — SQL callers (need parse + plan):
  HTTP/WS/CLI ─→ SQL text ─→ SqlExecutor ─→ DataFusion SessionContext ─→ TableProvider ─→ EntityStore
  Cluster fwd ─→ SQL text ─→ SqlExecutor ─→ DataFusion SessionContext ─→ TableProvider ─→ EntityStore

Tier 2 — Typed callers (skip parse + plan, skip DataFusion session):
  PG writes  ─→ typed req ─→ UnifiedApplier ─→ Raft ─→ DmlExecutor ─→ provider ─→ EntityStore ─→ subscribers
  Raft replay ─→ command   ─→ DmlExecutor ─→ provider ─→ EntityStore ─→ subscribers
  PG scans   ─→ typed req ─→ TableProvider.scan() ─→ collect(plan, TaskContext) ─→ RecordBatches
```

The key insight: **Tier 2 callers never create a DataFusion `SessionContext`.** PG mutations already go directly to the applier/EntityStore. PG scans should call `TableProvider::scan()` directly and execute the physical plan with just a `TaskContext` (~100 bytes) — no SQL reconstruction, no parsing, no logical planning, no per-user `SessionContext` clone.

### What `OperationService` is

A thin struct wrapping `Arc<AppContext>` in `kalamdb-core` that:

* accepts domain-typed scan/mutation requests
* resolves table provider via `SchemaRegistry`
* routes mutations to `UnifiedApplier` (no DataFusion involved)
* routes scans to `provider.scan(projection, filters, limit)` + `collect(plan, task_ctx)` (minimal DataFusion — physical plan execution only, no session)
* returns domain-typed results (`Vec<RecordBatch>` for scans, affected count for mutations)

It is NOT:

* a new abstract service trait with multiple implementations
* a replacement for `SqlExecutor` — SQL callers keep using `SqlExecutor`
* an event router — notifications stay where they are

## Architectural Decisions

### 1. Define a domain-typed execution interface in `kalamdb-core`

Replace the gRPC-typed `PgQueryExecutor` trait with domain-typed request/response types:

```rust
// kalamdb-core::operations::types
pub struct ScanRequest {
    pub table_id: TableId,
    pub columns: Vec<String>,
    pub limit: Option<u64>,
    pub user_id: Option<UserId>,
}

pub struct MutationRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub user_id: Option<UserId>,
    pub rows: Vec<Row>,
    pub filter: Option<String>, // for update/delete
}

pub struct ScanResult {
    pub batches: Vec<RecordBatch>,
}

pub struct MutationResult {
    pub affected_rows: u64,
}
```

These types use `TableId`, `UserId`, `TableType`, `Row`, `RecordBatch` — all existing domain types. No tonic, no prost, no transport concern.

### 2. Reuse existing Raft commands for all server-side writes

Server-side writes from any transport translate into:

* `MetaCommand` — for DDL (namespace/table/storage/user)
* `UserDataCommand` — for user table DML
* `SharedDataCommand` — for shared table DML

No parallel command hierarchy. `UnifiedApplier` already builds these from high-level calls (`insert_user_data`, `create_table`, etc.), so `OperationService` calls the same applier methods — it does NOT build Raft commands directly.

### 3. Fix scans to use direct provider execution (no DataFusion session)

`OperationService::scan` should:

1. Resolve the table provider via `SchemaRegistry`
2. Convert column names to projection indices using the provider's schema
3. Call `provider.scan(session, Some(&projection), &[], limit)` to get an `ExecutionPlan`
4. Call `datafusion::physical_plan::collect(plan, task_ctx)` to get `Vec<RecordBatch>`

The `&dyn Session` parameter that `scan()` requires is only used to extract a `TaskContext`. KalamDB already uses this pattern in `kalamdb-tables::utils::datafusion_dml::collect_input_rows()`.

This skips: SQL text reconstruction, SQL parsing, logical plan creation, logical→physical optimization, and `SessionContext` creation (which clones the entire `SessionState` per request). The only DataFusion cost is physical plan execution, which is unavoidable since `TableProvider::scan()` returns an `ExecutionPlan`.

### 4. Keep `KalamBackendExecutor` on the extension side

The extension crate still needs a stable abstraction (`KalamBackendExecutor` in `kalam-pg-api`) for the FDW callbacks. This stays. It uses extension-facing request types (`ScanRequest`, `InsertRequest`, etc. from `kalam-pg-api::request`).

The translation chain becomes:

```
FDW callback → KalamBackendExecutor (extension-side trait)
  → RemoteKalamClient (gRPC transport)
    → KalamPgService (server gRPC handler, translates wire → domain types)
      → OperationService (domain-typed execution)
        → UnifiedApplier (writes) / direct scan (reads)
```

### 5. gRPC consolidation is optional and deferred

The three hand-rolled tonic services work. Consolidating their boilerplate into a shared crate is nice-to-have but does not affect correctness, performance, or the PG bridge architecture. Defer to after the typed execution path is working.

## Module Layout

### New modules in `kalamdb-core`

```
backend/crates/kalamdb-core/src/operations/
├── mod.rs          // pub mod types, service, scan, error
├── types.rs        // ScanRequest, MutationRequest, ScanResult, MutationResult
├── service.rs      // OperationService: scan(), insert(), update(), delete()
├── scan.rs         // Direct provider scan via TableProvider::scan() + collect()
└── error.rs        // OperationError (maps to ApplierError / DataFusion errors)
```

~4 files, ~300-500 lines total. Not a new crate — modules inside `kalamdb-core` with access to `AppContext`.

`scan.rs` is the key file — it calls `TableProvider::scan()` directly and executes the physical plan with a `TaskContext`. No `SessionContext`, no SQL, no logical plan.

### Changes to existing code

* **`kalamdb-pg/src/service.rs`** (`KalamPgService`): translate gRPC requests into domain types, call `OperationService` instead of `PgQueryExecutor`
* **`kalamdb-core/src/pg_executor.rs`**: deprecate then delete once `KalamPgService` uses `OperationService`
* **`kalamdb-pg/src/query_executor.rs`** (`PgQueryExecutor` trait): deprecate — `KalamPgService` calls `OperationService` directly

### What stays unchanged

* `backend/crates/kalamdb-raft/` — all Raft commands, executor, applier
* `backend/crates/kalamdb-core/src/applier/` — `UnifiedApplier`, all sub-executors
* `backend/crates/kalamdb-core/src/live/` — `NotificationService`, `CoreClusterHandler`, `LiveQueryManager`
* `backend/crates/kalamdb-publisher/` — `TopicPublisherService`
* `pg/crates/kalam-pg-api/` — extension-side traits and request types
* `pg/crates/kalam-pg-client/` — gRPC client
* `pg/` — pgrx hooks and extension root package
* `pg/crates/kalam-pg-fdw/` — FDW implementation

## Refactor Plan

### Phase 1: Typed execution path (~3-5 files changed)

1. Create `kalamdb-core::operations` module with `types.rs`, `service.rs`, `scan.rs`, `error.rs`
2. Implement `OperationService::scan()` — resolve provider, build logical plan, execute directly
3. Implement `OperationService::insert()`, `update()`, `delete()` — resolve table type, call appropriate `UnifiedApplier` method
4. Wire `KalamPgService` to translate gRPC requests → domain types → `OperationService`
5. Delete `CorePgQueryExecutor` and `PgQueryExecutor` trait

**Acceptance criteria:**
* PG remote scan calls `TableProvider::scan()` directly — no SQL reconstruction, no `SessionContext`
* PG remote insert/update/delete still go directly to `UnifiedApplier` (unchanged)
* No new traits, no new crates — just functions/struct in `kalamdb-core`
* Write latency unchanged, scan latency improved (eliminates parse + plan + session clone overhead)

### Phase 2: Cleanup (~2-3 files changed)

1. Remove `kalamdb-pg/src/query_executor.rs`
2. Remove `kalamdb-core/src/pg_executor.rs`  
3. Remove any dead imports or feature flags left from embedded mode cleanup
4. Update `KalamPgService` constructor to take `Arc<OperationService>` instead of `Option<Arc<dyn PgQueryExecutor>>`

**Acceptance criteria:**
* No PG-specific executor trait exists on the backend
* `kalamdb-pg` depends on domain types from `kalamdb-core::operations::types`, not vice versa

### Phase 3 (optional, deferred): gRPC consolidation

1. Create `kalamdb-rpc` crate with shared tonic service scaffolding
2. Move proto definitions for Raft, cluster, and PG services
3. Share metadata constants, interceptors, channel construction helpers
4. Unified server builder: `RpcServer::builder(config).mount_raft(s).mount_cluster(s).mount_pg(s).serve(listener)`

**Acceptance criteria:**
* No hand-rolled duplicate tonic `Service<Body>` implementations
* One place for TLS, auth interceptors, metadata keys

## Testing Strategy

### Phase 1 tests

* Unit test: `OperationService::scan()` returns correct `RecordBatch` for user/shared/stream tables
* Unit test: `OperationService::insert()` routes to correct applier method by table type
* Integration test: PG remote scan e2e (extension → gRPC → OperationService → provider → response)
* Integration test: PG remote insert/update/delete e2e
* Regression: write latency bench shows no regression from the new path
* Regression: existing smoke tests pass (`cargo test --test smoke`)

### Phase 2 tests

* Compile test: `kalamdb-pg` builds without `PgQueryExecutor` trait
* PG remote e2e tests still pass

## Risks

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Direct provider scan misses DataFusion optimizations | Low | PG only sends simple projection + limit; the real optimization (predicate pushdown, partition pruning) happens inside `TableProvider::scan()` itself |
| Breaking PG session state during `KalamPgService` rewire | Medium | Keep `SessionRegistry` unchanged, only swap execution backend |
| `OperationService` grows into a god object | Low | It delegates to existing subsystems. If it grows, split by concern (scan vs mutation) |

## Definition of Done

* PG scans execute without SQL text reconstruction
* PG mutations still use `UnifiedApplier` (no regression)
* Backend has no PG-specific executor trait — only domain-typed `OperationService`
* `CorePgQueryExecutor` and `PgQueryExecutor` are deleted
* No new crates introduced in Phase 1-2
* Existing smoke tests pass
* Write latency unchanged, scan latency improved

---

## Unified mTLS for Cluster and PG Extension

### Current Problem

The gRPC listener (`start_rpc_server`) serves cluster, Raft, and PG services on one TCP port, but auth and TLS are fragmented:

| Caller | Transport | Auth | Problem |
|--------|-----------|------|---------|
| Cluster nodes | mTLS (file-path PEMs) via `[cluster.rpc_tls]` | Metadata headers `x-kalamdb-cluster-id` + node_id + IP check | Cert paths only — no inline PEM support |
| PG extension | **Plain HTTP** (hardcoded `http://`) | Simple string comparison on `authorization` header | No encryption, no mutual auth, shared secret in cleartext |
| HTTP API | No TLS (Actix) | JWT Bearer | Separate concern, unrelated to gRPC |

### Design: One mTLS config, two caller types

Both cluster nodes and PG extension clients present a client certificate signed by the same CA. The server validates the cert and identifies the caller type from the CN.

#### Server config (`server.toml`)

Replace `[cluster.rpc_tls]` with a top-level `[rpc_tls]` section. All values accept either a file path or an inline PEM string — detected by whether the value contains `-----BEGIN`:

```toml
[rpc_tls]
enabled = true

# CA cert for validating all incoming client certs (cluster nodes + PG extension)
ca_cert = "/etc/kalamdb/certs/ca.pem"
# OR inline: ca_cert = "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----"

# This server's identity cert and key
server_cert = "/etc/kalamdb/certs/node1.pem"
server_key  = "/etc/kalamdb/certs/node1.key"
# OR inline: server_cert = "-----BEGIN CERTIFICATE-----\n..."

# Require clients to present a cert signed by ca_cert (full mTLS)
require_client_cert = true
```

The old `[cluster.rpc_tls]` section is removed. The `[cluster]` block keeps only cluster-specific settings (`cluster_id`, `node_id`, `rpc_addr`, peers, shards, timeouts).

#### Cluster nodes

Each node uses `[rpc_tls]` for both its server identity and its outgoing client identity:

- When **accepting** incoming connections: presents `server_cert`/`server_key`, validates peers against `ca_cert`
- When **connecting to peers**: uses `server_cert`/`server_key` as client identity, validates the peer's `server_cert` against `ca_cert`, uses peer `rpc_server_name` (or hostname from `rpc_addr`) for SNI

Node certs must have CN = `kalamdb-node-{node_id}`.

#### PG extension

The extension passes cert material as inline PEM strings in `CREATE SERVER`:

```sql
CREATE SERVER kalam_remote
  FOREIGN DATA WRAPPER kalam_fdw
  OPTIONS (
    endpoint    'https://kalam.example.com:9188',
    ca_cert     '-----BEGIN CERTIFICATE-----\nMIIBxT...\n-----END CERTIFICATE-----',
    client_cert '-----BEGIN CERTIFICATE-----\nMIICpD...\n-----END CERTIFICATE-----',
    client_key  '-----BEGIN PRIVATE KEY-----\nMIIEv...\n-----END PRIVATE KEY-----'
  );
```

PG extension certs must have CN = `kalamdb-pg-{name}` (e.g. `kalamdb-pg-myapp`).

#### Caller identification

After the TLS handshake, the server reads the client cert CN:

| CN pattern | Caller type | Additional validation |
|------------|-------------|----------------------|
| `kalamdb-node-{id}` | Cluster node | `x-kalamdb-cluster-id` header must match; `{id}` must be a known membership node |
| `kalamdb-pg-{name}` | PG extension | CN name logged for audit; no further checks needed |
| anything else | Rejected | `PERMISSION_DENIED` |

The `authorization` header check in `KalamPgService` is removed — the cert IS the auth. The `x-kalamdb-cluster-id`/`x-kalamdb-node-id` metadata headers remain only for cluster-node audit/membership checks (not primary auth).

### Code Changes

#### `kalamdb-configs`

Add top-level `RpcTlsConfig` to `ServerConfig`. Delete `ClusterRpcTlsConfig` from `ClusterConfig`.

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RpcTlsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub ca_cert: Option<String>,      // file path or "-----BEGIN..." inline PEM
    pub server_cert: Option<String>,
    pub server_key: Option<String>,
    #[serde(default = "default_true")]
    pub require_client_cert: bool,
}
```

Add `fn load_pem(value: &str) -> Result<Vec<u8>>` helper in the configs crate — if value contains `-----BEGIN`, use it directly as bytes; otherwise read from disk. Used by both server and client cert loading.

Validation: when `enabled = true`, all three of `ca_cert`, `server_cert`, `server_key` are required.

#### `kalamdb-raft`

1. `start_rpc_server()` — accepts `RpcTlsConfig` directly (not from `cluster.rpc_tls`). Uses `load_pem()` for the three cert fields. Builds `ServerTlsConfig` as before.
2. `raft_manager.rs` client channel setup — reads `RpcTlsConfig` from the new top-level location. Uses `load_pem()`. Builds `ClientTlsConfig` with `server_cert`/`server_key` as client identity.
3. `authorize_incoming_rpc()` — extract peer cert CN from the tonic request context. Check CN prefix to distinguish cluster nodes from other callers. Keep `x-kalamdb-cluster-id` header check for cluster-node membership verification.

#### `kalamdb-pg` (backend)

1. `KalamPgService` — remove `expected_auth_header: Option<String>` field and the `authorize()` method that does string comparison. Auth is now enforced at the TLS layer before any RPC method is called.
2. `KalamPgService::new()` — remove the `expected_auth_header` parameter.

#### `kalam-pg-common`

Replace `RemoteServerConfig { host, port }` with:

```rust
pub struct RemoteServerConfig {
    pub endpoint: String,          // "https://host:port"
    pub ca_cert: Option<String>,   // inline PEM
    pub client_cert: Option<String>, // inline PEM
    pub client_key: Option<String>,  // inline PEM
}
```

#### `kalam-pg-fdw`

`ServerOptions::parse()` — replace `host` + `port` with `endpoint`, `ca_cert`, `client_cert`, `client_key`. All read from FDW `OPTIONS (...)`.

#### `kalam-pg-client`

`RemoteKalamClient::connect()` — build endpoint from `config.endpoint` (already includes scheme). When `ca_cert` + `client_cert` + `client_key` are all `Some`, build `ClientTlsConfig` with inline PEM via `Certificate::from_pem` and `Identity::from_pem`. When any cert is absent, connect without TLS (useful for local dev with no `[rpc_tls]` configured).

### What stays unchanged

* **HTTP API authentication** — JWT Bearer on Actix stays as-is. Different listener, different callers.
* **Raft consensus protocol** — RPC payloads (vote, append_entries, install_snapshot) are unchanged. Only the transport security layer changes.
* **Cluster membership checks** — `x-kalamdb-cluster-id` header check in `authorize_incoming_rpc()` stays as an extra validation layer for cluster nodes.
