# PostgreSQL Extension Reorganization Plan (SUPERSEDED)

> **This document has been superseded by [reorganization-plan-v2.md](./reorganization-plan-v2.md).** Kept for historical reference only.

## Date

2026-03-21

## Purpose

This document defines a concrete reorganization plan for the PostgreSQL extension and the backend layers it depends on.

The goals are:

* make DDL, DML, and apply paths generic enough that PostgreSQL remote mode, cluster forwarding, and future adapters all use the same backend application layer
* unify gRPC definitions, server mounting, metadata handling, and client helpers in one place instead of spreading them across multiple crates
* remove or quarantine code paths that duplicate behavior, bypass the intended layering, or keep obsolete temporary modes heavier than necessary

This is a plan document, not an implementation log.

## Non-Goals and Hard Constraints

This reorganization must preserve current server semantics and must not add extra hot-path overhead.

The refactor must therefore obey these rules:

* no extra network hop inside the server
* no extra async queue on the DML/DDL ack path beyond what already exists
* no new generic event bus in front of `UnifiedApplier`
* no second mutation-command model when the existing Raft commands already express the write intent
* no typed request -> SQL string -> SQL parser bounce for PostgreSQL remote operations in the final design
* no moving live-query notification fan-out into the replicated write path
* no moving synchronous topic publishing out of provider write execution unless durability semantics are explicitly redesigned

Those constraints come directly from the current implementation:

* `CorePgQueryExecutor` currently turns typed scan requests back into SQL strings before executing them
* `NotificationService` is intentionally async and post-commit
* `TopicPublisherService` is intentionally synchronous in provider writes for durability
* live-query subscription metadata already has Raft-backed user-data commands

The plan below is updated to keep those semantics intact.

## Current Problems

### 1. The apply boundary is split across three different abstractions

Current state:

* `kalam-pg-api` exposes `KalamBackendExecutor` for the extension side
* `kalamdb-pg` exposes `PgQueryExecutor` for backend gRPC handlers
* `kalamdb-core` exposes `UnifiedApplier` plus separate `DdlExecutor` and `DmlExecutor`

Result:

* PostgreSQL remote mode talks to a PostgreSQL-specific backend trait instead of the generic command layer
* cluster code already has its own forwarding/execution path
* embedded helper code can bypass the applier and issue direct SQL or direct registry changes

### 2. Transport types leak into execution logic

Current state:

* `kalamdb-pg` service methods operate on gRPC request structs directly
* `RemoteKalamClient` and `RemoteBackendExecutor` know about transport-specific row serialization details
* the backend-side query executor trait is defined in terms of `ScanRpcRequest`, `InsertRpcRequest`, and similar wire types
* `kalamdb-core::pg_executor::CorePgQueryExecutor` rebuilds typed scans as SQL strings and executes them through the SQL executor

Result:

* transport is coupled to business logic
* adding another caller, such as a cluster-internal typed bridge, would encourage another adapter layer or more duplication
* typed PostgreSQL requests still pay a SQL reconstruction and parse/planning path that should not be the long-term hot path

### 3. DDL orchestration is still extension-specific and partly embedded-specific

Current state:

* `ddl_orchestration.rs` performs embedded bootstrap, namespace creation, SQL string construction, and foreign table creation together
* `embedded_sql_service.rs` has a special direct-DDL fast path that reaches into core helpers directly
* DDL interception and explicit helper functions are useful, but their execution path is not aligned to the long-term remote-first architecture

Result:

* DDL behavior is harder to reuse for remote mode or cluster-backed execution
* multiple code paths can mutate metadata

### 4. gRPC ownership is scattered

Current state:

* Raft RPC types and service wrappers live in `kalamdb-raft`
* cluster RPC types and service wrappers live in `kalamdb-raft`
* PostgreSQL RPC types and service wrappers live in `kalamdb-pg`
* client helpers, metadata handling, and service registration patterns are repeated

Result:

* service definitions are inconsistent
* auth and metadata handling can drift
* every new RPC surface encourages copy-paste tonic boilerplate

### 5. Temporary compatibility code is starting to shape the structure

Current state:

* remote-first is the chosen direction, but embedded compatibility still influences module layout and helper APIs
* some direct paths exist because they were expedient during bring-up

Result:

* the extension crate risks staying too heavy
* backend layering stays blurry because compatibility code is mixed with target architecture

### 6. Cluster command ingress and cluster event ingress are currently split

Current state:

* cluster command forwarding lives through `CoreClusterHandler::handle_forward_sql()`
* PostgreSQL remote command ingress lives through `KalamPgService` and `CorePgQueryExecutor`
* cluster live notification ingress lives through `CoreClusterHandler::handle_notify_followers()` and `NotificationService`
* live-query subscription state is already represented as `UserDataCommand` variants in Raft

Result:

* command-like messages and event-like messages do not share one organizing backend ingress layer
* transport adapters still know too much about which internal service to call
* subscription state and notification delivery are conceptually related but structurally disconnected

## Target Architecture

The target organization should be:

```text
PostgreSQL FDW / pgrx hooks
    -> extension adapter
    -> shared PG request/session model
    -> transport adapter (remote gRPC or in-process test adapter)
  -> generic backend ingress service
  -> operation service / event router
  -> apply gateway
    -> Raft/local execution
```

The key rule is:

**transport adapters should translate into transport-neutral commands before execution begins.**

## Server Structure Graph

The target server structure should be:

```text
                 +----------------------+
                 |   RPC Listener       |
                 |  raft + cluster + pg |
                 +----------+-----------+
                      |
          +-----------------------+-----------------------+
          |                                               |
      command ingress                                   event ingress
          |                                               |
    +-----------+-----------+                       +-----------+-----------+
    |                       |                       |                       |
  PG remote RPC         Cluster typed RPC        Cluster notify RPC     Local post-commit
  KalamPgService        CoreClusterHandler       notify_followers       notifications
    |                       |                       |                       |
    +-----------+-----------+                       +-----------+-----------+
          |                                               |
          v                                               v
    +-----------------------+                     +------------------------+
    | CoreIngressService    |                     | CoreEventRouter        |
    | shared auth/context   |                     | notification routing   |
    | request translation   |                     | subscription fan-out   |
    +-----------+-----------+                     +-----------+------------+
          |                                             |
          v                                             v
    +-----------------------+                     +------------------------+
    | OperationService      |                     | NotificationService    |
    | ddl/dml/query         |                     | LiveQueryManager       |
    +-----------+-----------+                     +------------------------+
          |
     +----------+----------+
     |                     |
     v                     v
  typed read/query       mutation/apply path
  execution service      UnifiedApplier
                 |
                 v
          Raft executor / local executors
                 |
                 v
        providers -> TopicPublisherService (sync)
                 |
                 v
        post-commit change notification emission
```

Important interpretation:

* cluster messages and PostgreSQL extension messages should share the same backend ingress component
* mutation-carrying messages should converge on the same apply gateway and `UnifiedApplier`
* event notifications should converge on the same event router, but should not be forced through the mutation applier because that would change semantics and add latency

## Architectural Decisions

### 1. Introduce one generic command service above the applier

Create a transport-neutral backend application layer that owns the end-to-end handling of:

* namespace create/drop
* table create/alter/drop
* row insert/update/delete
* typed scans used by the PostgreSQL bridge
* session-aware execution context construction
* authorization mapping from caller/session into runtime permissions

Recommended ownership:

* keep this in backend code, inside `kalamdb-core` first
* implement it as `backend/crates/kalamdb-core/src/operations/` before considering crate extraction
* only extract to a dedicated crate such as `backend/crates/kalamdb-operations` after the API stabilizes and the dependency graph is clean

This layer should accept domain requests, not gRPC requests.

Important revision:

* do not introduce a brand new DDL/DML command enum hierarchy if the existing Raft command enums already fit
* the operations layer should primarily provide ingress, validation, context building, query execution, and command construction/submission helpers

Reason for module-first:

* `kalamdb-core` already owns `AppContext`, `UnifiedApplier`, live-query services, schema/runtime wiring, and the current `CorePgQueryExecutor`
* introducing a new crate too early would add churn, dependency pressure, and likely more adapter code with no performance benefit
* the reorganization goal is cleaner boundaries, not more crate count by itself

### 2. Reuse existing Raft commands as the canonical mutation model

For write paths, the server should reuse the existing command enums already used for replication:

* `MetaCommand`
* `UserDataCommand`
* `SharedDataCommand`

These should become the canonical server-side mutation model for:

* SQL-originated writes
* cluster typed writes
* PostgreSQL remote writes
* future internal write adapters

This means:

* no parallel `CatalogCommand` or `DataCommand` hierarchy for the same writes
* PostgreSQL remote ingress should translate into these commands on the server side
* cluster typed ingress should translate into these same commands on the server side

However, these commands should not become the external wire contract for the PostgreSQL extension.

Reason:

* they include replication-facing details such as `required_meta_index`
* some fields are server-generated, such as timestamps for delete/login/job operations
* keeping wire contracts separate lets the backend evolve command submission details without leaking Raft internals to the extension

The rule should be:

* transport layer owns request DTOs
* server ingress owns translation into canonical Raft mutation commands
* executor/applier owns command submission and replication

### 3. Keep query/read requests separate from mutation commands

The existing Raft command enums are write commands, not a full query model.

For reads, define only the minimum additional transport-neutral types needed, for example:

* `QueryRequest`
* `QueryResult`
* `OperationContext`

Representative read operations:

* `ScanTable`
* later, any typed point-read or pushdown-safe read operations needed by the PostgreSQL bridge

These types should use KalamDB domain identifiers directly:

* `TableId`
* `NamespaceId`
* `TableType`
* `UserId`

They should not depend on tonic, prost, HTTP metadata, or PostgreSQL-specific glue types.

### 4. Narrow the applier to the backend mutation submission port

`UnifiedApplier` should remain or evolve into the lowest application-facing execution port for replicated mutations.

The generic command service should be responsible for:

* request validation
* table type routing
* session/user resolution
* schema lookups and coercion preparation
* choosing whether the operation is read-only or write/apply

The applier should be responsible for:

* replicated mutation execution
* routing writes to the correct Raft group or local equivalent
* returning typed success/failure outcomes

This keeps callers from talking to Raft concerns directly.

Additional constraint:

* the applier remains for state mutations and replicated subscription-state mutations
* post-commit delivery events do not go through the applier

Additional revision:

* do not add a new `ApplyGateway` trait in the first pass if `UnifiedApplier` already provides the needed mutation submission boundary
* add thin command-submission helpers around `UnifiedApplier` only where they reduce duplication

### 5. Introduce one shared backend ingress component for cluster and extension messages

Add a single backend-facing ingress component, for example:

* `CoreIngressService`
* or `TransportIngress`

Responsibilities:

* accept transport-neutral request envelopes from PostgreSQL RPC, cluster RPC, and future in-process adapters
* build `OperationContext`
* perform auth/session/default-namespace normalization
* translate mutation requests into `MetaCommand`, `UserDataCommand`, or `SharedDataCommand`
* route query messages to `QueryService`
* submit mutation commands through `UnifiedApplier`
* route event messages to `CoreEventRouter`

This is the component that both cluster messages and extension messages should share.

This satisfies the organizational goal without forcing unrelated event delivery into the applier hot path.

### 6. Make PostgreSQL remote and cluster forwarding use the same application service

After the refactor:

* PostgreSQL remote gRPC handlers translate wire messages into domain commands and call the shared ingress component
* cluster forwarding handlers do the same when they need typed execution rather than raw SQL forwarding
* embedded or test adapters can call the same command service in-process

This removes the need for a PostgreSQL-specific backend executor trait.

Important refinement:

* `handle_forward_sql()` can remain as a compatibility path for raw SQL forwarding
* new typed cluster-side operations should go through the same ingress + `OperationService` path used by PostgreSQL remote messages
* the final goal is not to force every cluster RPC into one raw SQL handler, but to give all command-style RPCs one organizing ingress service

### 7. Keep the extension thin

The extension crate should own:

* FDW callbacks
* event trigger registration and PostgreSQL catalog interaction
* conversion between PostgreSQL rows/options and shared request structs
* lifecycle of remote client handles or in-process adapters

The extension crate should not own:

* backend mutation semantics
* table creation business logic duplicated from backend services
* custom one-off direct DDL logic that differs from remote mode semantics

### 8. Keep scans typed end-to-end

The final PostgreSQL remote scan path should not reconstruct SQL text from typed requests.

Instead it should:

* resolve table provider and schema through existing runtime services
* build typed projection/filter/limit execution inputs directly
* execute through a typed query service or DataFusion logical plan builder where needed

This preserves the existing behavior while removing avoidable SQL parse/plan overhead.

### 9. Treat subscriptions and pub/sub as adjacent consumers, not primary command executors

The subscription and pub/sub layer can help structure the refactor, but it should not become the generic command layer itself.

Use it this way:

* live-query subscription metadata stays in replicated commands via `UserDataCommand`
* `LiveQueryManager` stays the lifecycle owner for subscription registration and initial data
* `NotificationService` remains the async post-commit event delivery service
* `TopicPublisherService` remains synchronous in provider writes for durable topic publishing

Do not use it this way:

* do not route ordinary DDL/DML commands through `NotificationService`
* do not replace `UnifiedApplier` with pub/sub infrastructure
* do not move topic publishing behind a remote event queue just for structural symmetry

## Proposed Module and Crate Layout

### A. Shared PG-facing contracts

Keep in `pg/` and `pg/crates/*`:

* `kalam-pg-api`
* `kalam-pg-common`
* `kalam-pg-client`
* `kalam-pg-extension`
* `kalam-pg-fdw`

Refine responsibilities:

* `kalam-pg-api`: PostgreSQL-facing request/session/response types only
* `kalam-pg-client`: remote transport client only
* `kalam-pg-extension`: pgrx hooks and adapters only

Important change:

* `kalam-pg-api` should stop mirroring backend-internal executor concepts and instead model stable PG bridge requests and responses

### B. Generic backend operations layer

Implement this first as:

* `backend/crates/kalamdb-core/src/operations/`

Possible later extraction:

* `backend/crates/kalamdb-operations`

Only extract later if:

* `AppContext` dependencies are sufficiently narrow
* transport crates can depend on it without creating cycles
* the API is stable enough that extraction reduces complexity rather than adding adapters

Suggested modules:

* `context.rs`: caller identity, session, namespace/schema defaults, auth mapping
* `commands.rs`: helpers that build canonical `MetaCommand` / `UserDataCommand` / `SharedDataCommand`
* `query.rs`: typed scan/query operations
* `service.rs`: orchestration entrypoint used by transports
* `error.rs`: transport-neutral operation errors

Additional recommended modules:

* `ingress.rs`: shared transport ingress used by cluster and PostgreSQL messages
* `event.rs`: event routing contracts for post-commit notifications and follower delivery

### C. Mutation submission boundary

Use `UnifiedApplier` as the primary mutation submission boundary in the first pass.

If a helper layer is needed, keep it minimal:

* `MutationSubmissionService`
* or helper functions inside `service.rs`

Do not introduce a separate trait hierarchy unless it clearly removes duplication without adding indirection.

### D. Unified RPC crate

Create a single crate for RPC definitions and server/client wiring, recommended name:

* `backend/crates/kalamdb-rpc`

This crate should own:

* all tonic/prost service and message definitions for Raft, cluster, and PostgreSQL bridge RPCs
* common metadata constants and interceptors
* channel construction helpers
* shared server registration helpers
* TLS and auth middleware for inbound RPCs where possible

This crate should not own application logic.

It should only define transport contracts and wiring.

### E. Optional later live/event crate

Do not create this in the first implementation slice, but keep it as an explicit future option:

* `backend/crates/kalamdb-live`
* or `backend/crates/kalamdb-events`

This crate would be justified only after the ingress and operations layers stabilize.

If extracted later, it should own:

* `NotificationService`
* cluster notification event translation
* live subscription delivery helpers
* follower notification serialization helpers

It should not own:

* `UnifiedApplier`
* generic DDL/DML operations
* topic message durability logic

Reason:

* live subscriptions and cluster notification delivery are closely related
* topic publishing is already isolated in `kalamdb-publisher`
* this extraction is optional organizational cleanup, not a prerequisite for the main refactor

## Concrete Refactor Plan

### Phase 1. Freeze boundaries and stop adding new direct paths

Tasks:

* declare `UnifiedApplier` as the only backend write/apply port for new work
* declare the new generic operations layer as the only place allowed to orchestrate DDL/DML execution
* stop adding new transport-specific executor traits
* stop adding new embedded-only mutation shortcuts
* stop adding new typed request -> SQL reconstruction shortcuts for PostgreSQL remote execution

Acceptance criteria:

* no new feature work lands directly in `ddl_orchestration.rs`, `embedded_sql_service.rs`, or transport service implementations without going through the planned shared service

### Phase 2. Introduce minimal transport-neutral request types

Tasks:

* define only the minimal request/response types needed for typed reads, ingress normalization, and event routing
* add `OperationContext` carrying session id, current schema, caller identity, and role
* add translation functions from:
  * `kalam-pg-api` requests
  * cluster forwarding requests where needed
  * future HTTP or CLI callers if useful
* add a transport-neutral envelope for event-style messages that need shared handling

Important rule:

* do not introduce a new parallel DDL/DML mutation command hierarchy in this phase
* server-side writes should translate directly into `MetaCommand`, `UserDataCommand`, or `SharedDataCommand`

Acceptance criteria:

* backend service code no longer requires tonic/prost message types to execute business logic

### Phase 3. Build the generic backend operations service

Tasks:

* create `CoreIngressService` plus one service entrypoint such as `OperationService`
* implement DDL paths by building `MetaCommand` variants and submitting them through `UnifiedApplier`
* implement DML paths by building `UserDataCommand` / `SharedDataCommand` variants and submitting them through `UnifiedApplier`
* implement typed scan paths by reusing the existing runtime/session/query facilities without rebuilding SQL strings
* introduce `CoreEventRouter` for cluster notify + local post-commit notification convergence

Important rule:

* reads and writes should share context-building and auth resolution, even if they use different lower-level executors
* event routing should share ingress organization, but remain off the write ack path
* write submission should reuse the existing canonical command enums instead of wrapping them in another mutation DTO layer

Acceptance criteria:

* PostgreSQL backend bridge can call one service for scan/insert/update/delete and later for DDL
* cluster typed forwarding can call the same service
* cluster notification ingress and local notification emission converge on the same event router API

### Phase 4. Rework PostgreSQL backend bridge around the generic service

Tasks:

* remove the need for `PgQueryExecutor` as a separate backend abstraction
* convert `kalamdb-pg` into a thin transport adapter around the generic operations service
* keep session registry in the PG bridge if it remains PG-specific, but make session-to-context mapping reusable

Acceptance criteria:

* the PG gRPC service does translation and auth only
* execution lives in the generic backend operations layer
* `CorePgQueryExecutor` is eliminated or reduced to a temporary compatibility wrapper

### Phase 5. Move gRPC definitions and registration into one RPC crate

Tasks:

* move Raft, cluster, and PostgreSQL gRPC message/service definitions under `kalamdb-rpc`
* centralize metadata keys, interceptors, server builder helpers, and channel helpers
* replace hand-rolled repeated tonic wrappers with one consistent pattern
* make shared listener registration explicit, for example through a builder API
* keep the existing single shared listener model; do not introduce a second server hop or sidecar listener for organization alone

Example target API:

```text
RpcServer::builder(config)
    .mount_raft(raft_service)
    .mount_cluster(cluster_service)
    .mount_pg(pg_service)
    .serve(listener)
```

Acceptance criteria:

* service ownership is no longer split between `kalamdb-raft` and `kalamdb-pg`
* there is one place to add interceptors, TLS, auth metadata rules, and server registration

### Phase 6. Migrate extension DDL helpers to the same backend command model

Tasks:

* replace embedded-specific DDL execution shortcuts with calls into the shared request model
* keep DDL event triggers and helper SQL functions, but make them adapters only
* split PostgreSQL metadata work from KalamDB command submission

Proposed division:

* extension side: derive requested DDL intent from PostgreSQL command or helper arguments
* backend/shared side: execute namespace/table creation or alteration
* extension side: reflect the resulting table metadata into PostgreSQL foreign table objects

Acceptance criteria:

* DDL semantics are defined once in backend code
* extension DDL code is about PostgreSQL integration, not backend mutation logic

### Phase 7. Cleanup and removal pass

Tasks:

* remove obsolete executor traits and duplicated translation layers
* remove embedded-only direct DDL paths that are no longer needed
* quarantine any experimental compatibility code behind clearly named modules or features
* delete dead helpers and duplicated message definitions once migration is complete
* delete typed scan code paths that rebuild SQL strings when the typed query service is in place

Acceptance criteria:

* the extension, PG backend bridge, and cluster transport each have visibly smaller responsibilities
* there is one authoritative code path for DDL and DML execution
* there is one shared ingress component for cluster and PostgreSQL command messages
* there is one shared event router for cluster notify and local notification delivery

## Specific Code Moves

### 0. Introduce `CoreIngressService` in `kalamdb-core::operations`

Reason:

* cluster messages and PostgreSQL extension messages need one common organizing backend entrypoint
* this can be done without changing write semantics or adding another hop

Plan:

* add `ingress.rs` inside `kalamdb-core::operations`
* have `KalamPgService` and `CoreClusterHandler` delegate there
* let ingress translate write requests into canonical Raft command enums
* let ingress route query messages to `QueryService`
* let ingress route event messages to `CoreEventRouter`

### 1. Reuse `MetaCommand`, `UserDataCommand`, and `SharedDataCommand` for PG/server writes

Reason:

* these commands already represent the server’s canonical mutation intent
* `UnifiedApplier` already translates high-level mutation calls into these enums
* creating a second mutation-command hierarchy would duplicate logic and add maintenance overhead

Plan:

* let PostgreSQL remote ingress build these same commands on the server side
* let cluster typed ingress build these same commands on the server side
* keep `required_meta_index` and any server-generated fields under backend control

Important boundary:

* the PostgreSQL extension should not send raw Raft commands over the wire
* the backend should build them after auth, validation, and context normalization

### 2. Deprecate `PgQueryExecutor`

Reason:

* it is a PostgreSQL-specific backend execution trait expressed in wire types
* it duplicates the role of the future generic operations service

Plan:

* keep it only as a temporary compatibility shim
* replace it with direct calls from `KalamPgService` into the new generic operations service

Current anchor point to retire:

* `backend/crates/kalamdb-core/src/pg_executor.rs`

### 3. Keep `KalamBackendExecutor`, but narrow it to extension-facing use only

Reason:

* the extension still needs a stable local abstraction for remote vs in-process execution
* that abstraction is useful on the extension side

Plan:

* keep it in `kalam-pg-api`
* ensure implementations only build requests and delegate
* do not mirror it on the backend

### 4. Pull DDL business logic out of `ddl_orchestration.rs`

Reason:

* it mixes PostgreSQL reflection work with backend command execution

Plan:

* retain only PostgreSQL-facing orchestration there
* move namespace/table mutation intent building and backend submission into the shared operations layer

### 5. Remove direct-DDL mutation shortcuts from `EmbeddedSqlService`

Reason:

* the direct path is useful for expediency but weakens the long-term layering

Plan:

* replace `try_execute_direct_ddl()` with a call into the same domain command service used by remote mode
* preserve an in-process adapter only if tests or transitional embedded mode still require it

### 6. Centralize RPC auth and metadata rules

Plan:

* define shared metadata constants in the RPC crate
* define inbound auth helpers and outbound metadata application in one place
* keep peer-node auth and PostgreSQL-client auth as separate policies, but implemented through shared middleware primitives

### 7. Converge cluster notify and local notification emission behind one event router

Reason:

* follower notification ingress and local post-commit notification emission should not remain scattered
* they are related to subscription delivery organization, not to mutation replication

Plan:

* add `CoreEventRouter` in `kalamdb-core::operations::event`
* have `CoreClusterHandler::handle_notify_followers()` delegate into it
* have local post-commit emitters call the same router API where practical
* keep `NotificationService` as the delivery engine behind that router

Important constraint:

* do not route these events through `UnifiedApplier`
* the applier is for replicated state changes, while notification delivery is already post-commit and best-effort

### 8. Keep `TopicPublisherService` out of the generic ingress layer

Reason:

* it is already a focused, extracted crate with provider-local durable semantics
* moving it behind generic ingress would add abstraction without helping organization

Plan:

* leave `kalamdb-publisher` as its own crate
* reference it in the plan as the write-path sidecar that stays attached to providers
* allow the new operations layer to coordinate with it indirectly through providers and result/event hooks, not by absorbing it

## Cleanup Candidates

These should be reviewed and removed or reduced during the migration.

### High priority

* duplicated gRPC client/server boilerplate across:
  * `backend/crates/kalamdb-raft/src/network/service.rs`
  * `backend/crates/kalamdb-raft/src/network/cluster_service.rs`
  * `backend/crates/kalamdb-pg/src/service.rs`
* backend executor trait duplication between `PgQueryExecutor` and the generic apply path
* embedded direct-DDL shortcuts in `embedded_sql_service.rs`
* PostgreSQL-specific execution logic living in transport service implementations
* typed scan logic in `pg_executor.rs` that reconstructs SQL text from typed requests
* duplicated command ingress logic between `KalamPgService` and `CoreClusterHandler`
* any new parallel DDL/DML command enums that duplicate `MetaCommand`, `UserDataCommand`, or `SharedDataCommand`

### Medium priority

* repeated auth metadata construction in remote client code
* repeated request translation logic for namespace/table identifiers and user/session extraction
* feature-flag branches in FDW scan/modify code that can be replaced by executor selection earlier in setup
* event-routing logic split between local notification emitters and cluster follower handlers

### Low priority after the architectural migration

* legacy compatibility documents or modules that are still correct but can be marked as archived
* optional embedded-only helper SQL functions if remote-first UX makes them redundant

## Ordering Constraints

The refactor should happen in this order:

1. define the minimal query/context types and the command-builder helpers around existing Raft commands
2. introduce the shared ingress and event-router seams inside `kalamdb-core`
3. adapt PostgreSQL backend RPC handlers to it
4. adapt cluster typed execution paths and cluster notify handling to it
5. centralize RPC definitions and registration
6. remove duplicate traits and direct shortcuts

This order matters because deleting duplicated code before the shared backend service exists will likely create churn and regressions.

## Testing Strategy

### Required tests during the migration

* unit tests for the generic operations layer
* PG remote connectivity and request translation tests
* PG remote DML integration tests using the shared backend service
* PG remote scan tests proving typed execution no longer rebuilds SQL strings
* cluster forwarding tests for any migrated typed operations
* cluster notify follower tests through the shared event router
* regression tests for DDL cache invalidation and schema reflection
* subscription resume/checkpoint regression tests so the event-router change preserves `_seq` handling

### Required acceptance scenarios

* PostgreSQL remote scan uses the generic query service
* PostgreSQL remote insert/update/delete use the generic apply path
* namespace and table DDL use the same backend command model from both remote and in-process adapters
* cluster typed command messages and PostgreSQL extension command messages enter through the same ingress component
* cluster notify and local post-commit notifications enter through the same event-router component
* shared gRPC listener still mounts Raft, cluster, and PG services successfully
* auth metadata and TLS behavior remain consistent after RPC consolidation
* write latency does not regress due to added queues or extra dispatch layers

## Risks and Mitigations

### Risk: over-generalizing too early

Mitigation:

* keep the first generic service focused on DDL, DML, and typed scans only
* do not fold unrelated HTTP or SDK transport concerns into this work

### Risk: adding server overhead while improving organization

Mitigation:

* keep the first implementation as `kalamdb-core` modules, not extra runtime layers
* do not add an internal broker or queue in front of the applier
* keep topic publishing synchronous in providers
* keep notifications async and post-commit
* replace typed-scan-to-SQL reconstruction with typed execution instead of stacking abstractions

### Risk: destabilizing Raft networking while reorganizing RPC code

Mitigation:

* move service definitions and builders first, not core Raft logic
* preserve existing public wire contracts until all callers are migrated

### Risk: embedded compatibility slowing the remote-first cleanup

Mitigation:

* treat embedded support as an adapter or feature-gated compatibility layer only
* if a compatibility path requires a second copy of backend business logic, remove or quarantine that path instead of extending it

## Definition of Done

This plan is complete when all of the following are true:

* there is one generic backend operations layer for DDL, DML, and typed scan execution
* DDL/DML server-side writes reuse the existing canonical Raft command enums rather than a parallel mutation-command model
* PostgreSQL remote mode and cluster typed execution call that same layer
* cluster command ingress and PostgreSQL command ingress share one backend ingress component
* cluster notify ingress and local post-commit delivery share one event-router component
* the applier boundary is clearly below that layer and no longer bypassed by ad hoc mutation helpers
* gRPC message and service ownership lives in one dedicated RPC crate or module tree
* the PostgreSQL extension is thin and no longer owns backend mutation business logic
* duplicated and obsolete temporary code paths have been removed or isolated behind clearly temporary compatibility boundaries
* the final structure does not add extra network hops or write-path queues

## Recommended First Implementation Slice

The first slice to implement from this plan should be:

1. add `kalamdb-core::operations::{ingress, service, query, data}` as modules, not a new crate
2. add command-builder helpers that map PG/server write requests into `MetaCommand`, `UserDataCommand`, and `SharedDataCommand`
3. adapt `KalamPgService` and `CorePgQueryExecutor` into that layer and remove typed scan -> SQL reconstruction
4. add `CoreEventRouter` and adapt `CoreClusterHandler::handle_notify_followers()` to it
5. keep DDL helper cleanup for the next slice
6. after that, move RPC definitions into the unified RPC crate

Reason:

* it delivers immediate value to the remote PostgreSQL path
* it proves the generic execution layer before the larger gRPC consolidation
* it reduces the risk of changing transport and execution architecture at the same time
* it improves organization without adding unnecessary crate or runtime overhead in the first pass