# Feature Specification: DataFusion Modernization Cutover

**Feature Branch**: `[029-datafusion-modernization]`  
**Created**: 2026-04-20  
**Status**: Draft  
**Input**: User description: "Modernize KalamDB DataFusion integration to adopt lightweight provider and execution plan patterns, replace MemTable based scans, improve pushdown and statistics, rebuild the Parquet cold path around execution time planning, and increase query performance and concurrent throughput."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Lightweight Scan Planning (Priority: P1)

As a query engine maintainer, I need source planning to stay lightweight so that query planning remains fast and execution can begin without first materializing source data.

**Why this priority**: This is the foundational improvement that unlocks most of the other performance gains described in the DataFusion guidance.

**Independent Test**: Can be fully tested by migrating one provider family and verifying that planning no longer performs source I/O or full data materialization while query results remain unchanged.

**Acceptance Scenarios**:

1. **Given** a migrated query source, **When** the query is planned, **Then** the source produces a lightweight execution description instead of performing the actual read during planning.
2. **Given** a migrated query source, **When** the query executes, **Then** rows can begin flowing from execution without requiring a fully materialized scan result up front.

---

### User Story 2 - Right Source Model Per Provider Family (Priority: P1)

As a query engine maintainer, I need each provider family to use the most appropriate source model for its data behavior so that KalamDB does not force all sources through the same materializing wrapper.

**Why this priority**: Different source families benefit from different execution patterns, and matching the model to the source avoids unnecessary complexity and overhead.

**Independent Test**: Can be fully tested by migrating one provider family at a time and verifying that it uses the source model best suited to its behavior while preserving existing query results.

**Acceptance Scenarios**:

1. **Given** a provider family with naturally incremental data access, **When** it is modernized, **Then** it follows an execution model that preserves that incremental behavior instead of forcing full intermediate materialization.
2. **Given** a provider family whose data is better represented as a view-like or file-like source, **When** it is modernized, **Then** it uses a source model aligned with that behavior rather than a generic fallback path.

---

### User Story 3 - Precise Filter Pushdown (Priority: P1)

As an application team running selective queries, I need filters to be applied as early and as accurately as possible so that the database reads less irrelevant data.

**Why this priority**: Exact and source-aware filter handling is one of the most direct ways to reduce unnecessary I/O, CPU work, and memory use.

**Independent Test**: Can be fully tested by running selective queries against a migrated provider and verifying that less irrelevant data is read while the same visible rows are returned.

**Acceptance Scenarios**:

1. **Given** a filter that a source can safely satisfy itself, **When** the query executes, **Then** the source eliminates that data before later operators need to process it.
2. **Given** a filter that a source can only partially use, **When** the query executes, **Then** the source reduces the scanned data while the remaining correctness checks still produce the exact result set.

---

### User Story 4 - Multi-Layer Historical Pruning (Priority: P1)

As a database operator, I need historical reads to skip irrelevant data at every safe reduction stage so that cold-path queries touch as little storage as possible.

**Why this priority**: The DataFusion pruning guidance shows that major gains come from combining multiple pruning stages rather than relying on a single coarse filter.

**Independent Test**: Can be fully tested by running history-backed queries against benchmark datasets and verifying reductions in data touched for partition, file, row-group, or finer-grained pruning opportunities.

**Acceptance Scenarios**:

1. **Given** a historical query whose predicate excludes known sections of stored data, **When** the query executes, **Then** the system skips those sections before they are decoded.
2. **Given** a historical query where later pruning stages can further reduce work, **When** the query executes, **Then** the system applies those later reductions without changing the returned rows.

---

### User Story 5 - LIMIT-Aware Historical Reads (Priority: P1)

As an application team issuing small-result queries, I need eligible history-backed queries with row limits to stop early when enough guaranteed matches have been found so that the system avoids wasteful cold-path work.

**Why this priority**: Small-result queries are currently paying for more cold-path work than necessary, and the upstream DataFusion limit-pruning guidance offers a direct performance win.

**Independent Test**: Can be fully tested by running limit queries on benchmark data and verifying that the system touches less data than the baseline path while preserving result correctness and ordering rules.

**Acceptance Scenarios**:

1. **Given** a limit query whose safe early-return conditions are met, **When** the query executes, **Then** the system stops scanning additional historical data once enough qualifying rows are guaranteed.
2. **Given** a limit query whose ordering or semantics make early termination unsafe, **When** the query executes, **Then** the system preserves correctness instead of applying unsafe pruning.

---

### User Story 6 - Better Plan Metadata For Optimizer Decisions (Priority: P2)

As a query engine maintainer, I need trustworthy source metadata about partitioning, ordering, boundedness, and expected size so that the optimizer can avoid unnecessary repartitioning, sorting, and other redundant work.

**Why this priority**: Richer source metadata helps both single-query performance and concurrent throughput by reducing unnecessary downstream operators.

**Independent Test**: Can be fully tested by comparing explain plans and benchmark results before and after adding source metadata for a migrated provider.

**Acceptance Scenarios**:

1. **Given** a source with known execution properties, **When** a query is planned, **Then** the optimizer receives those properties and can avoid redundant work that the baseline path could not avoid.
2. **Given** a workload that repeatedly plans similar queries, **When** the modernized path is used, **Then** planning overhead is lower than the baseline because immutable plan metadata is cheaper to reuse.

---

### User Story 7 - Nested Field And Narrow Projection Access (Priority: P2)

As an application team querying structured or semi-structured data, I need queries that only access a subset of fields to avoid reading full nested values or unused columns so that narrow queries stay fast.

**Why this priority**: The DataFusion 53 guidance shows that nested field pushdown and narrow projection access are important for avoiding needless decoding work.

**Independent Test**: Can be fully tested by running projection-heavy and nested-field queries on representative data and verifying that less unnecessary data is touched while results remain identical.

**Acceptance Scenarios**:

1. **Given** a query that only needs selected fields from a wider value, **When** the query executes, **Then** the system avoids reading or decoding unrelated fields wherever safe.
2. **Given** a query that uses a narrow projection on historical data, **When** the query executes, **Then** the system limits source work to the needed fields and compatibility columns required for correctness.

---

### User Story 8 - Latest Supported Engine Interfaces Only (Priority: P2)

As a query engine maintainer, I need the modernized provider stack to rely only on the current supported query-engine interfaces so that future upgrades remain predictable and the codebase does not carry stale or deprecated usage.

**Why this priority**: Architecture work of this size should leave the provider stack on the supported API surface instead of modernizing behavior while retaining stale interfaces.

**Independent Test**: Can be fully tested by auditing the targeted provider, planner, and execution surfaces after modernization and confirming that no deprecated or obsolete engine-interface usage remains there.

**Acceptance Scenarios**:

1. **Given** a targeted provider or execution surface, **When** maintainers audit the modernized code path, **Then** it uses only the current supported query-engine interfaces for the selected dependency line.
2. **Given** a future maintenance pass or dependency refresh within the supported line, **When** the modernized stack is reviewed, **Then** there are no remaining legacy compatibility shims that were kept only for older interfaces.

---

### User Story 9 - Optimization Evidence And Safe Rollout (Priority: P3)

As a query engine maintainer, I need the new execution model to be observable and verifiable so that I can prove performance gains without breaking permissions, transaction visibility, or existing SQL behavior.

**Why this priority**: The migration is broad and high risk, so maintainers need proof that optimization changes preserve correctness before legacy paths are removed.

**Independent Test**: Can be fully tested by running explain-plan validation, regression suites, authorization checks, transaction visibility tests, and benchmark comparisons for each migrated provider family.

**Acceptance Scenarios**:

1. **Given** a migrated query source, **When** maintainers inspect validation evidence, **Then** they can confirm both the optimization behavior and the preserved result semantics.
2. **Given** a query that depends on access control or transactional visibility, **When** it runs on the modernized path, **Then** it produces the same authorized and visible result set as before the migration.

### Edge Cases

- Queries that include transaction-local changes must preserve snapshot visibility and read-your-writes behavior while using the new execution path.
- Reads against historical data with schema differences (columns added, removed, renamed, or retyped in later versions) must still return the expected columns and values without exposing stale or incompatible rows, and the modernized path MUST be regression-covered for each supported schema-evolution shape rather than relying on the legacy materializing fallback.
- Hot+cold version resolution for MVCC-backed tables must remain driven by the `(commit_seq, seq_id)` ordering rule, preserve snapshot-commit-seq visibility, and be implemented as a lazy, slice-oriented merge on the new execution substrate that preserves zero-copy Arrow buffers where possible instead of a per-scan `HashMap` materialization of owned rows.
- Queries that are eligible for aggressive optimization must still return correct results when ordering, permissions, or visibility rules prevent early termination.
- Narrow fast-path reads, such as primary-key lookups and count-only queries, must remain correct and should not regress into heavier generic scan behavior; the modernized path MUST keep these fast paths explicit with dedicated execution and regression coverage.
- Mixed workloads that include system tables, views, stream tables, vector results, and MVCC-backed tables must keep consistent query behavior despite different internal execution strategies.
- Sources that cannot prove an optimization is safe must fall back to a correct, less aggressive path rather than claiming unsupported capabilities.
- Queries that touch nested fields or partially projected historical rows must remain correct even when older stored data uses earlier schemas.
- Sources that rely on blocking work must avoid stalling the shared async execution environment while the new streaming model is in use.
- Highly partitioned sources must balance concurrency gains against scheduling overhead instead of assuming that more partitions always improve performance.
- Sessions shared across many concurrent users must remain lightweight: per-request session construction must not deep-clone session config options, must avoid per-user materialization of catalog or runtime state, and must not scale memory linearly with the number of registered tables or plan-cache entries.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST move scan-backed query sources to a lightweight planning model that does not require fully reading or materializing source data during query planning.
- **FR-002**: The system MUST perform the heavy source work during execution rather than during planning.
- **FR-003**: The system MUST allow eligible query sources to produce results incrementally during execution rather than requiring complete result materialization before any rows can be returned.
- **FR-004**: The system MUST choose a source model for each provider family that matches the source's actual behavior instead of forcing all providers through the same materializing wrapper.
- **FR-005**: The system MUST preserve current query results, access control outcomes, deletion visibility rules, and transactional visibility semantics throughout the modernization.
- **FR-006**: The system MUST expose source filter capabilities accurately, distinguishing between fully supported, partially helpful, and unsupported filters.
- **FR-007**: The system MUST reduce unnecessary source work for eligible queries by applying safe source-level filtering before later operators process the data.
- **FR-008**: The system MUST reduce unnecessary source work for eligible queries by applying safe source-level projection reduction, including nested-field or subfield reduction where supported by the source.
- **FR-009**: The system MUST support multi-layer historical-data reduction so that partition-level, file-level, row-group-level, or finer-grained pruning can be applied wherever the source can prove it is safe.
- **FR-010**: The system MUST support limit-aware historical-data reduction for limit queries when early termination is safe and MUST avoid it when ordering or semantics make it unsafe.
- **FR-011**: The system MUST expose trustworthy source metadata that helps the query planner make better decisions about query work, concurrency, and resource usage.
- **FR-012**: The system MUST provide source metadata about partitioning, ordering, boundedness, and expected size whenever the source can guarantee those properties.
- **FR-013**: The system MUST preserve existing historical-data compatibility, including the ability to read data written under older schemas without changing user-visible results.
- **FR-014**: The system MUST support a consistent optimization model across stream tables, system tables, views, vector-backed query results, user tables, and shared tables, while allowing source-specific behavior where semantics require it.
- **FR-015**: The system MUST keep specialized fast paths for narrow queries, including primary-key lookup behavior and count-only behavior, when those paths remain safe and beneficial.
- **FR-016**: The system MUST avoid blocking the shared async query runtime when a source performs blocking work as part of the new execution model.
- **FR-017**: The system MUST provide observable evidence of which optimizations were applied so maintainers can verify that expected source reductions and execution improvements occurred.
- **FR-018**: The system MUST provide a controlled migration path that allows maintainers to validate each source family before removing legacy scan behavior.
- **FR-019**: The system MUST support mixed concurrent read workloads with better memory stability than the baseline architecture for target query classes.
- **FR-020**: The system MUST preserve compatibility with existing client-visible SQL behavior and must not require application query rewrites to gain the new execution benefits.
- **FR-021**: The system MUST eliminate deprecated or obsolete query-engine interfaces (for example, planning-time `MemTable`-backed scan wrappers and legacy provider shims) from the targeted provider, planner, and execution surfaces by rollout completion.
- **FR-022**: The system MUST standardize the modernized provider stack on the current supported query-engine interface set for the selected DataFusion 53.x dependency line and MUST share one set of provider base traits, execution nodes, streams, pruning descriptors, and statistics across user, shared, stream, view, vector, and system sources instead of duplicating them per crate.
- **FR-023**: The system MUST define explicit rollout completion criteria based on correctness, performance, concurrency validation, and supported-interface cleanup before the legacy scan architecture is retired.
- **FR-024**: The system MUST align hot+cold MVCC version resolution with the new execution substrate by implementing the merge as a lazy, slice-oriented execution operator that honors the `(commit_seq, seq_id)` ordering and snapshot visibility rule, preserves zero-copy Arrow buffers where possible, and removes the per-scan owned-row HashMap materialization path once the new merge is validated.
- **FR-025**: The system MUST keep per-user session construction lightweight so per-request session memory does not grow linearly with registered tables or plan-cache entries; shared catalog, runtime, and provider state MUST be Arc-shared, and per-request user context injection MUST avoid cloning session config option trees on the hot path.
- **FR-026**: The system MUST prefer zero-copy data movement across the source substrate by operating on shared `ArrayRef`, `RecordBatch`, and `SchemaRef` references for projection, filter evaluation, and version merging, and MUST avoid widening ownership of hot-path byte buffers into owned copies unless a specific operator provably requires it.

### Key Entities *(include if feature involves data)*

- **Scan Source**: Any queryable data source that participates in query planning and execution, including tables, views, system metadata, stream-backed sources, and vector-backed sources.
- **Scan Plan**: The lightweight description of how a source will be read, including what data can be reduced early and what source properties are known to the planner.
- **Historical Read Selection**: The subset of stored historical data that remains eligible for a query after safe partition, file, row-group, page, or equivalent reduction.
- **Provider Capability Contract**: The declared set of source behaviors that describe what a provider can safely reduce or guarantee before later operators run.
- **Execution Validation Baseline**: The recorded set of latency, concurrency, memory, and plan-shape measurements used to judge whether the modernization is successful.
- **Optimization Evidence**: The explain output, execution metrics, and benchmark results that show which reductions and execution improvements were actually applied.

### Assumptions

- The feature may make broad internal changes as long as existing query semantics and security boundaries remain unchanged.
- The feature focuses on modernizing the current supported query-engine foundation rather than combining this work with a separate major dependency-line upgrade.
- The targeted DataFusion dependency line remains the current supported 53.x family during this effort.
- Performance validation will use representative workloads that cover selective reads, small-result queries, wide scans, joins, transactional reads, system sources, vector-related queries, nested-field access, and limit-heavy historical queries.
- Provider-family migrations may use different internal source models as long as they satisfy the same correctness and validation standards.

### Scope Boundaries

- Included: source planning and execution behavior, source-model selection per provider family, source capability reporting, historical read reduction, limit-aware pruning, nested-field and narrow projection access, source metadata exposure, supported-interface cleanup, execution observability, and migration validation across all provider families.
- Excluded: unrelated authentication redesign, new SQL syntax or user-facing query features, and unrelated storage-engine changes that do not directly support the new execution model.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Representative selective read queries complete at least 30% faster at median latency than the pre-migration baseline while returning identical results.
- **SC-002**: Representative small-result queries complete at least 50% faster than the pre-migration baseline.
- **SC-003**: Representative limit-heavy historical queries reduce unnecessary historical data touched by at least 50% compared with the baseline path.
- **SC-004**: Representative narrow-projection or nested-field queries reduce unnecessary data touched by at least 50% compared with the baseline path.
- **SC-005**: Representative prepared or repeatedly planned query workloads show at least 50% lower planning latency than the baseline path.
- **SC-006**: Under an agreed mixed-read workload, the system supports at least 2x as many concurrent query users before reaching the same latency or memory threshold as the baseline.
- **SC-007**: For targeted lightweight-readable query classes, peak per-query memory usage is reduced by at least 40% compared with the baseline architecture.
- **SC-008**: All priority validation suites for permissions, transactional visibility, and regression correctness pass with no behavior differences from the approved baseline.
- **SC-009**: A modernization audit of the targeted provider, planner, and execution surfaces finds zero remaining deprecated or obsolete query-engine interface usage.
- **SC-010**: Optimization evidence is available for all eligible validation queries, allowing maintainers to confirm that source reduction and execution improvements actually occurred before rollout completion.
- **SC-011**: Peak per-user session memory measured under the agreed concurrent-read workload is reduced by at least 30% versus the baseline and does not grow linearly with the number of registered tables or plan-cache entries.
- **SC-012**: Per-query allocations attributable to hot+cold version resolution are reduced by at least 50% versus the baseline `HashMap`-materializing merge, measured on the representative MVCC read workload with identical result correctness.
