# Tasks: DataFusion Modernization Cutover

**Input**: Design documents from `/specs/029-datafusion-modernization/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/provider-execution.md, contracts/cold-path-pruning.md, contracts/validation-rollout.md, quickstart.md
**Last Updated**: 2026-04-21

**Audit Note (2026-04-21)**: Previously open tasks were reviewed against the current branch state. Tasks with verifiable code or validation evidence are marked finished. Tasks without verifiable branch artifacts are marked skipped so nothing remains implicitly open.

**Tests**: The spec explicitly requires explain-shape validation, regression coverage, CLI smoke coverage, and benchmark evidence. This task list includes story-level validation tasks plus final batched end-to-end and performance verification rather than per-edit micro-tests.

**Organization**: Tasks are grouped by user story so each story is independently implementable and testable after the shared execution substrate is in place.

**Library Policy**: Prefer DataFusion, Arrow, Parquet, and object_store primitives wherever they already solve the problem. Do not introduce custom row-buffer, pruning, or plan-metadata frameworks when the upstream engine APIs already provide the needed surface.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel when earlier stabilizing tasks in the same phase are complete and the tasks touch different files
- **[Story]**: Which user story this task belongs to (`US1` through `US9`)
- **[S]**: Explicitly skipped or deferred after audit; no verified implementation or validation artifact landed on this branch for that task
- Every task includes exact file paths

## Path Conventions

- Shared execution crate: `backend/crates/kalamdb-datafusion-sources/src/`
- Backend table providers: `backend/crates/kalamdb-tables/src/`
- Backend system providers: `backend/crates/kalamdb-system/src/providers/`
- Session and SQL integration: `backend/crates/kalamdb-core/src/sql/` and `backend/crates/kalamdb-session-datafusion/src/`
- Validation surfaces: crate-owned integration tests under `backend/crates/*/tests/`, CLI smoke coverage under `cli/tests/`, and performance coverage under `benchv2/src/benchmarks/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish the shared execution crate and workspace wiring so the modernization removes duplication instead of spreading it across existing crates.

- [X] T001 Create the shared DataFusion source crate and workspace wiring in `Cargo.toml`, `backend/Cargo.toml`, `backend/crates/kalamdb-datafusion-sources/Cargo.toml`, and `backend/crates/kalamdb-datafusion-sources/src/lib.rs`
- [X] T002 [P] Add documented module boundaries and thin public APIs for descriptors, capability reporting, execution nodes, stream adapters, pruning, and statistics in `backend/crates/kalamdb-datafusion-sources/src/provider.rs`, `backend/crates/kalamdb-datafusion-sources/src/exec.rs`, `backend/crates/kalamdb-datafusion-sources/src/stream.rs`, `backend/crates/kalamdb-datafusion-sources/src/pruning.rs`, and `backend/crates/kalamdb-datafusion-sources/src/stats.rs`
- [X] T003 [P] Wire the new shared crate into consuming manifests in `backend/crates/kalamdb-tables/Cargo.toml`, `backend/crates/kalamdb-system/Cargo.toml`, `backend/crates/kalamdb-views/Cargo.toml`, `backend/crates/kalamdb-vector/Cargo.toml`, and `backend/crates/kalamdb-transactions/Cargo.toml`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build the shared execution substrate, baseline evidence, and deduplication seams that every story depends on.

**⚠️ CRITICAL**: No user story work should begin until this phase is complete.

- [X] T004 Record phase-0 explain and performance baselines in `benchv2/src/benchmarks/point_lookup_bench.rs`, `benchv2/src/benchmarks/load_sql_1k_bench.rs`, `benchv2/src/benchmarks/load_mixed_rw_bench.rs`, `cli/tests/smoke/query/smoke_test_queries_benchmark.rs`, and `cli/tests/smoke/query/smoke_test_00_parallel_query_burst.rs`
- [X] T005 [P] Extract common scan descriptors, provider traits, and plan-property builders from `backend/crates/kalamdb-tables/src/utils/base.rs` and `backend/crates/kalamdb-system/src/providers/base.rs` into `backend/crates/kalamdb-datafusion-sources/src/provider.rs` and `backend/crates/kalamdb-datafusion-sources/src/stats.rs`, leaving storage-specific behavior behind crate-local adapters
- [X] T006 [P] Extract shared execution-node and record-batch stream utilities from `backend/crates/kalamdb-transactions/src/overlay_exec.rs` and provider helpers into `backend/crates/kalamdb-datafusion-sources/src/exec.rs` and `backend/crates/kalamdb-datafusion-sources/src/stream.rs` using DataFusion `ExecutionPlan` and `SendableRecordBatchStream` primitives instead of custom row buffers
- [X] T007 [P] Create unified filter, projection, limit, and pruning descriptors in `backend/crates/kalamdb-datafusion-sources/src/pruning.rs` and wire them into `backend/crates/kalamdb-tables/src/utils/base.rs` and `backend/crates/kalamdb-system/src/providers/base.rs`
- [X] T008 [P] Wire the shared source crate into `backend/crates/kalamdb-tables/src/utils/mod.rs` and `backend/crates/kalamdb-system/src/providers/mod.rs`, and add only minimal integration entry points in `backend/crates/kalamdb-views/src/lib.rs` and `backend/crates/kalamdb-vector/src/sql/mod.rs`
- [X] T009 [P] Add shared descriptor, version-merge, plan-properties, and current-API contract tests in `backend/crates/kalamdb-datafusion-sources/tests/descriptor_contract.rs`, `backend/crates/kalamdb-datafusion-sources/tests/version_merge_contract.rs`, `backend/crates/kalamdb-datafusion-sources/tests/plan_properties_contract.rs`, and `backend/crates/kalamdb-datafusion-sources/tests/current_api_surface.rs`
- [X] T010 [P] Add module-level docs and focused architectural comments in `backend/crates/kalamdb-datafusion-sources/src/*.rs` and remove stale old-design comments from `backend/crates/kalamdb-tables/src/utils/base.rs` and `backend/crates/kalamdb-system/src/providers/base.rs`

**Checkpoint**: The new shared crate exists, common traits and execution helpers are centralized, baseline evidence is captured, and the codebase is ready for provider-family cutovers without layering new duplication onto the old design.

---

## Phase 3: User Story 1 - Lightweight Scan Planning (Priority: P1) 🎯 MVP

**Goal**: Make planning lightweight by moving stream-table work off the planning path and onto shared execution descriptors and streams.

**Independent Test**: Stream-table queries plan without source I/O or batch materialization, and explain output shows exec-backed source planning instead of hidden in-memory scan wrapping.

### Tests for User Story 1

- [X] T011 [P] [US1] Add lightweight planning regression coverage for stream tables in `backend/crates/kalamdb-tables/tests/stream_provider_lightweight_scan.rs`
- [X] T012 [P] [US1] Add CLI explain coverage for exec-backed stream planning in `cli/tests/smoke/query/smoke_test_stream_explain_planning.rs`

### Implementation for User Story 1

- [X] T013 [US1] Refactor `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs` to return shared execution descriptors from `backend/crates/kalamdb-datafusion-sources/src/provider.rs` instead of materialized scan results
- [X] T014 [US1] Move stream-table execution and batching logic onto shared execution nodes in `backend/crates/kalamdb-datafusion-sources/src/exec.rs`, `backend/crates/kalamdb-datafusion-sources/src/stream.rs`, and `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs`
- [X] T015 [US1] Delete legacy stream-table scan fallbacks and obsolete TODO-based update and delete paths from `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs` and `backend/crates/kalamdb-tables/src/utils/base.rs`

**Checkpoint**: Stream tables are the first exec-backed provider family and demonstrate lightweight planning without backward-compatibility shims.

---

## Phase 4: User Story 2 - Right Source Model Per Provider Family (Priority: P1)

**Goal**: Migrate each provider family onto the source model that matches its semantics while enforcing shared base traits instead of duplicating scan logic across user, shared, stream, view, vector, and system providers.

**Independent Test**: Explain output for system, view, vector, and table providers shows source-appropriate execution models while query results remain unchanged.

### Tests for User Story 2

- [X] T016 [P] [US2] Add provider-family execution-model coverage in `backend/crates/kalamdb-system/tests/system_provider_exec_models.rs`, `backend/crates/kalamdb-views/tests/view_provider_exec_models.rs`, `backend/crates/kalamdb-vector/tests/vector_provider_exec_models.rs`, and `backend/crates/kalamdb-tables/tests/provider_source_models.rs`
- [X] T017 [P] [US2] Add mixed-provider explain smoke coverage in `cli/tests/smoke/query/smoke_test_provider_exec_models.rs`

### Implementation for User Story 2

- [X] T018 [P] [US2] Migrate all system table providers onto shared descriptor and capability traits plus one streaming scan path in `backend/crates/kalamdb-system/src/providers/base.rs`, `backend/crates/kalamdb-system/src/macros.rs`, and `backend/crates/kalamdb-system/src/providers/`
- [X] T019 [P] [US2] Replace generic view `MemTable` wrapping with a shared one-shot execution descriptor and node in `backend/crates/kalamdb-views/src/view_base.rs`, `backend/crates/kalamdb-views/src/describe.rs`, and `backend/crates/kalamdb-views/src/lib.rs`
- [X] T020 [P] [US2] Wrap vector search results in the shared execution model while keeping TVF-specific lookup logic local in `backend/crates/kalamdb-vector/src/sql/vector_search.rs` and `backend/crates/kalamdb-vector/src/sql/mod.rs`
- [X] T021 [US2] Refactor `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`, and `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs` to consume one shared provider facade for schema, scan descriptors, pushdown hooks, and notification-row helpers, leaving MVCC merge details to US3
- [X] T022 [US2] Remove duplicate provider glue and old wrapper code from `backend/crates/kalamdb-tables/src/utils/base.rs`, `backend/crates/kalamdb-system/src/providers/base.rs`, `backend/crates/kalamdb-system/src/macros.rs`, and `backend/crates/kalamdb-views/src/view_base.rs`

**Checkpoint**: Provider families use the source model that fits their semantics, system tables share one logic path, and duplicated provider glue has been removed instead of preserved.

---

## Phase 5: User Story 3 - Precise Filter Pushdown (Priority: P1)

**Goal**: Replace blanket pushdown declarations with source-accurate capability reporting and shared predicate evaluation logic.

**Independent Test**: Selective queries show correct `Exact`, `Inexact`, or `Unsupported` behavior and read less irrelevant data without changing results.

### Tests for User Story 3

- [X] T023 [P] [US3] Add filter-pushdown contract coverage in `backend/crates/kalamdb-tables/tests/filter_pushdown_contract.rs` and `backend/crates/kalamdb-system/tests/filter_pushdown_contract.rs`
- [X] T024 [P] [US3] Add selective-read smoke coverage for source-side filtering in `cli/tests/smoke/query/smoke_test_filter_pushdown.rs`

### Implementation for User Story 3

- [X] T025 [US3] Replace blanket pushdown reporting with a shared capability matrix for `Exact`, `Inexact`, and `Unsupported` filter shapes in `backend/crates/kalamdb-datafusion-sources/src/provider.rs`, `backend/crates/kalamdb-tables/src/utils/base.rs`, and `backend/crates/kalamdb-system/src/providers/base.rs`
- [X] T026 [P] [US3] Implement precise pushdown handling for stream, system, view, and vector sources in `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs`, `backend/crates/kalamdb-system/src/providers/base.rs`, `backend/crates/kalamdb-views/src/view_base.rs`, and `backend/crates/kalamdb-vector/src/sql/vector_search.rs`
- [X] T027 [US3] Implement shared exact and inexact filter evaluation for MVCC-backed user and shared tables in `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`, and `backend/crates/kalamdb-datafusion-sources/src/pruning.rs`
- [S] T062 [US3] Rebuild hot+cold MVCC version resolution as a lazy metadata-first merge execution plan that honors `(commit_seq, seq_id)` ordering, snapshot visibility, and early-stop hints while emitting shared batch slices wherever possible in `backend/crates/kalamdb-datafusion-sources/src/exec.rs`, `backend/crates/kalamdb-datafusion-sources/src/stream.rs`, `backend/crates/kalamdb-tables/src/utils/version_resolution.rs`, `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, and `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`
- [S] T063 [US3] Replace callers of `merge_versioned_rows`, `resolve_latest_kvs_from_cold_batch`, and `count_resolved_from_metadata` with the shared merge execution plan and delete the superseded helper paths from `backend/crates/kalamdb-tables/src/utils/version_resolution.rs` and `backend/crates/kalamdb-tables/src/utils/base.rs` once the new contract tests pass
- [S] T064 [US3] Preserve and modernize primary-key lookup and count-only fast paths on the streaming merge in `backend/crates/kalamdb-tables/src/utils/pk/existence_checker.rs`, `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`, and `backend/crates/kalamdb-datafusion-sources/src/exec.rs`, with regression coverage in `backend/crates/kalamdb-tables/tests/pk_count_fast_paths.rs`

**Checkpoint**: Pushdown behavior is source-accurate, shared, and free of blanket fallback declarations from the old design.

---

## Phase 6: User Story 4 - Multi-Layer Historical Pruning (Priority: P1)

**Goal**: Rebuild historical reads so partition, file, row-group, and finer-grained pruning can happen through shared descriptors and execution-time Parquet work instead of planning-time materialization.

**Independent Test**: History-backed queries skip irrelevant data at multiple reduction layers while preserving visible results and schema-evolution correctness.

### Tests for User Story 4

- [S] T028 [P] [US4] Add cold-path pruning regression coverage in `backend/crates/kalamdb-tables/tests/historical_pruning.rs` and `backend/crates/kalamdb-filestore/tests/parquet_pruning.rs`
- [S] T029 [P] [US4] Add historical explain coverage for partition, file, and row-group reduction in `cli/tests/smoke/flushing/smoke_test_historical_pruning_explain.rs`

### Implementation for User Story 4

- [S] T030 [US4] Refactor historical selection descriptors and manifest pruning into shared reusable logic in `backend/crates/kalamdb-datafusion-sources/src/pruning.rs`, `backend/crates/kalamdb-tables/src/manifest/planner.rs`, and `backend/crates/kalamdb-tables/src/utils/parquet.rs` using existing Parquet and object-store metadata APIs instead of custom file-selection machinery
- [S] T031 [US4] Add execution-time Parquet row-group and page pruning hooks across table and filestore layers via Parquet/DataFusion reader APIs in `backend/crates/kalamdb-filestore/src/parquet/reader.rs`, `backend/crates/kalamdb-tables/src/utils/parquet.rs`, and `backend/crates/kalamdb-tables/src/manifest/manifest_helpers.rs`
- [S] T032 [US4] Delete batch-materializing historical scan leftovers from `backend/crates/kalamdb-tables/src/utils/base.rs` and `backend/crates/kalamdb-tables/src/utils/parquet.rs`
- [S] T065 [US4] Add historical schema-evolution regression coverage (columns added, removed, renamed, retyped) for the modernized path in `backend/crates/kalamdb-tables/tests/historical_schema_evolution.rs` and `cli/tests/smoke/flushing/smoke_test_historical_schema_evolution.rs`
- [S] T066 [US4] Ensure streaming schema projection aligns older Parquet row groups with the current table schema using Arrow projection utilities and without owned-column copies in `backend/crates/kalamdb-filestore/src/parquet/reader.rs`, `backend/crates/kalamdb-tables/src/utils/parquet.rs`, and `backend/crates/kalamdb-datafusion-sources/src/pruning.rs`

**Checkpoint**: Historical pruning is layered, shared, and executed at read time instead of hidden inside the old planning path.

---

## Phase 7: User Story 5 - LIMIT-Aware Historical Reads (Priority: P1)

**Goal**: Stop eligible historical reads early when the system can prove it is safe, without preserving any limit-oblivious legacy fallback path.

**Independent Test**: Eligible limit-heavy historical queries touch less data than the baseline while preserving result correctness and ordering semantics.

### Tests for User Story 5

- [S] T033 [P] [US5] Add limit-pruning correctness coverage in `backend/crates/kalamdb-tables/tests/historical_limit_pruning.rs`
- [S] T034 [P] [US5] Add limit-heavy historical benchmark coverage in `benchv2/src/benchmarks/flushed_parquet_query_bench.rs` and `benchv2/src/benchmarks/point_lookup_bench.rs`

### Implementation for User Story 5

- [S] T035 [US5] Add shared early-stop rules, explicit safety gates, and limit descriptors in `backend/crates/kalamdb-datafusion-sources/src/pruning.rs` and `backend/crates/kalamdb-tables/src/manifest/planner.rs`
- [S] T036 [US5] Implement safe limit-aware cold-path termination in `backend/crates/kalamdb-tables/src/utils/parquet.rs` and `backend/crates/kalamdb-filestore/src/parquet/reader.rs`
- [S] T037 [US5] Remove old limit-oblivious fallback logic from `backend/crates/kalamdb-tables/src/utils/parquet.rs` and `backend/crates/kalamdb-tables/src/manifest/planner.rs`

**Checkpoint**: Limit-aware pruning works only where it is safe, and the old unconditional limit handling path is gone.

---

## Phase 8: User Story 6 - Better Plan Metadata For Optimizer Decisions (Priority: P2)

**Goal**: Feed trustworthy plan properties and statistics through the shared execution substrate so DataFusion can optimize real source behavior.

**Independent Test**: Explain plans show better partitioning, ordering, and statistics behavior, and benchmark results demonstrate less redundant downstream work.

### Tests for User Story 6

- [S] T038 [P] [US6] Add plan-property, partition-statistics, and plan-cache metadata coverage in `backend/crates/kalamdb-datafusion-sources/tests/plan_properties_contract.rs`, `backend/crates/kalamdb-tables/tests/partition_statistics.rs`, `backend/crates/kalamdb-system/tests/partition_statistics.rs`, and `backend/crates/kalamdb-core/tests/plan_cache_metadata.rs`
- [S] T039 [P] [US6] Add explain regression coverage for redundant sort and repartition avoidance in `cli/tests/smoke/query/smoke_test_plan_metadata.rs`

### Implementation for User Story 6

- [S] T040 [US6] Implement shared `PlanProperties` and `partition_statistics` builders in `backend/crates/kalamdb-datafusion-sources/src/stats.rs`, `backend/crates/kalamdb-datafusion-sources/src/exec.rs`, and `backend/crates/kalamdb-transactions/src/overlay_exec.rs`
- [S] T041 [US6] Feed stable partitioning, ordering, boundedness, and conservative statistics through table providers in `backend/crates/kalamdb-tables/src/utils/base.rs`, `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs`, `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, and `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`
- [S] T042 [P] [US6] Align session-side partition planning and plan-cache behavior with the new source metadata in `backend/crates/kalamdb-core/src/sql/datafusion_session.rs`, `backend/crates/kalamdb-core/src/sql/context/execution_context.rs`, `backend/crates/kalamdb-core/src/sql/plan_cache.rs`, and `backend/crates/kalamdb-core/tests/plan_cache_metadata.rs`
- [S] T067 [US6] Refactor per-user `SessionContext` construction to stay lightweight and Arc-sharing friendly by avoiding avoidable `SessionConfig` option copies, keeping `SessionUserContext` injection minimal on the hot path, and reusing immutable catalog/runtime state in `backend/crates/kalamdb-core/src/sql/context/execution_context.rs`, `backend/crates/kalamdb-core/src/sql/datafusion_session.rs`, and `backend/crates/kalamdb-session-datafusion/src/context.rs`
- [S] T068 [P] [US6] Add per-user session footprint and concurrent-user memory regression coverage in `backend/crates/kalamdb-core/tests/session_memory_footprint.rs` and `benchv2/src/benchmarks/concurrent_user_session_bench.rs`

**Checkpoint**: Source metadata is explicit, reused, and visible to the optimizer rather than being hidden behind duplicate wrapper code.

---

## Phase 9: User Story 7 - Nested Field And Narrow Projection Access (Priority: P2)

**Goal**: Avoid reading unrelated columns or nested fields by pushing projection reduction through the shared pruning layer.

**Independent Test**: Narrow-projection and nested-field queries touch less data while preserving the same visible results.

### Tests for User Story 7

- [S] T043 [P] [US7] Add projection and nested-field regression coverage in `backend/crates/kalamdb-tables/tests/projection_pruning.rs`, `backend/crates/kalamdb-views/tests/view_projection_pruning.rs`, `backend/crates/kalamdb-vector/tests/vector_projection_pruning.rs`, and `cli/tests/smoke/query/smoke_test_projection_pruning.rs`

### Implementation for User Story 7

- [S] T044 [US7] Add shared projection and nested-field reduction utilities in `backend/crates/kalamdb-datafusion-sources/src/pruning.rs` and `backend/crates/kalamdb-tables/src/utils/base.rs`
- [S] T045 [US7] Apply narrow projection and nested-field reduction to historical, view, and vector reads in `backend/crates/kalamdb-tables/src/utils/parquet.rs`, `backend/crates/kalamdb-filestore/src/parquet/reader.rs`, `backend/crates/kalamdb-views/src/view_base.rs`, and `backend/crates/kalamdb-vector/src/sql/vector_search.rs` by reusing Arrow projection primitives instead of bespoke nested-field copying

**Checkpoint**: Projection reduction is centralized and reused rather than reimplemented per source family.

---

## Phase 10: User Story 8 - Latest Supported Engine Interfaces Only (Priority: P2)

**Goal**: Finish the modernization on the supported DataFusion 53.x surface only, with no stale compatibility branches left behind.

**Independent Test**: Targeted provider, planner, and execution surfaces compile and test cleanly with no deprecated or obsolete query-engine usage remaining.

### Tests for User Story 8

- [X] T046 [P] [US8] Add current-API compile guards and interface-audit coverage in `backend/crates/kalamdb-datafusion-sources/tests/current_api_surface.rs`, `backend/crates/kalamdb-tables/tests/current_api_surface.rs`, `backend/crates/kalamdb-system/tests/current_api_surface.rs`, `backend/crates/kalamdb-views/tests/current_api_surface.rs`, `backend/crates/kalamdb-vector/tests/current_api_surface.rs`, and `backend/crates/kalamdb-transactions/tests/current_api_surface.rs`

### Implementation for User Story 8

- [X] T047 [US8] Replace remaining MemTable-backed planning wrappers and outdated 53.x planning hooks with current `ExecutionPlan` and `PlanProperties` APIs in `backend/crates/kalamdb-tables/src/utils/base.rs`, `backend/crates/kalamdb-system/src/providers/base.rs`, `backend/crates/kalamdb-views/src/view_base.rs`, `backend/crates/kalamdb-vector/src/sql/vector_search.rs`, and `backend/crates/kalamdb-transactions/src/overlay_exec.rs`
- [X] T048 [US8] Delete obsolete compatibility shims and dead old-design helpers from `backend/crates/kalamdb-tables/src/utils/base.rs`, `backend/crates/kalamdb-tables/src/utils/mod.rs`, `backend/crates/kalamdb-system/src/providers/base.rs`, `backend/crates/kalamdb-system/src/macros.rs`, `backend/crates/kalamdb-views/src/view_base.rs`, and `backend/crates/kalamdb-vector/src/sql/vector_search.rs`
- [S] T049 [US8] Remove unused legacy dependency wiring and compatibility-only comments from `Cargo.toml`, `backend/Cargo.toml`, and touched backend crate `Cargo.toml` files

**Checkpoint**: The modernized provider stack is on one supported API surface with no backward-compatibility scaffolding left in targeted files.

---

## Phase 11: User Story 9 - Optimization Evidence And Safe Rollout (Priority: P3)

**Goal**: Prove the new execution model improves performance and preserves permissions, transaction visibility, and result semantics before the last legacy paths are deleted.

**Independent Test**: Explain evidence, regression suites, CLI smoke tests, and benchmarks all validate the modernized paths and rollout gates.

### Tests for User Story 9

- [S] T050 [P] [US9] Add transaction-visibility and authorization regression coverage for modernized scans in `cli/tests/smoke/impersonating/smoke_test_as_user_authorization.rs`, `cli/tests/smoke/tables/smoke_test_user_table_rls.rs`, and `cli/tests/smoke/query/smoke_test_00_parallel_query_burst.rs`
- [S] T051 [P] [US9] Add latency, planning, memory, and concurrency benchmark coverage in `benchv2/src/benchmarks/point_lookup_bench.rs`, `benchv2/src/benchmarks/load_sql_1k_bench.rs`, `benchv2/src/benchmarks/load_mixed_rw_bench.rs`, and `benchv2/src/reporter/json_reporter.rs`
- [S] T053 [P] [US9] Add transaction overlay correctness and read-your-writes regression coverage in `backend/crates/kalamdb-transactions/tests/overlay_visibility.rs`, `backend/crates/kalamdb-transactions/tests/overlay_read_your_writes.rs`, and `backend/crates/kalamdb-tables/tests/dml_overlay_integration.rs`
- [S] T071 [P] [US9] Add zero-copy and allocation-budget regression coverage for scans, version merges, and overlays in `backend/crates/kalamdb-datafusion-sources/tests/zero_copy_allocations.rs`, `backend/crates/kalamdb-transactions/tests/overlay_allocation_budget.rs`, and `backend/crates/kalamdb-tables/tests/scan_allocation_budget.rs`

### Implementation for User Story 9

- [S] T052 [US9] Add explain and analyze plan-shape capture, evidence helpers, and assertion utilities in `backend/crates/kalamdb-core/src/sql/executor/sql_executor.rs`, `cli/tests/smoke/query/smoke_test_stream_explain_planning.rs`, and `cli/tests/smoke/query/smoke_test_plan_metadata.rs`
- [S] T069 [US9] Modernize DML execution on the shared source substrate so `INSERT`, `UPDATE`, and `DELETE` consume the streaming version-resolution merge instead of owned-row scan fallbacks in `backend/crates/kalamdb-tables/src/utils/datafusion_dml.rs`, `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`, and `backend/crates/kalamdb-transactions/src/overlay_exec.rs`
- [S] T070 [US9] Rebuild the transaction overlay execution node to stream shared `RecordBatch` slices from the shared substrate and remove owned-row overlay buffers unless materialization is semantically required in `backend/crates/kalamdb-transactions/src/overlay_exec.rs`, `backend/crates/kalamdb-transactions/src/query_extension.rs`, and `backend/crates/kalamdb-datafusion-sources/src/exec.rs`
- [S] T054 [P] [US9] Wire benchmark comparison thresholds and rollout verdict logic into `benchv2/src/comparison.rs`, `benchv2/src/verdict.rs`, and `benchv2/src/metrics.rs`

**Checkpoint**: Rollout evidence is explicit, reproducible, and strong enough to justify deleting the last legacy scan paths.

---

## Phase 12: Polish & Cross-Cutting Concerns

**Purpose**: Finish the cleanup, comments, validation, and removal work so the codebase is left smaller, cleaner, and fully on the new design.

- [S] T055 [P] Add final module docs and focused inline comments for shared abstractions and remaining complex provider code in `backend/crates/kalamdb-datafusion-sources/src/*.rs`, `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`, `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`, and `backend/crates/kalamdb-system/src/providers/base.rs`
- [S] T056 [P] Tighten module boundaries, re-exports, and visibility after the story-scoped deletions land in `backend/crates/kalamdb-tables/src/utils/base.rs`, `backend/crates/kalamdb-tables/src/utils/mod.rs`, `backend/crates/kalamdb-system/src/providers/base.rs`, `backend/crates/kalamdb-system/src/macros.rs`, `backend/crates/kalamdb-views/src/view_base.rs`, and `backend/crates/kalamdb-vector/src/sql/vector_search.rs`
- [X] T057 [P] Run batched compile validation across modernized crates using `Cargo.toml`, `backend/crates/kalamdb-datafusion-sources/Cargo.toml`, and touched backend crate manifests, then fix errors in touched source files
- [S] T058 Run targeted `cargo nextest` validation for modernized crates and fix failures in touched files under `backend/crates/` and `cli/tests/`
- [S] T059 Run backend plus `cli/run-tests.sh` end-to-end validation and remove any remaining legacy-path usage in touched files under `backend/` and `cli/`
- [S] T060 Run final `benchv2` performance and concurrency validation, record per-test runtimes, and fix any remaining regressions in `benchv2/src/benchmarks/`, `benchv2/src/reporter/`, and touched backend source files
- [X] T061 Remove any remaining backward-compatibility branches, fallback scan paths, or dead cleanup TODOs discovered during final validation in touched files under `backend/crates/` and `Cargo.toml`
- [S] T072 Record final per-user session footprint and per-query allocation numbers for SC-011/SC-012 in `benchv2/src/benchmarks/concurrent_user_session_bench.rs`, `benchv2/src/reporter/json_reporter.rs`, and the rollout evidence surface referenced by `cli/tests/smoke/query/smoke_test_plan_metadata.rs`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: Can start immediately
- **Foundational (Phase 2)**: Depends on Setup and blocks all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational and establishes the first production exec-backed provider family
- **User Story 2 (Phase 4)**: Depends on Foundational and the shared substrate from US1; it expands the pattern across the rest of the provider families and establishes the shared provider facade before MVCC-specific merge work lands
- **User Stories 3-5 (Phases 5-7)**: Depend on the shared provider substrate from US1-US2 and should generally proceed in priority order because they touch the same pruning and provider surfaces
- **User Stories 6-8 (Phases 8-10)**: Depend on the P1 provider refactor being stable so metadata, projection reduction, and supported-interface cleanup are done once on the modernized path
- **User Story 9 (Phase 11)**: Depends on all architectural changes that it validates
- **Polish (Phase 12)**: Depends on all desired user stories being complete

### User Story Dependencies

- **US1**: First deliverable and MVP; proves lightweight planning on one production provider family
- **US2**: Builds on the shared substrate and is the main deduplication phase for provider-family architecture
- **US3**: Depends on the US2 provider-family split so capability reporting is implemented on the correct source model
- **US4**: Depends on the shared pruning abstractions and provider substrate from US2-US3
- **US5**: Depends on US4 because limit-aware pruning builds on the historical pruning descriptors
- **US6**: Depends on the exec-backed provider paths from US1-US5
- **US7**: Depends on the pruning and metadata layers from US4-US6
- **US8**: Depends on the new shared architecture being in place so targeted legacy API cleanup can delete old paths instead of bridging them
- **US9**: Depends on all prior stories because it is the rollout-evidence gate

### Within Each User Story

- Validation tasks come first so the success shape is encoded before the implementation changes land
- Shared abstractions and traits must be introduced before provider-family migrations that consume them
- New code should replace old code directly; do not add compatibility wrappers that preserve the legacy design
- Cleanup tasks in each story are required completion criteria, not optional refactors

### Parallel Opportunities

- Phase 1: `T002` and `T003` can run in parallel after `T001`
- Phase 2: `T005`, `T006`, `T007`, `T008`, `T009`, and `T010` can run in parallel after `T004`
- US1: `T011` and `T012` can run in parallel
- US2: `T016` and `T017` can run in parallel; after the shared source contract stabilizes, `T018`, `T019`, and `T020` can run in parallel while `T021` prepares the table-provider facade
- US3: `T023` and `T024` can run in parallel; after `T025`, `T026` can proceed while `T027` focuses on MVCC-specific handling; `T062`-`T064` land the streaming version-resolution merge and preserve PK/count fast paths
- US4: `T028` and `T029` can run in parallel; `T065` and `T066` add historical schema-evolution coverage and streaming schema projection
- US5: `T033` and `T034` can run in parallel
- US6: `T038` and `T039` can run in parallel; after `T040`, `T042` can run alongside provider metadata work in `T041`; `T067` reshapes per-user session construction and `T068` adds concurrent-user memory coverage
- US7: `T043` can run independently before implementation
- US8: `T046` can run before targeted cleanup work
- US9: `T050`, `T051`, `T053`, and `T071` can prepare in parallel; after evidence helpers land, `T054` can proceed while `T069` and `T070` modernize DML and rebuild the overlay exec node on the shared substrate
- Phase 12: `T055`, `T056`, and `T057` can run in parallel before the final validation sequence; `T072` records the final per-user session and allocation evidence

---

## Parallel Example: User Story 1

```bash
# Launch the validation tasks together:
Task: "Add lightweight planning regression coverage for stream tables in backend/crates/kalamdb-tables/tests/stream_provider_lightweight_scan.rs"
Task: "Add CLI explain coverage for exec-backed stream planning in cli/tests/smoke/query/smoke_test_stream_explain_planning.rs"
```

## Parallel Example: User Story 2

```bash
# After the shared source contract is stable, these migrations can proceed in parallel:
Task: "Migrate all system table providers onto one shared provider trait and scan implementation in backend/crates/kalamdb-system/src/providers/base.rs, backend/crates/kalamdb-system/src/macros.rs, and backend/crates/kalamdb-system/src/providers/"
Task: "Migrate view-backed sources onto the shared execution model in backend/crates/kalamdb-views/src/view_base.rs, backend/crates/kalamdb-views/src/describe.rs, and backend/crates/kalamdb-views/src/lib.rs"
Task: "Migrate vector search sources onto the shared execution model in backend/crates/kalamdb-vector/src/sql/vector_search.rs and backend/crates/kalamdb-vector/src/sql/mod.rs"
```

## Parallel Example: User Story 3

```bash
# Run the story validation tasks together:
Task: "Add filter-pushdown contract coverage in backend/crates/kalamdb-tables/tests/filter_pushdown_contract.rs and backend/crates/kalamdb-system/tests/filter_pushdown_contract.rs"
Task: "Add selective-read smoke coverage for source-side filtering in cli/tests/smoke/query/smoke_test_filter_pushdown.rs"
```

## Parallel Example: User Story 4

```bash
# Historical pruning validation can be prepared in parallel:
Task: "Add cold-path pruning regression coverage in backend/crates/kalamdb-tables/tests/historical_pruning.rs and backend/crates/kalamdb-filestore/tests/parquet_pruning.rs"
Task: "Add historical explain coverage for partition, file, and row-group reduction in cli/tests/smoke/flushing/smoke_test_historical_pruning_explain.rs"
```

## Parallel Example: User Story 5

```bash
# Limit-aware validation can be prepared in parallel:
Task: "Add limit-pruning correctness coverage in backend/crates/kalamdb-tables/tests/historical_limit_pruning.rs"
Task: "Add limit-heavy historical benchmark coverage in benchv2/src/benchmarks/flushed_parquet_query_bench.rs and benchv2/src/benchmarks/point_lookup_bench.rs"
```

## Parallel Example: User Story 6

```bash
# After shared metadata builders land, these can overlap:
Task: "Feed real partitioning, ordering, boundedness, and statistics through table providers in backend/crates/kalamdb-tables/src/utils/base.rs, backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs, backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs, and backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs"
Task: "Align session-side partition planning and plan-cache behavior with the new source metadata in backend/crates/kalamdb-core/src/sql/datafusion_session.rs, backend/crates/kalamdb-core/src/sql/context/execution_context.rs, and backend/crates/kalamdb-core/src/sql/plan_cache.rs"
```

## Parallel Example: User Story 7

```bash
# Validation can start before the projection-pruning implementation:
Task: "Add projection and nested-field regression coverage in backend/crates/kalamdb-tables/tests/projection_pruning.rs and cli/tests/smoke/query/smoke_test_projection_pruning.rs"
```

## Parallel Example: User Story 8

```bash
# Audit coverage can be prepared while implementation cleanup is scoped:
Task: "Add current-API compile guards and interface-audit coverage in backend/crates/kalamdb-datafusion-sources/tests/current_api_surface.rs, backend/crates/kalamdb-tables/tests/current_api_surface.rs, and backend/crates/kalamdb-system/tests/current_api_surface.rs"
```

## Parallel Example: User Story 9

```bash
# Evidence tasks that can run together:
Task: "Add transaction-visibility and authorization regression coverage for modernized scans in cli/tests/smoke/impersonating/smoke_test_as_user_authorization.rs, cli/tests/smoke/tables/smoke_test_user_table_rls.rs, and cli/tests/smoke/query/smoke_test_00_parallel_query_burst.rs"
Task: "Add latency, planning, memory, and concurrency benchmark coverage in benchv2/src/benchmarks/point_lookup_bench.rs, benchv2/src/benchmarks/load_sql_1k_bench.rs, benchv2/src/benchmarks/load_mixed_rw_bench.rs, and benchv2/src/reporter/json_reporter.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational shared execution substrate
3. Complete Phase 3: User Story 1
4. Stop and validate that stream tables now use lightweight planning with no retained legacy fallback path

### Incremental Delivery

1. Build the shared crate and foundational abstractions once
2. Land US1 to prove the first exec-backed provider family
3. Land US2 to eliminate provider-family duplication and centralize base traits
4. Land US3-US5 to finish shared pruning and cold-path behavior on the modernized path
5. Land US6-US8 to improve metadata, narrow reads, and remove stale interfaces
6. Land US9 and Phase 12 to prove performance and delete remaining legacy code

### Cleanup Policy

1. Do not keep backward-compatibility shims for the old scan architecture
2. Delete duplicate helpers as soon as the shared replacement is validated
3. Prefer one documented shared abstraction in the new crate over repeating small variations across provider families
4. Add comments only where the shared execution or MVCC behavior is non-obvious; avoid noise comments

---

## Notes

- `[P]` tasks mark real parallel opportunities, but many phases still touch the same shared abstractions and are safest in the listed priority order
- The task plan intentionally creates a new shared crate because the requested cleanup requires centralizing logic instead of leaving reusable execution code scattered across existing crates
- System tables are explicitly routed onto one shared logic path rather than keeping a separate MemTable-materializing scan implementation
- User, shared, and stream tables are explicitly required to consume shared base traits instead of carrying parallel scan implementations
- Legacy code removal is part of completion criteria for each story, not a later optional cleanup pass
- Total tasks: 61