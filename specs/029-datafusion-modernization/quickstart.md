# Quickstart: DataFusion Modernization Cutover

## Goal

Implement the provider and execution refactor in measured phases so KalamDB keeps the current DataFusion 53.x line, moves work out of planning, adopts custom execution plans and streams, improves pruning and metadata, removes stale query-engine usage on targeted surfaces, and proves both correctness and performance before deleting legacy MemTable-backed scan paths.

## Recommended Phase Order

### Phase 0: Freeze target and capture baselines

1. Lock the feature scope to the current DataFusion 53.x family already in the workspace.
2. Capture baseline `EXPLAIN` and `EXPLAIN ANALYZE` output for:
   - PK lookups
   - selective filters
   - `LIMIT` queries
   - `COUNT(*)`
   - joins
   - system tables
   - views
   - vector search
3. Capture baseline latency, planning time, memory, and concurrency results for the same classes.

### Phase 1: Build the shared scan execution substrate

1. Create reusable execution foundations that own `Arc<PlanProperties>` and implement:
   - projection and filter metadata carriage
   - `limit` carriage
   - `partition_statistics`
   - output partitioning and ordering
   - boundedness
   - `SendableRecordBatchStream` construction
2. Use [backend/crates/kalamdb-transactions/src/overlay_exec.rs](../../backend/crates/kalamdb-transactions/src/overlay_exec.rs) as the in-repo reference for modern execution-plan structure, but not as the final abstraction.

### Phase 2: Cut over non-MVCC providers first

1. Migrate stream tables.
2. Migrate system tables.
3. Migrate views.
4. Migrate vector search.
5. Remove production reliance on synthetic MemTable scan wrappers for those families.

### Phase 3: Replace blanket pushdown claims with exact capability reporting

1. Audit `supports_filters_pushdown` implementations across provider families.
2. Use `Exact` only when the source can guarantee semantic correctness.
3. Use `Inexact` only when the source measurably reduces work and later correctness handling remains in place.
4. Use `Unsupported` everywhere else.

### Phase 4: Add real statistics and plan properties

1. Surface global and partition-aware statistics where possible.
2. Expose meaningful partitioning, ordering, and boundedness.
3. Align partition counts with `target_partitions` where practical rather than defaulting blindly.

### Phase 5: Rebuild the cold Parquet path around execution-time planning

1. Preserve manifest-driven selection and schema evolution.
2. Stop collapsing historical reads into one batch during planning.
3. Create a KalamDB-owned Parquet scan exec first.
4. Revisit deeper DataFusion datasource/file-scan integration only if benchmarks show the first-stage design cannot meet the targets.

### Phase 6: Redesign MVCC execution for user and shared tables

1. Move hot-plus-cold merge into execution.
2. Move version resolution, tombstone filtering, and snapshot visibility into execution.
3. Preserve PK lookup and count-only fast paths where still safe and valuable.

### Phase 7: Modernize DML and transaction overlay integration

1. Update DML helpers that currently assume materialized provider scans.
2. Keep overlay composition correct on top of the new scan-exec model.
3. Preserve transaction semantics while the underlying scan architecture changes.

### Phase 8: Tighten session and planner integration for concurrency

1. Revisit `SessionContext` creation and reuse.
2. Re-tune batch size and `target_partitions`.
3. Re-evaluate memory-pool policy.
4. Measure plan-cache behavior under the new planning model.
5. Improve result streaming behavior for concurrent workloads.

### Phase 9: Verify, tune, and remove old paths

1. Add explain-shape assertions.
2. Run targeted performance and concurrency benchmarks.
3. Re-run correctness and smoke validation.
4. Remove legacy MemTable-backed scan code only after deltas are understood and accepted.

## Relevant Implementation Surfaces

- `Cargo.toml`
- `backend/crates/kalamdb-core/src/sql/datafusion_session.rs`
- `backend/crates/kalamdb-core/src/sql/context/execution_context.rs`
- `backend/crates/kalamdb-tables/src/utils/base.rs`
- `backend/crates/kalamdb-tables/src/user_tables/user_table_provider.rs`
- `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs`
- `backend/crates/kalamdb-tables/src/stream_tables/stream_table_provider.rs`
- `backend/crates/kalamdb-tables/src/utils/datafusion_dml.rs`
- `backend/crates/kalamdb-system/src/providers/base.rs`
- `backend/crates/kalamdb-system/src/macros.rs`
- `backend/crates/kalamdb-views/src/view_base.rs`
- `backend/crates/kalamdb-vector/src/sql/vector_search.rs`
- `backend/crates/kalamdb-tables/src/utils/parquet.rs`
- `backend/crates/kalamdb-tables/src/manifest/planner.rs`
- `backend/crates/kalamdb-filestore/src/parquet/reader.rs`
- `backend/crates/kalamdb-session-datafusion/src/secured_provider.rs`

## Batched Validation Flow

1. Run one meaningful `cargo check` batch per major phase and fix the full compile set in one pass.

```bash
cd /Users/jamal/git/KalamDB && CARGO_TERM_COLOR=never cargo check \
  -p kalamdb-core \
  -p kalamdb-session-datafusion \
  -p kalamdb-tables \
  -p kalamdb-system \
  -p kalamdb-views \
  -p kalamdb-vector \
  -p kalamdb-transactions \
  -p kalamdb-filestore \
  > datafusion-modernization-check.txt 2>&1
```

2. After each provider-family cutover, run targeted `nextest` coverage.

```bash
cd /Users/jamal/git/KalamDB && cargo nextest run \
  -p kalamdb-tables \
  -p kalamdb-system \
  -p kalamdb-views \
  -p kalamdb-transactions \
  -p kalamdb-vector \
  -p kalamdb-core
```

3. Keep a backend server running for smoke validation when end-to-end checks are needed.

```bash
cd /Users/jamal/git/KalamDB/backend && KALAMDB_ROOT_PASSWORD=kalamdb123 cargo run --bin kalamdb-server -- server.toml
```

4. Run CLI smoke coverage after the relevant phases stabilize.

```bash
cd /Users/jamal/git/KalamDB/cli && ./run-tests.sh
```

5. Use `benchv2` or focused performance harnesses to record latency, planning time, memory, and concurrency results, and report runtimes in seconds for all relevant performance tests.

## Expected Verification Outcomes

- Migrated provider families no longer rely on production synthetic MemTable scan paths.
- Explain output shows provider-specific execution nodes and source-level work reduction instead of hidden pre-materialized scans.
- Explain analyze output or equivalent evidence shows pruning, limit handling, and source reduction where expected.
- MVCC-backed reads preserve visibility, tombstone handling, and transaction overlay semantics.
- Historical reads preserve schema evolution correctness while touching less irrelevant data.
- Targeted provider, planner, and execution surfaces have no remaining deprecated or obsolete query-engine interface usage.
- Legacy scan paths are removed only after correctness and performance evidence is accepted.