# Scalar Secondary Indexes Implementation Plan

> **0.7 program:** This is one of three essential tracks for KalamDB 0.7. Implement it alongside [centralized serialization](2026-02-14-flatbuffers-flexbuffers-vortex-migration-plan.md) and [functions V1](functions-v1-implementation.md). Combined sequence and release gate: [2026-09-01-kalamdb-0.7.md](2026-09-01-kalamdb-0.7.md).

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Give USER and SHARED tables PK-shaped scalar secondary indexes so equality (and later composite) lookups are prefix scans, flattening chat history/reconnect and RLS membership bind as tables grow.

**Architecture:** One indexing core owns prefix keys, atomic CF maintenance, filter-to-prefix, and catalog types. Start from `IndexDefinition` + `IndexedEntityStore`; **rewrite and extract** rather than cloning PK adapters. Home is `kalamdb-store`’s index module, or a new `kalamdb-index` crate if that keeps store/tables/vector from each growing a private copy. Vector search **uses or extends** that core (hot PK identity, catalog, backfill); USearch remains the ANN engine only — not a second index runtime. **First** inventory what DataFusion 55 already gives `TableProvider` so we do not invent a parallel planner. Persist **logical** index catalog on `TableDefinition` / `system.schemas`. Parse scalar `CREATE INDEX` / `DROP INDEX`, attach extra indexes when opening the store, seek in the hot scan. **Every flush (and compaction) of an indexed table must write Parquet blooms and row-group / segment min-max for those columns**, then cold scans prune with that metadata — same machinery PK already uses, not a new cold index. The `kalam` CLI schema-diff must parse and emit those statements.

**Tech Stack:** Rust workspace, `IndexedEntityStore` / extracted index crate, storekey prefix encoding, DataFusion 55 `TableProvider` filter pushdown, `kalamdb-dialect` ALTER TABLE, `kalam-schema-diff`. Vector crate depends on the shared core.

**Related:** [2026-08-20-shared-table-rls.md](2026-08-20-shared-table-rls.md) already requires covering `(principal, relation_key)` indexes and forbids a parallel RLS index system. [2026-08-25-indexed-live-rls-routing.md](2026-08-25-indexed-live-rls-routing.md) covers live *fan-out* keys; this plan covers *SQL/storage* lookups. Index catalog and non-unique index **values** persist through `kalamdb-serialization` ([serialization plan](2026-02-14-flatbuffers-flexbuffers-vortex-migration-plan.md) §3.5 / Phase 4); do not keep JSON PK arrays as the 0.7 format. Functions/chat procedures on `messages` and `conversation_members` need these indexes in the same 0.7 release.

**Out of scope:** Teaching USearch to answer scalar `WHERE col = ?` (ANN ≠ prefix scan). Table-wide membership invalidation / per-principal live rebind. Growing `kalamdb-store::SecondaryIndex` JSON maps as a second user/shared index runtime — fold useful bits into the one core or leave them as a legacy system-table path that we stop copying. A private index persistence codec.

---

## Why

The Masky-style `chat_realtime` bench (500 conversations, 2 minutes) held live send/receive flat (keyed by `conversation_id`, 0 delivery timeouts, late/early SQL ops **1.08×**) but SQL latency and RSS climbed (avg SQL **3ms → 41ms**, RSS **262 MiB → 2.20 GiB**). History, reopen, and reconnect snapshots are:

```sql
SELECT … FROM messages WHERE conversation_id = ? ORDER BY created_at_ms DESC LIMIT 50
```

Shared/user tables only maintain a PK RocksDB index. `conversation_id` equality is applied after a full hot (and cold) scan. Concurrent reconnects allocate scan buffers proportional to `(all rows × concurrent snapshots)`.

RLS bind on `conversation_members` similarly scans all membership rows unless the principal column is the PK — and PK lookup returns **one** row, which is wrong for many rooms per user. A **non-unique** `(user_id, seq)` index is the correct bind path.

---

## Inspected baseline (one core — rewrite copies, do not fork a second engine)

- `backend/crates/kalamdb-store/src/indexed_store.rs` — `IndexDefinition`, atomic index writes, `find_best_index_for_filters`, `scan_by_index`, `get_latest_by_index_prefix`. Already takes `Vec<Arc<dyn IndexDefinition>>`.
- Three near-copies of PK `IndexDefinition`: `shared_tables/pk_index.rs`, `user_tables/pk_index.rs`, `kalamdb-vector/.../hot_staging/pk_index.rs`. **Do not add a fourth.** Extract one generic prefix-index adapter (column list + seq + optional user_id) and make PK / scalar / vector-hot-PK thin wrappers.
- `backend/crates/kalamdb-tables/src/shared_tables/shared_table_store.rs` `new_indexed_shared_table_store` — currently `vec![pk_index]` only. Same for user tables.
- System compound example: `backend/crates/kalamdb-system/src/providers/jobs/jobs_indexes.rs` `(status, created_at, job_id)` — another `IndexDefinition` impl. Prefer the shared adapter here too when the key shape matches; do not copy-paste a new jobs-style file for `conversation_id`.
- Vector ANN search: `backend/crates/kalamdb-vector/src/usearch_engine.rs`. Keep it as an **extension** of the core (identity via the shared PK/scalar index; similarity via USearch). Rewrite vector hot staging to call the extracted adapter instead of its private `pk_index.rs`.
- `kalamdb-store/src/index/` (`IndexManager`, `SecondaryIndex` JSON maps) vs `indexed_store.rs` (`IndexDefinition`): two styles. This plan consolidates on the `IndexDefinition` / `IndexedEntityStore` path for user/shared/vector-hot. Extract or delete overlap; do not implement scalar indexes on both.
- Dialect `CREATE INDEX` today is vector-only (`ColumnOperation::CreateVectorIndex` in `backend/crates/kalamdb-dialect/src/ddl/alter_table.rs`).
- SQL scan: providers report `mvcc_filter_capability` as Exact because MVCC re-checks predicates (`kalamdb-datafusion-sources`), **not** because they seek a secondary index. PK point lookup is a special path (`pk_exists_in_hot` / `find_by_pk`).
- `TableDefinition` (`backend/crates/kalamdb-commons/src/models/schemas/table_definition.rs`) has no scalar index list yet. `system.schemas` (`schemas_definition.rs` / `schemas_provider.rs`) projects `columns` and `options` JSON from that struct; there is no `indexes` column yet.
- Embedding/vector indexes today: **logical+operational** state lives on `manifest.vector_indexes` (`kalamdb-system` `Manifest`), not on `TableDefinition`. DDL `ALTER TABLE … CREATE INDEX col USING COSINE` updates the manifest. Scalar index **definitions** belong on `TableDefinition` so they version with schema history in `system.schemas`. Operational extras (backfill watermark, CF health) may still sit on the manifest the way `vector_indexes` stores snapshot/watermark fields — do not put only-runtime ANN snapshots into `system.schemas`.
- DataFusion 55: user/shared providers already implement `supports_filters_pushdown` → `base_supports_filters_pushdown` and `scan(..., filters)`. `IndexDefinition::supports_filter` / `filter_to_prefix` exist for that path. `TableProvider::statistics` is computed from the manifest but a comment in `base.rs` notes mainline DataFusion does not consume it yet. DataFusion does **not** own RocksDB CFs or `CREATE INDEX` for custom providers — index **use** is inside our `scan`, not a DataFusion physical IndexJoin.
- CLI schema-diff (`cli/crates/kalam-schema-diff`): `parse_schema` ignores unknown statements (`Statement` catch-all `_ => {}`). `CREATE INDEX` / Kalam `ALTER TABLE … CREATE INDEX` in `schema.sql` currently **vanish**. `diff_existing_table` only handles columns, constraints comments, and tblproperties. Vector indexes in project schemas have the same hole.
- Cold prune already exists **for PK only**: `CachedTableData::compute_indexed_columns` (`kalamdb-core/src/schema_registry/cached_table_data.rs`) puts PK names on `bloom_filter_columns` and PK+`_seq` on `indexed_columns`. Flush (`kalamdb-filestore` `writer_properties`, `kalamdb-flush` `extract_column_stats`) writes Parquet blooms (files ≥ 1024 rows) and manifest `column_stats` min/max. Readers prune segments via min/max (`planner.rs` / `existence_checker.rs`) and row groups via PK bloom (`filestore/src/parquet/reader.rs` `with_pk_bloom_values`). Scalar indexed columns are **not** on those lists today, so `WHERE conversation_id = ?` after flush still opens every segment.

---

## Product rules

1. Scalar indexes are **non-unique by default**. Unique scalar indexes are allowed only when the column (or column list) is unique in the live MVCC winner set; PK remains the unique live-row identity.
2. Index keys always append `seq` (and user-table `user_id` where the row id includes it) so versions stay distinct, matching PK.
3. `CREATE INDEX name ON t (col)` and `ALTER TABLE t CREATE INDEX col` (pick one PostgreSQL-shaped form and stick to it in dialect tests). Vector form stays `CREATE INDEX col USING COSINE` and must not be parsed as scalar.
4. Dropping a column fails if a scalar index still lists it; `DROP INDEX` removes the CF and catalog entry.
5. After flush, a `WHERE col = ?` query must not silently miss cold rows, and must not full-scan every Parquet file when an index exists on `col`. **On every flush and compaction** of a table that has scalar (or PK) indexes:
   - Enable Parquet **bloom filters** on each indexed column (reuse `writer_properties` / `bloom_filter_columns`; keep the existing ≥1024-row bloom skip for tiny files).
   - Record **min/max** for those columns in segment `column_stats` (reuse `indexed_columns` + `FlushManifestHelper::extract_column_stats` / `compute_min_max`).
   - Keep Parquet **row-group** statistics (writer already sets `max_row_group_row_count`); cold readers must prune row groups with blooms and/or RG min/max the way PK bloom pruning already does — generalize `with_pk_bloom_values`, do not add a second reader.
   - Compaction (`small_segment.rs`) must pass the same bloom/indexed column lists so rewritten files stay pruneable.
   - After `CREATE INDEX`, refresh `CachedTableData` so the **next** flush picks up the new columns. Segments written before the index may lack blooms until compacted; those files stay **correct** via filtered reads, but new flushes must not omit the metadata.
   Do not ship hot-only RocksDB indexes that go stale after `FLUSH_POLICY`.
6. Missing index is not a correctness bug; it is a scan. EXPLAIN / RLS already planned missing-index warnings — reuse that language.
7. Catalog split (match embedding indexes):
   - **Schema catalog:** index name, columns, uniqueness, kind (`scalar` vs `vector`) on `TableDefinition`, stored via `SchemasStore`, visible as JSON on `system.schemas` **alongside** `columns` and `options`. Schema version increments on CREATE/DROP INDEX like other ALTER TABLE.
   - **Operational catalog:** ANN snapshots, watermarks, engine/metric stay on `manifest.vector_indexes`. If scalar backfill needs a watermark, add a sibling map on the manifest — do not invent a second schema registry.
   - After this work, `SELECT … FROM system.schemas` must show scalar indexes without joining the manifest. Prefer also recording that a column **has** a vector index on the table definition so dump/diff see both kinds in one place; do not copy USearch snapshot paths into `system.schemas`.
8. Do not bypass DataFusion's existing filter-pushdown contract. If DF already classifies a predicate as Exact/Inexact, reuse that; only add seek logic inside the provider scan. Do not add a custom logical optimizer rule unless the inventory (Task 1) proves `scan` filters are insufficient.
9. `kalam` schema-diff is part of the product surface. Adding indexes without parser/emitter/diff tests means `kalam migration create` / `kalam dev` will not apply them from `schema.sql`.
10. **One indexing core, no duplicate implementations.** Do not be afraid to rewrite `IndexDefinition`, `IndexedEntityStore`, PK adapters, or vector hot PK so core features (key encoding, atomic multi-index writes, prefix scan, `filter_to_prefix`, catalog types, backfill) live in **one** module or crate. Reuse what is already there; if it is in the wrong crate, **move it**. If two copies exist, **merge them**. Vector search extends or uses that core — it must not keep a private RocksDB index stack. A dedicated `kalamdb-index` crate is allowed when leaving the core inside `kalamdb-store` would force tables/vector/system to reimplement it; RocksDB CFs still go through store backends per crate ownership. Copy-paste of `pk_index.rs` / `jobs_indexes.rs` is a plan failure.

Target indexes for the chat/RLS mix (first consumers, not hardcoded forever):

| Table | Key | Use |
| --- | --- | --- |
| `messages` (SHARED) | `(conversation_id, created_at_ms, seq)` or at least `(conversation_id, seq)` | history, reopen, live snapshot SQL |
| `conversation_members` (SHARED) | `(user_id, seq)` | RLS authorization bind: all rooms for this principal |
| `messages_ai` (USER) | `(conversation_id, seq)` | same history path on USER tables (smaller, still correct) |

Equality on `conversation_id` alone is enough to stop *global* table growth in the bench. Composite `created_at_ms` (DESC encoding) is a follow-on for huge rooms; do not block Task 5 on DESC.

---

### Task 1: Inventory DataFusion 55 — reuse vs implement

**Files:**
- Read (do not expand scope): `backend/crates/kalamdb-tables/src/utils/base.rs` (`base_supports_filters_pushdown`, `base_scan`, `statistics`)
- Read: `backend/crates/kalamdb-store/src/indexed_store.rs` (`filter_to_prefix`, `supports_filter`, `find_best_index_for_filters`)
- Read: DataFusion 55 `TableProvider` (`supports_filters_pushdown`, `scan`, `statistics`); Parquet pruning already used on cold path (PK bloom / `column_stats` min-max — Task 8 extends this to scalar index columns)
- Write a short “Decision” subsection at the bottom of this plan file (or a comment on the scan PR) listing: used APIs, unused APIs, and anything we will **not** build (e.g. DataFusion hash indexes, custom optimizer rules)

**Steps:**
1. List every DF hook the user/shared provider already implements. Note Exact vs Inexact vs the comment that `mvcc_filter_capability` Exact is MVCC re-check, not a secondary seek.
2. Confirm DF has no generic “secondary index catalog” for custom `TableProvider`s in 55.0.0 (workspace pin). If a crate feature or `IndexStatistics` exists and is unused, say whether it helps cardinality only or actual seeks.
3. Decision required before Task 5: **seek inside `scan` using already-pushed filters** (expected) vs a new planner rule (only if inventory finds a real gap).
4. Do not implement indexes in this task.

### Task 2: Persist index catalog on `TableDefinition` and `system.schemas`

Catalog bytes follow the 0.7 serialization track: persist `TableDefinition` through `kalamdb-serialization` / `EntityStore` once Phase 3 lands. Do not add a JSON-only index catalog sidecar.

**Files:**
- Modify: `backend/crates/kalamdb-commons/src/models/schemas/table_definition.rs` (and `TableDefinitionRepr` / binary serde tuple — old rows must deserialize)
- Modify: `backend/crates/kalamdb-system/src/providers/tables/schemas_definition.rs` — add JSON column next to `columns` / `options` (e.g. `indexes`)
- Modify: `backend/crates/kalamdb-system/src/providers/tables/schemas_provider.rs` `build_table_def_batch` — project the new field; tests that assert `batch.num_columns() == 14` must bump
- Modify: `schemas_store.rs` tests that build a 14-column schema if they hard-code width
- Test: commons serde tests; system.schemas provider tests

**Steps:**
1. Write failing tests: serialize/deserialize a table with two scalar indexes; omit the field and get `[]`. Query-shaped test: `TableDefinition` with indexes → `system.schemas` batch includes them in the new JSON column.
2. Run focused commons + `kalamdb-system` schema tests; confirm fail.
3. Add `ScalarIndexDefinition` (name, columns, unique) and `scalar_indexes: Vec<…>` with serde default. Optionally a `kind` or a small `vector_index` catalog list so embedding indexes are **named on the schema row** the same way, while `manifest.vector_indexes` remains the operational map.
4. Project JSON from `TableDefinition` in `build_table_def_batch` the same way `columns` and `options` are serialized. Do not read the manifest in the schemas provider for this column.
5. Run tests; confirm pass. Bump any hardcoded column-count assertions.

### Task 3: Unify the indexing core; generic prefix adapter; vector uses it

**Files:**
- Prefer extract into: `backend/crates/kalamdb-store/src/index/` (or new `backend/crates/kalamdb-index/` if store would mix catalog/DataFusion helpers with RocksDB too tightly)
- Rewrite/fold: `kalamdb-tables/.../shared_tables/pk_index.rs`, `user_tables/pk_index.rs`, `kalamdb-vector/.../hot_staging/pk_index.rs` onto the shared adapter
- Modify: `indexed_store.rs` only if the trait needs a richer interface (uniqueness, composite prefix, catalog name) — rewrite the trait rather than wrapping it in a second type
- Do **not** create parallel `column_index.rs` files in both user and shared tables
- Test: unit tests next to the extracted adapter (PK, composite `(conversation_id, seq)`, user-scoped prefix); vector hot-store tests still pass via the adapter

**Steps:**
1. Write failing tests on the **shared** adapter: extract_key for `(conversation_id, seq)`; same conversation_id different seq share prefix; `filter_to_prefix` on `conversation_id = 42`; no prefix for unrelated columns; user-scoped keys include `user_id` once, not via a forked type.
2. Run focused store/index tests; confirm fail.
3. Extract generic column-list prefix index (`scalar_value_to_bytes` + `encode_key` / `encode_prefix`). Partition name stable per table + columns. PK is that adapter with the PK column list. Point `SharedTablePkIndex` / `UserTablePkIndex` / vector hot PK at it (type aliases or thin newtypes), then delete duplicated impl bodies.
4. Vector crate: keep USearch for ANN; hot staging PK index **calls the core**. If `IndexDefinition` is missing something vector needs, add it to the core trait once.
5. Run `kalamdb-store` / `kalamdb-tables` / `kalamdb-vector` tests that touch PK indexes; confirm pass.

Do **not** implement USearch metric kinds in the prefix adapter. Do **not** leave the old `pk_index.rs` copies “for later.”

### Task 4: Open user/shared stores with catalog indexes, not PK-only

**Files:**
- Modify: `backend/crates/kalamdb-tables/src/shared_tables/shared_table_store.rs` `new_indexed_shared_table_store`
- Modify: `backend/crates/kalamdb-tables/src/user_tables/user_table_store.rs`
- Modify: `backend/crates/kalamdb-tables/src/common.rs` if a helper `indexes_from_table_def` belongs there
- Callers that construct stores from `TableDefinition` (schema registry / table provider constructors)

**Steps:**
1. Write a store test: insert two rows with the same `conversation_id`, scan index prefix, get both seqs; PK index still works (`scan_by_index(0, …)`).
2. Run focused tests; confirm fail (store still `vec![pk_index]`).
3. Build `Vec` from the **Task 3 adapter**: PK first (keep index 0 = PK so existing `scan_by_index(0, …)` / `get_latest_by_index_prefix(0, …)` stay valid), then catalog scalar indexes. Same constructor for user and shared; do not hand-roll a second index list builder.
4. Run tests; confirm pass.

### Task 5: Hot SQL scan seeks the best scalar index

**Files:**
- Modify: `backend/crates/kalamdb-tables/src/utils/base.rs` scan path (or shared/user `scan_with_version_resolution_*`)
- Modify: `backend/crates/kalamdb-tables/src/shared_tables/shared_table_provider.rs` and user equivalent only if the shared helper is not enough
- Test: table-provider or `kalamdb-tables` scan tests
- Honor Task 1: use DataFusion-pushed `filters` already passed into `base_scan`; do not add a custom logical plan node unless Task 1 required it

**Steps:**
1. Write a failing test: `SELECT … WHERE conversation_id = 1` on a shared table with an extra index does not visit unrelated conversations’ hot keys (assert via recording backend or row-count + planted extra rooms).
2. Run focused tests; confirm fail (full scan).
3. On hot scan, call `store.find_best_index_for_filters(filters)`; if `Some`, `scan_by_index` / prefix get + MVCC winner selection; else existing scan. Keep `supports_filters_pushdown` honest (Exact only when the index + MVCC path actually guarantees the predicate).
4. Keep inexact pre-winner pruning rules for mutable columns as documented in the RLS plan (do not push `group_id` into pre-MVCC inexact pruning).
5. Run tests; confirm pass.

### Task 6: Scalar `CREATE INDEX` / `DROP INDEX` DDL (not vector)

**Files:**
- Modify: `backend/crates/kalamdb-dialect/src/ddl/alter_table.rs` (`ColumnOperation` variants; keep `CreateVectorIndex` distinct)
- Modify: `backend/crates/kalamdb-handlers/crates/ddl/src/table/alter.rs`
- Test: dialect parse tests; handler tests
- See `docs/development/how-to-add-sql-statement.md`

**Steps:**
1. Write failing parse tests: `ALTER TABLE t CREATE INDEX conversation_id` (or chosen grammar) is scalar; `CREATE INDEX embedding USING COSINE` remains vector; mixing them errors.
2. Run dialect tests; confirm fail.
3. Parse, persist on `TableDefinition` (so `system.schemas` updates with `schema_version`), create index CF, **backfill** existing hot rows (iterate MVCC winners, `extract_key`, batch put). Empty table must work. Vector CREATE INDEX continues to update `manifest.vector_indexes`; if Task 2 added a logical vector entry on `TableDefinition`, write both.
4. DROP removes catalog + partition (same drop path as `IndexedEntityStore::drop_all_partitions` for that CF).
5. Run dialect + handler tests; confirm pass. Assert `system.schemas` JSON includes the new index after CREATE and omits it after DROP.

### Task 7: Kalam CLI schema-diff (`kalam-schema-diff`)

**Files:**
- Modify: `cli/crates/kalam-schema-diff/src/model.rs` — indexes on `Table` (name, columns, unique, kind: scalar vs vector metric)
- Create: `cli/crates/kalam-schema-diff/src/parser/index.rs` (or extend `parser/mod.rs`)
- Modify: `cli/crates/kalam-schema-diff/src/parser/mod.rs` — stop swallowing `CREATE INDEX` / Kalam `ALTER TABLE … CREATE INDEX` (today `_ => {}`)
- Create: `cli/crates/kalam-schema-diff/src/emitter/create_index.rs`, `drop_index.rs`
- Modify: `cli/crates/kalam-schema-diff/src/emitter/table.rs` `diff_existing_table` — add/drop indexes; DROP INDEX is destructive (follow `allow_destructive` like DROP COLUMN)
- Modify: `cli/crates/kalam-schema-diff/src/emitter/create_table.rs` if new tables should emit indexes after `CREATE TABLE`
- Test: `cli/crates/kalam-schema-diff/tests/schema_diff.rs`

**Steps:**
1. Write failing tests: `schema.sql` with `ALTER TABLE app.messages CREATE INDEX conversation_id;` (or chosen grammar) round-trips; diff empty → that file emits the CREATE INDEX; removing the index with `allow_destructive` emits DROP INDEX; without it, advisory comment only. Vector `CREATE INDEX embedding USING COSINE` must still parse and diff, not be classified as scalar.
2. Run `cargo nextest run -p kalam-schema-diff`; confirm fail (statements ignored).
3. Parse with the same custom-SQL path used for policies/topics when sqlparser cannot handle Kalam `ALTER TABLE CREATE INDEX`. Attach indexes to the table key. Duplicate index names error.
4. Emit statements that match dialect grammar from Task 6 (one form, no drift).
5. Run schema-diff tests; confirm pass. If CLI e2e covers `migration create` from `schema.sql`, add one case with a scalar index.

Do not leave indexes as “manual review required” comments unless the change is genuinely unsupported (e.g. unique index on a non-unique column).

### Task 8: Flush writes blooms + min/max; cold scan prunes indexed columns

**Files:**
- Modify: `backend/crates/kalamdb-core/src/schema_registry/cached_table_data.rs` `compute_indexed_columns` — PK **and** scalar index columns (from `TableDefinition`)
- Modify: `backend/crates/kalamdb-filestore/src/parquet/reader.rs` — generalize PK bloom row-group prune to any equality column (or share one helper)
- Modify: `backend/crates/kalamdb-tables/src/manifest/planner.rs` (and PK helpers in `utils/pk/`) — segment min/max prune for indexed columns, not PK only
- Modify: cold scan in `kalamdb-tables` (`base.rs` / shared+user providers) to apply that prune on `WHERE col = ?`
- Touch: `kalamdb-flush` only if flush metadata is not already driven by `CachedTableData` lists (`flush/base.rs`, `compaction/small_segment.rs`)
- Test: filestore bloom test pattern (`filestore/src/tests/bloom_filter_pk_test.rs`); table/e2e flush then indexed `WHERE`
- See [hot-cold-storage-unification.md](../architecture/hot-cold-storage-unification.md)

**Steps:**
1. Write a failing test: table with `conversation_id` index, many rooms, flush (enough rows to pass the 1024-row bloom threshold **or** still assert min/max on smaller files). `WHERE conversation_id = ?` returns the right rows **and** does not visit every segment/row group (spy on selected row groups, segment prune, or bloom skip counts). Assert flushed Parquet has bloom enabled on `conversation_id` and manifest `column_stats` has min/max for that column_id.
2. Run focused filestore/tables/flush tests; confirm fail (PK-only `compute_indexed_columns`).
3. Extend `compute_indexed_columns` from `TableDefinition.scalar_indexes` (all key columns, plus PK and `_seq`). Bloom: equality-friendly types (text/int/uuid/bool — skip embeddings). Min/max: same list. Do not fork a second flush writer.
4. Generalize row-group bloom prune and segment min/max prune to those columns. Cold `WHERE conversation_id = ?` uses them. If a segment has no stats for that column (pre-index file), include it (fail closed / conservative), then filter.
5. Confirm compaction uses the same lists. After CREATE INDEX, cache rebuild so the next flush is indexed.
6. Run tests; confirm pass.

Do not ship a release where flushed rows disappear from indexed queries, or where indexed cold queries ignore blooms/min-max that flush just wrote.

### Task 9: RLS membership bind uses `user_id` index

**Files:**
- Modify: `backend/crates/kalamdb-tables/src/shared_tables/shared_table_authorization.rs` `load_membership_authorization_set`
- Test: existing RLS tests plus a test with many unrelated membership rows where bind cost/result stays limited to the principal’s rooms

**Steps:**
1. Write a failing test: 1000 membership rows for other users, Alice has 2 rooms; authorization set contains only Alice’s keys; must not require scanning all 1002 as the *lookup* API (assert index prefix or spy if available).
2. Run focused tests; confirm fail (full `scan_with_version_resolution_to_kvs_async`).
3. If relation principal column is indexed (non-unique), prefix-scan that index and build `AuthorizationSet` from those rows. Keep the full scan as fallback when no index exists (fail closed, not fail open).
4. Do **not** use PK `find_by_pk` for `user_id` when a user has many rooms.
5. Run tests; confirm pass.

### Task 10: Chat bench + docs

**Files:**
- Modify: `benchv2/src/chat_runtime/chat_realtime_bench.rs` — `CREATE INDEX` on `messages(conversation_id)` and `conversation_members(user_id)` after CREATE TABLE / POLICY
- Modify: example `schema.sql` files that should declare those indexes (so CLI diff stays honest)
- Modify: `docs/reference/sql.md`, `docs/architecture/hot-cold-storage-unification.md` as needed
- Canonical skills in `../kalamdb-skills` if SQL syntax is user-facing (workspace rule)

**Steps:**
1. Add indexes to the Masky schema once DDL works; put the same statements in any tracked `schema.sql` the bench/example uses.
2. Re-run `./run-chat-realtime.sh --minutes 2 --users 4000 --realtime-convs 500 --messages-per-minute 20 --mutation-every-messages 10`.
3. Success: stability assessment **flat** (no late/early avg_sql ≫ 1.75 with end/start ≫ 1.75, no late RSS +50%). Throughput may stay ~300 SQL/s; historic_select p90 should drop vs the 2026-08-30 2-minute baseline (~295ms).
4. Document scalar vs vector `CREATE INDEX`, `system.schemas.indexes` (or chosen column name), and that `kalam migration create` emits index DDL.

---

## Explicit non-goals (do not fold into this plan)

- One **lookup** path for ANN similarity and scalar equality (USearch vs RocksDB prefix stay different engines). Sharing the **maintenance/catalog/PK** core is in scope.
- Changing live keyed routing (already done).
- Per-principal membership invalidation (still table-wide generation). Indexes do not fix that.
- A DataFusion-owned index engine or custom logical optimizer unless Task 1 proves `scan` filters are not enough.
- A second `column_index.rs` per table kind, or a vector-only RocksDB index that does not use the extracted adapter.

---

## Suggested order

Follow [0.7 waves](2026-09-01-kalamdb-0.7.md) when working this plan next to serialization and functions. Task 1 is a written DataFusion decision (no storage work). Task 2 is the catalog (`TableDefinition` + `system.schemas`). Task 3 extracts/rewrites the **one** indexing core and points vector hot PK at it. Tasks 4–5 unlock hot-path chat SELECTs using that core and DF-pushed filters. Task 6 makes indexes user-declarable. Task 7 is CLI schema-diff so `schema.sql` is not a lie. Task 8 is flush-time Parquet blooms + min/max/row-group prune for indexed columns (correctness and cold speed). Task 9 is RLS scale. Task 10 proves the original bench.

---

## Decision: DataFusion 55 seek vs planner (Task 1)

Written 2026-09-05. No storage or planner code in this task.

### Used APIs (keep)

- `TableProvider::supports_filters_pushdown` → `base_supports_filters_pushdown` → `pushdown_results_for_filters` + `SourceProvider::filter_capability`.
- User/shared `filter_capability` calls `mvcc_filter_capability`, which reports **`Exact` for every filter**. That means DataFusion 55 will not wrap the scan in an extra `FilterExec`. It does **not** mean a secondary index seek happened. Exact here is “we re-check the predicate on MVCC-resolved rows inside deferred exec.”
- `TableProvider::scan` / `base_scan` already receives the planner-pushed `filters` slice. `mvcc_filter_evaluation` splits Exact vs Inexact for hot/cold **pruning hints** only.
- `TableProvider::statistics` is implemented from the Parquet manifest. A comment in `base.rs` is still true: DataFusion 55 mainline does not consume these statistics for join/scan planning.
- `IndexedEntityStore::supports_filter` / `filter_to_prefix` / `find_best_index_for_filters` already exist. PK adapters use them. Scalar USER/SHARED indexes should plug into that same path.

### Unused / not available for custom providers

- DataFusion has **no generic secondary-index catalog** for custom `TableProvider`s in 55.0.0. There is no workspace `IndexStatistics` hook that performs RocksDB prefix seeks.
- DataFusion hash indexes, `IndexJoin`, and built-in catalog indexes apply to DF-owned tables, not Kalam `TableProvider` implementations.
- Custom logical optimizer rules are unused and unnecessary if `scan` already sees equality filters.

### Decision for Task 5

**Seek inside `scan` using already-pushed filters.** Call `find_best_index_for_filters` (or the extracted prefix adapter) from the user/shared hot scan. Do not add a Kalam logical optimizer rule, physical `IndexJoin`, or DataFusion-owned index engine.

Do not build: DF hash indexes, a second filter-pushdown contract, or a planner node whose only job is to attach `conversation_id = ?` to the provider. The provider already gets that predicate.
