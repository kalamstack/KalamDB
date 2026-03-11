# Vector Search Architecture

## Overview

KalamDB vector search is built around three principles:

1. Per-table and per-column hot staging in RocksDB for low-latency ingestion.
2. Authoritative vector index artifacts in cold storage (not persisted in RocksDB).
3. Manifest-driven metadata (`manifest.json`) for versioning, watermarking, and recovery.

This architecture keeps write latency low while ensuring vector index state is durable and queryable after restart.

## Crate Ownership

- `kalamdb-vector`: Vector-specific staging models, partition naming, flush logic, and query functions (`COSINE_DISTANCE`, `vector_search`).
- `kalamdb-tables`: Stages vector upsert/delete ops during table writes.
- `kalamdb-core`: Orchestrates DDL (`CREATE/DROP INDEX`), manifest updates, flush integration, and DataFusion runtime wiring.
- `kalamdb-system`: Manifest models (`vector_indexes`, engine/metric/state/watermark fields).

## Hot Staging Model (RocksDB)

Hot staging uses `IndexedEntityStore` pattern with separate partitions per `table_id + column`:

- User table ops partition: `vix_<table_id>_<column>_user_ops`
- User table PK index partition: `vix_<table_id>_<column>_user_pk_idx`
- Shared table ops partition: `vix_<table_id>_<column>_shared_ops`
- Shared table PK index partition: `vix_<table_id>_<column>_shared_pk_idx`

Primary keys:

- User tables: `(user_id, seq, pk)`
- Shared tables: `(seq, pk)`

Secondary PK index keys:

- User tables: `(user_id, pk, seq)`
- Shared tables: `(pk, seq)`

Stored operation payload (`VectorHotOp`) includes:

- `op_type` (`upsert` or `delete`)
- `vector` or `vector_ref`
- `dimensions`, `metric`, `updated_at`
- `table_id`, `column_name`, `pk`

## Flush and Cold Persistence

During flush, vector ops are read from staging stores and compacted to latest-op-per-PK after `last_applied_seq` watermark:

1. Scan staged ops for each embedding column.
2. Keep latest op per PK where `seq > last_applied_seq`.
3. Write snapshot artifact to cold storage under table root.
4. Update `manifest.json` `vector_indexes[column]` atomically through manifest service.
5. Prune applied hot-staging ops.

Current snapshot artifact format is binary `.vix`:

- User/shared scope (under each scope's table root): `vec-<column>-snapshot-<version>.vix`
- `.vix` payload stores:
  - serialized `usearch` ANN index bytes
  - key-to-row-id mapping (`u64 key` -> `pk`)
  - index metadata (`dimensions`, `metric`, `last_applied_seq`, `next_key`)

## Manifest Metadata

Each indexed column is tracked under `manifest.vector_indexes[column_name]`:

- `enabled`
- `metric` (`cosine`, `l2`, `dot`)
- `engine` (`usearch`)
- `dimensions`
- `snapshot_path`
- `snapshot_version`
- `last_applied_seq`
- `state` (`active`, `syncing`, `error`)

This makes cold artifacts authoritative while RocksDB remains transient staging.

## Query Path

Primary SQL query form:

```sql
SELECT *
FROM app.documents
ORDER BY COSINE_DISTANCE(embedding, '[0.12,0.44,...]')
LIMIT 10;
```

`vector_search` runtime resolves caller scope (user vs shared), then:

1. Loads cold `.vix` and searches the persisted `usearch` index.
2. Builds a transient hot `usearch` index from RocksDB ops with `seq > last_applied_seq`.
3. Merges cold/hot candidates (hot updates/deletes shadow cold entries).
4. Returns `(row_id, score)` candidates.

## DDL Lifecycle

Vector indexing is controlled per embedding column:

```sql
ALTER TABLE app.documents CREATE INDEX embedding USING COSINE;
ALTER TABLE app.documents DROP INDEX embedding;
```

`CREATE INDEX`:

- Validates column is `EMBEDDING(n)`.
- Enables/creates manifest metadata for each scope.
- Creates per-column hot staging partitions.

`DROP INDEX`:

- Deletes cold vector artifacts for that column/scope under `vec-<column>-snapshot-*.vix`.
- Marks index as disabled in manifest metadata.
- Drops per-column staging partitions.

## Isolation Model

- User tables: scope isolation by `user_id` in keys and per-user manifest/snapshot paths.
- Shared tables: single shared scope per table+column.

No per-user RocksDB column family explosion is required.

## FILE/Attachment Indexing Path

`VectorHotOp` supports `vector_ref` in addition to inlined vectors. This enables asynchronous pipelines (for example FILE/text extraction + embedding generation) to stage references first and materialize vectors before flush.

## Notes on Engines

- Current engine is `usearch` only.
- Engine selection is persisted in manifest metadata.
