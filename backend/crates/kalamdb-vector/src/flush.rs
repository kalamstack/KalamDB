use crate::hot_staging::{
    new_indexed_shared_vector_hot_store, new_indexed_user_vector_hot_store,
    normalize_vector_column_name, SharedVectorHotOpId, UserVectorHotOpId, VectorHotOp,
    VectorHotOpType,
};
use crate::snapshot_codec::{decode_snapshot, encode_snapshot, VixSnapshotEntry, VixSnapshotFile};
use crate::usearch_engine::{add_vector, create_index, export_vector, load_index, serialize_index};
use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_filestore::{FilestoreError, StorageCached};
use kalamdb_store::{EntityStore, StorageBackend};
use kalamdb_system::{Manifest, VectorEngine, VectorMetric};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

const VECTOR_SCAN_LIMIT: usize = 100_000;

#[derive(Debug, Clone)]
pub struct VectorFlushError {
    message: String,
}

impl VectorFlushError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn with_prefix(prefix: &str, err: impl Display) -> Self {
        Self::new(format!("{}: {}", prefix, err))
    }
}

impl Display for VectorFlushError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for VectorFlushError {}

pub trait VectorManifestStore: Send + Sync {
    fn ensure_manifest_initialized(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, VectorFlushError>;

    fn persist_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
    ) -> Result<(), VectorFlushError>;
}

fn embedding_columns(schema: &SchemaRef) -> Vec<(String, u32)> {
    schema
        .fields()
        .iter()
        .filter_map(|field| match field.data_type() {
            DataType::FixedSizeList(item, size)
                if matches!(item.data_type(), DataType::Float32)
                    && *size > 0
                    && !field.name().starts_with('_') =>
            {
                Some((field.name().clone(), *size as u32))
            },
            _ => None,
        })
        .collect()
}

fn vector_snapshot_filename(column_name: &str, version: u64) -> String {
    let normalized = normalize_vector_column_name(column_name);
    format!("vec-{}-snapshot-{}.vix", normalized, version)
}

fn load_existing_vector_state(
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    storage_cached: &StorageCached,
    snapshot_path: Option<&str>,
    column_name: &str,
    dimensions: u32,
    metric: VectorMetric,
) -> Result<(HashMap<String, (u64, Vec<f32>)>, u64), VectorFlushError> {
    let mut vectors_by_pk: HashMap<String, (u64, Vec<f32>)> = HashMap::new();
    let Some(path) = snapshot_path else {
        return Ok((vectors_by_pk, 1));
    };
    if path.is_empty() {
        return Ok((vectors_by_pk, 1));
    }

    let snapshot_bytes = match storage_cached.get_sync(table_type, table_id, user_id, path) {
        Ok(result) => result.data,
        Err(FilestoreError::NotFound(_)) => return Ok((vectors_by_pk, 1)),
        Err(err) => {
            return Err(VectorFlushError::with_prefix(
                "Failed to read existing vector snapshot",
                err,
            ))
        },
    };

    let snapshot = decode_snapshot(&snapshot_bytes).map_err(|e| {
        VectorFlushError::new(format!(
            "Failed to decode existing vector snapshot '{}': {}",
            path, e
        ))
    })?;

    if !snapshot.column_name.eq_ignore_ascii_case(column_name) {
        return Err(VectorFlushError::new(format!(
            "Snapshot column mismatch: expected '{}', found '{}'",
            column_name, snapshot.column_name
        )));
    }
    if snapshot.dimensions != dimensions {
        return Err(VectorFlushError::new(format!(
            "Snapshot dimensions mismatch for '{}': expected {}, found {}",
            column_name, dimensions, snapshot.dimensions
        )));
    }
    if snapshot.metric != metric {
        return Err(VectorFlushError::new(format!(
            "Snapshot metric mismatch for '{}': expected {:?}, found {:?}",
            column_name, metric, snapshot.metric
        )));
    }

    let index = load_index(dimensions, metric, &snapshot.index_blob).map_err(|e| {
        VectorFlushError::new(format!("Failed to load usearch snapshot index: {}", e))
    })?;

    for entry in snapshot.entries {
        if let Some(vector) = export_vector(&index, entry.key, dimensions).map_err(|e| {
            VectorFlushError::new(format!("Failed to export vector from index: {}", e))
        })? {
            vectors_by_pk.insert(entry.pk, (entry.key, vector));
        }
    }

    let next_key = snapshot.next_key.max(1);
    Ok((vectors_by_pk, next_key))
}

fn build_snapshot_bytes(
    table_id: &TableId,
    column_name: &str,
    dimensions: u32,
    metric: VectorMetric,
    last_applied_seq: SeqId,
    next_key: u64,
    vectors_by_pk: &HashMap<String, (u64, Vec<f32>)>,
) -> Result<Vec<u8>, VectorFlushError> {
    let index = create_index(dimensions, metric, vectors_by_pk.len())
        .map_err(|e| VectorFlushError::new(format!("Failed to build usearch index: {}", e)))?;

    let mut ordered: Vec<(&String, &(u64, Vec<f32>))> = vectors_by_pk.iter().collect();
    ordered.sort_by(|(pk_a, _), (pk_b, _)| pk_a.cmp(pk_b));

    let mut entries = Vec::with_capacity(ordered.len());
    for (pk, (key, vector)) in ordered {
        if vector.len() != dimensions as usize {
            return Err(VectorFlushError::new(format!(
                "Vector dimension mismatch for pk '{}': expected {}, found {}",
                pk,
                dimensions,
                vector.len()
            )));
        }
        add_vector(&index, *key, vector)
            .map_err(|e| VectorFlushError::new(format!("Failed to add vector to index: {}", e)))?;
        entries.push(VixSnapshotEntry {
            key: *key,
            pk: pk.clone(),
        });
    }

    let index_blob = serialize_index(&index)
        .map_err(|e| VectorFlushError::new(format!("Failed to serialize index: {}", e)))?;

    let snapshot = VixSnapshotFile {
        table_id: table_id.to_string(),
        column_name: column_name.to_string(),
        dimensions,
        metric,
        generated_at: chrono::Utc::now().timestamp_millis(),
        last_applied_seq: last_applied_seq.as_i64(),
        next_key,
        entries,
        index_blob,
    };

    encode_snapshot(&snapshot)
        .map_err(|e| VectorFlushError::new(format!("Failed to encode vector snapshot: {}", e)))
}

fn apply_hot_ops_to_state<K: Clone>(
    last_applied_seq: SeqId,
    scanned: Vec<(K, VectorHotOp, i64)>,
    vectors_by_pk: &mut HashMap<String, (u64, Vec<f32>)>,
    next_key: &mut u64,
    dimensions: u32,
) -> Result<(Vec<K>, SeqId), VectorFlushError> {
    let mut latest_by_pk: HashMap<String, (i64, VectorHotOp)> = HashMap::new();
    let mut applied_keys: Vec<K> = Vec::new();
    let mut max_seq = last_applied_seq;

    for (key, op, seq) in scanned {
        if seq <= last_applied_seq.as_i64() {
            continue;
        }
        if seq > max_seq.as_i64() {
            max_seq = SeqId::from(seq);
        }
        applied_keys.push(key.clone());
        match latest_by_pk.get(&op.pk) {
            Some((existing_seq, _)) if *existing_seq >= seq => {},
            _ => {
                latest_by_pk.insert(op.pk.clone(), (seq, op));
            },
        }
    }

    if applied_keys.is_empty() {
        return Ok((applied_keys, max_seq));
    }

    for (pk, (_, op)) in latest_by_pk {
        match op.op_type {
            VectorHotOpType::Delete => {
                vectors_by_pk.remove(&pk);
            },
            VectorHotOpType::Upsert => {
                let Some(vector) = op.vector else {
                    vectors_by_pk.remove(&pk);
                    continue;
                };
                if vector.len() != dimensions as usize {
                    return Err(VectorFlushError::new(format!(
                        "Vector dimension mismatch for pk '{}': expected {}, found {}",
                        pk,
                        dimensions,
                        vector.len()
                    )));
                }
                let key = vectors_by_pk.get(&pk).map(|(key, _)| *key).unwrap_or_else(|| {
                    let assigned = *next_key;
                    *next_key = next_key.saturating_add(1);
                    assigned
                });
                vectors_by_pk.insert(pk, (key, vector));
            },
        }
    }

    Ok((applied_keys, max_seq))
}

pub fn flush_user_scope_vectors(
    backend: Arc<dyn StorageBackend>,
    manifest_store: &dyn VectorManifestStore,
    table_id: &TableId,
    user_id: &UserId,
    schema: &SchemaRef,
    storage_cached: &StorageCached,
) -> Result<(), VectorFlushError> {
    let embedding_cols = embedding_columns(schema);
    if embedding_cols.is_empty() {
        return Ok(());
    }

    let mut manifest = manifest_store.ensure_manifest_initialized(table_id, Some(user_id))?;
    let mut any_manifest_change = false;

    for (column_name, dimensions) in embedding_cols {
        let existing = manifest.vector_indexes.get(&column_name).cloned();
        if existing.as_ref().is_some_and(|meta| !meta.enabled) {
            continue;
        }
        let last_applied_seq = existing
            .as_ref()
            .map(|m| m.last_applied_seq)
            .unwrap_or_else(|| SeqId::from(0i64));
        let metric = existing.as_ref().map(|m| m.metric).unwrap_or(VectorMetric::Cosine);
        let engine = VectorEngine::USearch;
        let next_version =
            existing.as_ref().map(|m| m.snapshot_version.saturating_add(1)).unwrap_or(1);

        let store = new_indexed_user_vector_hot_store(backend.clone(), table_id, &column_name);
        let prefix = UserVectorHotOpId::user_prefix(user_id);
        let scanned =
            store.scan_with_raw_prefix(&prefix, None, VECTOR_SCAN_LIMIT).map_err(|e| {
                VectorFlushError::with_prefix("Failed to scan user vector hot store", e)
            })?;

        if scanned.is_empty() {
            continue;
        }

        let mut vectors_by_pk;
        let mut next_key;
        (vectors_by_pk, next_key) = load_existing_vector_state(
            TableType::User,
            table_id,
            Some(user_id),
            storage_cached,
            existing.as_ref().and_then(|m| m.snapshot_path.as_deref()),
            &column_name,
            dimensions,
            metric,
        )?;

        let scanned_ops: Vec<(UserVectorHotOpId, VectorHotOp, i64)> = scanned
            .into_iter()
            .map(|(key, op)| (key.clone(), op, key.seq.as_i64()))
            .collect();
        let (applied_keys, max_seq) = apply_hot_ops_to_state(
            last_applied_seq,
            scanned_ops,
            &mut vectors_by_pk,
            &mut next_key,
            dimensions,
        )?;

        if applied_keys.is_empty() {
            continue;
        }

        let snapshot_path = vector_snapshot_filename(&column_name, next_version);
        let bytes = build_snapshot_bytes(
            table_id,
            &column_name,
            dimensions,
            metric,
            max_seq,
            next_key,
            &vectors_by_pk,
        )?;

        storage_cached
            .put_sync(TableType::User, table_id, Some(user_id), &snapshot_path, Bytes::from(bytes))
            .map_err(|e| {
                VectorFlushError::with_prefix("Failed to write user vector snapshot", e)
            })?;

        manifest.record_vector_snapshot(
            &column_name,
            dimensions,
            metric,
            engine,
            snapshot_path,
            max_seq,
        );
        any_manifest_change = true;

        for key in &applied_keys {
            store.delete(key).map_err(|e| {
                VectorFlushError::with_prefix("Failed to prune user vector hot op", e)
            })?;
        }
    }

    if any_manifest_change {
        manifest_store.persist_manifest(table_id, Some(user_id), &manifest)?;
    }

    Ok(())
}

pub fn flush_shared_scope_vectors(
    backend: Arc<dyn StorageBackend>,
    manifest_store: &dyn VectorManifestStore,
    table_id: &TableId,
    schema: &SchemaRef,
    storage_cached: &StorageCached,
) -> Result<(), VectorFlushError> {
    let embedding_cols = embedding_columns(schema);
    if embedding_cols.is_empty() {
        return Ok(());
    }

    let mut manifest = manifest_store.ensure_manifest_initialized(table_id, None)?;
    let mut any_manifest_change = false;

    for (column_name, dimensions) in embedding_cols {
        let existing = manifest.vector_indexes.get(&column_name).cloned();
        if existing.as_ref().is_some_and(|meta| !meta.enabled) {
            continue;
        }
        let last_applied_seq = existing
            .as_ref()
            .map(|m| m.last_applied_seq)
            .unwrap_or_else(|| SeqId::from(0i64));
        let metric = existing.as_ref().map(|m| m.metric).unwrap_or(VectorMetric::Cosine);
        let engine = VectorEngine::USearch;
        let next_version =
            existing.as_ref().map(|m| m.snapshot_version.saturating_add(1)).unwrap_or(1);

        let store = new_indexed_shared_vector_hot_store(backend.clone(), table_id, &column_name);
        let scanned = store.scan_all_typed(Some(VECTOR_SCAN_LIMIT), None, None).map_err(|e| {
            VectorFlushError::with_prefix("Failed to scan shared vector hot store", e)
        })?;

        if scanned.is_empty() {
            continue;
        }

        let mut vectors_by_pk;
        let mut next_key;
        (vectors_by_pk, next_key) = load_existing_vector_state(
            TableType::Shared,
            table_id,
            None,
            storage_cached,
            existing.as_ref().and_then(|m| m.snapshot_path.as_deref()),
            &column_name,
            dimensions,
            metric,
        )?;

        let scanned_ops: Vec<(SharedVectorHotOpId, VectorHotOp, i64)> = scanned
            .into_iter()
            .map(|(key, op)| (key.clone(), op, key.seq.as_i64()))
            .collect();
        let (applied_keys, max_seq) = apply_hot_ops_to_state(
            last_applied_seq,
            scanned_ops,
            &mut vectors_by_pk,
            &mut next_key,
            dimensions,
        )?;

        if applied_keys.is_empty() {
            continue;
        }

        let snapshot_path = vector_snapshot_filename(&column_name, next_version);
        let bytes = build_snapshot_bytes(
            table_id,
            &column_name,
            dimensions,
            metric,
            max_seq,
            next_key,
            &vectors_by_pk,
        )?;

        storage_cached
            .put_sync(TableType::Shared, table_id, None, &snapshot_path, Bytes::from(bytes))
            .map_err(|e| {
                VectorFlushError::with_prefix("Failed to write shared vector snapshot", e)
            })?;

        manifest.record_vector_snapshot(
            &column_name,
            dimensions,
            metric,
            engine,
            snapshot_path,
            max_seq,
        );
        any_manifest_change = true;

        for key in &applied_keys {
            store.delete(key).map_err(|e| {
                VectorFlushError::with_prefix("Failed to prune shared vector hot op", e)
            })?;
        }
    }

    if any_manifest_change {
        manifest_store.persist_manifest(table_id, None, &manifest)?;
    }

    Ok(())
}
