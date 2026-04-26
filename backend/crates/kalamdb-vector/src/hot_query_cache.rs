use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dashmap::DashMap;
use datafusion::common::DataFusionError;
use kalamdb_commons::{
    ids::SeqId,
    models::{TableId, UserId},
    schemas::TableType,
    StorageKey,
};
use kalamdb_store::{EntityStore, StorageBackend};
use kalamdb_system::VectorMetric;
use once_cell::sync::Lazy;
use usearch::Index;

use crate::{
    hot_staging::{
        new_indexed_shared_vector_hot_store, new_indexed_user_vector_hot_store,
        SharedVectorHotOpId, UserVectorHotOpId, VectorHotOp, VectorHotOpType,
    },
    usearch_engine::{add_vector, create_index, search_index},
};

const HOT_INCREMENTAL_SCAN_LIMIT: usize = 100_000;
const HOT_RESERVE_STEP: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HotCacheKey {
    table_id: String,
    column_name: String,
    table_type: TableType,
    user_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct HotSearchResult {
    pub candidates: Vec<(String, f32)>,
    pub touched_pks: HashSet<String>,
}

struct HotCacheState {
    base_last_applied_seq: i64,
    last_scanned_seq: i64,
    dimensions: u32,
    metric: VectorMetric,
    next_key: u64,
    index: Index,
    pk_to_key: HashMap<String, u64>,
    key_to_pk: HashMap<u64, String>,
    latest_seq_by_pk: HashMap<String, i64>,
}

impl HotCacheState {
    fn new(
        dimensions: u32,
        metric: VectorMetric,
        base_last_applied_seq: i64,
    ) -> Result<Self, String> {
        Ok(Self {
            base_last_applied_seq,
            last_scanned_seq: base_last_applied_seq,
            dimensions,
            metric,
            next_key: 1,
            index: create_index(dimensions, metric, 0)?,
            pk_to_key: HashMap::new(),
            key_to_pk: HashMap::new(),
            latest_seq_by_pk: HashMap::new(),
        })
    }

    fn reset(
        &mut self,
        dimensions: u32,
        metric: VectorMetric,
        base_last_applied_seq: i64,
    ) -> Result<(), String> {
        self.base_last_applied_seq = base_last_applied_seq;
        self.last_scanned_seq = base_last_applied_seq;
        self.dimensions = dimensions;
        self.metric = metric;
        self.next_key = 1;
        self.index = create_index(dimensions, metric, 0)?;
        self.pk_to_key.clear();
        self.key_to_pk.clear();
        self.latest_seq_by_pk.clear();
        Ok(())
    }

    fn ensure_capacity_for_next(&self) -> Result<(), String> {
        let size = self.index.size();
        let capacity = self.index.capacity();
        if size + 1 <= capacity {
            return Ok(());
        }
        let new_capacity = capacity.saturating_add(HOT_RESERVE_STEP).max(size + 1);
        self.index
            .reserve(new_capacity)
            .map_err(|e| format!("Failed to reserve hot usearch capacity: {}", e))
    }

    fn apply_hot_op(&mut self, seq: i64, op: &VectorHotOp) -> Result<(), String> {
        let pk = &op.pk;
        if self.latest_seq_by_pk.get(pk).copied().unwrap_or(i64::MIN) >= seq {
            return Ok(());
        }
        self.latest_seq_by_pk.insert(pk.clone(), seq);
        if seq > self.last_scanned_seq {
            self.last_scanned_seq = seq;
        }

        match op.op_type {
            VectorHotOpType::Delete => {
                if let Some(key) = self.pk_to_key.remove(pk) {
                    let _ = self.index.remove(key);
                    self.key_to_pk.remove(&key);
                }
            },
            VectorHotOpType::Upsert => {
                let Some(vector) = op.vector.as_ref() else {
                    if let Some(key) = self.pk_to_key.remove(pk) {
                        let _ = self.index.remove(key);
                        self.key_to_pk.remove(&key);
                    }
                    return Ok(());
                };
                if vector.len() != self.dimensions as usize {
                    return Err(format!(
                        "Hot vector dimension mismatch for pk '{}': expected {}, found {}",
                        pk,
                        self.dimensions,
                        vector.len()
                    ));
                }

                let key = if let Some(existing) = self.pk_to_key.get(pk).copied() {
                    let _ = self.index.remove(existing);
                    existing
                } else {
                    let assigned = self.next_key;
                    self.next_key = self.next_key.saturating_add(1);
                    assigned
                };

                self.ensure_capacity_for_next()?;
                add_vector(&self.index, key, vector)?;
                self.pk_to_key.insert(pk.clone(), key);
                self.key_to_pk.insert(key, pk.clone());
            },
        }
        Ok(())
    }
}

static HOT_QUERY_CACHE: Lazy<DashMap<HotCacheKey, HotCacheState>> = Lazy::new(DashMap::new);

fn build_cache_key(
    table_id: &TableId,
    column_name: &str,
    table_type: TableType,
    user_id: Option<&UserId>,
) -> HotCacheKey {
    HotCacheKey {
        table_id: table_id.to_string(),
        column_name: column_name.to_string(),
        table_type,
        user_id: user_id.map(|v| v.as_str().to_string()),
    }
}

pub(crate) fn search_hot_candidates(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
    column_name: &str,
    table_type: TableType,
    session_user: &UserId,
    last_applied_seq: SeqId,
    metric: VectorMetric,
    query_vector: &[f32],
    candidate_limit: usize,
) -> Result<HotSearchResult, DataFusionError> {
    if query_vector.is_empty() {
        return Ok(HotSearchResult::default());
    }
    if !matches!(table_type, TableType::User | TableType::Shared) {
        return Err(DataFusionError::Execution(format!(
            "vector_search supports only user/shared tables, got {:?}",
            table_type
        )));
    }

    let dimensions = query_vector.len() as u32;
    let cache_key = build_cache_key(
        table_id,
        column_name,
        table_type,
        if matches!(table_type, TableType::User) {
            Some(session_user)
        } else {
            None
        },
    );

    if !HOT_QUERY_CACHE.contains_key(&cache_key) {
        let state =
            HotCacheState::new(dimensions, metric, last_applied_seq.as_i64()).map_err(|e| {
                DataFusionError::Execution(format!("Failed to initialize hot cache: {}", e))
            })?;
        HOT_QUERY_CACHE.insert(cache_key.clone(), state);
    }

    let mut state = HOT_QUERY_CACHE.get_mut(&cache_key).ok_or_else(|| {
        DataFusionError::Execution("Failed to access hot cache state".to_string())
    })?;

    if state.dimensions != dimensions
        || state.metric != metric
        || state.base_last_applied_seq != last_applied_seq.as_i64()
    {
        state.reset(dimensions, metric, last_applied_seq.as_i64()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to reset hot cache state: {}", e))
        })?;
    }

    match table_type {
        TableType::User => {
            let store = new_indexed_user_vector_hot_store(backend, table_id, column_name);
            let prefix = UserVectorHotOpId::user_prefix(session_user);
            let start_seq = state.last_scanned_seq.saturating_add(1);
            let start_key =
                UserVectorHotOpId::new(session_user.clone(), SeqId::from(start_seq), "");
            let start_key_bytes = start_key.storage_key();
            let staged = store
                .scan_with_raw_prefix(
                    &prefix,
                    Some(start_key_bytes.as_slice()),
                    HOT_INCREMENTAL_SCAN_LIMIT,
                )
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to scan user vector hot staging incrementally: {}",
                        e
                    ))
                })?;
            for (key, op) in staged {
                let seq = key.seq.as_i64();
                if seq <= last_applied_seq.as_i64() || seq <= state.last_scanned_seq {
                    continue;
                }
                state.apply_hot_op(seq, &op).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to apply hot user op: {}", e))
                })?;
            }
        },
        TableType::Shared => {
            let store = new_indexed_shared_vector_hot_store(backend, table_id, column_name);
            let start_seq = state.last_scanned_seq.saturating_add(1);
            let start_key = SharedVectorHotOpId::new(SeqId::from(start_seq), "");
            let staged = store
                .scan_typed_with_prefix_and_start(
                    None,
                    Some(&start_key),
                    HOT_INCREMENTAL_SCAN_LIMIT,
                )
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to scan shared vector hot staging incrementally: {}",
                        e
                    ))
                })?;
            for (key, op) in staged {
                let seq = key.seq.as_i64();
                if seq <= last_applied_seq.as_i64() || seq <= state.last_scanned_seq {
                    continue;
                }
                state.apply_hot_op(seq, &op).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to apply hot shared op: {}", e))
                })?;
            }
        },
        TableType::System | TableType::Stream => unreachable!(),
    }

    let touched_pks: HashSet<String> = state.latest_seq_by_pk.keys().cloned().collect();
    let effective_limit = candidate_limit.saturating_add(touched_pks.len()).max(candidate_limit);
    if state.key_to_pk.is_empty() || effective_limit == 0 {
        return Ok(HotSearchResult {
            candidates: Vec::new(),
            touched_pks,
        });
    }

    let raw = search_index(&state.index, query_vector, effective_limit).map_err(|e| {
        DataFusionError::Execution(format!("Failed to search hot usearch index: {}", e))
    })?;
    let mut candidates = Vec::with_capacity(raw.len());
    for (key, distance) in raw {
        if let Some(pk) = state.key_to_pk.get(&key) {
            candidates.push((pk.clone(), distance));
        }
    }

    Ok(HotSearchResult {
        candidates,
        touched_pks,
    })
}

#[cfg(test)]
pub(crate) fn clear_hot_query_cache_for_tests() {
    HOT_QUERY_CACHE.clear();
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::ids::SeqId;
    use kalamdb_store::test_utils::TestDb;

    use super::*;
    use crate::hot_staging::{
        new_indexed_shared_vector_hot_store, new_indexed_user_vector_hot_store,
        SharedVectorHotOpId, UserVectorHotOpId, VectorHotOp, VectorHotOpType,
    };

    fn shared_op(
        table_id: &TableId,
        column_name: &str,
        pk: &str,
        vector: Option<Vec<f32>>,
        op_type: VectorHotOpType,
    ) -> VectorHotOp {
        VectorHotOp::new(
            table_id.clone(),
            column_name.to_string(),
            pk.to_string(),
            op_type,
            vector,
            None,
            3,
            VectorMetric::Cosine,
        )
    }

    fn user_op(
        table_id: &TableId,
        column_name: &str,
        pk: &str,
        vector: Option<Vec<f32>>,
        op_type: VectorHotOpType,
    ) -> VectorHotOp {
        VectorHotOp::new(
            table_id.clone(),
            column_name.to_string(),
            pk.to_string(),
            op_type,
            vector,
            None,
            3,
            VectorMetric::Cosine,
        )
    }

    #[test]
    fn test_shared_hot_cache_incremental_update_delete_and_reset() {
        clear_hot_query_cache_for_tests();

        let test_db = TestDb::with_system_tables().expect("test db");
        let backend = test_db.backend();
        let table_id = TableId::from_strings("test_ns", "docs_hot_cache_shared");
        let column = "embedding";
        let query = vec![1.0_f32, 0.0, 0.0];
        let session_user = UserId::new("u_cache_reader");
        let store = new_indexed_shared_vector_hot_store(backend.clone(), &table_id, column);

        store
            .insert(
                &SharedVectorHotOpId::new(SeqId::from(1i64), "1"),
                &shared_op(
                    &table_id,
                    column,
                    "1",
                    Some(vec![1.0, 0.0, 0.0]),
                    VectorHotOpType::Upsert,
                ),
            )
            .expect("insert seq1");
        store
            .insert(
                &SharedVectorHotOpId::new(SeqId::from(2i64), "2"),
                &shared_op(
                    &table_id,
                    column,
                    "2",
                    Some(vec![0.0, 1.0, 0.0]),
                    VectorHotOpType::Upsert,
                ),
            )
            .expect("insert seq2");
        store
            .insert(
                &SharedVectorHotOpId::new(SeqId::from(3i64), "1"),
                &shared_op(
                    &table_id,
                    column,
                    "1",
                    Some(vec![0.99, 0.01, 0.0]),
                    VectorHotOpType::Upsert,
                ),
            )
            .expect("insert seq3");

        let first = search_hot_candidates(
            backend.clone(),
            &table_id,
            column,
            TableType::Shared,
            &session_user,
            SeqId::from(0i64),
            VectorMetric::Cosine,
            &query,
            4,
        )
        .expect("first hot search");
        assert!(first.touched_pks.contains("1"));
        assert!(first.touched_pks.contains("2"));
        assert_eq!(first.candidates.first().map(|v| v.0.as_str()), Some("1"));

        store
            .insert(
                &SharedVectorHotOpId::new(SeqId::from(4i64), "1"),
                &shared_op(&table_id, column, "1", None, VectorHotOpType::Delete),
            )
            .expect("insert delete seq4");

        let second = search_hot_candidates(
            backend.clone(),
            &table_id,
            column,
            TableType::Shared,
            &session_user,
            SeqId::from(0i64),
            VectorMetric::Cosine,
            &query,
            4,
        )
        .expect("second hot search");
        assert!(second.touched_pks.contains("1"));
        assert!(second.touched_pks.contains("2"));
        assert!(
            !second.candidates.iter().any(|(pk, _)| pk == "1"),
            "deleted pk should be removed from hot candidates"
        );
        assert!(
            second.candidates.iter().any(|(pk, _)| pk == "2"),
            "remaining pk should still be searchable"
        );

        let after_flush = search_hot_candidates(
            backend,
            &table_id,
            column,
            TableType::Shared,
            &session_user,
            SeqId::from(4i64),
            VectorMetric::Cosine,
            &query,
            4,
        )
        .expect("search after flush watermark");
        assert!(after_flush.touched_pks.is_empty());
        assert!(after_flush.candidates.is_empty());

        clear_hot_query_cache_for_tests();
    }

    #[test]
    fn test_user_hot_cache_isolated_by_user_scope() {
        clear_hot_query_cache_for_tests();

        let test_db = TestDb::with_system_tables().expect("test db");
        let backend = test_db.backend();
        let table_id = TableId::from_strings("test_ns", "docs_hot_cache_user");
        let column = "embedding";
        let query = vec![1.0_f32, 0.0, 0.0];
        let u1 = UserId::new("u_hot_1");
        let u2 = UserId::new("u_hot_2");
        let store = new_indexed_user_vector_hot_store(backend.clone(), &table_id, column);

        store
            .insert(
                &UserVectorHotOpId::new(u1.clone(), SeqId::from(1i64), "11"),
                &user_op(
                    &table_id,
                    column,
                    "11",
                    Some(vec![1.0, 0.0, 0.0]),
                    VectorHotOpType::Upsert,
                ),
            )
            .expect("insert u1 op");
        store
            .insert(
                &UserVectorHotOpId::new(u2.clone(), SeqId::from(1i64), "22"),
                &user_op(
                    &table_id,
                    column,
                    "22",
                    Some(vec![1.0, 0.0, 0.0]),
                    VectorHotOpType::Upsert,
                ),
            )
            .expect("insert u2 op");

        let u1_result = search_hot_candidates(
            backend.clone(),
            &table_id,
            column,
            TableType::User,
            &u1,
            SeqId::from(0i64),
            VectorMetric::Cosine,
            &query,
            4,
        )
        .expect("u1 hot search");
        assert!(u1_result.touched_pks.contains("11"));
        assert!(!u1_result.touched_pks.contains("22"));
        assert!(u1_result.candidates.iter().any(|(pk, _)| pk == "11"));
        assert!(!u1_result.candidates.iter().any(|(pk, _)| pk == "22"));

        let u2_result = search_hot_candidates(
            backend,
            &table_id,
            column,
            TableType::User,
            &u2,
            SeqId::from(0i64),
            VectorMetric::Cosine,
            &query,
            4,
        )
        .expect("u2 hot search");
        assert!(u2_result.touched_pks.contains("22"));
        assert!(!u2_result.touched_pks.contains("11"));
        assert!(u2_result.candidates.iter().any(|(pk, _)| pk == "22"));
        assert!(!u2_result.candidates.iter().any(|(pk, _)| pk == "11"));

        clear_hot_query_cache_for_tests();
    }
}
