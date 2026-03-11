use crate::hot_query_cache::search_hot_candidates;
use crate::snapshot_codec::decode_snapshot;
use crate::usearch_engine::{load_index, search_index};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType as DataFusionTableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_filestore::{FilestoreError, StorageCached};
use kalamdb_session::extract_user_id;
use kalamdb_store::StorageBackend;
use kalamdb_system::VectorMetric;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

const DEFAULT_TOP_K: usize = 10;
const CANDIDATE_MULTIPLIER: usize = 4;

#[derive(Debug, Clone)]
struct VectorSearchArgs {
    table_id: TableId,
    column_name: String,
    query_vector: Vec<f32>,
    top_k: usize,
}

#[derive(Clone)]
pub struct VectorSearchScope {
    pub table_type: TableType,
    pub manifest_user: Option<UserId>,
    pub metric: VectorMetric,
    pub last_applied_seq: SeqId,
    pub snapshot_path: Option<String>,
    pub storage_cached: Arc<StorageCached>,
    pub backend: Arc<dyn StorageBackend>,
}

pub trait VectorSearchRuntime: Send + Sync + Debug {
    fn resolve_scope(
        &self,
        table_id: &TableId,
        column_name: &str,
        session_user: &UserId,
    ) -> Result<Option<VectorSearchScope>>;
}

#[derive(Debug, Clone, Default)]
pub struct UnavailableVectorSearchRuntime;

impl VectorSearchRuntime for UnavailableVectorSearchRuntime {
    fn resolve_scope(
        &self,
        _table_id: &TableId,
        _column_name: &str,
        _session_user: &UserId,
    ) -> Result<Option<VectorSearchScope>> {
        Err(DataFusionError::Execution(
            "vector_search runtime is not initialized".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct VectorSearchTableFunction {
    runtime: Arc<dyn VectorSearchRuntime>,
}

impl VectorSearchTableFunction {
    pub fn new(runtime: Arc<dyn VectorSearchRuntime>) -> Self {
        Self { runtime }
    }

    pub fn unavailable() -> Self {
        Self::new(Arc::new(UnavailableVectorSearchRuntime))
    }
}

impl Default for VectorSearchTableFunction {
    fn default() -> Self {
        Self::unavailable()
    }
}

impl TableFunctionImpl for VectorSearchTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let parsed = parse_args(args)?;
        Ok(Arc::new(VectorSearchTableProvider::new(Arc::clone(&self.runtime), parsed)))
    }
}

#[derive(Debug, Clone)]
struct VectorSearchTableProvider {
    runtime: Arc<dyn VectorSearchRuntime>,
    args: VectorSearchArgs,
    output_schema: SchemaRef,
}

impl VectorSearchTableProvider {
    fn new(runtime: Arc<dyn VectorSearchRuntime>, args: VectorSearchArgs) -> Self {
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
        ]));

        Self {
            runtime,
            args,
            output_schema,
        }
    }

    fn empty_batch(&self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
                Arc::new(Float32Array::from(Vec::<f32>::new())) as ArrayRef,
            ],
        )
        .map_err(DataFusionError::from)
    }
}

fn distance_to_score(metric: VectorMetric, distance: f32) -> f32 {
    match metric {
        VectorMetric::Cosine | VectorMetric::Dot => 1.0 - distance,
        VectorMetric::L2 => -distance,
    }
}

fn parse_table_id(table: &str) -> Result<TableId> {
    let (namespace, table_name) = table.split_once('.').ok_or_else(|| {
        DataFusionError::Plan(format!(
            "vector_search table_id must be in 'namespace.table' format, got '{}'",
            table
        ))
    })?;

    TableId::try_from_strings(namespace, table_name).map_err(DataFusionError::Plan)
}

fn parse_i64_literal(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(v) => *v,
        ScalarValue::Int32(v) => v.map(i64::from),
        ScalarValue::UInt64(v) => v.map(|v| v as i64),
        ScalarValue::UInt32(v) => v.map(i64::from),
        _ => None,
    }
}

fn parse_f32_vec_from_array(array: &dyn Array) -> Result<Vec<f32>> {
    match array.data_type() {
        DataType::Float32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| DataFusionError::Plan("Invalid Float32 vector array".to_string()))?;
            Ok((0..typed.len())
                .filter(|idx| !typed.is_null(*idx))
                .map(|idx| typed.value(idx))
                .collect())
        },
        DataType::Float64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Plan("Invalid Float64 vector array".to_string()))?;
            Ok((0..typed.len())
                .filter(|idx| !typed.is_null(*idx))
                .map(|idx| typed.value(idx) as f32)
                .collect())
        },
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Plan("Invalid Int64 vector array".to_string()))?;
            Ok((0..typed.len())
                .filter(|idx| !typed.is_null(*idx))
                .map(|idx| typed.value(idx) as f32)
                .collect())
        },
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Plan("Invalid Int32 vector array".to_string()))?;
            Ok((0..typed.len())
                .filter(|idx| !typed.is_null(*idx))
                .map(|idx| typed.value(idx) as f32)
                .collect())
        },
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Plan("Invalid UInt64 vector array".to_string()))?;
            Ok((0..typed.len())
                .filter(|idx| !typed.is_null(*idx))
                .map(|idx| typed.value(idx) as f32)
                .collect())
        },
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| DataFusionError::Plan("Invalid UInt32 vector array".to_string()))?;
            Ok((0..typed.len())
                .filter(|idx| !typed.is_null(*idx))
                .map(|idx| typed.value(idx) as f32)
                .collect())
        },
        _ => Err(DataFusionError::Plan(format!(
            "vector_search query vector list type must be numeric, got {:?}",
            array.data_type()
        ))),
    }
}

fn parse_query_vector(value: &ScalarValue) -> Result<Vec<f32>> {
    match value {
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => serde_json::from_str::<
            Vec<f32>,
        >(v)
        .map_err(|e| {
            DataFusionError::Plan(format!("vector_search query vector JSON parsing failed: {}", e))
        }),
        ScalarValue::FixedSizeList(values) => {
            if values.is_empty() || values.is_null(0) {
                return Err(DataFusionError::Plan(
                    "vector_search query vector cannot be empty".to_string(),
                ));
            }
            parse_f32_vec_from_array(values.value(0).as_ref())
        },
        ScalarValue::List(values) => {
            if values.is_empty() || values.is_null(0) {
                return Err(DataFusionError::Plan(
                    "vector_search query vector cannot be empty".to_string(),
                ));
            }
            parse_f32_vec_from_array(values.value(0).as_ref())
        },
        ScalarValue::LargeList(values) => {
            if values.is_empty() || values.is_null(0) {
                return Err(DataFusionError::Plan(
                    "vector_search query vector cannot be empty".to_string(),
                ));
            }
            parse_f32_vec_from_array(values.value(0).as_ref())
        },
        _ => Err(DataFusionError::Plan(
            "vector_search query vector must be a JSON string or numeric ARRAY literal".to_string(),
        )),
    }
}

fn parse_args(args: &[Expr]) -> Result<VectorSearchArgs> {
    if args.len() < 3 || args.len() > 4 {
        return Err(DataFusionError::Plan(
            "vector_search(table_id, column_name, query_vector[, top_k]) expects 3 or 4 literal arguments".to_string(),
        ));
    }

    let table_name = match &args[0] {
        Expr::Literal(ScalarValue::Utf8(Some(v)), _) => v.clone(),
        Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => v.clone(),
        _ => {
            return Err(DataFusionError::Plan(
                "vector_search table_id must be a string literal".to_string(),
            ))
        },
    };
    let table_id = parse_table_id(&table_name)?;

    let column_name = match &args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(v)), _) => v.clone(),
        Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => v.clone(),
        _ => {
            return Err(DataFusionError::Plan(
                "vector_search column_name must be a string literal".to_string(),
            ))
        },
    };

    let query_vector = match &args[2] {
        Expr::Literal(value, _) => parse_query_vector(value)?,
        _ => {
            return Err(DataFusionError::Plan(
                "vector_search query_vector must be a literal".to_string(),
            ))
        },
    };
    if query_vector.is_empty() {
        return Err(DataFusionError::Plan(
            "vector_search query_vector cannot be empty".to_string(),
        ));
    }

    let top_k = if let Some(top_k_expr) = args.get(3) {
        let literal = match top_k_expr {
            Expr::Literal(value, _) => value,
            _ => {
                return Err(DataFusionError::Plan(
                    "vector_search top_k must be an integer literal".to_string(),
                ))
            },
        };
        let parsed = parse_i64_literal(literal).ok_or_else(|| {
            DataFusionError::Plan("vector_search top_k must be an integer literal".to_string())
        })?;
        if parsed <= 0 {
            return Err(DataFusionError::Plan("vector_search top_k must be > 0".to_string()));
        }
        parsed as usize
    } else {
        DEFAULT_TOP_K
    };

    Ok(VectorSearchArgs {
        table_id,
        column_name,
        query_vector,
        top_k,
    })
}

#[async_trait]
impl TableProvider for VectorSearchTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn table_type(&self) -> DataFusionTableType {
        DataFusionTableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_user = extract_user_id(state);
        let scope = match self.runtime.resolve_scope(
            &self.args.table_id,
            &self.args.column_name,
            &session_user,
        )? {
            Some(scope) => scope,
            None => {
                let batch = self.empty_batch()?;
                let mem = MemTable::try_new(self.schema(), vec![vec![batch]])?;
                return mem.scan(state, projection, filters, limit).await;
            },
        };

        let mut base_candidates: Vec<(String, f32)> = Vec::new();
        let mut base_snapshot_key_to_pk: HashMap<u64, String> = HashMap::new();
        let mut base_index = None;

        if let Some(snapshot_path) = scope.snapshot_path.as_deref() {
            if !snapshot_path.is_empty() {
                match scope.storage_cached.get_sync(
                    scope.table_type,
                    &self.args.table_id,
                    scope.manifest_user.as_ref(),
                    snapshot_path,
                ) {
                    Ok(get_result) => {
                        let parsed = decode_snapshot(&get_result.data).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to decode vector snapshot '{}': {}",
                                snapshot_path, e
                            ))
                        })?;
                        if parsed.dimensions as usize != self.args.query_vector.len() {
                            return Err(DataFusionError::Execution(format!(
                                "vector_search query vector dimensions mismatch: query has {}, index has {}",
                                self.args.query_vector.len(),
                                parsed.dimensions
                            )));
                        }
                        let loaded_index =
                            load_index(parsed.dimensions, parsed.metric, &parsed.index_blob).map_err(
                                |e| {
                                    DataFusionError::Execution(format!(
                                        "Failed to load vector snapshot index '{}': {}",
                                        snapshot_path, e
                                    ))
                                },
                            )?;
                        for entry in parsed.entries {
                            base_snapshot_key_to_pk.insert(entry.key, entry.pk);
                        }
                        base_index = Some(loaded_index);
                    },
                    Err(FilestoreError::NotFound(_)) => {},
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!(
                            "Failed to read vector snapshot '{}': {}",
                            snapshot_path, e
                        )))
                    },
                }
            }
        }

        let requested_limit = limit.unwrap_or(self.args.top_k);
        let final_limit = requested_limit.min(self.args.top_k);
        let candidate_limit =
            final_limit.saturating_mul(CANDIDATE_MULTIPLIER).max(final_limit);

        let hot_search = search_hot_candidates(
            Arc::clone(&scope.backend),
            &self.args.table_id,
            &self.args.column_name,
            scope.table_type,
            &session_user,
            scope.last_applied_seq,
            scope.metric,
            &self.args.query_vector,
            candidate_limit,
        )?;

        let cold_candidate_limit = candidate_limit
            .saturating_add(hot_search.touched_pks.len())
            .max(candidate_limit);

        if let Some(index) = &base_index {
            let raw = search_index(index, &self.args.query_vector, cold_candidate_limit)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to search base vector index: {}", e))
                })?;
            for (key, distance) in raw {
                let Some(pk) = base_snapshot_key_to_pk.get(&key) else {
                    continue;
                };
                if hot_search.touched_pks.contains(pk) {
                    continue;
                }
                base_candidates.push((pk.clone(), distance));
            }
        }

        let mut best_distance_by_pk: HashMap<String, f32> = HashMap::new();
        for (pk, distance) in base_candidates
            .into_iter()
            .chain(hot_search.candidates.into_iter())
        {
            match best_distance_by_pk.get_mut(&pk) {
                Some(existing) => {
                    if distance < *existing {
                        *existing = distance;
                    }
                },
                None => {
                    best_distance_by_pk.insert(pk, distance);
                },
            }
        }

        let mut ranked: Vec<(String, f32)> = best_distance_by_pk.into_iter().collect();
        ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(final_limit);

        let row_ids: Vec<String> = ranked.iter().map(|(pk, _)| pk.clone()).collect();
        let scores: Vec<f32> =
            ranked.iter().map(|(_, distance)| distance_to_score(scope.metric, *distance)).collect();

        let batch = RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(StringArray::from(row_ids)) as ArrayRef,
                Arc::new(Float32Array::from(scores)) as ArrayRef,
            ],
        )?;
        let mem = MemTable::try_new(self.schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, Some(final_limit)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args_json_vector() {
        let args = vec![
            Expr::Literal(ScalarValue::Utf8(Some("app.docs".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("embedding".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("[0.1, 0.2, 0.3]".to_string())), None),
            Expr::Literal(ScalarValue::Int64(Some(7)), None),
        ];

        let parsed = parse_args(&args).expect("args should parse");
        assert_eq!(parsed.table_id.full_name(), "app.docs");
        assert_eq!(parsed.column_name, "embedding");
        assert_eq!(parsed.query_vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(parsed.top_k, 7);
    }

    #[test]
    fn test_distance_to_score() {
        assert!((distance_to_score(VectorMetric::Cosine, 0.0) - 1.0).abs() < 1e-6);
        assert!((distance_to_score(VectorMetric::Dot, 0.0) - 1.0).abs() < 1e-6);
        assert!((distance_to_score(VectorMetric::L2, 0.0) - 0.0).abs() < 1e-6);
    }
}
