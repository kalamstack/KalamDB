// T052-T054: Version Resolution using Arrow Compute Kernels
//
// Selects MAX(_seq) per row_id with tie-breaker: FastStorage (priority=2) > Parquet (priority=1)

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::{SharedTableRow, UserTableRow};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::arrow_json_conversion::arrow_value_to_scalar;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::serialization::row_codec::RowMetadata;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub async fn resolve_latest_version(
    fast_batch: RecordBatch,
    long_batch: RecordBatch,
    schema: Arc<Schema>,
) -> Result<RecordBatch, KalamDbError> {
    // Handle empty batches - but still project to target schema for type conversions
    if fast_batch.num_rows() == 0 && long_batch.num_rows() == 0 {
        return create_empty_batch(&schema);
    }

    // If only one batch has data, still project it to ensure schema alignment
    if fast_batch.num_rows() == 0 {
        return project_to_target_schema(long_batch, schema);
    }
    if long_batch.num_rows() == 0 {
        return project_to_target_schema(fast_batch, schema);
    }

    let fast_with_priority = add_source_priority(fast_batch, 2)?;
    let long_with_priority = add_source_priority(long_batch, 1)?;

    let combined_schema = fast_with_priority.schema();
    let combined =
        compute::concat_batches(&combined_schema, &[fast_with_priority, long_with_priority])
            .into_kalamdb_error("concat")?;

    let row_id_idx = combined_schema
        .fields()
        .iter()
        .position(|f| f.name() == "row_id")
        .ok_or_else(|| KalamDbError::Other("Missing row_id".into()))?;
    let seq_idx = combined_schema
        .fields()
        .iter()
        .position(|f| f.name() == SystemColumnNames::SEQ)
        .ok_or_else(|| KalamDbError::Other("Missing _seq".into()))?;
    let priority_idx = combined_schema
        .fields()
        .iter()
        .position(|f| f.name() == "source_priority")
        .ok_or_else(|| KalamDbError::Other("Missing source_priority".into()))?;

    let row_id_array = combined
        .column(row_id_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KalamDbError::Other("row_id not StringArray".into()))?;
    let seq_array = combined
        .column(seq_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| KalamDbError::Other("_seq not Int64Array".into()))?;
    let priority_array = combined
        .column(priority_idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| KalamDbError::Other("source_priority not Int32Array".into()))?;

    // Pre-allocate HashMap with estimated capacity (num_rows is upper bound for unique row_ids)
    // Use &str keys to avoid String allocation per row
    let num_rows = combined.num_rows();
    let mut groups: HashMap<&str, Vec<usize>> = HashMap::with_capacity(num_rows);
    for i in 0..num_rows {
        groups.entry(row_id_array.value(i)).or_default().push(i);
    }

    let mut keep_indices = Vec::with_capacity(groups.len());
    for indices in groups.values() {
        if indices.len() == 1 {
            keep_indices.push(indices[0]);
            continue;
        }
        let mut best_idx = indices[0];
        let mut best_seq = seq_array.value(best_idx);
        let mut best_priority = priority_array.value(best_idx);
        for &idx in &indices[1..] {
            let seq = seq_array.value(idx);
            let priority = priority_array.value(idx);
            if seq > best_seq || (seq == best_seq && priority > best_priority) {
                best_idx = idx;
                best_seq = seq;
                best_priority = priority;
            }
        }
        keep_indices.push(best_idx);
    }
    keep_indices.sort_unstable();

    let indices_array =
        Arc::new(UInt64Array::from(keep_indices.iter().map(|&i| i as u64).collect::<Vec<_>>()));
    let result_columns: Result<Vec<ArrayRef>, _> = combined
        .columns()
        .iter()
        .map(|col| {
            compute::take(col.as_ref(), indices_array.as_ref(), None).into_kalamdb_error("take")
        })
        .collect();
    let result_batch = RecordBatch::try_new(combined_schema.clone(), result_columns?)
        .into_kalamdb_error("create batch")?;

    // Project to target schema with type conversions
    project_to_target_schema(result_batch, schema)
}

/// Project RecordBatch to target schema with type conversions
///
/// Handles type conversions needed after version resolution:
/// - _deleted: Should already be Boolean, but verify and convert if needed
fn project_to_target_schema(
    batch: RecordBatch,
    schema: Arc<Schema>,
) -> Result<RecordBatch, KalamDbError> {
    let mut final_columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields().iter() {
        let col = batch.column_by_name(field.name()).ok_or_else(|| {
            KalamDbError::Other(format!("Missing column {} in batch", field.name()))
        })?;

        // Ensure _deleted remains Boolean (should already be, but verify)
        if field.name() == SystemColumnNames::DELETED && field.data_type() == &DataType::Boolean {
            // _deleted should already be BooleanArray, but double-check
            if col.as_any().downcast_ref::<BooleanArray>().is_none() {
                log::warn!(
                    "_deleted column is not BooleanArray in projection, attempting conversion"
                );
                // Fallback: try to convert from other types if needed
                if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
                    let bools: Vec<bool> =
                        (0..string_array.len()).map(|i| string_array.value(i) == "true").collect();
                    final_columns.push(Arc::new(BooleanArray::from(bools)) as ArrayRef);
                    continue;
                }
            }
        }

        final_columns.push(col.clone());
    }

    RecordBatch::try_new(schema, final_columns).into_kalamdb_error("project_to_target_schema")
}

fn add_source_priority(batch: RecordBatch, priority: u8) -> Result<RecordBatch, KalamDbError> {
    let num_rows = batch.num_rows();
    let priority_array: ArrayRef = Arc::new(Int32Array::from(vec![priority as i32; num_rows]));
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("source_priority", DataType::Int32, false)));
    let new_schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(priority_array);
    RecordBatch::try_new(new_schema, columns).into_kalamdb_error("add_source_priority")
}

fn create_empty_batch(schema: &Arc<Schema>) -> Result<RecordBatch, KalamDbError> {
    use datafusion::arrow::array::Int64Array;
    let empty_columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())) as ArrayRef,
            DataType::Timestamp(_, _) => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            _ => Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        })
        .collect();
    RecordBatch::try_new(schema.clone(), empty_columns).into_kalamdb_error("empty_batch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn test_empty() {
        let s = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let e = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
        )
        .unwrap();
        assert_eq!(resolve_latest_version(e.clone(), e, s).await.unwrap().num_rows(), 0);
    }
    #[tokio::test]
    async fn test_max_seq() {
        let s = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let f = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["Alice_v2"])),
            ],
        )
        .unwrap();
        let l = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice_v1"])),
            ],
        )
        .unwrap();
        let r = resolve_latest_version(f, l, s).await.unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(
            r.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "Alice_v2"
        );
    }
    #[tokio::test]
    async fn test_tie_breaker() {
        let s = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let f = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Fast"])),
            ],
        )
        .unwrap();
        let l = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Parquet"])),
            ],
        )
        .unwrap();
        let r = resolve_latest_version(f, l, s).await.unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(r.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0), "Fast");
    }

    #[derive(Debug, Clone)]
    struct TestVersionedRow {
        seq: SeqId,
        deleted: bool,
        pk: String,
        value: String,
    }

    impl VersionedRow for TestVersionedRow {
        fn seq_id(&self) -> SeqId {
            self.seq
        }

        fn commit_seq(&self) -> u64 {
            0
        }

        fn deleted(&self) -> bool {
            self.deleted
        }

        fn pk_value(&self, _pk_name: &str) -> Option<String> {
            Some(self.pk.clone())
        }
    }

    #[test]
    fn resolve_latest_kvs_from_cold_batch_only_builds_winning_cold_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new(SystemColumnNames::DELETED, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let cold_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![1, 3, 2])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
                Arc::new(StringArray::from(vec!["cold-a", "cold-b", "cold-c"])),
            ],
        )
        .unwrap();

        let hot_rows = vec![(
            "hot-a".to_string(),
            TestVersionedRow {
                seq: SeqId::from_i64(2),
                deleted: false,
                pk: "a".to_string(),
                value: "hot-a".to_string(),
            },
        )];
        let build_calls = AtomicUsize::new(0);

        let resolved = resolve_latest_kvs_from_cold_batch(
            "id",
            hot_rows,
            &cold_batch,
            false,
            None,
            |row_data| {
                build_calls.fetch_add(1, Ordering::SeqCst);
                let pk = match row_data.fields.get("id").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                let name = match row_data.fields.get("name").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                Ok((
                    pk.clone(),
                    TestVersionedRow {
                        seq: row_data.seq_id,
                        deleted: false,
                        pk,
                        value: name,
                    },
                ))
            },
        )
        .unwrap();

        assert_eq!(build_calls.load(Ordering::SeqCst), 2);
        assert_eq!(resolved.len(), 3);
        assert!(resolved.iter().any(|(_, row)| row.pk == "a" && row.value == "hot-a"));
        assert!(resolved.iter().any(|(_, row)| row.pk == "b" && row.value == "cold-b"));
        assert!(resolved.iter().any(|(_, row)| row.pk == "c" && row.value == "cold-c"));
    }

    #[test]
    fn resolve_latest_kvs_from_cold_batch_honors_snapshot_commit_seq() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new(SystemColumnNames::COMMIT_SEQ, DataType::UInt64, false),
            Field::new(SystemColumnNames::DELETED, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let cold_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(UInt64Array::from(vec![2_u64])),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(StringArray::from(vec!["cold-new"])),
            ],
        )
        .unwrap();

        let hot_rows = vec![(
            "hot-a".to_string(),
            TestVersionedRow {
                seq: SeqId::from_i64(4),
                deleted: false,
                pk: "a".to_string(),
                value: "hot-visible".to_string(),
            },
        )];

        let resolved = resolve_latest_kvs_from_cold_batch(
            "id",
            hot_rows,
            &cold_batch,
            false,
            Some(1),
            |row_data| {
                let pk = match row_data.fields.get("id").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                let name = match row_data.fields.get("name").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                Ok((
                    pk.clone(),
                    TestVersionedRow {
                        seq: row_data.seq_id,
                        deleted: row_data.deleted,
                        pk,
                        value: name,
                    },
                ))
            },
        )
        .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].1.value, "hot-visible");
    }
}

/// Generic helper for UPDATE/DELETE operations that need to scan hot+cold storage with version resolution
///
/// This is a generic helper for UPDATE/DELETE operations that need to:
/// 1. Scan both RocksDB (fast storage) and Parquet (flushed storage)
/// 2. Apply version resolution (latest _seq wins)
/// 3. Filter out deleted records (_deleted = true)
/// 4. Convert Arrow RecordBatch back to row structures
///
/// **Phase 3, US1, T061-T068**: Support UPDATE/DELETE on flushed records
/// **Phase 13.6**: Moved from base_table_provider.rs during cleanup
///
/// # Type Parameters
/// * `K` - Storage key type
/// * `V` - Row value type
/// * `F` - Function to scan RocksDB, returns RecordBatch
/// * `G` - Function to scan Parquet, returns RecordBatch
/// * `H` - Function to convert Arrow row to (key, value) pair
///
/// # Arguments
/// * `schema` - Arrow schema with system columns
/// * `scan_rocksdb` - Async function to scan RocksDB
/// * `scan_parquet` - Async function to scan Parquet files
/// * `row_converter` - Function to convert Arrow row to (key, value)
///
/// # Returns
/// Vector of (key, value) pairs representing latest non-deleted records
pub async fn scan_with_version_resolution_to_kvs<K, V, F, G, H>(
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    scan_rocksdb: F,
    scan_parquet: G,
    row_converter: H,
) -> Result<Vec<(K, V)>, datafusion::error::DataFusionError>
where
    K: Clone,
    F: std::future::Future<
        Output = Result<
            datafusion::arrow::record_batch::RecordBatch,
            datafusion::error::DataFusionError,
        >,
    >,
    G: std::future::Future<
        Output = Result<
            datafusion::arrow::record_batch::RecordBatch,
            datafusion::error::DataFusionError,
        >,
    >,
    H: Fn(
        &datafusion::arrow::record_batch::RecordBatch,
        usize,
    ) -> Result<(K, V), datafusion::error::DataFusionError>,
{
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute;

    // Step 1: Scan RocksDB (fast storage)
    let rocksdb_batch = scan_rocksdb.await?;

    // Step 2: Scan Parquet files (flushed storage)
    let parquet_batch = scan_parquet.await?;

    // Step 3: Apply version resolution (latest _seq wins)
    let resolved_batch = resolve_latest_version(rocksdb_batch, parquet_batch, schema.clone())
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Version resolution failed: {}",
                e
            ))
        })?;

    // Step 4: Filter out deleted records (_deleted = true)
    let filtered_batch = {
        let deleted_col =
            resolved_batch.column_by_name(SystemColumnNames::DELETED).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("Missing _deleted column".to_string())
            })?;
        let deleted_array = deleted_col.as_boolean();

        // Create filter: NOT deleted (keep rows where _deleted = false)
        let filter_array = compute::not(deleted_array).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to compute NOT filter: {}",
                e
            ))
        })?;

        compute::filter_record_batch(&resolved_batch, &filter_array).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to filter deleted records: {}",
                e
            ))
        })?
    };

    // Step 5: Convert Arrow RecordBatch to Vec<(K, V)>
    let num_rows = filtered_batch.num_rows();
    let mut results = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let (key, value) = row_converter(&filtered_batch, row_idx)?;
        results.push((key, value));
    }

    Ok(results)
}

/// Parsed representation of a Parquet row used for version resolution merging
#[derive(Debug, Clone)]
pub struct ParquetRowData {
    pub seq_id: SeqId,
    pub commit_seq: u64,
    pub deleted: bool,
    pub fields: Row,
}

/// Convert Parquet RecordBatch rows into SeqId + JSON field maps
pub fn parquet_batch_to_rows(batch: &RecordBatch) -> Result<Vec<ParquetRowData>, KalamDbError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let decoder = ParquetBatchDecoder::new(batch, None)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        rows.push(decoder.row_at(row_idx)?);
    }

    Ok(rows)
}

/// Trait covering the minimal information needed for merging versioned rows
pub trait VersionedRow {
    fn seq_id(&self) -> SeqId;
    fn commit_seq(&self) -> u64;
    fn deleted(&self) -> bool;
    fn pk_value(&self, pk_name: &str) -> Option<String>;
}

#[inline]
fn is_visible_at_snapshot(commit_seq: u64, snapshot_commit_seq: Option<u64>) -> bool {
    snapshot_commit_seq.is_none_or(|snapshot| commit_seq <= snapshot)
}

#[inline]
fn prefers_version(
    candidate_commit_seq: u64,
    candidate_seq: SeqId,
    current_commit_seq: u64,
    current_seq: SeqId,
) -> bool {
    candidate_commit_seq > current_commit_seq
        || (candidate_commit_seq == current_commit_seq && candidate_seq > current_seq)
}

impl VersionedRow for SharedTableRow {
    fn seq_id(&self) -> SeqId {
        self._seq
    }

    fn commit_seq(&self) -> u64 {
        self._commit_seq
    }

    fn deleted(&self) -> bool {
        self._deleted
    }

    fn pk_value(&self, pk_name: &str) -> Option<String> {
        self.fields.get(pk_name).and_then(|val| {
            if val.is_null() {
                None
            } else {
                match val {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
                    _ => Some(val.to_string()),
                }
            }
        })
    }
}

impl VersionedRow for UserTableRow {
    fn seq_id(&self) -> SeqId {
        self._seq
    }

    fn commit_seq(&self) -> u64 {
        self._commit_seq
    }

    fn deleted(&self) -> bool {
        self._deleted
    }

    fn pk_value(&self, pk_name: &str) -> Option<String> {
        self.fields.get(pk_name).and_then(|val| {
            if val.is_null() {
                None
            } else {
                match val {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
                    _ => Some(val.to_string()),
                }
            }
        })
    }
}

impl VersionedRow for RowMetadata {
    fn seq_id(&self) -> SeqId {
        self.seq
    }

    fn commit_seq(&self) -> u64 {
        self.commit_seq
    }

    fn deleted(&self) -> bool {
        self.deleted
    }

    fn pk_value(&self, _pk_name: &str) -> Option<String> {
        self.pk_value.clone()
    }
}

/// Extract lightweight metadata (seq, deleted, pk_value) from a Parquet RecordBatch
/// without materializing full Row objects. Used for count-only scan paths.
pub fn parquet_batch_to_metadata(
    batch: &RecordBatch,
    pk_name: &str,
) -> Result<Vec<(SeqId, RowMetadata)>, KalamDbError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let decoder = ParquetBatchDecoder::new(batch, Some(pk_name))?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let metadata = decoder.metadata_at(row_idx);
        rows.push((metadata.seq, metadata));
    }

    Ok(rows)
}

#[derive(Debug)]
struct ParquetBatchDecoder<'a> {
    batch: &'a RecordBatch,
    seq_array: &'a Int64Array,
    commit_seq_array: Option<&'a UInt64Array>,
    deleted_array: Option<&'a BooleanArray>,
    pk_idx: Option<usize>,
    /// Cached downcast of the PK column for fast string extraction without
    /// going through ScalarValue intermediate.
    pk_string_array: Option<&'a StringArray>,
    value_columns: Vec<(usize, String)>,
}

impl<'a> ParquetBatchDecoder<'a> {
    fn new(batch: &'a RecordBatch, pk_name: Option<&str>) -> Result<Self, KalamDbError> {
        use datafusion::arrow::array::{Array, BooleanArray, Int64Array};

        let schema = batch.schema();
        let seq_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == SystemColumnNames::SEQ)
            .ok_or_else(|| {
                KalamDbError::Other("Missing _seq column in Parquet batch".to_string())
            })?;
        let deleted_idx =
            schema.fields().iter().position(|f| f.name() == SystemColumnNames::DELETED);
        let commit_seq_idx =
            schema.fields().iter().position(|f| f.name() == SystemColumnNames::COMMIT_SEQ);
        let pk_idx = pk_name.and_then(|name| schema.fields().iter().position(|f| f.name() == name));

        let seq_array = batch
            .column(seq_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| KalamDbError::Other("_seq column is not Int64Array".to_string()))?;
        let deleted_array =
            deleted_idx.and_then(|idx| batch.column(idx).as_any().downcast_ref::<BooleanArray>());
        let commit_seq_array = commit_seq_idx
            .and_then(|idx| batch.column(idx).as_any().downcast_ref::<UInt64Array>());
        // Try to cache the PK column as StringArray for fast extraction.
        let pk_string_array = pk_idx
            .and_then(|idx| batch.column(idx).as_any().downcast_ref::<StringArray>());
        let value_columns = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() != SystemColumnNames::SEQ
                    && field.name() != SystemColumnNames::COMMIT_SEQ
                    && field.name() != SystemColumnNames::DELETED
            })
            .map(|(idx, field)| (idx, field.name().clone()))
            .collect();

        Ok(Self {
            batch,
            seq_array,
            commit_seq_array,
            deleted_array,
            pk_idx,
            pk_string_array,
            value_columns,
        })
    }

    fn metadata_at(&self, row_idx: usize) -> RowMetadata {
        let seq = SeqId::from_i64(self.seq_array.value(row_idx));
        let deleted = self
            .deleted_array
            .and_then(|arr| (!arr.is_null(row_idx)).then(|| arr.value(row_idx)))
            .unwrap_or(false);
        let commit_seq = self
            .commit_seq_array
            .and_then(|arr| (!arr.is_null(row_idx)).then(|| arr.value(row_idx)))
            .unwrap_or(0);
        let pk_value = if let Some(str_arr) = self.pk_string_array {
            // Fast path: PK is StringArray, read directly (avoids ScalarValue intermediate).
            if str_arr.is_null(row_idx) {
                None
            } else {
                let v = str_arr.value(row_idx);
                if v.is_empty() { None } else { Some(v.to_owned()) }
            }
        } else {
            // Fallback for non-Utf8 PK types (Int64, etc.)
            self.pk_idx.and_then(|idx| {
                let array = self.batch.column(idx);
                arrow_value_to_scalar(array.as_ref(), row_idx).ok().and_then(|sv| match &sv {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        Some(s.clone())
                    },
                    other if other.is_null() => None,
                    other => Some(other.to_string()),
                })
            })
        };

        RowMetadata {
            seq,
            commit_seq,
            deleted,
            pk_value,
        }
    }

    fn row_at(&self, row_idx: usize) -> Result<ParquetRowData, KalamDbError> {
        let metadata = self.metadata_at(row_idx);
        let mut values = BTreeMap::new();

        for (col_idx, col_name) in &self.value_columns {
            let array = self.batch.column(*col_idx);
            match arrow_value_to_scalar(array.as_ref(), row_idx) {
                Ok(val) => {
                    values.insert(col_name.clone(), val);
                },
                Err(e) => {
                    log::warn!("Failed to convert column {} for row {}: {}", col_name, row_idx, e);
                },
            }
        }

        Ok(ParquetRowData {
            seq_id: metadata.seq,
            commit_seq: metadata.commit_seq,
            deleted: metadata.deleted,
            fields: Row::new(values),
        })
    }
}

/// Count unique non-deleted rows after version resolution (hot + cold merge).
/// Only tracks (SeqId, deleted) per PK — avoids storing full row metadata.
pub fn count_merged_rows<R, I, J>(
    pk_name: &str,
    hot_rows: I,
    cold_rows: J,
    snapshot_commit_seq: Option<u64>,
) -> usize
where
    I: IntoIterator<Item = R>,
    J: IntoIterator<Item = R>,
    R: VersionedRow,
{
    use std::collections::hash_map::Entry;

    let mut best: HashMap<String, (u64, SeqId, bool)> = HashMap::new();

    for row in hot_rows.into_iter().chain(cold_rows) {
        if !is_visible_at_snapshot(row.commit_seq(), snapshot_commit_seq) {
            continue;
        }
        let pk_key = row
            .pk_value(pk_name)
            .filter(|val| !val.is_empty())
            .unwrap_or_else(|| format!("_seq:{}", row.seq_id().as_i64()));
        let commit_seq = row.commit_seq();
        let seq = row.seq_id();
        let deleted = row.deleted();

        match best.entry(pk_key) {
            Entry::Occupied(mut entry) => {
                let (current_commit_seq, current_seq, _) = *entry.get();
                if prefers_version(commit_seq, seq, current_commit_seq, current_seq) {
                    *entry.get_mut() = (commit_seq, seq, deleted);
                }
            },
            Entry::Vacant(entry) => {
                entry.insert((commit_seq, seq, deleted));
            },
        }
    }

    best.values().filter(|(_, _, deleted)| !deleted).count()
}

/// Count resolved rows from hot + cold storage using metadata-only decode.
///
/// Generic helper used by both SharedTableProvider and UserTableProvider for COUNT(*)
/// queries. Accepts the hot metadata (pre-scanned from RocksDB with metadata-only decode)
/// and a Parquet RecordBatch from cold storage.
pub fn count_resolved_from_metadata(
    pk_name: &str,
    hot_metadata: Vec<RowMetadata>,
    cold_batch: &RecordBatch,
    snapshot_commit_seq: Option<u64>,
) -> Result<usize, KalamDbError> {
    let cold_metadata = parquet_batch_to_metadata(cold_batch, pk_name)?;

    Ok(count_merged_rows(
        pk_name,
        hot_metadata,
        cold_metadata.into_iter().map(|(_, m)| m),
        snapshot_commit_seq,
    ))
}

/// Merge hot (RocksDB) and cold (Parquet) rows keeping latest version per PK
pub fn merge_versioned_rows<K, R, I, J>(
    pk_name: &str,
    hot_rows: I,
    cold_rows: J,
    keep_deleted: bool,
    snapshot_commit_seq: Option<u64>,
) -> Vec<(K, R)>
where
    I: IntoIterator<Item = (K, R)>,
    J: IntoIterator<Item = (K, R)>,
    K: Clone,
    R: VersionedRow,
{
    use std::collections::hash_map::Entry;

    let mut best: HashMap<String, (K, R)> = HashMap::new();

    for (key, row) in hot_rows.into_iter().chain(cold_rows) {
        if !is_visible_at_snapshot(row.commit_seq(), snapshot_commit_seq) {
            continue;
        }
        let pk_key = row
            .pk_value(pk_name)
            .filter(|val| !val.is_empty())
            .unwrap_or_else(|| format!("_seq:{}", row.seq_id().as_i64()));

        match best.entry(pk_key) {
            Entry::Occupied(mut entry) => {
                if prefers_version(
                    row.commit_seq(),
                    row.seq_id(),
                    entry.get().1.commit_seq(),
                    entry.get().1.seq_id(),
                ) {
                    entry.insert((key, row));
                }
            },
            Entry::Vacant(entry) => {
                entry.insert((key, row));
            },
        }
    }

    best.into_values().filter(|(_, row)| keep_deleted || !row.deleted()).collect()
}

/// Resolve latest rows by merging fully decoded hot rows with metadata-first cold rows.
///
/// Cold Parquet rows are only materialized if they win version resolution for a PK.
pub fn resolve_latest_kvs_from_cold_batch<K, R, I, F>(
    pk_name: &str,
    hot_rows: I,
    cold_batch: &RecordBatch,
    keep_deleted: bool,
    snapshot_commit_seq: Option<u64>,
    build_cold_row: F,
) -> Result<Vec<(K, R)>, KalamDbError>
where
    I: IntoIterator<Item = (K, R)>,
    F: Fn(ParquetRowData) -> Result<(K, R), KalamDbError>,
    K: Clone,
    R: VersionedRow,
{
    use std::collections::hash_map::Entry;

    enum Winner<K, R> {
        Hot((K, R)),
        Cold {
            row_idx: usize,
            metadata: RowMetadata,
        },
    }

    impl<K, R> Winner<K, R>
    where
        R: VersionedRow,
    {
        fn commit_seq(&self) -> u64 {
            match self {
                Winner::Hot((_, row)) => row.commit_seq(),
                Winner::Cold { metadata, .. } => metadata.commit_seq,
            }
        }

        fn seq_id(&self) -> SeqId {
            match self {
                Winner::Hot((_, row)) => row.seq_id(),
                Winner::Cold { metadata, .. } => metadata.seq,
            }
        }

        fn deleted(&self) -> bool {
            match self {
                Winner::Hot((_, row)) => row.deleted(),
                Winner::Cold { metadata, .. } => metadata.deleted,
            }
        }
    }

    // Pre-allocate with a reasonable estimate: cold rows are an upper bound on unique PKs.
    let estimated_capacity = cold_batch.num_rows().max(64);
    let mut best: HashMap<String, Winner<K, R>> = HashMap::with_capacity(estimated_capacity);

    for (key, row) in hot_rows {
        if !is_visible_at_snapshot(row.commit_seq(), snapshot_commit_seq) {
            continue;
        }
        let pk_key = row
            .pk_value(pk_name)
            .filter(|val| !val.is_empty())
            .unwrap_or_else(|| format!("_seq:{}", row.seq_id().as_i64()));

        match best.entry(pk_key) {
            Entry::Occupied(mut entry) => {
                if prefers_version(
                    row.commit_seq(),
                    row.seq_id(),
                    entry.get().commit_seq(),
                    entry.get().seq_id(),
                ) {
                    entry.insert(Winner::Hot((key, row)));
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(Winner::Hot((key, row)));
            },
        }
    }

    let decoder = ParquetBatchDecoder::new(cold_batch, Some(pk_name))?;
    for row_idx in 0..cold_batch.num_rows() {
        let metadata = decoder.metadata_at(row_idx);
        if !is_visible_at_snapshot(metadata.commit_seq, snapshot_commit_seq) {
            continue;
        }
        let pk_key = metadata
            .pk_value
            .clone()
            .filter(|val| !val.is_empty())
            .unwrap_or_else(|| format!("_seq:{}", metadata.seq.as_i64()));

        match best.entry(pk_key) {
            Entry::Occupied(mut entry) => {
                if prefers_version(
                    metadata.commit_seq,
                    metadata.seq,
                    entry.get().commit_seq(),
                    entry.get().seq_id(),
                ) {
                    entry.insert(Winner::Cold { row_idx, metadata });
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(Winner::Cold { row_idx, metadata });
            },
        }
    }

    let mut resolved = Vec::with_capacity(best.len());
    for winner in best.into_values() {
        if !keep_deleted && winner.deleted() {
            continue;
        }

        match winner {
            Winner::Hot(row) => resolved.push(row),
            Winner::Cold { row_idx, .. } => {
                resolved.push(build_cold_row(decoder.row_at(row_idx)?)?)
            },
        }
    }

    Ok(resolved)
}
