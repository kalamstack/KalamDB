//! Parquet reading operations using StorageCached.
//!
//! Provides utilities to read Parquet files from any storage backend
//! with StorageCached-managed file access.
//!
//! # Performance Features
//!
//! - **Column Projection**: Pushes a Parquet projection mask so unneeded column chunks are not read
//!   or decoded
//! - **Streaming I/O**: All reads use an `AsyncFileReader` over ObjectStore — reads only the footer
//!   eagerly and fetches column chunks on demand via range requests (remote) or file seeks (local).
//!   No full-file downloads.
//!
//! # Usage Tiers
//!
//! | Function | Column Projection | Streaming | Use Case |
//! |----------|:-:|:-:|----------|
//! | `parse_parquet_stream` | Optional | ✓ | General streaming read (recommended) |

use std::{ops::Range, pin::Pin, sync::Arc};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjectPath, GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStreamBuilder},
        ProjectionMask,
    },
    basic::Type as ParquetPhysicalType,
    bloom_filter::Sbbf,
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        statistics::Statistics,
    },
    schema::types::SchemaDescriptor,
};

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

use crate::error::{FilestoreError, Result};

// ========== Async streaming reader (ObjectStore-backed) ==========

/// A boxed async stream of `RecordBatch`es yielded one row-group at a time.
///
/// The underlying reader only loads the Parquet metadata footer eagerly;
/// column chunks are fetched on demand as the stream is polled, keeping
/// peak memory proportional to a single row group rather than the whole file.
pub type RecordBatchFileStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<RecordBatch>> + Send>>;

/// Optional pruning and projection controls for ObjectStore-backed Parquet reads.
#[derive(Debug, Clone, Default)]
pub struct ParquetReadOptions {
    columns:    Vec<String>,
    row_groups: Option<Vec<usize>>,
    seq_range:  Option<SeqRangePruning>,
    pk_bloom:   Option<PkBloomPruning>,
}

impl ParquetReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_row_groups(mut self, row_groups: Vec<usize>) -> Self {
        self.row_groups = Some(row_groups);
        self
    }

    pub fn with_seq_range(mut self, column: impl Into<String>, min: i64, max: i64) -> Self {
        self.seq_range = Some(SeqRangePruning {
            column: column.into(),
            min,
            max,
        });
        self
    }

    pub fn with_column_bloom_values<I, S>(mut self, column: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.pk_bloom = Some(PkBloomPruning {
            column: column.into(),
            values: values.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Equality Bloom prune for a named column (PK or scalar index).
    pub fn with_pk_bloom_values<I, S>(self, column: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.with_column_bloom_values(column, values)
    }
}

#[derive(Debug, Clone)]
struct SeqRangePruning {
    column: String,
    min:    i64,
    max:    i64,
}

#[derive(Debug, Clone)]
struct PkBloomPruning {
    column: String,
    values: Vec<String>,
}

/// ObjectStore-backed [`AsyncFileReader`], matching the parquet 59 example that
/// replaced deprecated [`parquet::arrow::async_reader::ParquetObjectReader`].
#[derive(Clone, Debug)]
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path:  ObjectPath,
}

impl ObjectStoreReader {
    fn new(store: Arc<dyn ObjectStore>, path: ObjectPath) -> Self {
        Self { store, path }
    }
}

fn to_parquet_err(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            async move { self.store.get_range(&self.path, range).await.map_err(to_parquet_err) },
        )
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        Box::pin(
            async move { self.store.get_ranges(&self.path, &ranges).await.map_err(to_parquet_err) },
        )
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        Box::pin(async move {
            let response =
                self.store.get_opts(&self.path, options).await.map_err(to_parquet_err)?;
            response.bytes().await.map_err(to_parquet_err)
        })
    }
}

/// Open an async streaming reader over a Parquet file via ObjectStore.
///
/// Works for any backend (local filesystem, S3, GCS, Azure). Only reads the
/// Parquet footer eagerly; column chunks are fetched on demand via HTTP range
/// requests (remote) or file seeks (local), keeping peak memory proportional
/// to a single row group rather than the whole file.
///
/// If `columns` is non-empty, only the specified columns are decoded.
/// Pass `&[]` to read all columns.
pub async fn parse_parquet_stream(
    store: Arc<dyn ObjectStore>,
    path: &ObjectPath,
    columns: &[&str],
) -> Result<RecordBatchFileStream> {
    let options = ParquetReadOptions::new().with_columns(columns.iter().copied());
    parse_parquet_stream_with_options(store, path, &options).await
}

/// Open an async streaming reader with projection and row-group pruning options.
pub async fn parse_parquet_stream_with_options(
    store: Arc<dyn ObjectStore>,
    path: &ObjectPath,
    options: &ParquetReadOptions,
) -> Result<RecordBatchFileStream> {
    let reader = ObjectStoreReader::new(store, path.clone());
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    let row_groups_before_pruning = builder.metadata().num_row_groups();
    let mut selected_row_groups =
        initial_row_groups(row_groups_before_pruning, &options.row_groups);
    let mut row_group_pruning_applied = options.row_groups.is_some();

    if let Some(seq_range) = &options.seq_range {
        selected_row_groups = prune_row_groups_by_seq_range(
            builder.metadata(),
            builder.parquet_schema(),
            &selected_row_groups,
            seq_range,
        );
        row_group_pruning_applied = true;
    }

    if let Some(pk_bloom) = &options.pk_bloom {
        selected_row_groups =
            prune_row_groups_by_pk_bloom(&mut builder, &selected_row_groups, pk_bloom).await?;
        row_group_pruning_applied = true;
    }

    let builder = if row_group_pruning_applied {
        builder.with_row_groups(selected_row_groups.clone())
    } else {
        builder
    };

    let builder = if !options.columns.is_empty() {
        let parquet_schema = builder.parquet_schema();
        let column_refs: Vec<&str> = options.columns.iter().map(String::as_str).collect();
        let indices = resolve_column_indices(parquet_schema, &column_refs);
        if indices.is_empty() {
            builder
        } else {
            let mask = ProjectionMask::leaves(parquet_schema, indices);
            builder.with_projection(mask)
        }
    } else {
        builder
    };

    tracing::trace!(
        path = %path,
        row_groups = row_groups_before_pruning,
        selected_row_groups = if row_group_pruning_applied {
            selected_row_groups.len()
        } else {
            row_groups_before_pruning
        },
        projected_cols = options.columns.len(),
        "Opened ObjectStore Parquet stream"
    );
    let stream = builder.build().map_err(|e| FilestoreError::Parquet(e.to_string()))?;
    Ok(Box::pin(stream.map_err(|e| FilestoreError::Parquet(e.to_string()))))
}

// ========== Internal helpers ==========

/// Resolve column names to Parquet leaf column indices.
fn resolve_column_indices(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    columns: &[&str],
) -> Vec<usize> {
    columns
        .iter()
        .filter_map(|name| parquet_schema.columns().iter().position(|c| c.name() == *name))
        .collect()
}

fn initial_row_groups(total_row_groups: usize, requested: &Option<Vec<usize>>) -> Vec<usize> {
    match requested {
        Some(row_groups) => {
            row_groups.iter().copied().filter(|idx| *idx < total_row_groups).collect()
        },
        None => (0..total_row_groups).collect(),
    }
}

fn prune_row_groups_by_seq_range(
    metadata: &ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    row_groups: &[usize],
    range: &SeqRangePruning,
) -> Vec<usize> {
    let Some(column_idx) =
        parquet_schema.columns().iter().position(|column| column.name() == range.column)
    else {
        return row_groups.to_vec();
    };

    row_groups
        .iter()
        .copied()
        .filter(|row_group_idx| {
            let Some(stats) = metadata.row_group(*row_group_idx).column(column_idx).statistics()
            else {
                return true;
            };
            seq_stats_overlap(stats, range)
        })
        .collect()
}

fn seq_stats_overlap(stats: &Statistics, range: &SeqRangePruning) -> bool {
    match stats {
        Statistics::Int64(values) => values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| *max >= range.min && *min <= range.max)
            .unwrap_or(true),
        Statistics::Int32(values) => values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| i64::from(*max) >= range.min && i64::from(*min) <= range.max)
            .unwrap_or(true),
        _ => true,
    }
}

async fn prune_row_groups_by_pk_bloom(
    builder: &mut ParquetRecordBatchStreamBuilder<ObjectStoreReader>,
    row_groups: &[usize],
    pruning: &PkBloomPruning,
) -> Result<Vec<usize>> {
    if pruning.values.is_empty() {
        return Ok(row_groups.to_vec());
    }

    let Some(column_idx) = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| column.name() == pruning.column)
    else {
        return Ok(row_groups.to_vec());
    };
    let physical_type = builder.parquet_schema().column(column_idx).physical_type();

    let mut selected = Vec::with_capacity(row_groups.len());
    for row_group_idx in row_groups {
        let bloom_filter =
            match builder.get_row_group_column_bloom_filter(*row_group_idx, column_idx).await {
                Ok(Some(filter)) => filter,
                Ok(None) => {
                    selected.push(*row_group_idx);
                    continue;
                },
                Err(error) => {
                    tracing::debug!(
                        row_group = *row_group_idx,
                        column = %pruning.column,
                        error = %error,
                        "Ignoring Parquet bloom filter read error"
                    );
                    selected.push(*row_group_idx);
                    continue;
                },
            };

        if pruning
            .values
            .iter()
            .any(|value| bloom_may_contain(&bloom_filter, physical_type, value))
        {
            selected.push(*row_group_idx);
        }
    }

    Ok(selected)
}

fn bloom_may_contain(filter: &Sbbf, physical_type: ParquetPhysicalType, value: &str) -> bool {
    match physical_type {
        ParquetPhysicalType::INT32 => value.parse::<i32>().map_or(true, |v| filter.check(&v)),
        ParquetPhysicalType::INT64 => value.parse::<i64>().map_or(true, |v| filter.check(&v)),
        ParquetPhysicalType::BYTE_ARRAY | ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY => {
            filter.check(value)
        },
        ParquetPhysicalType::BOOLEAN => value.parse::<bool>().map_or(true, |v| filter.check(&v)),
        ParquetPhysicalType::FLOAT => value.parse::<f32>().map_or(true, |v| filter.check(&v)),
        ParquetPhysicalType::DOUBLE => value.parse::<f64>().map_or(true, |v| filter.check(&v)),
        ParquetPhysicalType::INT96 => true,
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, sync::Arc};

    use arrow::{
        array::{Array, BooleanArray, Float64Array, Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use kalamdb_commons::{
        arrow_utils::{field_boolean, field_float64, field_int64, field_utf8, schema},
        models::{ids::StorageId, TableId},
        schemas::TableType,
    };
    use kalamdb_system::{providers::storages::models::StorageType, Storage};

    use super::*;
    use crate::registry::StorageCached;

    fn create_test_storage(temp_dir: &std::path::Path) -> Storage {
        let now = chrono::Utc::now().timestamp_millis();
        Storage {
            storage_id:             StorageId::from("test_parquet_read"),
            storage_name:           "test_parquet_read".to_string(),
            description:            None,
            storage_type:           StorageType::Filesystem,
            base_directory:         temp_dir.to_string_lossy().to_string(),
            credentials:            None,
            config_json:            None,
            shared_tables_template: "{namespace}/{tableName}".to_string(),
            user_tables_template:   "{namespace}/{tableName}/{userId}".to_string(),
            created_at:             now,
            updated_at:             now,
        }
    }

    fn create_simple_batch(num_rows: usize) -> RecordBatch {
        let schema = schema(vec![
            field_int64("id", false),
            field_utf8("name", true),
            field_int64("_seq", false),
        ]);

        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();
        let seqs: Vec<i64> = (0..num_rows as i64).map(|i| i * 1000).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(seqs)),
            ],
        )
        .unwrap()
    }

    /// Helper: write a batch via StorageCached and return (object_store, object_path)
    /// so tests can use the streaming reader.
    fn write_test_parquet(
        temp_dir: &std::path::Path,
        file_path: &str,
        batches: Vec<RecordBatch>,
    ) -> (Arc<dyn ObjectStore>, ObjectPath) {
        write_test_parquet_with_bloom(temp_dir, file_path, batches, None)
    }

    fn write_test_parquet_with_bloom(
        temp_dir: &std::path::Path,
        file_path: &str,
        batches: Vec<RecordBatch>,
        bloom_filter_columns: Option<Vec<String>>,
    ) -> (Arc<dyn ObjectStore>, ObjectPath) {
        let storage = create_test_storage(temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "data");
        let schema_ref = batches[0].schema();

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema_ref,
                batches,
                bloom_filter_columns,
            )
            .unwrap();

        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp_dir).unwrap());
        let object_path = ObjectPath::from("test/data/".to_owned() + file_path);
        (store, object_path)
    }

    #[tokio::test]
    async fn test_streaming_read_simple() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_simple");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let batch = create_simple_batch(100);
        let (store, path) = write_test_parquet(&temp_dir, "data.parquet", vec![batch]);

        let stream = parse_parquet_stream(store, &path, &[]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 100);
        assert_eq!(batches[0].num_columns(), 3);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_projected() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_projected");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let batch = create_simple_batch(50);
        let (store, path) = write_test_parquet(&temp_dir, "proj.parquet", vec![batch]);

        let stream = parse_parquet_stream(store, &path, &["id", "_seq"]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 50);
        assert_eq!(batches[0].num_columns(), 2);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_empty_parquet() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_empty");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let s = schema(vec![field_int64("id", false), field_utf8("value", true)]);
        let empty_batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
            ],
        )
        .unwrap();

        let (store, path) = write_test_parquet(&temp_dir, "empty.parquet", vec![empty_batch]);

        let stream = parse_parquet_stream(store, &path, &[]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_multiple_batches() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_multi");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let batch1 = create_simple_batch(50);
        let batch2 = create_simple_batch(75);
        let batch3 = create_simple_batch(100);
        let (store, path) =
            write_test_parquet(&temp_dir, "multi.parquet", vec![batch1, batch2, batch3]);

        let stream = parse_parquet_stream(store, &path, &[]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 225);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_with_types() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_types");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let s = schema(vec![
            field_int64("int_col", false),
            field_utf8("str_col", true),
            field_float64("float_col", true),
            field_boolean("bool_col", false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "types");

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                "types.parquet",
                s,
                vec![batch],
                None,
            )
            .unwrap();

        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&temp_dir).unwrap());
        let path = ObjectPath::from("test/types/types.parquet");

        let stream = parse_parquet_stream(store, &path, &[]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 4);
        assert_eq!(batches[0].num_rows(), 3);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_with_nulls() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_nulls");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let s = schema(vec![field_int64("id", false), field_utf8("nullable_str", true)]);

        let batch = RecordBatch::try_new(
            Arc::clone(&s),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c"), None])),
            ],
        )
        .unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "nulls");

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                "nulls.parquet",
                s,
                vec![batch],
                None,
            )
            .unwrap();

        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&temp_dir).unwrap());
        let path = ObjectPath::from("test/nulls/nulls.parquet");

        let stream = parse_parquet_stream(store, &path, &[]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);

        let str_array = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(str_array.is_null(1));
        assert!(str_array.is_null(3));
        assert!(!str_array.is_null(0));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_large() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_large");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let large_batch = create_simple_batch(10_000);
        let (store, path) = write_test_parquet(&temp_dir, "large.parquet", vec![large_batch]);

        let stream = parse_parquet_stream(store, &path, &[]).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10_000);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_with_row_group_selection() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_row_groups");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let batch = create_simple_batch(150_000);
        let (store, path) = write_test_parquet(&temp_dir, "row_groups.parquet", vec![batch]);

        let options = ParquetReadOptions::new().with_row_groups(vec![1]);
        let stream = parse_parquet_stream_with_options(store, &path, &options).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 18_928);
        let first_id = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(first_id, 131_072);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_prunes_row_groups_by_seq_stats() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_seq_pruning");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let batch = create_simple_batch(150_000);
        let (store, path) = write_test_parquet(&temp_dir, "seq_pruned.parquet", vec![batch]);

        let options = ParquetReadOptions::new().with_seq_range("_seq", 132_000_000, 132_010_000);
        let stream = parse_parquet_stream_with_options(store, &path, &options).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 18_928);
        let first_seq =
            batches[0].column(2).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(first_seq, 131_072_000);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_streaming_read_prunes_row_groups_by_pk_bloom() {
        let temp_dir = env::temp_dir().join("kalamdb_test_stream_bloom_pruning");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let batch = create_simple_batch(150_000);
        let (store, path) = write_test_parquet_with_bloom(
            &temp_dir,
            "bloom_pruned.parquet",
            vec![batch],
            Some(vec!["id".to_string()]),
        );

        let options = ParquetReadOptions::new().with_pk_bloom_values("id", vec!["132000"]);
        let stream = parse_parquet_stream_with_options(store, &path, &options).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 18_928);
        let first_id = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(first_id, 131_072);

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
