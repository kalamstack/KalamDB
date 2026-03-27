//! Parquet reading operations using StorageCached.
//!
//! Provides utilities to read Parquet files from any storage backend
//! with StorageCached-managed file access.
//!
//! # Performance Features
//!
//! - **Column Projection**: Only read the columns you need, reducing I/O and memory
//! - **Row Group Pruning via Bloom Filters**: Skip entire row groups where the bloom
//!   filter reports a value is "definitely not present"
//! - **Streaming I/O**: All reads use `ParquetObjectReader` — reads only the footer
//!   eagerly and fetches column chunks on demand via range requests (remote) or
//!   file seeks (local). No full-file downloads.
//!
//! # Usage Tiers
//!
//! | Function | Column Projection | Streaming | Use Case |
//! |----------|:-:|:-:|----------|
//! | `parse_parquet_stream` | Optional | ✓ | General streaming read (recommended) |

use crate::error::{FilestoreError, Result};
use arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::arrow::ProjectionMask;
use futures_util::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::pin::Pin;
use std::sync::Arc;

// ========== Async streaming reader (ObjectStore-backed) ==========

/// A boxed async stream of `RecordBatch`es yielded one row-group at a time.
///
/// The underlying reader only loads the Parquet metadata footer eagerly;
/// column chunks are fetched on demand as the stream is polled, keeping
/// peak memory proportional to a single row group rather than the whole file.
pub type RecordBatchFileStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<RecordBatch>> + Send>>;

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
    let reader = ParquetObjectReader::new(store, path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    let builder = if !columns.is_empty() {
        let parquet_schema = builder.parquet_schema();
        let indices = resolve_column_indices(parquet_schema, columns);
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
        row_groups = builder.metadata().num_row_groups(),
        projected_cols = columns.len(),
        "Opened ObjectStore Parquet stream"
    );
    let stream = builder
        .build()
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::StorageCached;
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use kalamdb_commons::arrow_utils::{
        field_boolean, field_float64, field_int64, field_utf8, schema,
    };
    use kalamdb_commons::models::ids::StorageId;
    use kalamdb_commons::models::TableId;
    use kalamdb_commons::schemas::TableType;
    use kalamdb_system::providers::storages::models::StorageType;
    use kalamdb_system::Storage;
    use std::env;
    use std::fs;
    use std::sync::Arc;

    fn create_test_storage(temp_dir: &std::path::Path) -> Storage {
        let now = chrono::Utc::now().timestamp_millis();
        Storage {
            storage_id: StorageId::from("test_parquet_read"),
            storage_name: "test_parquet_read".to_string(),
            description: None,
            storage_type: StorageType::Filesystem,
            base_directory: temp_dir.to_string_lossy().to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: "{namespace}/{tableName}".to_string(),
            user_tables_template: "{namespace}/{tableName}/{userId}".to_string(),
            created_at: now,
            updated_at: now,
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
                None,
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
}
