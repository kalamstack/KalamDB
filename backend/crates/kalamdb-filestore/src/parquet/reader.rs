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
//! - **Row Group Selection**: Read only specific row groups by index
//!
//! # Usage Tiers
//!
//! | Function | Column Projection | Bloom Filter | Use Case |
//! |----------|:-:|:-:|----------|
//! | `parse_parquet_from_bytes` | ✗ | ✗ | Full file read |
//! | `parse_parquet_projected` | ✓ | ✗ | Read specific columns |
//! | `parse_parquet_with_bloom_filter` | ✓ | ✓ | PK lookups, point queries |

use crate::core::runtime::run_blocking;
use crate::error::{FilestoreError, Result};
use crate::registry::StorageCached;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use tracing::Instrument;

// ========== High-level StorageCached functions ==========

/// Read all RecordBatches from a Parquet file using StorageCached.
///
/// This reads the entire file into memory, so use carefully for large files.
pub async fn read_parquet_batches(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    parquet_filename: &str,
) -> Result<Vec<RecordBatch>> {
    let span = tracing::debug_span!(
        "parquet.read",
        table_id = %table_id,
        filename = parquet_filename
    );
    async move {
        let bytes = storage_cached.get(table_type, table_id, user_id, parquet_filename).await?.data;
        let batches = parse_parquet_from_bytes(bytes)?;
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        tracing::debug!(
            batch_count = batches.len(),
            row_count = row_count,
            "Parquet read completed"
        );
        Ok(batches)
    }
    .instrument(span)
    .await
}

/// Synchronous wrapper for read_parquet_batches.
pub fn read_parquet_batches_sync(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    parquet_filename: &str,
) -> Result<Vec<RecordBatch>> {
    let table_id = table_id.clone();
    let user_id = user_id.cloned();
    let parquet_filename = parquet_filename.to_string();

    run_blocking(|| async {
        read_parquet_batches(
            storage_cached,
            table_type,
            &table_id,
            user_id.as_ref(),
            &parquet_filename,
        )
        .await
    })
}

/// Read Parquet file schema without reading data.
pub async fn read_parquet_schema(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    parquet_filename: &str,
) -> Result<SchemaRef> {
    let bytes = storage_cached.get(table_type, table_id, user_id, parquet_filename).await?.data;
    parse_parquet_schema_from_bytes(bytes)
}

/// Synchronous wrapper for read_parquet_schema.
pub fn read_parquet_schema_sync(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    parquet_filename: &str,
) -> Result<SchemaRef> {
    let table_id = table_id.clone();
    let user_id = user_id.cloned();
    let parquet_filename = parquet_filename.to_string();

    run_blocking(|| async {
        read_parquet_schema(
            storage_cached,
            table_type,
            &table_id,
            user_id.as_ref(),
            &parquet_filename,
        )
        .await
    })
}

// ========== Bytes-based core functions ==========

/// Parse Parquet RecordBatches from in-memory bytes (full read, no filtering).
///
/// ```ignore
/// let result = storage_cached.get_sync(table_type, &table_id, user_id, filename)?;
/// let batches = parse_parquet_from_bytes(result.data)?;
/// ```
pub fn parse_parquet_from_bytes(bytes: bytes::Bytes) -> Result<Vec<RecordBatch>> {
    let span = tracing::trace_span!("parquet.decode_bytes", bytes = bytes.len());
    let _span_guard = span.entered();
    let batches = build_and_collect(
        ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| FilestoreError::Parquet(e.to_string()))?,
    )?;
    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
    tracing::trace!(batch_count = batches.len(), row_count = row_count, "Parquet decode completed");
    Ok(batches)
}

/// Parse Parquet schema from in-memory bytes without reading data.
pub fn parse_parquet_schema_from_bytes(bytes: bytes::Bytes) -> Result<SchemaRef> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;
    Ok(builder.schema().clone())
}

/// Read only the specified columns from a Parquet file in memory.
///
/// `columns` is a list of column names to read. If empty, reads all columns.
///
/// # Performance
/// Avoids decoding and allocating memory for columns that are not needed.
pub fn parse_parquet_projected(bytes: bytes::Bytes, columns: &[&str]) -> Result<Vec<RecordBatch>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    if columns.is_empty() {
        return build_and_collect(builder);
    }

    let parquet_schema = builder.parquet_schema();
    let indices = resolve_column_indices(parquet_schema, columns);
    if indices.is_empty() {
        return build_and_collect(builder);
    }

    let mask = ProjectionMask::leaves(parquet_schema, indices);
    build_and_collect(builder.with_projection(mask))
}

/// Read a Parquet file using bloom-filter-based row-group pruning and optional
/// column projection.
///
/// This is the **highest-performance reader** for point-query lookups (e.g.
/// `WHERE pk = 'some_value'`). It:
///
/// 1. Inspects each row group's bloom filter for `bloom_column`.
/// 2. Skips row groups where the bloom filter says the value is **definitely absent**.
/// 3. Reads only the requested `columns` from surviving row groups.
///
/// # Arguments
/// * `bytes`         – Parquet file bytes (from `StorageCached.get()`)
/// * `bloom_column`  – Column name whose bloom filter should be checked
/// * `bloom_value`   – The value to probe (must implement `AsBytes`)
/// * `columns`       – Columns to read (empty = all columns)
///
/// # Returns
/// * `Ok(None)` – bloom filter proved the value is absent in **all** row groups
/// * `Ok(Some(batches))` – batches from row groups that may contain the value
///
/// # Example
/// ```ignore
/// let data = storage.get(table_type, &table_id, user_id, "batch-0.parquet").await?.data;
/// if let Some(batches) = parse_parquet_with_bloom_filter(
///     data, "id", &42_i64, &["id", "_seq", "_deleted"]
/// )? {
///     // batches only from row groups that *may* contain id=42
/// }
/// ```
pub fn parse_parquet_with_bloom_filter<V: parquet::data_type::AsBytes>(
    bytes: bytes::Bytes,
    bloom_column: &str,
    bloom_value: &V,
    columns: &[&str],
) -> Result<Option<Vec<RecordBatch>>> {
    // Build the low-level reader to access bloom filters per row group
    let file_reader = SerializedFileReader::new(bytes.clone())
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    let metadata = file_reader.metadata();
    let num_row_groups = metadata.num_row_groups();

    if num_row_groups == 0 {
        return Ok(Some(Vec::new()));
    }

    // Find the column index for bloom filter probing
    let parquet_schema = metadata.file_metadata().schema_descr();
    let bloom_col_idx = parquet_schema.columns().iter().position(|c| c.name() == bloom_column);

    // Collect row groups that are NOT pruned by bloom filter
    let mut selected_row_groups: Vec<usize> = Vec::new();

    for rg_idx in 0..num_row_groups {
        if let Some(col_idx) = bloom_col_idx {
            let rg_reader = file_reader
                .get_row_group(rg_idx)
                .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

            if let Some(bloom) = rg_reader.get_column_bloom_filter(col_idx) {
                if !bloom.check(bloom_value) {
                    // Bloom filter says "definitely not present" → skip this row group
                    log::trace!(
                        "[parquet::reader] Bloom filter pruned row group {} for column '{}'",
                        rg_idx,
                        bloom_column
                    );
                    continue;
                }
            }
            // No bloom filter or bloom says "maybe present" → include
        }
        // No bloom column found → include (conservative)

        selected_row_groups.push(rg_idx);
    }

    // All row groups were pruned → value definitely not in file
    if selected_row_groups.is_empty() {
        log::trace!(
            "[parquet::reader] All {} row groups pruned by bloom filter for column '{}'",
            num_row_groups,
            bloom_column
        );
        return Ok(None);
    }

    // Build the Arrow reader with row-group selection + column projection
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    let builder = builder.with_row_groups(selected_row_groups);

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

    let batches = build_and_collect(builder)?;
    Ok(Some(batches))
}

/// Check if a value is **definitely absent** from a Parquet file using only bloom
/// filters, without reading any row data.
///
/// Returns `true` if the value is definitely absent from ALL row groups.
/// Returns `false` if the value might be present (bloom filter says maybe, or no
/// bloom filter exists).
///
/// This is an O(metadata) operation — no row data is decoded.
pub fn bloom_filter_check_absent<V: parquet::data_type::AsBytes>(
    bytes: bytes::Bytes,
    column: &str,
    value: &V,
) -> Result<bool> {
    let file_reader =
        SerializedFileReader::new(bytes).map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    let metadata = file_reader.metadata();
    let num_row_groups = metadata.num_row_groups();

    if num_row_groups == 0 {
        return Ok(true); // No data → value is absent
    }

    let parquet_schema = metadata.file_metadata().schema_descr();
    let col_idx = match parquet_schema.columns().iter().position(|c| c.name() == column) {
        Some(idx) => idx,
        None => return Ok(false), // Column not in file → can't prove absence
    };

    for rg_idx in 0..num_row_groups {
        let rg_reader = file_reader
            .get_row_group(rg_idx)
            .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

        match rg_reader.get_column_bloom_filter(col_idx) {
            Some(bloom) => {
                if bloom.check(value) {
                    return Ok(false); // Maybe present → not definitely absent
                }
            },
            None => {
                return Ok(false); // No bloom filter → can't prove absence
            },
        }
    }

    Ok(true) // All row groups say "definitely not present"
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

/// Build a ParquetRecordBatchReader and collect all batches.
fn build_and_collect(
    builder: ParquetRecordBatchReaderBuilder<bytes::Bytes>,
) -> Result<Vec<RecordBatch>> {
    let row_group_count = builder.metadata().num_row_groups();
    let reader = builder.build().map_err(|e| FilestoreError::Parquet(e.to_string()))?;

    let mut batches = Vec::with_capacity(row_group_count);
    for batch_result in reader {
        let batch = batch_result.map_err(|e| FilestoreError::Parquet(e.to_string()))?;
        batches.push(batch);
    }
    Ok(batches)
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

    #[test]
    fn test_read_parquet_batches_sync_simple() {
        let temp_dir = env::temp_dir().join("kalamdb_test_read_batches");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "data");

        // Write a test parquet file
        let batch = create_simple_batch(100);
        let schema = batch.schema();
        let file_path = "data.parquet";

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema,
                vec![batch.clone()],
                None,
            )
            .unwrap();

        // Read it back
        let read_batches = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 100);
        assert_eq!(read_batches[0].num_columns(), 3);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_parquet_schema_sync() {
        let temp_dir = env::temp_dir().join("kalamdb_test_read_schema");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "schema");

        // Write a test parquet file
        let batch = create_simple_batch(50);
        let original_schema = batch.schema();
        let file_path = "schema_test.parquet";

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                Arc::clone(&original_schema),
                vec![batch],
                None,
            )
            .unwrap();

        // Read schema back (without reading data)
        let read_schema = read_parquet_schema_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        assert_eq!(read_schema.fields().len(), 3);
        assert_eq!(read_schema.field(0).name(), "id");
        assert_eq!(read_schema.field(1).name(), "name");
        assert_eq!(read_schema.field(2).name(), "_seq");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_empty_parquet_file() {
        let temp_dir = env::temp_dir().join("kalamdb_test_empty_parquet");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "empty");

        // Create empty batch (0 rows)
        let schema = schema(vec![field_int64("id", false), field_utf8("value", true)]);

        let empty_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
            ],
        )
        .unwrap();

        let file_path = "empty.parquet";
        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema,
                vec![empty_batch],
                None,
            )
            .unwrap();

        // Read it back - empty files may return 0 batches or 1 batch with 0 rows
        let read_batches = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        // Either no batches or one empty batch is acceptable
        if !read_batches.is_empty() {
            assert_eq!(read_batches[0].num_rows(), 0);
        }
        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Should have 0 total rows");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_multiple_batches() {
        let temp_dir = env::temp_dir().join("kalamdb_test_multi_batches");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "multi");

        // Create multiple batches
        let batch1 = create_simple_batch(50);
        let batch2 = create_simple_batch(75);
        let batch3 = create_simple_batch(100);

        let schema = batch1.schema();
        let file_path = "multi_batch.parquet";

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema,
                vec![batch1, batch2, batch3],
                None,
            )
            .unwrap();

        // Read back
        let read_batches = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        // Should be combined into batches based on row group size
        assert!(!read_batches.is_empty());

        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50 + 75 + 100);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_parquet_with_different_types() {
        let temp_dir = env::temp_dir().join("kalamdb_test_types");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "types");

        // Schema with multiple data types
        let schema = schema(vec![
            field_int64("int_col", false),
            field_utf8("str_col", true),
            field_float64("float_col", true),
            field_boolean("bool_col", false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        let file_path = "types.parquet";
        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema,
                vec![batch.clone()],
                None,
            )
            .unwrap();

        // Read back
        let read_batches = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_columns(), 4);
        assert_eq!(read_batches[0].num_rows(), 3);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_nonexistent_parquet() {
        let temp_dir = env::temp_dir().join("kalamdb_test_nonexistent");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("nonexistent", "file");

        let result = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            "file.parquet",
        );

        assert!(result.is_err(), "Should fail for nonexistent file");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_parquet_with_nulls() {
        let temp_dir = env::temp_dir().join("kalamdb_test_nulls");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "nulls");

        let schema = schema(vec![field_int64("id", false), field_utf8("nullable_str", true)]);

        // Batch with null values
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c"), None])),
            ],
        )
        .unwrap();

        let file_path = "nulls.parquet";
        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema,
                vec![batch],
                None,
            )
            .unwrap();

        // Read back
        let read_batches = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 4);

        // Verify null handling
        let str_array = read_batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(str_array.is_null(1));
        assert!(str_array.is_null(3));
        assert!(!str_array.is_null(0));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_large_parquet_file() {
        let temp_dir = env::temp_dir().join("kalamdb_test_large");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("test", "large");

        // Create a large batch (10K rows)
        let large_batch = create_simple_batch(10_000);
        let schema = large_batch.schema();
        let file_path = "large.parquet";

        storage_cached
            .write_parquet_sync(
                TableType::Shared,
                &table_id,
                None,
                file_path,
                schema,
                vec![large_batch],
                None,
            )
            .unwrap();

        // Read back
        let read_batches = read_parquet_batches_sync(
            &storage_cached,
            TableType::Shared,
            &table_id,
            None,
            file_path,
        )
        .unwrap();

        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10_000);

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
