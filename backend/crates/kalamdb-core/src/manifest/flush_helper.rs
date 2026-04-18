//! Flush Helper for Manifest Operations
//!
//! Centralizes manifest-related logic used during flush operations to eliminate
//! code duplication between user and shared table flush implementations.

use super::ManifestService;
use crate::error::KalamDbError;
use datafusion::arrow::array::*;
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use kalamdb_commons::arrow_utils::compute_min_max;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::{TableId, UserId};
use kalamdb_system::{ColumnStats, Manifest, SegmentMetadata};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Helper for manifest operations during flush
pub struct FlushManifestHelper {
    manifest_service: Arc<ManifestService>,
}

impl FlushManifestHelper {
    /// Create a new FlushManifestHelper
    pub fn new(manifest_service: Arc<ManifestService>) -> Self {
        Self { manifest_service }
    }

    /// Generate temp filename for atomic writes
    ///
    /// Returns the temp filename (e.g., "batch-5.parquet.tmp")
    /// Delegates to kalamdb_filestore::PathResolver for consistency.
    #[inline]
    pub fn generate_temp_filename(batch_number: u64) -> String {
        kalamdb_filestore::StorageCached::temp_batch_filename(batch_number)
    }

    /// Generate batch filename from batch number
    /// Delegates to kalamdb_filestore::PathResolver for consistency.
    #[inline]
    pub fn generate_batch_filename(batch_number: u64) -> String {
        kalamdb_filestore::StorageCached::batch_filename(batch_number)
    }

    /// Mark manifest cache entry as syncing (flush in progress)
    ///
    /// This is called BEFORE writing Parquet to temp location.
    /// If the server crashes during the write, on restart:
    /// - Manifest with Syncing state indicates incomplete flush
    /// - Temp files (.parquet.tmp) can be cleaned up
    /// - Data remains safely in RocksDB
    pub fn mark_syncing(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), KalamDbError> {
        self.manifest_service.mark_syncing(table_id, user_id).map_err(|e| {
            KalamDbError::Other(format!(
                "Failed to mark manifest as syncing for {}: {}",
                table_id, e
            ))
        })
    }

    /// Get next batch number for a table/scope by reading manifest
    ///
    /// Returns 0 if no manifest exists (first batch)
    pub fn get_next_batch_number(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<u64, KalamDbError> {
        if let Ok(Some(entry)) = self.manifest_service.get_or_load(table_id, user_id) {
            return Ok(if entry.manifest.segments.is_empty() {
                0
            } else {
                entry.manifest.last_sequence_number + 1
            });
        }

        match self.manifest_service.read_manifest(table_id, user_id) {
            Ok(manifest) => Ok(if manifest.segments.is_empty() {
                0
            } else {
                manifest.last_sequence_number + 1
            }),
            Err(_) => Ok(0), // No manifest exists yet, start with batch 0
        }
    }

    /// Extract min/max _seq values from RecordBatch
    ///
    /// Returns (min_seq, max_seq) tuple
    pub fn extract_seq_range(batch: &RecordBatch) -> (SeqId, SeqId) {
        let seq_column = batch
            .column_by_name(SystemColumnNames::SEQ)
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>());

        if let Some(seq_arr) = seq_column {
            let min = compute::min(seq_arr).unwrap_or(0);
            let max = compute::max(seq_arr).unwrap_or(0);
            (SeqId::from(min), SeqId::from(max))
        } else {
            (SeqId::from(0i64), SeqId::from(0i64))
        }
    }

    /// Extract column statistics (min/max/nulls) from RecordBatch
    /// Returns HashMap keyed by column_id for stable column identification
    pub fn extract_column_stats(
        batch: &RecordBatch,
        indexed_columns: &[(u64, String)],
    ) -> HashMap<u64, ColumnStats> {
        let mut stats = HashMap::new();

        for (column_id, column_name) in indexed_columns {
            // Skip _seq system column (handled separately)
            if column_name == SystemColumnNames::SEQ {
                continue;
            }

            if let Some(col) = batch.column_by_name(column_name) {
                let null_count = col.null_count() as i64;
                let (min, max) = compute_min_max(col);

                stats.insert(
                    *column_id,
                    ColumnStats {
                        min,
                        max,
                        null_count: Some(null_count),
                    },
                );
            }
        }
        stats
    }

    /// Update manifest and cache after successful flush
    ///
    /// This is the canonical flow for updating manifest during flush:
    /// 1. Create SegmentMetadata with metadata
    /// 2. Update manifest file on disk (via service)
    /// 3. Update cache with new manifest
    ///
    /// # Arguments
    /// * `table_id` - TableId (namespace + table name)
    /// * `user_id` - User ID for user tables, None for shared tables
    /// * `batch_number` - Batch number
    /// * `batch_filename` - Batch filename
    /// * `file_path` - Full path to the written Parquet file
    /// * `batch` - RecordBatch (for extracting stats)
    /// * `file_size_bytes` - Size of written Parquet file
    /// * `indexed_columns` - Columns with Bloom filters/page stats enabled
    /// * `schema_version` - Schema version (Phase 16) to link Parquet file to specific schema
    ///
    /// # Returns
    /// Updated Manifest
    #[allow(clippy::too_many_arguments)]
    pub fn update_manifest_after_flush(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        batch_filename: String,
        file_path: &Path,
        batch: &RecordBatch,
        file_size_bytes: u64,
        indexed_columns: &[(u64, String)],
        schema_version: u32,
    ) -> Result<Manifest, KalamDbError> {
        // Extract metadata from batch (batch-level)
        let (min_seq, max_seq) = Self::extract_seq_range(batch);
        let column_stats = Self::extract_column_stats(batch, indexed_columns);

        let row_count = batch.num_rows() as u64;

        // Create segment metadata
        // Use filename as ID for now, or generate UUID
        let segment_id = batch_filename.clone();

        // Store just the filename in manifest - path resolution happens at read time
        // The storage_path_template + user_id substitution is used during reads,
        // so we only need to store the batch filename here
        let relative_path = batch_filename.clone();

        // Phase 16: Use with_schema_version to record schema version in manifest
        // IMPORTANT: Use relative_path instead of batch_filename for the path field
        let segment = SegmentMetadata::with_schema_version(
            segment_id,
            relative_path,
            column_stats,
            min_seq,
            max_seq,
            row_count,
            file_size_bytes,
            schema_version,
        );

        // Update manifest (Hot Store update)
        let updated_manifest =
            self.manifest_service.update_manifest(table_id, user_id, segment).map_err(|e| {
                KalamDbError::Other(format!(
                    "Failed to update manifest for {} (user_id={:?}): {}",
                    table_id,
                    user_id.map(|u| u.as_str()),
                    e
                ))
            })?;

        // Flush manifest to disk (Cold Store persistence)
        self.manifest_service.flush_manifest(table_id, user_id).map_err(|e| {
            KalamDbError::Other(format!(
                "Failed to flush manifest for {} (user_id={:?}): {}",
                table_id,
                user_id.map(|u| u.as_str()),
                e
            ))
        })?;

        // ManifestService now handles all caching internally
        self.manifest_service
            .update_after_flush(table_id, user_id, &updated_manifest, None)
            .map_err(|e| {
                KalamDbError::Other(format!(
                    "Failed to update manifest cache for {} (user_id={:?}): {}",
                    table_id,
                    user_id.map(|u| u.as_str()),
                    e
                ))
            })?;

        log::debug!(
            "[MANIFEST] ✅ Updated manifest and cache: {} (user_id={:?}, file={}, rows={}, size={} bytes)",
            table_id,
            user_id.map(|u| u.as_str()),
            file_path.display(),
            row_count,
            file_size_bytes
        );

        Ok(updated_manifest)
    }

    /// Update manifest with pre-extracted stats (avoids needing the RecordBatch).
    ///
    /// Use this when stats have been extracted before the batch was consumed
    /// (e.g., moved into write_parquet_sync to avoid cloning).
    #[allow(clippy::too_many_arguments)]
    pub fn update_manifest_after_flush_with_stats(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        batch_filename: String,
        min_seq: SeqId,
        max_seq: SeqId,
        column_stats: HashMap<u64, ColumnStats>,
        row_count: u64,
        file_size_bytes: u64,
        schema_version: u32,
    ) -> Result<Manifest, KalamDbError> {
        let segment_id = batch_filename.clone();
        let relative_path = batch_filename;

        let segment = SegmentMetadata::with_schema_version(
            segment_id,
            relative_path,
            column_stats,
            min_seq,
            max_seq,
            row_count,
            file_size_bytes,
            schema_version,
        );

        let updated_manifest =
            self.manifest_service.update_manifest(table_id, user_id, segment).map_err(|e| {
                KalamDbError::Other(format!(
                    "Failed to update manifest for {} (user_id={:?}): {}",
                    table_id,
                    user_id.map(|u| u.as_str()),
                    e
                ))
            })?;

        self.manifest_service.flush_manifest(table_id, user_id).map_err(|e| {
            KalamDbError::Other(format!(
                "Failed to flush manifest for {} (user_id={:?}): {}",
                table_id,
                user_id.map(|u| u.as_str()),
                e
            ))
        })?;

        self.manifest_service
            .update_after_flush(table_id, user_id, &updated_manifest, None)
            .map_err(|e| {
                KalamDbError::Other(format!(
                    "Failed to update manifest cache for {} (user_id={:?}): {}",
                    table_id,
                    user_id.map(|u| u.as_str()),
                    e
                ))
            })?;

        log::debug!(
            "[MANIFEST] ✅ Updated manifest and cache: {} (user_id={:?}, rows={}, size={} bytes)",
            table_id,
            user_id.map(|u| u.as_str()),
            row_count,
            file_size_bytes
        );

        Ok(updated_manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    #[test]
    fn test_generate_batch_filename() {
        assert_eq!(FlushManifestHelper::generate_batch_filename(0), "batch-0.parquet");
        assert_eq!(FlushManifestHelper::generate_batch_filename(42), "batch-42.parquet");
        assert_eq!(FlushManifestHelper::generate_batch_filename(999), "batch-999.parquet");
    }

    #[test]
    fn test_extract_seq_range() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new("data", DataType::Utf8, true),
        ]));

        let seq_array = Int64Array::from(vec![100, 200, 150, 175]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d"]);

        let batch =
            RecordBatch::try_new(schema, vec![StdArc::new(seq_array), StdArc::new(data_array)])
                .unwrap();

        let (min, max) = FlushManifestHelper::extract_seq_range(&batch);
        assert_eq!(min, SeqId::from(100i64));
        assert_eq!(max, SeqId::from(200i64));
    }

    #[test]
    fn test_extract_seq_range_empty() {
        let schema = StdArc::new(Schema::new(vec![Field::new(
            SystemColumnNames::SEQ,
            DataType::Int64,
            false,
        )]));
        let seq_array = Int64Array::from(vec![] as Vec<i64>);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(seq_array)]).unwrap();

        let (min_seq, max_seq) = FlushManifestHelper::extract_seq_range(&batch);
        assert_eq!(min_seq, SeqId::from(0i64));
        assert_eq!(max_seq, SeqId::from(0i64));
    }

    #[test]
    fn test_extract_seq_range_missing_column() {
        let schema =
            StdArc::new(Schema::new(vec![Field::new("other_column", DataType::Int64, false)]));
        let col_array = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(col_array)]).unwrap();

        let (min_seq, max_seq) = FlushManifestHelper::extract_seq_range(&batch);
        assert_eq!(min_seq, SeqId::from(0i64));
        assert_eq!(max_seq, SeqId::from(0i64));
    }

    #[test]
    fn test_extract_column_stats_int_types() {
        use datafusion::arrow::array::{Int32Array, Int64Array};
        use kalamdb_commons::models::rows::StoredScalarValue;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("timestamp", DataType::Int64, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
        ]));
        let id_array = Int32Array::from(vec![Some(5), Some(10), None, Some(1), Some(8)]);
        let ts_array = Int64Array::from(vec![1000000, 2000000, 1500000, 1800000, 1200000]);
        let seq_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(id_array),
                StdArc::new(ts_array),
                StdArc::new(seq_array),
            ],
        )
        .unwrap();

        let indexed_columns = vec![(1u64, "id".to_string()), (2u64, "timestamp".to_string())];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        assert_eq!(stats.len(), 2);
        let id_stats = stats.get(&1u64).unwrap();
        assert_eq!(id_stats.min, Some(StoredScalarValue::Int32(Some(1))));
        assert_eq!(id_stats.max, Some(StoredScalarValue::Int32(Some(10))));
        assert_eq!(id_stats.null_count, Some(1));

        let ts_stats = stats.get(&2u64).unwrap();
        assert_eq!(ts_stats.min, Some(StoredScalarValue::Int64(Some("1000000".to_string()))));
        assert_eq!(ts_stats.max, Some(StoredScalarValue::Int64(Some("2000000".to_string()))));
        assert_eq!(ts_stats.null_count, Some(0));
    }

    #[test]
    fn test_extract_column_stats_string() {
        use kalamdb_commons::models::rows::StoredScalarValue;

        let schema = StdArc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let array = StringArray::from(vec![Some("alice"), Some("bob"), None, Some("charlie")]);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(array)]).unwrap();

        let indexed_columns = vec![(1u64, "name".to_string())];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        let name_stats = stats.get(&1u64).unwrap();
        assert_eq!(name_stats.min, Some(StoredScalarValue::Utf8(Some("alice".to_string()))));
        assert_eq!(name_stats.max, Some(StoredScalarValue::Utf8(Some("charlie".to_string()))));
        assert_eq!(name_stats.null_count, Some(1));
    }

    #[test]
    fn test_extract_column_stats_skips_seq_column() {
        use datafusion::arrow::array::Int32Array;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
        ]));
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let seq_array = Int64Array::from(vec![10, 20, 30]);
        let batch =
            RecordBatch::try_new(schema, vec![StdArc::new(id_array), StdArc::new(seq_array)])
                .unwrap();

        let indexed_columns = vec![
            (1u64, "id".to_string()),
            (2u64, SystemColumnNames::SEQ.to_string()),
        ];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        // Should only have stats for "id", not "_seq"
        assert_eq!(stats.len(), 1);
        assert!(stats.contains_key(&1u64));
        assert!(!stats.contains_key(&2u64));
    }

    #[test]
    fn test_extract_column_stats_only_indexed_columns() {
        use datafusion::arrow::array::Int32Array;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("alice"), Some("bob"), Some("charlie")]);
        let age_array = Int32Array::from(vec![Some(30), Some(25), Some(35)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(id_array),
                StdArc::new(name_array),
                StdArc::new(age_array),
            ],
        )
        .unwrap();

        // Only index "id" and "age", not "name"
        let indexed_columns = vec![(1u64, "id".to_string()), (3u64, "age".to_string())];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        assert_eq!(stats.len(), 2);
        assert!(stats.contains_key(&1u64));
        assert!(stats.contains_key(&3u64));
        assert!(!stats.contains_key(&2u64));
    }

    #[test]
    fn test_extract_column_stats_empty_batch() {
        use datafusion::arrow::array::Int32Array;

        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_array = Int32Array::from(vec![] as Vec<i32>);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(id_array)]).unwrap();

        let indexed_columns = vec![(1u64, "id".to_string())];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        // Empty batch should still produce stats entry but with None min/max
        assert_eq!(stats.len(), 1);
        let id_stats = stats.get(&1u64).unwrap();
        assert_eq!(id_stats.min, None);
        assert_eq!(id_stats.max, None);
        assert_eq!(id_stats.null_count, Some(0));
    }

    #[test]
    fn test_extract_column_stats_all_nulls() {
        use datafusion::arrow::array::Int32Array;

        let schema = StdArc::new(Schema::new(vec![Field::new("value", DataType::Int32, true)]));
        let array = Int32Array::from(vec![None, None, None]);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(array)]).unwrap();

        let indexed_columns = vec![(1u64, "value".to_string())];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        let value_stats = stats.get(&1u64).unwrap();
        assert_eq!(value_stats.min, None);
        assert_eq!(value_stats.max, None);
        assert_eq!(value_stats.null_count, Some(3));
    }

    #[test]
    fn test_extract_column_stats_float_types() {
        use datafusion::arrow::array::{Float32Array, Float64Array};

        let schema = StdArc::new(Schema::new(vec![
            Field::new("f32_col", DataType::Float32, true),
            Field::new("f64_col", DataType::Float64, true),
        ]));
        let f32_array = Float32Array::from(vec![Some(1.5), Some(2.5), Some(0.5)]);
        let f64_array = Float64Array::from(vec![Some(10.5), Some(20.5), Some(5.5)]);
        let batch =
            RecordBatch::try_new(schema, vec![StdArc::new(f32_array), StdArc::new(f64_array)])
                .unwrap();

        let indexed_columns = vec![(1u64, "f32_col".to_string()), (2u64, "f64_col".to_string())];
        let stats = FlushManifestHelper::extract_column_stats(&batch, &indexed_columns);

        assert_eq!(stats.len(), 2);
        assert!(stats.get(&1u64).unwrap().min.is_some());
        assert!(stats.get(&1u64).unwrap().max.is_some());
        assert!(stats.get(&2u64).unwrap().min.is_some());
        assert!(stats.get(&2u64).unwrap().max.is_some());
    }

    #[test]
    fn test_generate_temp_filename() {
        // Test temp filename generation for atomic flush pattern
        assert_eq!(FlushManifestHelper::generate_temp_filename(0), "batch-0.parquet.tmp");
        assert_eq!(FlushManifestHelper::generate_temp_filename(1), "batch-1.parquet.tmp");
        assert_eq!(FlushManifestHelper::generate_temp_filename(42), "batch-42.parquet.tmp");
        assert_eq!(FlushManifestHelper::generate_temp_filename(999), "batch-999.parquet.tmp");
    }

    #[test]
    fn test_temp_and_final_filename_consistency() {
        // Ensure temp and final filenames are consistent (same batch number)
        for batch_num in [0, 1, 5, 10, 100, 999] {
            let temp = FlushManifestHelper::generate_temp_filename(batch_num);
            let final_name = FlushManifestHelper::generate_batch_filename(batch_num);

            // Temp should be final + ".tmp"
            assert!(temp.ends_with(".tmp"), "Temp filename should end with .tmp");
            assert_eq!(
                temp,
                format!("{}.tmp", final_name),
                "Temp filename should be final filename + .tmp"
            );

            // Both should contain the same batch number
            let temp_num: u64 = temp
                .strip_prefix("batch-")
                .and_then(|s| s.strip_suffix(".parquet.tmp"))
                .and_then(|s| s.parse().ok())
                .expect("Should parse batch number from temp");
            let final_num: u64 = final_name
                .strip_prefix("batch-")
                .and_then(|s| s.strip_suffix(".parquet"))
                .and_then(|s| s.parse().ok())
                .expect("Should parse batch number from final");

            assert_eq!(temp_num, final_num);
            assert_eq!(temp_num, batch_num);
        }
    }

    #[test]
    fn test_atomic_flush_filename_pattern() {
        // Test the complete filename pattern for atomic flush
        let batch_number = 5u64;

        let temp_filename = FlushManifestHelper::generate_temp_filename(batch_number);
        let final_filename = FlushManifestHelper::generate_batch_filename(batch_number);

        // Verify temp filename pattern
        assert_eq!(temp_filename, "batch-5.parquet.tmp");

        // Verify final filename pattern
        assert_eq!(final_filename, "batch-5.parquet");

        // Verify the rename path: temp -> final
        let temp_path = format!("myns/mytable/{}", temp_filename);
        let final_path = format!("myns/mytable/{}", final_filename);

        assert_eq!(temp_path, "myns/mytable/batch-5.parquet.tmp");
        assert_eq!(final_path, "myns/mytable/batch-5.parquet");
    }
}
