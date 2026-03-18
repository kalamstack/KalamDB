//! Manifest-driven access planner (Phase: pruning integration)
//!
//! Provides utilities to translate `Manifest` metadata into
//! concrete file/row-group selections for efficient reads.

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::future::try_join_all;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::UserId;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::TableId;
use kalamdb_filestore::StorageCached;
use kalamdb_system::Manifest;
use kalamdb_system::SchemaRegistry as SchemaRegistryTrait;
use std::sync::Arc;

/// Planned selection for a single Parquet file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowGroupSelection {
    /// Relative file path (e.g., "batch-0.parquet")
    pub file_path: String,
    /// Row-group indexes to read from that file
    pub row_groups: Vec<usize>,
}

impl RowGroupSelection {
    pub fn new(file_path: String, row_groups: Vec<usize>) -> Self {
        Self {
            file_path,
            row_groups,
        }
    }
}

/// Planner that produces pruning-aware selections from the manifest
#[derive(Debug, Default)]
pub struct ManifestAccessPlanner;

impl ManifestAccessPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Plan file selections (all files, no row-group pruning)
    ///
    /// Returns a list of all batch files to scan.
    pub fn plan_all_files(&self, manifest: &Manifest) -> Vec<String> {
        manifest.segments.iter().map(|s| s.path.clone()).collect()
    }

    /// Unified scan method: returns combined RecordBatch from Parquet files
    ///
    /// Handles manifest-based pruning, file loading, schema evolution, and batch concatenation.
    ///
    /// # Arguments
    /// * `manifest_opt` - Optional manifest for metadata-driven selection
    /// * `storage_cached` - StorageCached instance for file operations
    /// * `table_type` - Table type (User, Shared, Stream, System)
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID for user tables
    /// * `seq_range` - Optional (min, max) seq range for pruning
    /// * `use_degraded_mode` - If true, skip manifest and list directory
    /// * `schema` - Current Arrow schema (target schema for projection)
    /// * `schema_registry` - Schema registry for historical schemas
    ///
    /// # Returns
    /// (batch: RecordBatch, stats: (total_batches, skipped, scanned))
    /// Simple planner: select files overlapping a given `_seq` range
    ///
    /// This is a first step towards full predicate-based pruning.
    pub fn plan_by_seq_range(
        &self,
        manifest: &Manifest,
        min_seq: SeqId,
        max_seq: SeqId,
    ) -> Vec<RowGroupSelection> {
        if manifest.segments.is_empty() {
            return Vec::new();
        }

        let mut selections: Vec<RowGroupSelection> = Vec::new();

        for segment in &manifest.segments {
            // Skip segments that don't overlap at all
            if segment.max_seq < min_seq || segment.min_seq > max_seq {
                continue;
            }

            // We don't have row group stats anymore, so we select the whole file
            selections.push(RowGroupSelection::new(segment.path.clone(), Vec::new()));
        }

        selections
    }

    /// Scan Parquet files using async file I/O.
    ///
    /// Uses async file I/O to avoid blocking the tokio runtime.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_parquet_files_async(
        &self,
        manifest_opt: Option<&Manifest>,
        storage_cached: Arc<StorageCached>,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        seq_range: Option<(SeqId, SeqId)>,
        use_degraded_mode: bool,
        schema: SchemaRef,
        schema_registry: &dyn SchemaRegistryTrait<Error = KalamDbError>,
    ) -> Result<(RecordBatch, (usize, usize, usize)), KalamDbError> {
        let mut parquet_files: Vec<String> = Vec::new();
        let mut file_schema_versions: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();
        let (mut total_batches, mut skipped, mut scanned) = (0usize, 0usize, 0usize);

        if !use_degraded_mode {
            if let Some(manifest) = manifest_opt {
                total_batches = manifest.segments.len();

                let selected_files: Vec<String> = if let Some((min_seq, max_seq)) = seq_range {
                    let selections = self.plan_by_seq_range(manifest, min_seq, max_seq);
                    selections.into_iter().map(|s| s.file_path).collect()
                } else {
                    self.plan_all_files(manifest)
                };

                scanned = selected_files.len();
                skipped = total_batches.saturating_sub(scanned);

                for file_path in selected_files {
                    if let Some(segment) = manifest.segments.iter().find(|s| s.path == file_path) {
                        file_schema_versions.insert(file_path.clone(), segment.schema_version);
                    }
                    parquet_files.push(file_path);
                }
            }
        }

        // Fallback: only when no manifest (or degraded mode)
        if parquet_files.is_empty() && (manifest_opt.is_none() || use_degraded_mode) {
            let files = storage_cached
                .list_parquet_files(table_type, table_id, user_id)
                .await
                .into_kalamdb_error("Failed to list files")?;

            parquet_files.extend(files);

            total_batches = parquet_files.len();
            scanned = total_batches;
            skipped = 0;
        }

        // Return empty batch if no files found
        if parquet_files.is_empty() {
            return Ok((RecordBatch::new_empty(schema), (total_batches, skipped, scanned)));
        }

        let mut all_batches = Vec::new();

        // Read all parquet files concurrently instead of sequentially.
        // Each file is independent so we can issue all reads in parallel.
        let read_futures: Vec<_> = parquet_files
            .iter()
            .map(|parquet_file| {
                let sc = storage_cached.clone();
                let file = parquet_file.clone();
                async move {
                    sc.read_parquet_files(table_type, table_id, user_id, &[file])
                        .await
                        .into_kalamdb_error("Failed to read Parquet file")
                }
            })
            .collect();

        let results = try_join_all(read_futures).await?;

        // Look up current schema version once for all files
        let current_version = schema_registry
            .get_table_if_exists(table_id)?
            .map(|table_def| table_def.schema_version)
            .unwrap_or(1);

        for (parquet_file, batches) in parquet_files.iter().zip(results) {
            let file_schema_version = file_schema_versions.get(parquet_file).copied().unwrap_or(1);

            for batch in batches {
                let projected_batch = if file_schema_version != current_version {
                    self.project_batch_to_current_schema(
                        batch,
                        file_schema_version,
                        &schema,
                        table_id,
                        schema_registry,
                    )?
                } else {
                    batch
                };

                all_batches.push(projected_batch);
            }
        }

        // Return empty batch if all files were empty
        if all_batches.is_empty() {
            return Ok((RecordBatch::new_empty(schema), (total_batches, skipped, scanned)));
        }

        // Concatenate all batches
        let combined = datafusion::arrow::compute::concat_batches(&schema, &all_batches)
            .into_arrow_error_ctx("Failed to concatenate Parquet batches")?;

        Ok((combined, (total_batches, skipped, scanned)))
    }

    /// Project a RecordBatch from an old schema version to the current schema
    ///
    /// Handles:
    /// - New columns added after flush (filled with NULLs)
    /// - Dropped columns (removed from projection)
    /// - Column reordering
    ///
    /// # Arguments
    /// * `batch` - RecordBatch with old schema
    /// * `old_schema_version` - Schema version used when data was flushed
    /// * `current_schema` - Target Arrow schema (current version)
    /// * `table_id` - Table identifier
    /// * `schema_registry` - Schema registry for accessing historical schemas
    fn project_batch_to_current_schema(
        &self,
        batch: RecordBatch,
        old_schema_version: u32,
        current_schema: &SchemaRef,
        table_id: &TableId,
        _schema_registry: &dyn SchemaRegistryTrait<Error = KalamDbError>,
    ) -> Result<RecordBatch, KalamDbError> {
        let batch_schema = batch.schema();

        // If schemas are identical, no projection needed
        if batch_schema.fields() == current_schema.fields() {
            return Ok(batch);
        }

        log::debug!(
            "[Schema Evolution] Projecting batch from schema v{} to current schema for table {}",
            old_schema_version,
            table_id
        );

        // Build projection: for each field in current_schema, find it in old_schema or create NULL array
        let mut projected_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = Vec::new();

        for current_field in current_schema.fields() {
            // Check if field exists in old schema
            if let Ok(old_col_index) = batch_schema.index_of(current_field.name()) {
                // Column existed in old schema - extract it
                let old_column = batch.column(old_col_index).clone();

                // Check if data types match
                let old_field = batch_schema.field(old_col_index);
                if old_field.data_type() == current_field.data_type() {
                    // Types match - use as-is
                    projected_columns.push(old_column);
                } else {
                    // Type changed - attempt cast
                    let casted = cast(&old_column, current_field.data_type())
                        .into_arrow_error_ctx(&format!(
                            "Failed to cast column '{}' from {:?} to {:?}",
                            current_field.name(),
                            old_field.data_type(),
                            current_field.data_type()
                        ))?;
                    projected_columns.push(casted);
                }
            } else {
                // Column didn't exist in old schema - create NULL array
                use datafusion::arrow::array::{new_null_array, ArrayRef};
                let null_array: ArrayRef =
                    new_null_array(current_field.data_type(), batch.num_rows());
                projected_columns.push(null_array);

                log::trace!(
                    "[Schema Evolution] Column '{}' not in old schema v{}, filled with NULLs",
                    current_field.name(),
                    old_schema_version
                );
            }
        }

        // Create new RecordBatch with projected columns
        let projected_batch = RecordBatch::try_new(current_schema.clone(), projected_columns)
            .into_arrow_error_ctx("Failed to create projected RecordBatch")?;

        Ok(projected_batch)
    }

    /// Prune segments that definitely cannot contain a PK value based on column_stats min/max
    ///
    /// Returns segments where the PK value could exist (i.e., value is within [min, max] range).
    /// If a segment has no column_stats for the PK column, it's included (conservative).
    ///
    /// # Arguments
    /// * `manifest` - The manifest containing segment metadata
    /// * `pk_column_id` - Column ID of the primary key column
    /// * `pk_value` - The PK value to search for (as string for comparison)
    ///
    /// # Returns
    /// List of segment file paths that could contain the PK value
    pub fn plan_by_pk_value(
        &self,
        manifest: &Manifest,
        pk_column_id: u64,
        pk_value: &str,
    ) -> Vec<String> {
        if manifest.segments.is_empty() {
            return Vec::new();
        }

        let mut selected_paths: Vec<String> = Vec::new();

        for segment in &manifest.segments {
            // Skip non-readable segments (in_progress or tombstoned)
            if !segment.is_readable() {
                continue;
            }

            // Check if segment has column_stats for the PK column
            if let Some(stats) = segment.column_stats.get(&pk_column_id) {
                // Check if PK value could be in this segment's range
                if !Self::pk_value_in_range(pk_value, stats) {
                    // Definitely not in this segment, skip
                    continue;
                }
            }
            // No column_stats for PK column = conservative, include the segment

            selected_paths.push(segment.path.clone());
        }

        selected_paths
    }

    /// Check if a PK value could be within the min/max range of column stats
    ///
    /// Supports string and numeric comparisons.
    fn pk_value_in_range(pk_value: &str, stats: &kalamdb_system::ColumnStats) -> bool {
        // If no min/max stats, conservatively assume it could be in range
        if stats.min.is_none() || stats.max.is_none() {
            return true;
        }

        // Try numeric comparison first (most common for PKs)
        if let Ok(pk_num) = pk_value.parse::<i64>() {
            // ColumnStats.min/max are JSON-encoded strings, so parse them
            if let (Some(min_n), Some(max_n)) = (stats.min_as_i64(), stats.max_as_i64()) {
                return pk_num >= min_n && pk_num <= max_n;
            }
        }

        // Fall back to string comparison
        if let (Some(min_s), Some(max_s)) = (stats.min_as_str(), stats.max_as_str()) {
            return pk_value >= min_s.as_str() && pk_value <= max_s.as_str();
        }

        // Can't compare, conservatively include
        true
    }
}
