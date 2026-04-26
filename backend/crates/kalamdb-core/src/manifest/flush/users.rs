//! User table flush implementation
//!
//! Flushes user table data from RocksDB to Parquet files, grouping by UserId.
//! Each user's data is written to a separate Parquet file for RLS isolation.

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use kalamdb_commons::{
    constants::SystemColumnNames,
    ids::UserTableRowId,
    models::{rows::Row, TableId, UserId},
    schemas::TableType,
    StorageKey,
};
use kalamdb_store::entity_store::EntityStore;
use kalamdb_tables::UserTableIndexedStore;

use super::base::{config, helpers, FlushDedupStats, FlushJobResult, FlushMetadata, TableFlush};
use crate::{
    app_context::AppContext,
    error::KalamDbError,
    error_extensions::KalamDbResultExt,
    manifest::{FlushManifestHelper, ManifestService},
    schema_registry::SchemaRegistry,
    vector::flush_user_scope_vectors,
};

/// User table flush job
///
/// Flushes user table data to Parquet files. Each user's data is written to a
/// separate Parquet file for RLS isolation. Uses Bloom filters on PRIMARY KEY + _seq
/// columns for efficient query pruning.
pub struct UserTableFlushJob {
    store: Arc<UserTableIndexedStore>,
    table_id: Arc<TableId>,
    schema: SchemaRef,
    unified_cache: Arc<SchemaRegistry>,
    app_context: Arc<AppContext>,
    manifest_helper: FlushManifestHelper,
    /// Bloom filter columns (PRIMARY KEY + _seq) - fetched once per job for efficiency
    bloom_filter_columns: Vec<String>,
    /// Indexed columns with column_id for stats extraction (column_id, column_name)
    indexed_columns: Vec<(u64, String)>,
}

impl UserTableFlushJob {
    /// Create a new user table flush job
    pub fn new(
        app_context: Arc<AppContext>,
        table_id: Arc<TableId>,
        store: Arc<UserTableIndexedStore>,
        schema: SchemaRef,
        unified_cache: Arc<SchemaRegistry>,
        manifest_service: Arc<ManifestService>,
    ) -> Self {
        let manifest_helper = FlushManifestHelper::new(manifest_service);

        // Get cached values from CachedTableData (computed once at cache entry creation)
        // This avoids any recomputation - values are already cached in the schema registry
        let (bloom_filter_columns, indexed_columns) = unified_cache
            .get(&table_id)
            .map(|cached| {
                (cached.bloom_filter_columns().to_vec(), cached.indexed_columns().to_vec())
            })
            .unwrap_or_else(|| {
                log::warn!(
                    "⚠️  Table {} not in cache. Using default Bloom filter columns (_seq only)",
                    table_id
                );
                (vec![SystemColumnNames::SEQ.to_string()], vec![])
            });

        log::debug!("🌸 Bloom filters enabled for columns: {:?}", bloom_filter_columns);

        Self {
            store,
            table_id,
            schema,
            unified_cache,
            app_context,
            manifest_helper,
            bloom_filter_columns,
            indexed_columns,
        }
    }

    /// Get current schema version for the table
    fn get_schema_version(&self) -> u32 {
        super::base::helpers::get_schema_version(&self.unified_cache, &self.table_id)
    }

    /// Convert JSON rows to Arrow RecordBatch
    fn rows_to_record_batch(&self, rows: &[(Vec<u8>, Row)]) -> Result<RecordBatch, KalamDbError> {
        super::base::helpers::rows_to_arrow_batch(&self.schema, rows)
    }

    /// Flush accumulated rows for a single user to Parquet
    fn flush_user_data(
        &self,
        user_id: &UserId,
        rows: &[(Vec<u8>, Row)],
        parquet_files: &mut Vec<String>,
        bloom_filter_columns: &[String],
        indexed_columns: &[(u64, String)],
    ) -> Result<usize, KalamDbError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let rows_count = rows.len();
        log::debug!(
            "💾 Flushing {} rows for user {} (table={})",
            rows_count,
            user_id.as_str(),
            self.table_id
        );

        // Convert rows to RecordBatch
        let batch = self.rows_to_record_batch(rows)?;

        // Resolve storage path for this user
        let user_id_typed = user_id;
        let cached = self.unified_cache.get(&self.table_id).ok_or_else(|| {
            KalamDbError::TableNotFound(format!("Table not found: {}", self.table_id))
        })?;
        let storage_cached = cached
            .storage_cached(&self.app_context.storage_registry())
            .into_kalamdb_error("Failed to get storage cache")?;
        let storage_path = storage_cached
            .get_relative_path(TableType::User, &self.table_id, Some(&user_id_typed))
            .full_path;

        // RLS ASSERTION: Verify storage path contains user_id
        if !storage_path.contains(user_id.as_str()) {
            log::error!(
                "🚨 RLS VIOLATION: Flush storage path does NOT contain user_id! user={}, path={}",
                user_id.as_str(),
                storage_path
            );
            return Err(KalamDbError::Other(format!(
                "RLS violation: flush path missing user_id isolation for user {}",
                user_id.as_str()
            )));
        }

        // Generate batch filename using manifest
        let batch_number = self
            .manifest_helper
            .get_next_batch_number(&self.table_id, Some(&user_id_typed))?;
        let batch_filename = FlushManifestHelper::generate_batch_filename(batch_number);
        let temp_filename = FlushManifestHelper::generate_temp_filename(batch_number);

        let temp_path = storage_cached
            .get_file_path(TableType::User, &self.table_id, Some(&user_id_typed), &temp_filename)
            .full_path;
        let destination_path = storage_cached
            .get_file_path(TableType::User, &self.table_id, Some(&user_id_typed), &batch_filename)
            .full_path;

        // ===== ATOMIC FLUSH PATTERN =====
        // Step 1: Mark manifest as syncing (flush in progress)
        if let Err(e) = self.manifest_helper.mark_syncing(&self.table_id, Some(&user_id_typed)) {
            log::warn!(
                "⚠️  Failed to mark manifest as syncing for user {}: {} (continuing)",
                user_id.as_str(),
                e
            );
        }

        // Extract manifest stats from the batch BEFORE writing Parquet,
        // so we can move the batch into write_parquet_sync without cloning.
        let (min_seq, max_seq) = FlushManifestHelper::extract_seq_range(&batch);
        let column_stats = FlushManifestHelper::extract_column_stats(&batch, indexed_columns);
        let row_count = batch.num_rows() as u64;

        // Step 2: Write Parquet to TEMP location first (consumes batch — no clone)
        log::debug!("📝 [ATOMIC] Writing Parquet to temp path: {}, rows={}", temp_path, rows_count);
        let result = storage_cached
            .write_parquet_sync(
                TableType::User,
                &self.table_id,
                Some(&user_id_typed),
                &temp_filename,
                self.schema.clone(),
                vec![batch],
                Some(bloom_filter_columns.to_vec()),
            )
            .into_kalamdb_error("Filestore error")?;

        // Step 3: Rename temp file to final location (atomic operation)
        log::debug!("📝 [ATOMIC] Renaming {} -> {}", temp_path, destination_path);
        storage_cached
            .rename_sync(
                TableType::User,
                &self.table_id,
                Some(&user_id_typed),
                &temp_filename,
                &batch_filename,
            )
            .into_kalamdb_error("Failed to rename Parquet file to final location")?;

        let size_bytes = result.size_bytes;

        // Update manifest using pre-extracted stats (batch was consumed above)
        let schema_version = self.get_schema_version();
        self.manifest_helper.update_manifest_after_flush_with_stats(
            &self.table_id,
            Some(&user_id_typed),
            batch_filename.clone(),
            min_seq,
            max_seq,
            column_stats,
            row_count,
            size_bytes,
            schema_version,
        )?;

        // Flush vector hot-staging artifacts for embedding columns in this user scope.
        flush_user_scope_vectors(
            &self.app_context,
            &self.table_id,
            &user_id_typed,
            &self.schema,
            &storage_cached,
        )?;

        log::debug!(
            "✅ Flushed {} rows for user {} to {} (batch={})",
            rows_count,
            user_id,
            destination_path,
            batch_number
        );

        parquet_files.push(destination_path);
        Ok(rows_count)
    }

    /// Delete flushed rows from RocksDB
    fn delete_flushed_keys(&self, keys: &[Vec<u8>]) -> Result<(), KalamDbError> {
        if keys.is_empty() {
            return Ok(());
        }

        let parsed_keys: Result<Vec<_>, _> = keys
            .iter()
            .map(|key_bytes| {
                kalamdb_commons::ids::UserTableRowId::from_storage_key(key_bytes)
                    .into_invalid_operation("Invalid key bytes")
            })
            .collect();
        let parsed_keys = parsed_keys?;

        // Batch delete: single RocksDB batch write for all main + index entries.
        // Much faster than per-key delete() which does get+batch per key.
        self.store
            .delete_batch(&parsed_keys)
            .into_kalamdb_error("Failed to delete flushed rows")?;

        log::debug!("Deleted {} flushed rows from storage", keys.len());
        Ok(())
    }
}

impl TableFlush for UserTableFlushJob {
    fn execute(&self) -> Result<FlushJobResult, KalamDbError> {
        log::debug!(
            "🔄 Starting user table flush: table={}, partition={}",
            self.table_id,
            self.store.partition()
        );

        // Get primary key field name from schema
        let pk_field = helpers::extract_pk_field_name(&self.schema);
        log::debug!("📊 [FLUSH DEDUP] Using primary key field: {}", pk_field);

        // Map: (user_id, pk_value) -> (key_bytes, row, _seq)
        let mut latest_versions: HashMap<
            (UserId, String),
            (Vec<u8>, kalamdb_tables::UserTableRow, i64),
        > = HashMap::new();
        // Track ALL keys to delete (including old versions)
        // Pre-allocate with reasonable capacity to reduce reallocations during scan
        let mut all_keys_to_delete: Vec<Vec<u8>> = Vec::with_capacity(1024);
        let mut stats = FlushDedupStats::default();

        // Batched scan with cursor
        let mut cursor: Option<UserTableRowId> = None;
        loop {
            let batch = self
                .store
                .scan_typed_with_prefix_and_start(None, cursor.as_ref(), config::BATCH_SIZE)
                .map_err(|e| {
                    log::error!("❌ Failed to scan table={}: {}", self.table_id, e);
                    KalamDbError::Other(format!("Failed to scan table: {}", e))
                })?;

            if batch.is_empty() {
                break;
            }

            log::trace!(
                "[FLUSH] Processing batch of {} rows (cursor={:?})",
                batch.len(),
                cursor.as_ref().map(|c| c.seq().as_i64())
            );

            // Update cursor for next batch
            cursor = batch.last().map(|(key, _)| key.clone());

            let batch_len = batch.len();
            stats.rows_before_dedup += batch_len;

            for (row_id, row) in batch {
                // Track ALL keys for deletion (before dedup)
                all_keys_to_delete.push(row_id.storage_key());

                // Parse user_id from key
                let user_id = row_id.user_id().clone();

                // Extract PK value from fields
                let seq_val = row._seq.as_i64();
                let pk_value = helpers::extract_pk_value(&row.fields, &pk_field, seq_val);

                let group_key = (user_id.clone(), pk_value.clone());

                // Track deleted rows
                if row._deleted {
                    stats.deleted_count += 1;
                }

                // Keep MAX(_seq) per (user_id, pk_value)
                match latest_versions.get(&group_key) {
                    Some((_existing_key, _existing_row, existing_seq)) => {
                        if seq_val > *existing_seq {
                            log::trace!(
                                "[FLUSH DEDUP] Replacing user={}, pk={}: old_seq={}, new_seq={}, \
                                 deleted={}",
                                user_id.as_str(),
                                pk_value,
                                existing_seq,
                                seq_val,
                                row._deleted
                            );
                            latest_versions.insert(group_key, (row_id.storage_key(), row, seq_val));
                        }
                    },
                    None => {
                        latest_versions.insert(group_key, (row_id.storage_key(), row, seq_val));
                    },
                }
            }

            // Check if we got fewer rows than batch size (end of data)
            if batch_len < config::BATCH_SIZE {
                break;
            }
        }

        stats.rows_after_dedup = latest_versions.len();

        // STEP 2: Filter out deleted rows (tombstones)
        let mut rows_by_user: HashMap<UserId, Vec<(Vec<u8>, Row)>> = HashMap::new();

        for ((user_id, _pk_value), (key_bytes, row, _seq)) in latest_versions {
            // Skip soft-deleted rows (tombstones)
            if row._deleted {
                stats.tombstones_filtered += 1;
                continue;
            }

            // Convert to JSON and inject system columns
            let row_data =
                helpers::add_system_columns(row.fields.clone(), row._seq.as_i64(), false);

            rows_by_user.entry(user_id).or_default().push((key_bytes, row_data));
        }

        // Log dedup statistics
        stats.log_summary(&self.table_id.to_string());
        let rows_to_flush = rows_by_user.values().map(|v| v.len()).sum::<usize>();
        log::debug!(
            "📊 [FLUSH USER] Partitioned into {} users, {} rows to flush",
            rows_by_user.len(),
            rows_to_flush
        );

        // If no rows to flush, return early
        if rows_by_user.is_empty() {
            log::debug!(
                "⚠️  No rows to flush for user table={} (empty table or all deleted)",
                self.table_id
            );
            return Ok(FlushJobResult {
                rows_flushed: 0,
                parquet_files: vec![],
                metadata: FlushMetadata::user_table(0, vec![]),
            });
        }

        // Flush each user's data to separate Parquet file
        let mut parquet_files: Vec<String> = Vec::new();
        let mut total_rows_flushed = 0;
        let mut error_messages: Vec<String> = Vec::new();
        let mut flush_succeeded = true;

        for (user_id, rows) in &rows_by_user {
            match self.flush_user_data(
                user_id,
                rows,
                &mut parquet_files,
                &self.bloom_filter_columns,
                &self.indexed_columns,
            ) {
                Ok(rows_count) => {
                    total_rows_flushed += rows_count;
                },
                Err(e) => {
                    let error_msg = format!(
                        "Failed to flush {} rows for user {}: {}",
                        rows.len(),
                        user_id.as_str(),
                        e
                    );
                    log::error!("{}. Rows kept in buffer.", error_msg);
                    error_messages.push(error_msg);
                    flush_succeeded = false;
                },
            }
        }

        // Only delete ALL rows (including old versions) if ALL users flushed successfully
        if flush_succeeded {
            log::debug!(
                "📊 [FLUSH CLEANUP] Deleting {} rows from hot storage (including {} old versions)",
                all_keys_to_delete.len(),
                all_keys_to_delete.len() - rows_by_user.values().map(|v| v.len()).sum::<usize>()
            );
            if let Err(e) = self.delete_flushed_keys(&all_keys_to_delete) {
                log::error!("Failed to delete flushed rows: {}", e);
                error_messages.push(format!("Failed to delete flushed rows: {}", e));
            }

            // Manifest cache is already updated during flush; keep entries to
            // ensure system.manifest reflects the latest segments.
        }

        // If any user flush failed, treat entire job as failed
        if !error_messages.is_empty() {
            let summary = format!(
                "One or more user partitions failed to flush ({} errors). Rows flushed before \
                 failure: {}. First error: {}",
                error_messages.len(),
                total_rows_flushed,
                error_messages.first().cloned().unwrap_or_else(|| "unknown error".to_string())
            );
            log::error!("❌ User table flush failed: table={} — {}", self.table_id, summary);
            return Err(KalamDbError::Other(summary));
        }

        log::debug!(
            "✅ User table flush completed: table={}, rows_flushed={}, users_count={}, \
             parquet_files={}",
            self.table_id,
            total_rows_flushed,
            rows_by_user.len(),
            parquet_files.len()
        );

        Ok(FlushJobResult {
            rows_flushed: total_rows_flushed,
            parquet_files,
            metadata: FlushMetadata::user_table(rows_by_user.len(), error_messages),
        })
    }

    fn table_identifier(&self) -> String {
        self.table_id.full_name()
    }
}
