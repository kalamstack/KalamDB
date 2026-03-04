//! Unified storage operations via StorageCached.
//!
//! This module provides the `StorageCached` struct which is the single entry point
//! for all storage operations. It encapsulates:
//! - Storage configuration (from system.storages)
//! - Lazy ObjectStore initialization
//! - Path template resolution
//! - All file operations (list, get, put, delete)
//!
//! # Design Principle
//!
//! **Nobody except kalamdb-filestore should depend on object_store crate.**
//! All storage operations go through `StorageCached.operation(...)` methods.
//!
//! # Example
//!
//! ```ignore
//! // Get relative path for display
//! let path = cached.get_relative_path(&table_id, user_id, shard)?;
//!
//! // List files for a table
//! let files = cached.list(&table_id, user_id, shard).await?;
//!
//! // Write data to storage
//! let result = cached.put(&table_id, user_id, shard, "batch-5.parquet", data).await?;
//!
//! // Read data from storage
//! let result = cached.get(&table_id, user_id, shard, "manifest.json").await?;
//!
//! // Delete a file
//! let result = cached.delete(&table_id, user_id, shard, "batch-5.parquet").await?;
//! ```

use super::operations::{
    DeletePrefixResult, DeleteResult, ExistsResult, FileInfo, GetResult, ListResult, PathResult,
    PutResult, RenameResult,
};
use crate::core::runtime::run_blocking;
use crate::error::{FilestoreError, Result};
use crate::parquet::writer::{serialize_to_parquet, ParquetWriteResult};
use crate::paths::{PathResolver, TemplateResolver};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::StreamExt;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::Storage;
use object_store::{path::Path as ObjectPath, ObjectStore, ObjectStoreExt};
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::Instrument;

/// Unified storage interface with lazy ObjectStore and template resolution.
///
/// This is the **single entry point** for all storage operations.
/// Encapsulates ObjectStore and path resolution so callers don't need
/// to depend on object_store crate directly.
///
/// # Thread Safety
///
/// Uses double-check locking for lazy ObjectStore initialization.
/// All methods are safe to call from multiple threads concurrently.
///
/// # Performance
///
/// - ObjectStore is built once on first use (~50-200μs for cloud backends)
/// - Template resolution is O(1) string substitution
/// - All operations use the same cached ObjectStore instance
#[derive(Debug)]
pub struct StorageCached {
    /// The storage configuration from system.storages
    pub storage: Arc<Storage>,
    /// Remote storage timeout configuration
    timeouts: kalamdb_configs::config::types::RemoteStorageTimeouts,
    /// Lazily-initialized ObjectStore instance
    object_store: Arc<RwLock<Option<Arc<dyn ObjectStore>>>>,
}

impl StorageCached {
    /// Create a new StorageCached with the given storage configuration.
    ///
    /// The ObjectStore is not built until first access.
    pub fn new(
        storage: Storage,
        timeouts: kalamdb_configs::config::types::RemoteStorageTimeouts,
    ) -> Self {
        Self {
            storage: Arc::new(storage),
            timeouts,
            object_store: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new StorageCached with default timeouts.
    ///
    /// Convenience method that uses RemoteStorageTimeouts::default().
    pub fn with_default_timeouts(storage: Storage) -> Self {
        Self::new(storage, kalamdb_configs::config::types::RemoteStorageTimeouts::default())
    }

    // ========== Path Resolution Methods ==========

    /// Get the appropriate template for a table type.
    fn get_template(&self, table_type: TableType) -> &str {
        match table_type {
            TableType::User => &self.storage.user_tables_template,
            TableType::Shared | TableType::Stream => &self.storage.shared_tables_template,
            TableType::System => "system/{namespace}/{tableName}",
        }
    }

    /// Resolve static placeholders in template (namespace, tableName).
    fn resolve_static(&self, table_type: TableType, table_id: &TableId) -> String {
        let template = self.get_template(table_type);
        TemplateResolver::resolve_static_placeholders(template, table_id)
    }

    /// Resolve full template with all placeholders.
    fn resolve_full_template(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> String {
        let static_resolved = self.resolve_static(table_type, table_id);

        // If user_id is provided, resolve dynamic placeholders
        // Otherwise, return as-is (shared tables don't have {userId} placeholder)
        if let Some(uid) = user_id {
            TemplateResolver::resolve_dynamic_placeholders(&static_resolved, uid).into_owned()
        } else {
            static_resolved
        }
    }

    /// Get the base directory for this storage.
    fn base_directory(&self) -> &str {
        let trimmed = self.storage.base_directory.trim();
        if trimmed.is_empty() {
            "."
        } else {
            trimmed
        }
    }

    /// Join base directory with relative path.
    fn join_paths(&self, relative: &str) -> String {
        let base = self.base_directory();

        // Check for cloud storage schemes
        if base.starts_with("s3://")
            || base.starts_with("gs://")
            || base.starts_with("gcs://")
            || base.starts_with("az://")
            || base.starts_with("azure://")
        {
            format!("{}/{}", base.trim_end_matches('/'), relative.trim_start_matches('/'))
        } else {
            std::path::PathBuf::from(base).join(relative).to_string_lossy().into_owned()
        }
    }

    /// Get the relative path for a table (for display or storage-relative references).
    ///
    /// # Arguments
    /// * `table_type` - Type of table (User, Shared, Stream, System)
    /// * `table_id` - Table identifier (namespace + table name)
    /// * `user_id` - Optional user ID for user tables
    ///
    /// # Returns
    /// PathResult with full and relative paths
    pub fn get_relative_path(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> PathResult {
        let relative = self.resolve_full_template(table_type, table_id, user_id);
        let full = self.join_paths(&relative);
        PathResult::new(full, relative, self.base_directory().to_string())
    }

    /// Get the path for a specific file within a table's storage.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID
    /// * `shard` - Optional shard ID
    /// * `filename` - Filename (e.g., "manifest.json", "batch-5.parquet")
    ///
    /// # Returns
    /// PathResult with full and relative paths to the file
    pub fn get_file_path(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> PathResult {
        let dir_relative = self.resolve_full_template(table_type, table_id, user_id);
        let file_relative =
            format!("{}/{}", dir_relative.trim_end_matches('/'), filename.trim_start_matches('/'));
        let full = self.join_paths(&file_relative);
        PathResult::new(full, file_relative, self.base_directory().to_string())
    }

    /// Get the manifest.json path for a table.
    pub fn get_manifest_path(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> PathResult {
        self.get_file_path(table_type, table_id, user_id, "manifest.json")
    }

    /// Generate batch filename for a given batch number.
    #[inline]
    pub fn batch_filename(batch_number: u64) -> String {
        format!("batch-{}.parquet", batch_number)
    }

    /// Generate temp batch filename for atomic writes.
    #[inline]
    pub fn temp_batch_filename(batch_number: u64) -> String {
        format!("batch-{}.parquet.tmp", batch_number)
    }

    // ========== ObjectStore Access ==========

    /// Get or lazily initialize the ObjectStore.
    ///
    /// **Internal use only**: This method exposes the underlying ObjectStore for
    /// kalamdb-filestore internal functions. External crates should use the
    /// high-level operations (list, get, put, delete) instead.
    ///
    /// # Returns
    /// Arc-wrapped ObjectStore for zero-copy sharing
    pub fn object_store_internal(&self) -> Result<Arc<dyn ObjectStore>> {
        // Fast path: check if already initialized
        {
            let read_guard = self.object_store.read();
            if let Some(store) = read_guard.as_ref() {
                return Ok(Arc::clone(store));
            }
        }

        // Slow path: build and cache
        {
            let mut write_guard = self.object_store.write();

            // Double-check after acquiring write lock
            if let Some(store) = write_guard.as_ref() {
                return Ok(Arc::clone(store));
            }

            let store = crate::core::factory::build_object_store(&self.storage, &self.timeouts)?;
            *write_guard = Some(Arc::clone(&store));
            Ok(store)
        }
    }

    /// Convert a relative path to ObjectStore Path.
    fn to_object_path(&self, relative_path: &str) -> Result<ObjectPath> {
        ObjectPath::parse(relative_path)
            .map_err(|e| FilestoreError::Path(format!("Invalid object path: {}", e)))
    }

    // ========== File Operations (Async) ==========

    /// List all files under a table's storage path.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID (for user tables)
    ///
    /// # Returns
    /// ListResult containing all file paths
    pub async fn list(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<ListResult> {
        let span = tracing::info_span!(
            "storage.list",
            storage_id = %self.storage.storage_id,
            table_type = ?table_type,
            table_id = %table_id,
            has_user_id = user_id.is_some()
        );
        async move {
            let store = self.object_store_internal()?;
            let relative = self.resolve_full_template(table_type, table_id, user_id);
            let prefix_path = self.to_object_path(&relative)?;

            let mut stream = store.list(Some(&prefix_path));
            let mut paths = Vec::new();

            while let Some(result) = stream.next().await {
                let meta = result.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
                paths.push(meta.location.to_string());
            }
            tracing::debug!(count = paths.len(), "Storage list completed");

            Ok(ListResult::new(paths, relative))
        }
        .instrument(span)
        .await
    }

    /// Read manifest.json directly (Task 102)
    pub fn read_manifest_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<serde_json::Value>> {
        let file_content = match self.get_sync(table_type, table_id, user_id, "manifest.json") {
            Ok(res) => res.data,
            Err(FilestoreError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };

        let manifest: serde_json::Value = serde_json::from_slice(&file_content)
            .map_err(|e| FilestoreError::Format(format!("Invalid manifest json: {}", e)))?;

        Ok(Some(manifest))
    }

    /// Write manifest.json directly (Task 102)
    pub fn write_manifest_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &serde_json::Value,
    ) -> Result<()> {
        let span = tracing::info_span!(
            "manifest.write",
            storage_id = %self.storage.storage_id,
            table_type = ?table_type,
            table_id = %table_id,
            has_user_id = user_id.is_some()
        );
        let _span_guard = span.entered();
        let data = serde_json::to_vec_pretty(manifest)
            .map_err(|e| FilestoreError::Format(format!("Failed to serialize manifest: {}", e)))?;

        // Pass just the filename, not the full path - put_sync will construct the full path
        self.put_sync(table_type, table_id, user_id, "manifest.json", bytes::Bytes::from(data))?;
        tracing::debug!("Manifest write completed");
        Ok(())
    }

    /// List all files under a table's storage path (sync).
    pub fn list_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<ListResult> {
        // Clone what we need for the closure
        let table_type = table_type;
        let table_id = table_id.clone();
        let user_id = user_id.cloned();

        run_blocking(|| async { self.list(table_type, &table_id, user_id.as_ref()).await })
    }

    /// List all Parquet files under a table's storage path (sync).
    /// Returns duplicate-free list of filenames ending in .parquet.
    /// TODO: This is problematic for large datasets - consider async version with streaming.
    pub fn list_parquet_files_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Vec<String>> {
        let list_result = self.list_sync(table_type, table_id, user_id)?;
        let prefix = list_result.prefix.trim_end_matches('/');

        let mut files: Vec<String> = list_result
            .paths
            .into_iter()
            .filter_map(|path| {
                // Extract filename relative to the listed prefix
                // Note: ObjectStore paths are usually consistent but handling prefix stripping is safer
                let suffix = if path.starts_with(prefix) {
                    path[prefix.len()..].trim_start_matches('/')
                } else {
                    // Fallback for paths that might be full absolute paths or differently formatted
                    path.rsplit('/').next().unwrap_or(path.as_str())
                };

                if suffix.ends_with(".parquet") {
                    Some(suffix.to_string())
                } else {
                    None
                }
            })
            .collect();

        files.sort();
        files.dedup();
        Ok(files)
    }

    /// List all Parquet files under a table's storage path (async).
    /// Returns duplicate-free list of filenames ending in .parquet.
    pub async fn list_parquet_files(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Vec<String>> {
        let list_result = self.list(table_type, table_id, user_id).await?;
        let prefix = list_result.prefix.trim_end_matches('/');

        let mut files: Vec<String> = list_result
            .paths
            .into_iter()
            .filter_map(|path| {
                let suffix = if path.starts_with(prefix) {
                    path[prefix.len()..].trim_start_matches('/')
                } else {
                    path.rsplit('/').next().unwrap_or(path.as_str())
                };

                if suffix.ends_with(".parquet") {
                    Some(suffix.to_string())
                } else {
                    None
                }
            })
            .collect();

        files.sort();
        files.dedup();
        Ok(files)
    }

    /// Read one or multiple Parquet files and return RecordBatch(es) (async).
    pub async fn read_parquet_files(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        files: &[String],
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let mut batches = Vec::new();
        for file in files {
            let data = self.get(table_type, table_id, user_id, file).await?.data;
            let file_batches = crate::parquet::reader::parse_parquet_from_bytes(data)?;
            batches.extend(file_batches);
        }
        Ok(batches)
    }

    /// Read Parquet files with column projection (only decode specified columns).
    ///
    /// `columns` lists the column names to read. If empty, reads all columns.
    /// This avoids decoding and allocating memory for unneeded columns.
    pub async fn read_parquet_files_projected(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        files: &[String],
        columns: &[&str],
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let mut batches = Vec::new();
        for file in files {
            let data = self.get(table_type, table_id, user_id, file).await?.data;
            let file_batches = crate::parquet::reader::parse_parquet_projected(data, columns)?;
            batches.extend(file_batches);
        }
        Ok(batches)
    }

    /// Read Parquet files using bloom-filter-based row-group pruning.
    ///
    /// For each file, row groups whose bloom filter proves the `bloom_value` is
    /// absent are skipped entirely, and only `columns` are decoded from surviving
    /// row groups.
    ///
    /// Returns `None` for files where bloom filter proved absence across all row
    /// groups, and `Some(batches)` otherwise.
    ///
    /// This is ideal for PK existence checks and point-query lookups.
    pub async fn read_parquet_files_with_bloom_filter<V: parquet::data_type::AsBytes>(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        files: &[String],
        bloom_column: &str,
        bloom_value: &V,
        columns: &[&str],
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let mut batches = Vec::new();
        for file in files {
            let data = self.get(table_type, table_id, user_id, file).await?.data;
            if let Some(file_batches) = crate::parquet::reader::parse_parquet_with_bloom_filter(
                data,
                bloom_column,
                bloom_value,
                columns,
            )? {
                batches.extend(file_batches);
            }
        }
        Ok(batches)
    }

    /// Check if a value is definitely absent from Parquet files using only bloom
    /// filters (no row data decoded).
    ///
    /// Returns `true` if the value is provably absent from ALL files.
    /// Returns `false` if the value *might* be present in any file.
    pub async fn bloom_filter_check_absent<V: parquet::data_type::AsBytes>(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        files: &[String],
        column: &str,
        value: &V,
    ) -> Result<bool> {
        for file in files {
            let data = self.get(table_type, table_id, user_id, file).await?.data;
            if !crate::parquet::reader::bloom_filter_check_absent(data, column, value)? {
                return Ok(false); // Value might be present in this file
            }
        }
        Ok(true) // All files confirmed absence
    }

    /// Read a file from storage.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID
    /// * `filename` - Filename to read
    ///
    /// # Returns
    /// GetResult containing file data
    pub async fn get(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<GetResult> {
        let span = tracing::info_span!(
            "storage.get",
            storage_id = %self.storage.storage_id,
            table_type = ?table_type,
            table_id = %table_id,
            has_user_id = user_id.is_some(),
            filename = filename
        );
        async move {
            let store = self.object_store_internal()?;
            let path_result = self.get_file_path(table_type, table_id, user_id, filename);
            let object_path = self.to_object_path(&path_result.relative_path)?;

            let result = store.get(&object_path).await.map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    FilestoreError::NotFound(path_result.full_path.clone())
                },
                _ => FilestoreError::ObjectStore(e.to_string()),
            })?;

            let bytes =
                result.bytes().await.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            tracing::debug!(bytes = bytes.len(), "Storage get completed");

            Ok(GetResult::new(bytes, path_result.full_path))
        }
        .instrument(span)
        .await
    }

    /// Read a file from storage (sync).
    pub fn get_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<GetResult> {
        let table_id = table_id.clone();
        let user_id = user_id.cloned();
        let filename = filename.to_string();

        run_blocking(|| async {
            self.get(table_type, &table_id, user_id.as_ref(), &filename).await
        })
    }

    /// Write data to a file in storage.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID
    /// * `shard` - Optional shard ID
    /// * `filename` - Filename to write
    /// * `data` - Data to write
    ///
    /// # Returns
    /// PutResult with path and size information
    pub async fn put(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
        data: Bytes,
    ) -> Result<PutResult> {
        let bytes_len = data.len();
        let span = tracing::info_span!(
            "storage.put",
            storage_id = %self.storage.storage_id,
            table_type = ?table_type,
            table_id = %table_id,
            has_user_id = user_id.is_some(),
            filename = filename,
            bytes = bytes_len
        );
        async move {
            let store = self.object_store_internal()?;
            let path_result = self.get_file_path(table_type, table_id, user_id, filename);
            let object_path = self.to_object_path(&path_result.relative_path)?;

            // Check if file exists for is_new flag
            let existed = store.head(&object_path).await.is_ok();

            store
                .put(&object_path, data.into())
                .await
                .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            tracing::debug!(is_new = !existed, "Storage put completed");

            Ok(PutResult::new(path_result.full_path, bytes_len, !existed))
        }
        .instrument(span)
        .await
    }

    /// Write data to a file in storage (sync).
    pub fn put_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
        data: Bytes,
    ) -> Result<PutResult> {
        let table_id = table_id.clone();
        let user_id = user_id.cloned();
        let filename = filename.to_string();

        run_blocking(|| async {
            self.put(table_type, &table_id, user_id.as_ref(), &filename, data).await
        })
    }

    /// Write Parquet batches to storage using the cached ObjectStore.
    pub async fn write_parquet(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        bloom_filter_columns: Option<Vec<String>>,
    ) -> Result<ParquetWriteResult> {
        let batch_count = batches.len();
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let span = tracing::info_span!(
            "parquet.write",
            storage_id = %self.storage.storage_id,
            table_type = ?table_type,
            table_id = %table_id,
            has_user_id = user_id.is_some(),
            filename = filename,
            batch_count = batch_count,
            row_count = row_count
        );
        async move {
            let parquet_bytes = serialize_to_parquet(schema, batches, bloom_filter_columns)?;
            let size_bytes = parquet_bytes.len() as u64;

            self.put(table_type, table_id, user_id, filename, parquet_bytes).await?;
            tracing::debug!(size_bytes = size_bytes, "Parquet write completed");

            Ok(ParquetWriteResult { size_bytes })
        }
        .instrument(span)
        .await
    }

    /// Write Parquet batches to storage (sync).
    pub fn write_parquet_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        bloom_filter_columns: Option<Vec<String>>,
    ) -> Result<ParquetWriteResult> {
        let table_id = table_id.clone();
        let user_id = user_id.cloned();
        let filename = filename.to_string();

        run_blocking(|| async {
            self.write_parquet(
                table_type,
                &table_id,
                user_id.as_ref(),
                &filename,
                schema,
                batches,
                bloom_filter_columns,
            )
            .await
        })
    }

    /// Delete a file from storage.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID
    /// * `shard` - Optional shard ID
    /// * `filename` - Filename to delete
    ///
    /// # Returns
    /// DeleteResult with path and existence information
    pub async fn delete(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<DeleteResult> {
        let span = tracing::info_span!(
            "storage.delete",
            storage_id = %self.storage.storage_id,
            table_type = ?table_type,
            table_id = %table_id,
            has_user_id = user_id.is_some(),
            filename = filename
        );
        async move {
            let store = self.object_store_internal()?;
            let path_result = self.get_file_path(table_type, table_id, user_id, filename);
            let object_path = self.to_object_path(&path_result.relative_path)?;

            // Check if file exists
            let existed = store.head(&object_path).await.is_ok();

            if existed {
                store
                    .delete(&object_path)
                    .await
                    .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            }
            tracing::debug!(existed = existed, "Storage delete completed");

            Ok(DeleteResult::new(path_result.full_path, existed))
        }
        .instrument(span)
        .await
    }

    /// Delete a file from storage (sync).
    pub fn delete_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<DeleteResult> {
        let table_id = table_id.clone();
        let user_id = user_id.cloned();
        let filename = filename.to_string();

        run_blocking(|| async {
            self.delete(table_type, &table_id, user_id.as_ref(), &filename).await
        })
    }

    /// Delete all files under a table's storage path (e.g., for DROP TABLE).
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID (None to delete all users' data)
    /// * `shard` - Optional shard ID (None to delete all shards)
    ///
    /// # Returns
    /// DeletePrefixResult with count of deleted files
    pub async fn delete_prefix(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<DeletePrefixResult> {
        let store = self.object_store_internal()?;

        // For user tables without user_id, we want to delete the table prefix
        // (which will delete all user directories under it)
        let prefix = if user_id.is_none() && table_type == TableType::User {
            // Just resolve static placeholders for user tables without user_id
            self.resolve_static(table_type, table_id)
        } else {
            self.resolve_full_template(table_type, table_id, user_id)
        };

        let cleanup_prefix = PathResolver::resolve_cleanup_prefix(&prefix);

        let prefix_path = self.to_object_path(cleanup_prefix.as_ref())?;

        // List all files under prefix
        let mut stream = store.list(Some(&prefix_path));
        let mut deleted_paths = Vec::new();

        while let Some(result) = stream.next().await {
            let meta = result.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            let path = meta.location;

            // Delete each file
            store
                .delete(&path)
                .await
                .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;

            deleted_paths.push(path.to_string());
        }

        if matches!(self.storage.storage_type, StorageType::Filesystem) {
            let full_path = self.join_paths(cleanup_prefix.as_ref());
            let dir_path = std::path::Path::new(&full_path);
            if dir_path.exists() {
                std::fs::remove_dir_all(dir_path)?;
            }
        }

        Ok(DeletePrefixResult::new(cleanup_prefix.into_owned(), deleted_paths))
    }

    /// Check if a file exists.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID
    /// * `filename` - Filename to check
    ///
    /// # Returns
    /// ExistsResult with existence and size information
    pub async fn exists(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<ExistsResult> {
        let store = self.object_store_internal()?;
        let path_result = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&path_result.relative_path)?;

        match store.head(&object_path).await {
            Ok(meta) => {
                Ok(ExistsResult::new(path_result.full_path, true, Some(meta.size as usize)))
            },
            Err(object_store::Error::NotFound { .. }) => {
                Ok(ExistsResult::new(path_result.full_path, false, None))
            },
            Err(e) => Err(FilestoreError::ObjectStore(e.to_string())),
        }
    }

    /// Get file metadata (size, last modified).
    pub async fn head(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<FileInfo> {
        let store = self.object_store_internal()?;
        let path_result = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&path_result.relative_path)?;

        let meta = store
            .head(&object_path)
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;

        Ok(FileInfo::new(
            path_result.full_path,
            meta.size as usize,
            Some(meta.last_modified.timestamp_millis()),
        ))
    }

    /// Get file metadata (sync).
    pub fn head_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<FileInfo> {
        let table_id = table_id.clone();
        let user_id = user_id.cloned();
        let filename = filename.to_string();

        run_blocking(|| async {
            self.head(table_type, &table_id, user_id.as_ref(), &filename).await
        })
    }

    /// Rename/move a file within the same table's storage.
    pub async fn rename(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        from_filename: &str,
        to_filename: &str,
    ) -> Result<RenameResult> {
        let store = self.object_store_internal()?;

        let from_path_result = self.get_file_path(table_type, table_id, user_id, from_filename);
        let to_path_result = self.get_file_path(table_type, table_id, user_id, to_filename);

        let from_object_path = self.to_object_path(&from_path_result.relative_path)?;
        let to_object_path = self.to_object_path(&to_path_result.relative_path)?;

        store
            .rename(&from_object_path, &to_object_path)
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;

        Ok(RenameResult::new(from_path_result.full_path, to_path_result.full_path, true))
    }

    /// Rename/move a file (sync).
    pub fn rename_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        from_filename: &str,
        to_filename: &str,
    ) -> Result<RenameResult> {
        let table_id = table_id.clone();
        let user_id = user_id.cloned();
        let from_filename = from_filename.to_string();
        let to_filename = to_filename.to_string();

        run_blocking(|| async {
            self.rename(table_type, &table_id, user_id.as_ref(), &from_filename, &to_filename)
                .await
        })
    }

    /// Check if any files exist under a table's storage path.
    ///
    /// # Arguments
    /// * `table_type` - Type of table
    /// * `table_id` - Table identifier
    /// * `user_id` - Optional user ID
    ///
    /// # Returns
    /// true if at least one file exists under the prefix
    pub async fn prefix_exists(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<bool> {
        let store = self.object_store_internal()?;
        let relative = self.resolve_full_template(table_type, table_id, user_id);
        let prefix_path = self.to_object_path(&relative)?;

        let mut stream = store.list(Some(&prefix_path));

        // Check if at least one file exists
        Ok(stream.next().await.is_some())
    }

    // ========== Cache Management ==========

    /// Invalidate the cached ObjectStore (forces rebuild on next use).
    ///
    /// Call this when storage configuration changes (e.g., ALTER STORAGE).
    pub fn invalidate_object_store(&self) {
        let mut write_guard = self.object_store.write();
        *write_guard = None;
    }

    /// Check if ObjectStore has been initialized.
    pub fn is_object_store_initialized(&self) -> bool {
        let read_guard = self.object_store.read();
        read_guard.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::ids::StorageId;
    use kalamdb_commons::{NamespaceId, TableName};
    use kalamdb_system::providers::storages::models::StorageType;
    use std::env;

    fn create_test_storage() -> Storage {
        // Create a unique temp directory for each test invocation
        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = env::temp_dir().join(format!("storage_cached_test_{}", unique_id));
        std::fs::create_dir_all(&temp_dir).ok();

        let now = chrono::Utc::now().timestamp_millis();
        Storage {
            storage_id: StorageId::from("test_storage"),
            storage_name: "test_storage".to_string(),
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

    fn make_table_id(ns: &str, tbl: &str) -> TableId {
        TableId::new(NamespaceId::new(ns), TableName::new(tbl))
    }

    #[test]
    fn test_get_relative_path_shared() {
        let cached = StorageCached::with_default_timeouts(create_test_storage());
        let table_id = make_table_id("myns", "mytable");

        let result = cached.get_relative_path(TableType::Shared, &table_id, None);

        assert_eq!(result.relative_path, "myns/mytable");
        assert!(!result.base_directory.is_empty());
    }

    #[test]
    fn test_get_relative_path_user() {
        let cached = StorageCached::new(
            create_test_storage(),
            kalamdb_configs::config::types::RemoteStorageTimeouts::default(),
        );
        let table_id = make_table_id("chat", "messages");
        let user_id = UserId::from("alice");

        let result = cached.get_relative_path(TableType::User, &table_id, Some(&user_id));

        assert_eq!(result.relative_path, "chat/messages/alice");
    }

    #[test]
    fn test_get_file_path() {
        let cached = StorageCached::new(
            create_test_storage(),
            kalamdb_configs::config::types::RemoteStorageTimeouts::default(),
        );
        let table_id = make_table_id("myns", "mytable");

        let result = cached.get_file_path(TableType::Shared, &table_id, None, "manifest.json");

        assert!(result.relative_path.ends_with("manifest.json"));
        assert!(result.relative_path.contains("myns/mytable"));
    }

    #[test]
    fn test_get_manifest_path() {
        let cached = StorageCached::new(
            create_test_storage(),
            kalamdb_configs::config::types::RemoteStorageTimeouts::default(),
        );
        let table_id = make_table_id("myns", "mytable");

        let result = cached.get_manifest_path(TableType::Shared, &table_id, None);

        assert_eq!(result.relative_path, "myns/mytable/manifest.json");
    }

    #[test]
    fn test_batch_filenames() {
        assert_eq!(StorageCached::batch_filename(0), "batch-0.parquet");
        assert_eq!(StorageCached::batch_filename(42), "batch-42.parquet");
        assert_eq!(StorageCached::temp_batch_filename(5), "batch-5.parquet.tmp");
    }

    // ========== Operation Tests (matching object_store.rs tests) ==========

    #[test]
    fn test_write_and_read_file_sync() {
        let temp_dir = env::temp_dir().join("storage_cached_test_write_read");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "data");
        let content = Bytes::from("Hello, KalamDB!");

        // Write file
        let put_result = cached
            .put_sync(TableType::Shared, &table_id, None, "data.txt", content.clone())
            .unwrap();
        assert!(put_result.is_new);
        assert_eq!(put_result.size, content.len());

        // Read file back
        let get_result = cached.get_sync(TableType::Shared, &table_id, None, "data.txt").unwrap();
        assert_eq!(get_result.data, content);
        assert_eq!(get_result.size, content.len());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_list_files_sync() {
        let temp_dir = env::temp_dir().join("storage_cached_test_list_files");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        // Write multiple files
        let table_id1 = make_table_id("namespace1", "table1");
        let files = vec!["file1.txt", "file2.txt", "subdir/file3.txt"];

        for file in &files {
            cached
                .put_sync(TableType::Shared, &table_id1, None, file, Bytes::from("test"))
                .unwrap();
        }

        // Also write a file in a different namespace to ensure isolation
        let table_id2 = make_table_id("namespace2", "table2");
        cached
            .put_sync(TableType::Shared, &table_id2, None, "file4.txt", Bytes::from("test"))
            .unwrap();

        // List all files under namespace1/table1
        let listed = cached.list_sync(TableType::Shared, &table_id1, None).unwrap();

        // Should include namespace1 files
        assert!(listed.count >= 3, "Should list at least 3 files");

        // Check that namespace1 files are present
        let has_file1 = listed.paths.iter().any(|p| p.contains("file1.txt"));
        let has_file2 = listed.paths.iter().any(|p| p.contains("file2.txt"));
        assert!(has_file1, "Should contain file1.txt");
        assert!(has_file2, "Should contain file2.txt");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_head_file_sync() {
        let temp_dir = env::temp_dir().join("storage_cached_test_head_file");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "metadata");
        let content = Bytes::from("Some content for metadata test");

        // Write file
        cached
            .put_sync(TableType::Shared, &table_id, None, "metadata.txt", content.clone())
            .unwrap();

        // Get metadata
        let meta = cached.head_sync(TableType::Shared, &table_id, None, "metadata.txt").unwrap();

        assert_eq!(meta.size, content.len());
        assert!(meta.last_modified_ms.is_some());
        assert!(meta.last_modified_ms.unwrap() > 0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_delete_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_delete_file");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "delete");
        let content = Bytes::from("Delete this");

        // Write file
        cached
            .put(TableType::Shared, &table_id, None, "delete_me.txt", content)
            .await
            .unwrap();

        // Verify exists
        let exists_before = cached
            .exists(TableType::Shared, &table_id, None, "delete_me.txt")
            .await
            .unwrap();
        assert!(exists_before.exists);

        // Delete file
        let delete_result = cached
            .delete(TableType::Shared, &table_id, None, "delete_me.txt")
            .await
            .unwrap();
        assert!(delete_result.existed);

        // Verify deleted
        let exists_after = cached
            .exists(TableType::Shared, &table_id, None, "delete_me.txt")
            .await
            .unwrap();
        assert!(!exists_after.exists);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_delete_prefix() {
        let temp_dir = env::temp_dir().join("storage_cached_test_delete_prefix");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("ns", "table");

        // Write multiple files under a prefix
        let files = vec!["seg1.parquet", "seg2.parquet", "manifest.json"];

        for file in &files {
            cached
                .put(TableType::Shared, &table_id, None, file, Bytes::from("test"))
                .await
                .unwrap();
        }

        // Verify files exist
        let listed_before = cached.list(TableType::Shared, &table_id, None).await.unwrap();
        assert!(listed_before.count >= 3);

        // Delete all files under prefix
        let delete_result = cached.delete_prefix(TableType::Shared, &table_id, None).await.unwrap();
        assert_eq!(delete_result.files_deleted, 3, "Should delete 3 files");

        // Verify all deleted
        let listed_after = cached.list(TableType::Shared, &table_id, None).await.unwrap();
        assert_eq!(listed_after.count, 0, "All files should be deleted");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_prefix_exists() {
        let temp_dir = env::temp_dir().join("storage_cached_test_prefix_exists");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("exists", "test");

        // Check non-existent prefix
        let exists_before = cached.prefix_exists(TableType::Shared, &table_id, None).await.unwrap();
        assert!(!exists_before);

        // Write a file
        cached
            .put(TableType::Shared, &table_id, None, "file.txt", Bytes::from("test"))
            .await
            .unwrap();

        // Check prefix now exists
        let exists_after = cached.prefix_exists(TableType::Shared, &table_id, None).await.unwrap();
        assert!(exists_after);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_write_overwrite_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_overwrite");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "overwrite");

        // Write first version
        let put1 = cached
            .put_sync(TableType::Shared, &table_id, None, "overwrite.txt", Bytes::from("version 1"))
            .unwrap();
        assert!(put1.is_new);

        let v1 = cached.get_sync(TableType::Shared, &table_id, None, "overwrite.txt").unwrap();
        assert_eq!(v1.data, Bytes::from("version 1"));

        // Overwrite with version 2
        let put2 = cached
            .put_sync(TableType::Shared, &table_id, None, "overwrite.txt", Bytes::from("version 2"))
            .unwrap();
        assert!(!put2.is_new); // Should be overwrite

        let v2 = cached.get_sync(TableType::Shared, &table_id, None, "overwrite.txt").unwrap();
        assert_eq!(v2.data, Bytes::from("version 2"));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_nonexistent_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_nonexistent_read");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("nonexistent", "table");

        let result = cached.get_sync(TableType::Shared, &table_id, None, "file.txt");
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_large_file_operations() {
        let temp_dir = env::temp_dir().join("storage_cached_test_large_file");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "large");

        // Create 1MB of data
        let large_data = vec![42u8; 1024 * 1024];
        let content = Bytes::from(large_data);

        // Write large file
        cached
            .put_sync(TableType::Shared, &table_id, None, "large.bin", content.clone())
            .unwrap();

        // Read back
        let read_content =
            cached.get_sync(TableType::Shared, &table_id, None, "large.bin").unwrap();
        assert_eq!(read_content.size, 1024 * 1024);
        assert_eq!(read_content.data, content);

        // Check metadata
        let meta = cached.head_sync(TableType::Shared, &table_id, None, "large.bin").unwrap();
        assert_eq!(meta.size, 1024 * 1024);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_nested_directory_creation() {
        let temp_dir = env::temp_dir().join("storage_cached_test_nested");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("a", "b");

        // Write to deeply nested path
        cached
            .put_sync(TableType::Shared, &table_id, None, "c/d/e/f/file.txt", Bytes::from("nested"))
            .unwrap();

        // Read back
        let content =
            cached.get_sync(TableType::Shared, &table_id, None, "c/d/e/f/file.txt").unwrap();
        assert_eq!(content.data, Bytes::from("nested"));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_empty_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_empty_file");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "empty");
        let empty = Bytes::new();

        // Write empty file
        cached.put_sync(TableType::Shared, &table_id, None, "empty.txt", empty).unwrap();

        // Read back
        let content = cached.get_sync(TableType::Shared, &table_id, None, "empty.txt").unwrap();
        assert_eq!(content.size, 0);

        // Check metadata
        let meta = cached.head_sync(TableType::Shared, &table_id, None, "empty.txt").unwrap();
        assert_eq!(meta.size, 0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_list_empty_prefix() {
        let temp_dir = env::temp_dir().join("storage_cached_test_list_empty");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("empty", "prefix");

        // List non-existent prefix
        let files = cached.list_sync(TableType::Shared, &table_id, None).unwrap();
        assert_eq!(files.count, 0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_delete_nonexistent_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_delete_nonexistent");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("nonexistent", "table");

        // Deleting non-existent file should not error (idempotent)
        let result = cached.delete_sync(TableType::Shared, &table_id, None, "file.txt");
        assert!(result.is_ok());
        assert!(!result.unwrap().existed);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_binary_data() {
        let temp_dir = env::temp_dir().join("storage_cached_test_binary");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "binary");

        // Create binary data with all byte values
        let binary_data: Vec<u8> = (0..=255).collect();
        let content = Bytes::from(binary_data);

        cached
            .put_sync(TableType::Shared, &table_id, None, "binary.bin", content.clone())
            .unwrap();

        let read_content =
            cached.get_sync(TableType::Shared, &table_id, None, "binary.bin").unwrap();
        assert_eq!(read_content.data, content);
        assert_eq!(read_content.size, 256);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_rename_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_rename");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "rename");
        let content = Bytes::from("rename test content");

        // Write source file
        cached
            .put(TableType::Shared, &table_id, None, "original.txt", content.clone())
            .await
            .unwrap();

        // Verify source exists
        assert!(
            cached
                .exists(TableType::Shared, &table_id, None, "original.txt")
                .await
                .unwrap()
                .exists
        );

        // Rename file
        cached
            .rename_sync(TableType::Shared, &table_id, None, "original.txt", "renamed.txt")
            .unwrap();

        // Verify destination exists with correct content
        let read_content =
            cached.get_sync(TableType::Shared, &table_id, None, "renamed.txt").unwrap();
        assert_eq!(read_content.data, content);

        // Verify source no longer exists
        assert!(
            !cached
                .exists(TableType::Shared, &table_id, None, "original.txt")
                .await
                .unwrap()
                .exists
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_rename_file_to_different_directory() {
        let temp_dir = env::temp_dir().join("storage_cached_test_rename_dir");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("test", "rename");
        let content = Bytes::from("cross-directory rename");

        // Write source file
        cached
            .put(TableType::Shared, &table_id, None, "file.txt", content.clone())
            .await
            .unwrap();

        // Rename to different directory
        cached
            .rename_sync(TableType::Shared, &table_id, None, "file.txt", "subdir/file.txt")
            .unwrap();

        // Verify destination exists
        let read_content =
            cached.get_sync(TableType::Shared, &table_id, None, "subdir/file.txt").unwrap();
        assert_eq!(read_content.data, content);

        // Verify source no longer exists
        assert!(
            !cached
                .exists(TableType::Shared, &table_id, None, "file.txt")
                .await
                .unwrap()
                .exists
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_rename_temp_parquet_to_final() {
        // Simulates the atomic flush pattern: write to .tmp, rename to .parquet
        let temp_dir = env::temp_dir().join("storage_cached_test_rename_parquet");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("ns", "table");

        // Simulate Parquet content (just bytes for this test)
        let parquet_content = Bytes::from(vec![0x50, 0x41, 0x52, 0x31]); // "PAR1" magic

        // Step 1: Write to temp location
        cached
            .put_sync(
                TableType::Shared,
                &table_id,
                None,
                "batch-0.parquet.tmp",
                parquet_content.clone(),
            )
            .unwrap();

        // Step 2: Atomic rename to final location
        cached
            .rename_sync(
                TableType::Shared,
                &table_id,
                None,
                "batch-0.parquet.tmp",
                "batch-0.parquet",
            )
            .unwrap();

        // Step 3: Verify final file exists with correct content
        let read_content =
            cached.get_sync(TableType::Shared, &table_id, None, "batch-0.parquet").unwrap();
        assert_eq!(read_content.data, parquet_content);

        // Step 4: Verify temp file is gone
        assert!(
            !cached
                .exists(TableType::Shared, &table_id, None, "batch-0.parquet.tmp")
                .await
                .unwrap()
                .exists
        );

        // List files - should only see the final file
        let files = cached.list_sync(TableType::Shared, &table_id, None).unwrap();
        assert_eq!(files.count, 1);
        assert!(files.paths[0].contains("batch-0.parquet"));
        assert!(!files.paths[0].contains(".tmp"));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_rename_preserves_large_file() {
        let temp_dir = env::temp_dir().join("storage_cached_test_rename_large");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("large", "file");

        // Create a larger file (1MB)
        let large_content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let content = Bytes::from(large_content);

        // Write large file
        cached
            .put_sync(TableType::Shared, &table_id, None, "file.tmp", content.clone())
            .unwrap();

        // Rename
        cached
            .rename_sync(TableType::Shared, &table_id, None, "file.tmp", "file.dat")
            .unwrap();

        // Verify content is preserved
        let read_content = cached.get_sync(TableType::Shared, &table_id, None, "file.dat").unwrap();
        assert_eq!(read_content.size, 1024 * 1024);
        assert_eq!(read_content.data, content);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_atomic_flush_pattern_simulation() {
        // Full simulation of the atomic flush pattern with manifest update
        let temp_dir = env::temp_dir().join("storage_cached_test_atomic_flush");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage();
        let cached = StorageCached::with_default_timeouts(storage);

        let table_id = make_table_id("myns", "mytable");

        // Initial state: manifest exists (from previous flush)
        let initial_manifest = r#"{"version":1,"segments":[]}"#;
        cached
            .put_sync(
                TableType::Shared,
                &table_id,
                None,
                "manifest.json",
                Bytes::from(initial_manifest),
            )
            .unwrap();

        // Step 1: Write Parquet to temp location
        let parquet_data = Bytes::from(vec![0x50, 0x41, 0x52, 0x31, 0x00, 0x01, 0x02, 0x03]);

        cached
            .put_sync(
                TableType::Shared,
                &table_id,
                None,
                "batch-0.parquet.tmp",
                parquet_data.clone(),
            )
            .unwrap();

        // Step 2: Atomic rename temp -> final
        cached
            .rename_sync(
                TableType::Shared,
                &table_id,
                None,
                "batch-0.parquet.tmp",
                "batch-0.parquet",
            )
            .unwrap();

        // Step 3: Update manifest with new segment
        let updated_manifest = r#"{"version":2,"segments":[{"path":"batch-0.parquet"}]}"#;
        cached
            .put_sync(
                TableType::Shared,
                &table_id,
                None,
                "manifest.json",
                Bytes::from(updated_manifest),
            )
            .unwrap();

        // Verify final state
        let files = cached.list_sync(TableType::Shared, &table_id, None).unwrap();
        assert_eq!(files.count, 2); // manifest.json + batch-0.parquet

        // No temp files should exist
        let temp_files: Vec<_> = files.paths.iter().filter(|f| f.contains(".tmp")).collect();
        assert!(temp_files.is_empty(), "No temp files should remain after atomic flush");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
