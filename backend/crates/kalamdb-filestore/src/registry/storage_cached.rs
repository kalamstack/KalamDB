//! Unified storage operations.
//!
//! `StorageCached` wraps a `Storage` config with a lazy `ObjectStore` and provides
//! the complete file-operation API for KalamDB cold storage. All operations are
//! async-first; thin `_sync` wrappers delegate via `run_blocking`.

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

/// Unified storage interface with lazy `ObjectStore` and template resolution.
///
/// Single entry point for all file operations. Thread-safe via double-check
/// locking for lazy initialization. The `ObjectStore` is built once on first
/// use and shared across all subsequent calls.
#[derive(Debug)]
pub struct StorageCached {
    /// Storage configuration from `system.storages`.
    pub storage: Arc<Storage>,
    timeouts: kalamdb_configs::config::types::RemoteStorageTimeouts,
    object_store: Arc<RwLock<Option<Arc<dyn ObjectStore>>>>,
}

impl StorageCached {
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

    pub fn with_default_timeouts(storage: Storage) -> Self {
        Self::new(storage, kalamdb_configs::config::types::RemoteStorageTimeouts::default())
    }

    // ── Path Resolution ──────────────────────────────────────────────

    fn get_template(&self, table_type: TableType) -> &str {
        match table_type {
            TableType::User => &self.storage.user_tables_template,
            TableType::Shared | TableType::Stream => &self.storage.shared_tables_template,
            TableType::System => "system/{namespace}/{tableName}",
        }
    }

    fn resolve_static(&self, table_type: TableType, table_id: &TableId) -> String {
        TemplateResolver::resolve_static_placeholders(self.get_template(table_type), table_id)
    }

    fn resolve_full_template(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> String {
        let resolved = self.resolve_static(table_type, table_id);
        match user_id {
            Some(uid) => {
                TemplateResolver::resolve_dynamic_placeholders(&resolved, uid).into_owned()
            },
            None => resolved,
        }
    }

    fn base_directory(&self) -> &str {
        let trimmed = self.storage.base_directory.trim();
        if trimmed.is_empty() { "." } else { trimmed }
    }

    fn join_paths(&self, relative: &str) -> String {
        let base = self.base_directory();
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

    // ── Public Path API ──────────────────────────────────────────────

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

    pub fn get_file_path(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> PathResult {
        let dir = self.resolve_full_template(table_type, table_id, user_id);
        let file_relative =
            format!("{}/{}", dir.trim_end_matches('/'), filename.trim_start_matches('/'));
        let full = self.join_paths(&file_relative);
        PathResult::new(full, file_relative, self.base_directory().to_string())
    }

    pub fn get_manifest_path(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> PathResult {
        self.get_file_path(table_type, table_id, user_id, "manifest.json")
    }

    #[inline]
    pub fn batch_filename(batch_number: u64) -> String {
        format!("batch-{}.parquet", batch_number)
    }

    #[inline]
    pub fn temp_batch_filename(batch_number: u64) -> String {
        format!("batch-{}.parquet.tmp", batch_number)
    }

    // ── ObjectStore Access ───────────────────────────────────────────

    /// Get or lazily initialize the `ObjectStore`.
    ///
    /// Internal: external crates should use the high-level operations instead.
    pub fn object_store_internal(&self) -> Result<Arc<dyn ObjectStore>> {
        {
            let guard = self.object_store.read();
            if let Some(store) = guard.as_ref() {
                return Ok(Arc::clone(store));
            }
        }
        let mut guard = self.object_store.write();
        if let Some(store) = guard.as_ref() {
            return Ok(Arc::clone(store));
        }
        let store = crate::core::factory::build_object_store(&self.storage, &self.timeouts)?;
        *guard = Some(Arc::clone(&store));
        Ok(store)
    }

    fn to_object_path(&self, relative_path: &str) -> Result<ObjectPath> {
        ObjectPath::parse(relative_path)
            .map_err(|e| FilestoreError::Path(format!("Invalid object path: {}", e)))
    }

    // ── Async Operations ─────────────────────────────────────────────

    /// List all files under a table's storage path.
    pub async fn list(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<ListResult> {
        let store = self.object_store_internal()?;
        let relative = self.resolve_full_template(table_type, table_id, user_id);
        let prefix_path = self.to_object_path(&relative)?;

        let mut stream = store.list(Some(&prefix_path));
        let mut paths = Vec::new();
        while let Some(result) = stream.next().await {
            let meta = result.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            paths.push(meta.location.to_string());
        }
        Ok(ListResult::new(paths, relative))
    }

    /// Read a file from storage.
    pub async fn get(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<GetResult> {
        let store = self.object_store_internal()?;
        let pr = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&pr.relative_path)?;

        let result = store.get(&object_path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => FilestoreError::NotFound(pr.full_path.clone()),
            _ => FilestoreError::ObjectStore(e.to_string()),
        })?;
        let bytes = result.bytes().await.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        Ok(GetResult::new(bytes, pr.full_path))
    }

    /// Write data to a file in storage.
    pub async fn put(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
        data: Bytes,
    ) -> Result<PutResult> {
        let size = data.len();
        let store = self.object_store_internal()?;
        let pr = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&pr.relative_path)?;

        store
            .put(&object_path, data.into())
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;

        Ok(PutResult::new(pr.full_path, size))
    }

    /// Delete a file from storage.
    pub async fn delete(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<DeleteResult> {
        let store = self.object_store_internal()?;
        let pr = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&pr.relative_path)?;

        let existed = store.head(&object_path).await.is_ok();
        if existed {
            store
                .delete(&object_path)
                .await
                .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        }
        Ok(DeleteResult::new(pr.full_path, existed))
    }

    /// Delete all files under a table's storage prefix (e.g., DROP TABLE).
    pub async fn delete_prefix(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<DeletePrefixResult> {
        let store = self.object_store_internal()?;

        let prefix = if user_id.is_none() && table_type == TableType::User {
            self.resolve_static(table_type, table_id)
        } else {
            self.resolve_full_template(table_type, table_id, user_id)
        };

        let cleanup_prefix = PathResolver::resolve_cleanup_prefix(&prefix);
        let prefix_path = self.to_object_path(cleanup_prefix.as_ref())?;

        let mut stream = store.list(Some(&prefix_path));
        let mut deleted_paths = Vec::new();
        while let Some(result) = stream.next().await {
            let meta = result.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            store
                .delete(&meta.location)
                .await
                .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
            deleted_paths.push(meta.location.to_string());
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
    pub async fn exists(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        filename: &str,
    ) -> Result<ExistsResult> {
        let store = self.object_store_internal()?;
        let pr = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&pr.relative_path)?;

        match store.head(&object_path).await {
            Ok(meta) => Ok(ExistsResult::new(pr.full_path, true, Some(meta.size as usize))),
            Err(object_store::Error::NotFound { .. }) => {
                Ok(ExistsResult::new(pr.full_path, false, None))
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
        let pr = self.get_file_path(table_type, table_id, user_id, filename);
        let object_path = self.to_object_path(&pr.relative_path)?;

        let meta = store
            .head(&object_path)
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        Ok(FileInfo::new(
            pr.full_path,
            meta.size as usize,
            Some(meta.last_modified.timestamp_millis()),
        ))
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
        let from_pr = self.get_file_path(table_type, table_id, user_id, from_filename);
        let to_pr = self.get_file_path(table_type, table_id, user_id, to_filename);
        let from_path = self.to_object_path(&from_pr.relative_path)?;
        let to_path = self.to_object_path(&to_pr.relative_path)?;

        store
            .rename(&from_path, &to_path)
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        Ok(RenameResult::new(from_pr.full_path, to_pr.full_path, true))
    }

    /// Check if any files exist under a table's storage prefix.
    pub async fn prefix_exists(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<bool> {
        let store = self.object_store_internal()?;
        let relative = self.resolve_full_template(table_type, table_id, user_id);
        let prefix_path = self.to_object_path(&relative)?;
        Ok(store.list(Some(&prefix_path)).next().await.is_some())
    }

    // ── Parquet Operations ───────────────────────────────────────────

    /// Stream `RecordBatch`es from a Parquet file without loading it entirely.
    ///
    /// Uses `ObjectStore`-backed async reader. Only the footer is read eagerly;
    /// column data is fetched on demand. Pass `&[]` for `columns` to read all.
    pub async fn read_parquet_file_stream(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        file: &str,
        columns: &[&str],
    ) -> Result<crate::parquet::reader::RecordBatchFileStream> {
        let store = self.object_store_internal()?;
        let pr = self.get_file_path(table_type, table_id, user_id, file);
        let object_path = self.to_object_path(&pr.relative_path)?;
        crate::parquet::reader::parse_parquet_stream(store, &object_path, columns).await
    }

    /// Write `RecordBatch`es as Parquet with bloom filters.
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
        let parquet_bytes = serialize_to_parquet(schema, batches, bloom_filter_columns)?;
        let size_bytes = parquet_bytes.len() as u64;
        self.put(table_type, table_id, user_id, filename, parquet_bytes).await?;
        Ok(ParquetWriteResult { size_bytes })
    }

    /// List Parquet filenames under a table's storage path (sorted, deduplicated).
    pub async fn list_parquet_files(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Vec<String>> {
        let list_result = self.list(table_type, table_id, user_id).await?;
        Ok(extract_parquet_filenames(&list_result))
    }

    // ── Manifest Convenience ─────────────────────────────────────────

    /// Read and parse `manifest.json` (sync). Returns `None` if not found.
    pub fn read_manifest_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<serde_json::Value>> {
        match self.get_sync(table_type, table_id, user_id, "manifest.json") {
            Ok(res) => serde_json::from_slice(&res.data)
                .map(Some)
                .map_err(|e| FilestoreError::Format(format!("Invalid manifest json: {}", e))),
            Err(FilestoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Serialize and write `manifest.json` (sync).
    pub fn write_manifest_sync(
        &self,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &serde_json::Value,
    ) -> Result<()> {
        let data = serde_json::to_vec_pretty(manifest)
            .map_err(|e| FilestoreError::Format(format!("Failed to serialize manifest: {}", e)))?;
        self.put_sync(table_type, table_id, user_id, "manifest.json", Bytes::from(data))?;
        Ok(())
    }

    // ── Sync Wrappers ────────────────────────────────────────────────
    //
    // Thin delegates to async methods via `run_blocking`. Used by flush
    // paths and other sync contexts.

    pub fn list_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>) -> Result<ListResult> {
        let (tid, uid) = (table_id.clone(), user_id.cloned());
        run_blocking(|| async { self.list(table_type, &tid, uid.as_ref()).await })
    }

    pub fn list_parquet_files_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>) -> Result<Vec<String>> {
        let (tid, uid) = (table_id.clone(), user_id.cloned());
        run_blocking(|| async { self.list_parquet_files(table_type, &tid, uid.as_ref()).await })
    }

    pub fn get_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>, filename: &str) -> Result<GetResult> {
        let (tid, uid, f) = (table_id.clone(), user_id.cloned(), filename.to_string());
        run_blocking(|| async { self.get(table_type, &tid, uid.as_ref(), &f).await })
    }

    pub fn put_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>, filename: &str, data: Bytes) -> Result<PutResult> {
        let (tid, uid, f) = (table_id.clone(), user_id.cloned(), filename.to_string());
        run_blocking(|| async { self.put(table_type, &tid, uid.as_ref(), &f, data).await })
    }

    pub fn delete_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>, filename: &str) -> Result<DeleteResult> {
        let (tid, uid, f) = (table_id.clone(), user_id.cloned(), filename.to_string());
        run_blocking(|| async { self.delete(table_type, &tid, uid.as_ref(), &f).await })
    }

    pub fn head_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>, filename: &str) -> Result<FileInfo> {
        let (tid, uid, f) = (table_id.clone(), user_id.cloned(), filename.to_string());
        run_blocking(|| async { self.head(table_type, &tid, uid.as_ref(), &f).await })
    }

    pub fn rename_sync(&self, table_type: TableType, table_id: &TableId, user_id: Option<&UserId>, from: &str, to: &str) -> Result<RenameResult> {
        let (tid, uid, from, to) = (table_id.clone(), user_id.cloned(), from.to_string(), to.to_string());
        run_blocking(|| async { self.rename(table_type, &tid, uid.as_ref(), &from, &to).await })
    }

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
        let (tid, uid, f) = (table_id.clone(), user_id.cloned(), filename.to_string());
        run_blocking(|| async {
            self.write_parquet(table_type, &tid, uid.as_ref(), &f, schema, batches, bloom_filter_columns).await
        })
    }

    // ── Cache Management ─────────────────────────────────────────────

    /// Invalidate the cached `ObjectStore` (forces rebuild on next use).
    pub fn invalidate_object_store(&self) {
        *self.object_store.write() = None;
    }

    /// Check if `ObjectStore` has been initialized.
    pub fn is_object_store_initialized(&self) -> bool {
        self.object_store.read().is_some()
    }
}

/// Extract sorted, deduplicated `.parquet` filenames from a list result.
fn extract_parquet_filenames(list_result: &ListResult) -> Vec<String> {
    let prefix = list_result.prefix.trim_end_matches('/');
    let mut files: Vec<String> = list_result
        .paths
        .iter()
        .filter_map(|path| {
            let suffix = if path.starts_with(prefix) {
                path[prefix.len()..].trim_start_matches('/')
            } else {
                path.rsplit('/').next().unwrap_or(path.as_str())
            };
            suffix.ends_with(".parquet").then(|| suffix.to_string())
        })
        .collect();
    files.sort();
    files.dedup();
    files
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
        cached
            .put_sync(TableType::Shared, &table_id, None, "overwrite.txt", Bytes::from("version 1"))
            .unwrap();

        let v1 = cached.get_sync(TableType::Shared, &table_id, None, "overwrite.txt").unwrap();
        assert_eq!(v1.data, Bytes::from("version 1"));

        // Overwrite with version 2
        cached
            .put_sync(TableType::Shared, &table_id, None, "overwrite.txt", Bytes::from("version 2"))
            .unwrap();

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
