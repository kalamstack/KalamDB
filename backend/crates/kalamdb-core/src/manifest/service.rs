//! Unified ManifestService for batch file metadata tracking.
//!
//! Provides manifest.json management with two-tier caching:
//! 1. Hot cache (moka) for sub-millisecond lookups with automatic TTI-based eviction
//! 2. Persistent cache (RocksDB) for crash recovery
//! 3. Cold storage (filestore) for manifest.json files
//!
//! Key type: (TableId, Option<UserId>) for type-safe cache access.

use kalamdb_commons::ids::SeqId;
use kalamdb_commons::{ManifestId, TableId, UserId};
use kalamdb_configs::ManifestCacheSettings;
use kalamdb_filestore::StorageRegistry;
use kalamdb_store::{StorageBackend, StorageError};
use kalamdb_system::providers::ManifestTableProvider;
use kalamdb_system::{
    FileSubfolderState, Manifest, ManifestCacheEntry, SegmentMetadata, SyncState,
};
use kalamdb_system::{
    ManifestService as ManifestServiceTrait, SchemaRegistry as SchemaRegistryTrait,
};
use kalamdb_tables::TableError;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

const MAX_MANIFEST_SCAN_LIMIT: usize = 100000;

/// Unified ManifestService with hot cache + RocksDB persistence + cold storage.
///
/// Architecture:
/// - Persistent store: RocksDB manifest_cache column family for crash recovery
/// - Cold store: manifest.json files in filestore (S3/local filesystem)
pub struct ManifestService {
    /// Provider wrapping the store
    provider: Arc<ManifestTableProvider>,

    /// Configuration settings
    config: ManifestCacheSettings,

    /// Optional registries for path/object store resolution.
    ///
    /// In production these are injected via `new_with_registries()`
    schema_registry: Option<Arc<dyn SchemaRegistryTrait<Error = TableError>>>,
    storage_registry: Option<Arc<StorageRegistry>>,
}

impl ManifestService {
    fn delete_manifest_ids(&self, keys: Vec<ManifestId>) -> Result<usize, StorageError> {
        let deleted = keys.len();
        if deleted > 0 {
            self.provider.delete_manifest_ids_batch(&keys)?;
        }
        Ok(deleted)
    }

    /// Create a new ManifestService
    pub fn new(provider: Arc<ManifestTableProvider>, config: ManifestCacheSettings) -> Self {
        Self {
            provider,
            config,
            schema_registry: None,
            storage_registry: None,
        }
    }

    /// Create a ManifestService with injected registries (compat helper for tests).
    pub fn new_with_registries(
        backend: Arc<dyn StorageBackend>,
        _base_path: String,
        config: ManifestCacheSettings,
        schema_registry: Arc<dyn SchemaRegistryTrait<Error = TableError>>,
        storage_registry: Arc<StorageRegistry>,
    ) -> Self {
        let provider = Arc::new(ManifestTableProvider::new(backend));
        let mut service = Self::new(provider, config);
        service.set_schema_registry(schema_registry);
        service.set_storage_registry(storage_registry);
        service
    }

    /// Set SchemaRegistry (break circular dependency)
    pub fn set_schema_registry(
        &mut self,
        registry: Arc<dyn SchemaRegistryTrait<Error = TableError>>,
    ) {
        self.schema_registry = Some(registry);
    }

    /// Set StorageRegistry
    pub fn set_storage_registry(&mut self, registry: Arc<StorageRegistry>) {
        self.storage_registry = Some(registry);
    }

    // Internal helper to get registries (panics if not set in production flows)
    fn get_schema_registry(&self) -> &Arc<dyn SchemaRegistryTrait<Error = TableError>> {
        self.schema_registry
            .as_ref()
            .expect("SchemaRegistry not initialized in ManifestService")
    }

    fn get_storage_registry(&self) -> &Arc<StorageRegistry> {
        self.storage_registry
            .as_ref()
            .expect("StorageRegistry not initialized in ManifestService")
    }

    // ========== Cache Operations (now mostly passthrough to RocksDB) ==========

    /// Get or load a manifest cache entry.
    ///
    /// Flow:
    /// 1. Check RocksDB CF
    /// 2. Return Option<Arc<ManifestCacheEntry>>
    pub fn get_or_load(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());
        match self.provider.get_cache_entry(&rocksdb_key) {
            Ok(Some(entry)) => Ok(Some(Arc::new(entry))),
            Ok(None) => Ok(None),
            Err(StorageError::SerializationError(err)) => {
                warn!(
                    "Manifest cache entry corrupted for key {}: {} (dropping)",
                    rocksdb_key.as_str(),
                    err
                );
                let _ = self.provider.delete_cache_entry(&rocksdb_key);
                Ok(None)
            },
            Err(err) => Err(err),
        }
    }

    /// Async version of get_or_load to avoid blocking the tokio runtime.
    pub async fn get_or_load_async(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());
        match self.provider.get_cache_entry_async(&rocksdb_key).await {
            Ok(Some(entry)) => Ok(Some(Arc::new(entry))),
            Ok(None) => Ok(None),
            Err(StorageError::SerializationError(err)) => {
                warn!(
                    "Manifest cache entry corrupted for key {}: {} (dropping)",
                    rocksdb_key.as_str(),
                    err
                );
                let _ = self.provider.delete_cache_entry_async(&rocksdb_key).await;
                Ok(None)
            },
            Err(err) => Err(err),
        }
    }

    /// Count all cached manifest entries.
    pub fn count(&self) -> Result<usize, StorageError> {
        self.provider.count_entries()
    }

    /// Compute max weighted capacity based on configuration.
    pub fn max_weighted_capacity(&self) -> usize {
        self.config.max_entries * self.config.user_table_weight_factor as usize
    }

    /// Update manifest cache after successful flush.
    ///
    /// Sets sync_state to InSync. Index automatically updated by IndexedEntityStore.
    pub fn update_after_flush(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
        etag: Option<String>,
    ) -> Result<(), StorageError> {
        // Index automatically updated by IndexedEntityStore when state changes
        self.upsert_cache_entry(table_id, user_id, manifest, etag, SyncState::InSync)
    }

    /// Stage manifest metadata in the cache before the first flush writes manifest.json to disk.
    pub fn stage_before_flush(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
    ) -> Result<(), StorageError> {
        self.upsert_cache_entry(table_id, user_id, manifest, None, SyncState::InSync)
    }

    /// Mark a cache entry as stale.
    pub fn mark_as_stale(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());

        if let Some(old_entry) = self.provider.get_cache_entry(&rocksdb_key)? {
            let mut new_entry = old_entry.clone();
            new_entry.mark_stale();
            self.provider
                .update_cache_entry_with_old(&rocksdb_key, &old_entry, &new_entry)?;
        }

        Ok(())
    }

    /// Mark a cache entry as having an error state.
    pub fn mark_as_error(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());

        match self.provider.get_cache_entry(&rocksdb_key) {
            Ok(Some(old_entry)) => {
                let mut new_entry = old_entry.clone();
                new_entry.mark_error();
                self.provider
                    .update_cache_entry_with_old(&rocksdb_key, &old_entry, &new_entry)?;
            },
            Ok(None) => {},
            Err(StorageError::SerializationError(err)) => {
                warn!(
                    "Manifest cache entry corrupted for key {}: {} (dropping)",
                    rocksdb_key.as_str(),
                    err
                );
                let _ = self.provider.delete_cache_entry(&rocksdb_key);
            },
            Err(err) => return Err(err),
        }

        Ok(())
    }

    /// Mark a cache entry as syncing (flush in progress).
    pub fn mark_syncing(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());

        match self.provider.get_cache_entry(&rocksdb_key) {
            Ok(Some(old_entry)) => {
                let mut new_entry = old_entry.clone();
                new_entry.mark_syncing();
                self.provider
                    .update_cache_entry_with_old(&rocksdb_key, &old_entry, &new_entry)?;
            },
            Ok(None) => {},
            Err(StorageError::SerializationError(err)) => {
                warn!(
                    "Manifest cache entry corrupted for key {}: {} (dropping)",
                    rocksdb_key.as_str(),
                    err
                );
                let _ = self.provider.delete_cache_entry(&rocksdb_key);
            },
            Err(err) => return Err(err),
        }

        Ok(())
    }

    /// Mark a cache entry as having pending writes (hot data not yet flushed to cold storage).
    ///
    /// This should be called after any write operation (INSERT, UPDATE, DELETE) to indicate
    /// that the RocksDB hot store has data that needs to be flushed to Parquet cold storage.
    /// The sync_state will transition from InSync to PendingWrite.
    ///
    /// Index automatically updated by IndexedEntityStore for O(1) flush job discovery.
    pub fn mark_pending_write(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());

        match self.provider.get_cache_entry(&rocksdb_key) {
            Ok(Some(old_entry)) => {
                if old_entry.sync_state == SyncState::PendingWrite {
                    return Ok(());
                }

                let mut new_entry = old_entry.clone();
                new_entry.mark_pending_write();
                self.provider
                    .update_cache_entry_with_old(&rocksdb_key, &old_entry, &new_entry)?;

                // Index automatically updated by IndexedEntityStore

                debug!(
                    "Marked manifest entry as pending_write: table={}, user={:?}",
                    table_id,
                    user_id.map(|u| u.as_str())
                );
            },
            Ok(None) => {
                // If no cache entry exists yet, create one with PendingWrite state
                // This shouldn't happen in normal flow since ensure_manifest_ready is called first
                warn!(
                    "mark_pending_write called but no cache entry exists: table={}, user={:?}",
                    table_id,
                    user_id.map(|u| u.as_str())
                );
            },
            Err(StorageError::SerializationError(err)) => {
                warn!(
                    "Manifest cache entry corrupted for key {}: {} (dropping)",
                    rocksdb_key.as_str(),
                    err
                );
                let _ = self.provider.delete_cache_entry(&rocksdb_key);
            },
            Err(err) => return Err(err),
        }

        Ok(())
    }

    /// Validate freshness of cached entry based on TTL.
    pub fn validate_freshness(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<bool, StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());

        if let Some(entry) = self.provider.get_cache_entry(&rocksdb_key)? {
            let now = chrono::Utc::now().timestamp_millis();
            Ok(!entry.is_stale(self.config.ttl_millis(), now))
        } else {
            Ok(false)
        }
    }

    /// Invalidate (delete) a cache entry.
    pub fn invalidate(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());
        self.provider.delete_cache_entry(&rocksdb_key)
    }

    /// Invalidate all cache entries for a table (all users + shared).
    pub fn invalidate_table(&self, table_id: &TableId) -> Result<usize, StorageError> {
        // Use table prefix to include ALL scopes (shared + all users)
        let prefix = ManifestId::table_prefix(table_id);
        let keys = self.provider.scan_manifest_ids_with_raw_prefix(
            &prefix,
            None,
            MAX_MANIFEST_SCAN_LIMIT,
        )?;
        let invalidated = self.delete_manifest_ids(keys)?;

        debug!("Invalidated {} manifest cache entries for table {}", invalidated, table_id);

        Ok(invalidated)
    }

    /// Check if a cache key is currently in the hot cache (RAM).
    /// With no hot cache, we check RocksDB existence.
    pub fn is_in_hot_cache(&self, table_id: &TableId, user_id: Option<&UserId>) -> bool {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());
        self.provider.get_cache_entry(&rocksdb_key).unwrap_or(None).is_some()
    }

    /// Check if a cache key string is in hot cache (for system.manifest table compatibility).
    pub fn is_in_hot_cache_by_string(&self, cache_key_str: &str) -> bool {
        let rocksdb_key = ManifestId::from(cache_key_str);
        self.provider.get_cache_entry(&rocksdb_key).unwrap_or(None).is_some()
    }

    /// Evict stale manifest entries from RocksDB.
    ///
    /// TODO: Optimize with secondary index on `last_refreshed` timestamp.
    /// Currently scans all manifests - at scale, maintain a BTree/skip-list
    /// or RocksDB secondary index to find stale entries efficiently O(log N)
    /// instead of O(N) full scan.
    pub fn evict_stale_entries(&self, ttl_seconds: i64) -> Result<usize, StorageError> {
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - (ttl_seconds * 1000);
        let entries = self.provider.scan_manifest_entries(MAX_MANIFEST_SCAN_LIMIT)?;

        let delete_keys: Vec<ManifestId> = entries
            .into_iter()
            .filter_map(|(key, entry)| {
                if entry.last_refreshed_millis() < cutoff {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();

        let evicted_count = self.delete_manifest_ids(delete_keys)?;

        info!(
            "Manifest eviction: removed {} stale entries (ttl_seconds={}, cutoff={})",
            evicted_count, ttl_seconds, cutoff
        );

        Ok(evicted_count)
    }

    /// Get cache configuration.
    pub fn config(&self) -> &ManifestCacheSettings {
        &self.config
    }

    // ========== Pending Write Index Operations ==========

    /// Iterator over all manifests with pending writes.
    ///
    /// Uses the pending-write index (index 0) for O(1) discovery.
    pub fn pending_manifest_ids_iter(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<ManifestId, StorageError>> + Send + '_>, StorageError>
    {
        let iter = self
            .provider
            .pending_manifest_ids_iter(None, None)
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mapped = iter.map(|res| res.map_err(|e| StorageError::Other(e.to_string())));
        Ok(Box::new(mapped))
    }

    /// Get all manifests with pending writes.
    ///
    /// Uses the pending-write index (index 0) for O(1) discovery.
    pub fn get_pending_manifests(&self) -> Result<Vec<ManifestId>, StorageError> {
        self.provider
            .pending_manifest_ids()
            .map_err(|e| StorageError::Other(e.to_string()))
    }

    /// Get pending manifests for a specific table.
    pub fn get_pending_for_table(
        &self,
        table_id: &TableId,
    ) -> Result<Vec<ManifestId>, StorageError> {
        self.provider
            .pending_manifest_ids_for_table(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))
    }

    /// Check if a manifest has pending writes.
    pub fn has_pending_writes(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<bool, StorageError> {
        let manifest_id = ManifestId::new(table_id.clone(), user_id.cloned());
        self.provider
            .pending_exists(&manifest_id)
            .map_err(|e| StorageError::Other(e.to_string()))
    }

    /// Get count of pending manifests.
    pub fn pending_count(&self) -> Result<usize, StorageError> {
        self.provider.pending_count().map_err(|e| StorageError::Other(e.to_string()))
    }

    // ========== Cold Storage Operations (formerly ManifestService) ==========

    /// Create an in-memory manifest for a table scope.
    pub fn create_manifest(&self, table_id: &TableId, user_id: Option<&UserId>) -> Manifest {
        Manifest::new(table_id.clone(), user_id.cloned())
    }

    /// Ensure a manifest exists (checking cache, then disk, otherwise creating in-memory).
    pub fn ensure_manifest_initialized(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        // 1. Check Hot Store (Cache)
        if let Some(entry) = self.get_or_load(table_id, user_id)? {
            return Ok(entry.manifest.clone());
        }

        // 2. Check Cold Store (via filestore)
        match self.read_manifest(table_id, user_id) {
            Ok(manifest) => {
                self.stage_before_flush(table_id, user_id, &manifest)?;
                return Ok(manifest);
            },
            Err(_) => {
                // Manifest doesn't exist or can't be read, create new one
            },
        }

        // 3. Create New (In-Memory only)
        let manifest = self.create_manifest(table_id, user_id);
        Ok(manifest)
    }

    /// Update manifest: append segment to cache.
    pub fn update_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        segment: SegmentMetadata,
    ) -> Result<Manifest, StorageError> {
        // Ensure manifest is loaded/initialized
        let mut manifest = self.ensure_manifest_initialized(table_id, user_id)?;

        // Add segment
        manifest.add_segment(segment);

        self.upsert_cache_entry(table_id, user_id, &manifest, None, SyncState::PendingWrite)?;

        Ok(manifest)
    }

    /// Update manifest in cache using a caller-provided mutator.
    ///
    /// This is used for metadata updates that are not segment appends
    /// (for example vector index watermark/snapshot pointers).
    pub fn update_manifest_with<F>(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        mutator: F,
    ) -> Result<Manifest, StorageError>
    where
        F: FnOnce(&mut Manifest),
    {
        let mut manifest = self.ensure_manifest_initialized(table_id, user_id)?;
        mutator(&mut manifest);
        self.upsert_cache_entry(table_id, user_id, &manifest, None, SyncState::PendingWrite)?;
        Ok(manifest)
    }

    /// Persist a manifest to cold storage and mark cache state as in-sync.
    pub fn persist_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
    ) -> Result<(), StorageError> {
        self.upsert_cache_entry(table_id, user_id, manifest, None, SyncState::PendingWrite)?;
        self.flush_manifest(table_id, user_id)?;
        self.update_after_flush(table_id, user_id, manifest, None)?;
        Ok(())
    }

    /// Clear all manifest segments for a table scope and delete their associated Parquet files.
    ///
    /// This is the first step of cold-storage compaction cleanup for scopes that
    /// resolve to zero live rows after flush. It keeps an empty manifest on disk
    /// so future flushes can continue with a clean state.
    pub fn clear_segments_and_delete_files(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<usize, StorageError> {
        let schema_registry = self.get_schema_registry();
        let storage_registry = self.get_storage_registry();

        let table = schema_registry
            .get_table_if_exists(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| StorageError::Other(format!("Table not found: {}", table_id)))?;
        let storage_id = schema_registry
            .get_storage_id(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let storage_cached = storage_registry
            .get_cached(&storage_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| {
                StorageError::Other(format!("Storage '{}' not found in registry", storage_id))
            })?;

        let mut manifest = self.ensure_manifest_initialized(table_id, user_id)?;
        if manifest.segments.is_empty() {
            return Ok(0);
        }

        let segment_paths =
            manifest.segments.iter().map(|segment| segment.path.clone()).collect::<Vec<_>>();

        manifest.segments.clear();
        manifest.last_sequence_number = 0;
        manifest.updated_at = chrono::Utc::now().timestamp_millis();
        manifest.version += 1;

        self.persist_manifest(table_id, user_id, &manifest)?;

        let mut deleted_files = 0;
        for path in segment_paths {
            let delete_result = storage_cached
                .delete_sync(table.table_type, table_id, user_id, &path)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
            if delete_result.existed {
                deleted_files += 1;
            }
        }

        Ok(deleted_files)
    }

    /// Flush manifest: Write to Cold Store (storage via StorageCached).
    pub fn flush_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        if let Some(entry) = self.get_or_load(table_id, user_id)? {
            let schema_registry = self.get_schema_registry();
            let storage_registry = self.get_storage_registry();

            let table = schema_registry
                .get_table_if_exists(table_id)
                .map_err(|e| StorageError::Other(e.to_string()))?
                .ok_or_else(|| StorageError::Other(format!("Table not found: {}", table_id)))?;
            let storage_id = schema_registry
                .get_storage_id(table_id)
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let storage_cached = storage_registry
                .get_cached(&storage_id)
                .map_err(|e| StorageError::Other(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Other(format!("Storage '{}' not found in registry", storage_id))
                })?;

            // Serialize to Value for write_manifest_sync
            let json_value = serde_json::to_value(&entry.manifest).map_err(|e| {
                StorageError::SerializationError(format!("Failed to serialize manifest: {}", e))
            })?;

            storage_cached
                .write_manifest_sync(table.table_type, table_id, user_id, &json_value)
                .map_err(|e| StorageError::IoError(e.to_string()))?;

            debug!("Flushed manifest for {} (ver: {})", table_id, entry.manifest.version);
        } else {
            warn!("Attempted to flush manifest for {} but it was not in cache", table_id);
        }
        Ok(())
    }

    /// Read manifest.json from storage.
    pub fn read_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        let schema_registry = self.get_schema_registry();
        let storage_registry = self.get_storage_registry();

        let table = schema_registry
            .get_table_if_exists(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| StorageError::Other(format!("Table not found: {}", table_id)))?;
        let storage_id = schema_registry
            .get_storage_id(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let storage_cached = storage_registry
            .get_cached(&storage_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| {
                StorageError::Other(format!("Storage '{}' not found in registry", storage_id))
            })?;

        let manifest_value = storage_cached
            .read_manifest_sync(table.table_type, table_id, user_id)
            .map_err(|e| StorageError::IoError(e.to_string()))?
            .ok_or_else(|| StorageError::Other("Manifest not found".to_string()))?;

        serde_json::from_value(manifest_value).map_err(|e| {
            StorageError::SerializationError(format!("Failed to deserialize manifest: {}", e))
        })
    }

    /// Rebuild manifest from Parquet footers.
    pub fn rebuild_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        let schema_registry = self.get_schema_registry();
        let storage_registry = self.get_storage_registry();

        let table = schema_registry
            .get_table_if_exists(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| StorageError::Other(format!("Table not found: {}", table_id)))?;
        let storage_id = schema_registry
            .get_storage_id(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let storage_cached = storage_registry
            .get_cached(&storage_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| {
                StorageError::Other(format!("Storage '{}' not found in registry", storage_id))
            })?;

        let mut manifest = Manifest::new(table_id.clone(), user_id.cloned());

        // List all parquet files using the optimized method
        let mut batch_files = storage_cached
            .list_parquet_files_sync(table.table_type, table_id, user_id)
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        // Filter only batch files (exclude compaction temp files etc)
        batch_files.retain(|f| f.contains("batch-"));
        batch_files.sort();

        // Batch fetch metadata if possible (TODO: Add bulk head to filestore)
        // For now, sequential head
        for file_name in &batch_files {
            let id = file_name.clone();

            // Get file size via head operation
            let file_info = storage_cached
                .head_sync(table.table_type, table_id, user_id, file_name)
                .map_err(|e| StorageError::IoError(e.to_string()))?;

            let size_bytes = file_info.size as u64;

            // Create segment metadata (we don't parse full footer for rebuild, just size)
            let segment = SegmentMetadata::new(
                id,
                file_name.clone(),
                HashMap::new(),
                SeqId::from(0i64),
                SeqId::from(0i64),
                0,
                size_bytes,
            );
            manifest.add_segment(segment);
        }

        self.upsert_cache_entry(table_id, user_id, &manifest, None, SyncState::InSync)?;

        // Write manifest to storage using direct helper (Task 102)
        let json_value = serde_json::to_value(&manifest).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize manifest: {}", e))
        })?;

        storage_cached
            .write_manifest_sync(table.table_type, table_id, user_id, &json_value)
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        Ok(manifest)
    }

    /// Validate manifest consistency.
    pub fn validate_manifest(&self, _manifest: &Manifest) -> Result<(), StorageError> {
        // Basic validation - can be expanded
        Ok(())
    }

    /// Public helper for consumers that need the resolved manifest.json path.
    pub fn manifest_path(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<String, StorageError> {
        let schema_registry = self.get_schema_registry();
        let storage_registry = self.get_storage_registry();

        let table = schema_registry
            .get_table_if_exists(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| StorageError::Other(format!("Table not found: {}", table_id)))?;
        let storage_id = schema_registry
            .get_storage_id(table_id)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let storage_cached = storage_registry
            .get_cached(&storage_id)
            .map_err(|e| StorageError::Other(e.to_string()))?
            .ok_or_else(|| {
                StorageError::Other(format!("Storage '{}' not found in registry", storage_id))
            })?;

        let manifest_path_result =
            storage_cached.get_manifest_path(table.table_type, table_id, user_id);

        Ok(manifest_path_result.full_path)
    }

    pub fn get_manifest_user_ids(&self, table_id: &TableId) -> Result<Vec<UserId>, StorageError> {
        // Use storekey-encoded prefix for proper RocksDB scan
        let prefix = ManifestId::table_prefix(table_id);
        log::debug!(
            "[MANIFEST_CACHE_DEBUG] get_manifest_user_ids: table={} prefix_len={}",
            table_id,
            prefix.len()
        );

        // Use scan_keys_with_raw_prefix to only fetch keys (no value deserialization)
        let keys: Vec<ManifestId> = self.provider.scan_manifest_ids_with_raw_prefix(
            &prefix,
            None,
            MAX_MANIFEST_SCAN_LIMIT,
        )?;

        let mut user_ids = HashSet::new();

        for manifest_id in keys {
            log::debug!(
                "[MANIFEST_CACHE_DEBUG] get_manifest_user_ids: found manifest_id={}",
                manifest_id.as_str()
            );
            if let Some(user_id) = manifest_id.user_id() {
                user_ids.insert(user_id.clone());
            }
        }

        log::debug!(
            "[MANIFEST_CACHE_DEBUG] get_manifest_user_ids: result user_ids={:?}",
            user_ids.iter().map(|u| u.as_str()).collect::<Vec<_>>()
        );

        Ok(user_ids.into_iter().collect())
    }

    // ========== Private Helper Methods ==========

    fn upsert_cache_entry(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
        etag: Option<String>,
        sync_state: SyncState,
    ) -> Result<(), StorageError> {
        let rocksdb_key = ManifestId::new(table_id.clone(), user_id.cloned());
        let now = chrono::Utc::now().timestamp_millis();

        log::debug!(
            "[MANIFEST_CACHE_DEBUG] upsert_cache_entry: key={} segments={} sync_state={:?}",
            rocksdb_key.as_str(),
            manifest.segments.len(),
            sync_state
        );

        let entry = ManifestCacheEntry::new(manifest.clone(), etag, now, sync_state);
        let existing = self.provider.get_cache_entry(&rocksdb_key)?;
        match existing {
            Some(old_entry) => {
                self.provider.update_cache_entry_with_old(&rocksdb_key, &old_entry, &entry)?;
            },
            None => {
                self.provider.put_cache_entry(&rocksdb_key, &entry)?;
            },
        }

        Ok(())
    }

    // ========== File Subfolder State Methods ==========

    /// Get the file subfolder state for a shared table (user_id = None).
    /// Returns None if files are not enabled for this table.
    pub fn get_file_subfolder_state(
        &self,
        table_id: &TableId,
    ) -> Result<Option<FileSubfolderState>, StorageError> {
        let entry = self.get_or_load(table_id, None)?;
        match entry {
            Some(cache_entry) => Ok(cache_entry.manifest.files.clone()),
            None => Ok(None),
        }
    }

    /// Update the file subfolder state for a shared table.
    /// This is used when files are uploaded and the subfolder needs rotation.
    pub fn update_file_subfolder_state(
        &self,
        table_id: &TableId,
        state: FileSubfolderState,
    ) -> Result<(), StorageError> {
        let entry = self.get_or_load(table_id, None)?;
        let mut manifest = match entry {
            Some(cache_entry) => cache_entry.manifest.clone(),
            None => {
                // Create a minimal manifest for files tracking
                Manifest::new(table_id.clone(), None)
            },
        };
        manifest.files = Some(state);
        self.upsert_cache_entry(table_id, None, &manifest, None, SyncState::PendingWrite)
    }

    // Private helper methods removed - now using StorageCached operations directly
}

#[async_trait::async_trait]
impl ManifestServiceTrait for ManifestService {
    fn get_or_load(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        self.get_or_load(table_id, user_id)
    }

    async fn get_or_load_async(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        self.get_or_load_async(table_id, user_id).await
    }

    fn validate_manifest(&self, manifest: &Manifest) -> Result<(), StorageError> {
        self.validate_manifest(manifest)
    }

    fn mark_as_stale(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        self.mark_as_stale(table_id, user_id)
    }

    fn rebuild_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        self.rebuild_manifest(table_id, user_id)
    }

    fn mark_pending_write(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        self.mark_pending_write(table_id, user_id)
    }

    fn ensure_manifest_initialized(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        self.ensure_manifest_initialized(table_id, user_id)
    }

    fn stage_before_flush(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
    ) -> Result<(), StorageError> {
        self.stage_before_flush(table_id, user_id, manifest)
    }

    fn get_manifest_user_ids(&self, table_id: &TableId) -> Result<Vec<UserId>, StorageError> {
        self.get_manifest_user_ids(table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::{NamespaceId, TableName};
    use kalamdb_store::test_utils::InMemoryBackend;
    use kalamdb_store::StorageBackend;

    fn create_test_service() -> ManifestService {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let provider = Arc::new(ManifestTableProvider::new(backend));
        let config = ManifestCacheSettings {
            eviction_interval_seconds: 300,
            max_entries: 1000,
            eviction_ttl_days: 7,
            user_table_weight_factor: 10,
        };
        ManifestService::new(provider, config)
    }

    fn create_test_manifest(table_id: &TableId, user_id: Option<&UserId>) -> Manifest {
        Manifest::new(table_id.clone(), user_id.cloned())
    }

    fn build_table_id(ns: &str, tbl: &str) -> TableId {
        TableId::new(NamespaceId::new(ns), TableName::new(tbl))
    }

    #[test]
    fn test_create_manifest() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "products");

        let manifest = service.create_manifest(&table_id, None);

        assert_eq!(manifest.table_id, table_id);
        assert_eq!(manifest.user_id, None);
        assert_eq!(manifest.segments.len(), 0);
    }

    #[test]
    fn test_get_or_load_miss() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "tbl1");

        let result = service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update_after_flush() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "tbl1");
        let manifest = create_test_manifest(&table_id, Some(&UserId::from("u_123")));

        service
            .update_after_flush(
                &table_id,
                Some(&UserId::from("u_123")),
                &manifest,
                Some("etag123".to_string()),
            )
            .unwrap();

        let cached = service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap();
        assert!(cached.is_some());
        let entry = cached.unwrap();
        assert_eq!(entry.etag, Some("etag123".to_string()));
        assert_eq!(entry.sync_state, SyncState::InSync);
    }

    #[test]
    fn test_hot_cache_hit() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "tbl1");
        let manifest = create_test_manifest(&table_id, Some(&UserId::from("u_123")));

        service
            .update_after_flush(&table_id, Some(&UserId::from("u_123")), &manifest, None)
            .unwrap();

        let result = service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap();
        assert!(result.is_some());

        assert!(service.is_in_hot_cache(&table_id, Some(&UserId::from("u_123"))));
    }

    #[test]
    fn test_invalidate() {
        let service = create_test_service();
        let namespace = NamespaceId::new("ns1");
        let table = TableName::new("tbl1");
        let table_id = TableId::new(namespace.clone(), table.clone());
        let manifest = create_test_manifest(&table_id, Some(&UserId::from("u_123")));

        service
            .update_after_flush(&table_id, Some(&UserId::from("u_123")), &manifest, None)
            .unwrap();

        assert!(service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap().is_some());

        service.invalidate(&table_id, Some(&UserId::from("u_123"))).unwrap();

        assert!(service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap().is_none());
    }

    #[test]
    fn test_mark_syncing_updates_state() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "tbl1");
        let manifest = create_test_manifest(&table_id, Some(&UserId::from("u_123")));

        service
            .update_after_flush(&table_id, Some(&UserId::from("u_123")), &manifest, None)
            .unwrap();

        let cached = service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap().unwrap();
        assert_eq!(cached.sync_state, SyncState::InSync);

        service.mark_syncing(&table_id, Some(&UserId::from("u_123"))).unwrap();

        let cached_after =
            service.get_or_load(&table_id, Some(&UserId::from("u_123"))).unwrap().unwrap();
        assert_eq!(cached_after.sync_state, SyncState::Syncing);
    }

    #[test]
    fn test_pending_write_index_integration() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "tbl1");
        let user_id = UserId::from("u_123");
        let manifest = create_test_manifest(&table_id, Some(&user_id));

        // Stage manifest first (creates entry in cache)
        service.stage_before_flush(&table_id, Some(&user_id), &manifest).unwrap();

        // Initially, pending index should be empty
        assert_eq!(service.pending_count().unwrap(), 0);
        assert!(!service.has_pending_writes(&table_id, Some(&user_id)).unwrap());

        // Mark as pending write
        service.mark_pending_write(&table_id, Some(&user_id)).unwrap();

        // Now should be in pending index
        assert_eq!(service.pending_count().unwrap(), 1);
        assert!(service.has_pending_writes(&table_id, Some(&user_id)).unwrap());

        // Get all pending - should return our entry
        let pending = service.get_pending_manifests().unwrap();
        assert_eq!(pending.len(), 1);

        // After flush, pending should be removed
        service.update_after_flush(&table_id, Some(&user_id), &manifest, None).unwrap();
        assert_eq!(service.pending_count().unwrap(), 0);
        assert!(!service.has_pending_writes(&table_id, Some(&user_id)).unwrap());
    }

    #[test]
    fn test_mark_pending_write_is_idempotent() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "tbl1");
        let user_id = UserId::from("u_123");
        let manifest = create_test_manifest(&table_id, Some(&user_id));

        service.stage_before_flush(&table_id, Some(&user_id), &manifest).unwrap();

        service.mark_pending_write(&table_id, Some(&user_id)).unwrap();
        let pending = service.get_or_load(&table_id, Some(&user_id)).unwrap().unwrap();
        let pending_last_refreshed = pending.last_refreshed_millis();
        assert_eq!(pending.sync_state, SyncState::PendingWrite);
        assert!(service.has_pending_writes(&table_id, Some(&user_id)).unwrap());

        service.mark_pending_write(&table_id, Some(&user_id)).unwrap();

        let pending_again = service.get_or_load(&table_id, Some(&user_id)).unwrap().unwrap();
        assert_eq!(pending_again.sync_state, SyncState::PendingWrite);
        assert_eq!(pending_again.last_refreshed_millis(), pending_last_refreshed);
        assert_eq!(service.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_get_pending_for_table() {
        let service = create_test_service();
        let table_id = build_table_id("ns1", "user_table");
        let user1 = UserId::from("user1");
        let user2 = UserId::from("user2");
        let other_table = build_table_id("ns1", "other_table");

        // Stage manifests for multiple users
        let manifest1 = create_test_manifest(&table_id, Some(&user1));
        let manifest2 = create_test_manifest(&table_id, Some(&user2));
        let other_manifest = create_test_manifest(&other_table, None);

        service.stage_before_flush(&table_id, Some(&user1), &manifest1).unwrap();
        service.stage_before_flush(&table_id, Some(&user2), &manifest2).unwrap();
        service.stage_before_flush(&other_table, None, &other_manifest).unwrap();

        // Mark all as pending
        service.mark_pending_write(&table_id, Some(&user1)).unwrap();
        service.mark_pending_write(&table_id, Some(&user2)).unwrap();
        service.mark_pending_write(&other_table, None).unwrap();

        // Get pending for specific table
        let pending = service.get_pending_for_table(&table_id).unwrap();
        assert_eq!(pending.len(), 2);

        // Total should be 3
        assert_eq!(service.pending_count().unwrap(), 3);
    }

    // #[test]
    // fn test_restore_from_rocksdb() {
    //     let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    //     let config = ManifestCacheSettings::default();

    //     let service1 = ManifestService::new(Arc::clone(&backend), config.clone());
    //     let table_id = build_table_id("ns1", "tbl1");
    //     let manifest = create_test_manifest(&table_id, Some(&UserId::from("u_123")));

    //     service1
    //         .update_after_flush(
    //             &table_id,
    //             Some(&UserId::from("u_123")),
    //             &manifest,
    //             None,
    //         )
    //         .unwrap();

    //     // Create new service (simulating restart)
    //     let service2 = ManifestService::new(backend, config);
    //     service2.restore_from_rocksdb().unwrap();

    //     let cached = service2
    //         .get_or_load(&table_id, Some(&UserId::from("u_123")))
    //         .unwrap();
    //     assert!(cached.is_some());
    // }
}
