//! System.manifest table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.manifest table.
//! Exposes manifest cache entries as a queryable system table.

use std::sync::{Arc, OnceLock, RwLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{models::rows::SystemTableRow, ManifestId, StorageKey, TableId};
use kalamdb_store::{entity_store::EntityStore, IndexedEntityStore, StorageBackend, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    create_manifest_indexes, manifest_arrow_schema, manifest_table_definition, Manifest, SyncState,
};
use crate::{
    error::{SystemError, SystemResultExt},
    providers::{
        base::{extract_filter_value, SimpleProviderDefinition},
        manifest::ManifestCacheEntry,
    },
    system_row_mapper::{model_to_system_row, system_row_to_model},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestStorageRow {
    cache_key: String,
    namespace_id: String,
    table_name: String,
    scope: String,
    etag: Option<String>,
    last_refreshed: i64,
    last_accessed: i64,
    in_memory: bool,
    sync_state: String,
    manifest_json: Value,
}

/// Callback type for checking if a cache key is in hot memory
pub type InMemoryChecker = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// System.manifest table provider using IndexedEntityStore architecture
pub struct ManifestTableProvider {
    store: IndexedEntityStore<ManifestId, SystemTableRow>,
    /// Optional callback to check if a cache key is in hot memory (injected from kalamdb-core)
    in_memory_checker: RwLock<Option<InMemoryChecker>>,
}

impl Clone for ManifestTableProvider {
    fn clone(&self) -> Self {
        let in_memory_checker = match self.in_memory_checker.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        Self {
            store: self.store.clone(),
            in_memory_checker: RwLock::new(in_memory_checker),
        }
    }
}

impl ManifestTableProvider {
    /// Create a new manifest table provider with indexes
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new ManifestTableProvider instance
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            crate::SystemTable::Manifest.column_family_name().expect("Manifest is a table"),
            create_manifest_indexes(),
        );

        Self {
            store,
            in_memory_checker: RwLock::new(None),
        }
    }

    /// Set the in-memory checker callback
    ///
    /// This callback is injected from kalamdb-core to check if a cache key
    /// is currently in the hot cache (RAM).
    pub fn set_in_memory_checker(&self, checker: InMemoryChecker) {
        if let Ok(mut guard) = self.in_memory_checker.write() {
            *guard = Some(checker);
        }
    }

    /// Get a typed manifest cache entry by key.
    pub fn get_cache_entry(
        &self,
        manifest_id: &ManifestId,
    ) -> Result<Option<ManifestCacheEntry>, StorageError> {
        let row = self.store.get(manifest_id)?;
        row.map(|value| Self::decode_manifest_row(&value).map_err(Self::to_storage_error))
            .transpose()
    }

    /// Async typed get by key.
    pub async fn get_cache_entry_async(
        &self,
        manifest_id: &ManifestId,
    ) -> Result<Option<ManifestCacheEntry>, StorageError> {
        let row = self.store.get_async(manifest_id.clone()).await?;
        row.map(|value| Self::decode_manifest_row(&value).map_err(Self::to_storage_error))
            .transpose()
    }

    /// Insert/replace a typed manifest cache entry.
    pub fn put_cache_entry(
        &self,
        manifest_id: &ManifestId,
        entry: &ManifestCacheEntry,
    ) -> Result<(), StorageError> {
        let row = Self::encode_manifest_row(entry).map_err(Self::to_storage_error)?;
        self.store.insert(manifest_id, &row)
    }

    /// Update with old and new typed entries (for index maintenance correctness).
    pub fn update_cache_entry_with_old(
        &self,
        manifest_id: &ManifestId,
        old_entry: &ManifestCacheEntry,
        new_entry: &ManifestCacheEntry,
    ) -> Result<(), StorageError> {
        let old_row = Self::encode_manifest_row(old_entry).map_err(Self::to_storage_error)?;
        let new_row = Self::encode_manifest_row(new_entry).map_err(Self::to_storage_error)?;
        self.store.update_with_old(manifest_id, Some(&old_row), &new_row)
    }

    pub fn delete_cache_entry(&self, manifest_id: &ManifestId) -> Result<(), StorageError> {
        self.store.delete(manifest_id)
    }

    pub async fn delete_cache_entry_async(
        &self,
        manifest_id: &ManifestId,
    ) -> Result<(), StorageError> {
        self.store.delete_async(manifest_id.clone()).await
    }

    pub fn count_entries(&self) -> Result<usize, StorageError> {
        self.store.count_all()
    }

    pub fn scan_manifest_entries(
        &self,
        limit: usize,
    ) -> Result<Vec<(ManifestId, ManifestCacheEntry)>, StorageError> {
        let rows = self.store.scan_all_typed(Some(limit), None, None)?;
        rows.into_iter()
            .map(|(key, row)| {
                Self::decode_manifest_row(&row)
                    .map(|entry| (key, entry))
                    .map_err(Self::to_storage_error)
            })
            .collect()
    }

    pub fn scan_manifest_ids_with_raw_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<ManifestId>, StorageError> {
        self.store.scan_keys_with_raw_prefix(prefix, start_key, limit)
    }

    pub fn delete_manifest_ids_batch(&self, ids: &[ManifestId]) -> Result<(), StorageError> {
        self.store.delete_batch(ids)
    }

    /// Check if a cache key is in hot memory
    fn is_in_memory(&self, cache_key: &str) -> bool {
        if let Ok(guard) = self.in_memory_checker.read() {
            if let Some(ref checker) = *guard {
                return checker(cache_key);
            }
        }
        false // Default to false if no checker is set
    }

    /// Scan all manifest cache entries and return as RecordBatch
    ///
    /// This is the main method used by DataFusion to read the table.
    ///
    /// Uses schema-driven array building for optimal performance and correctness.
    pub fn scan_to_record_batch(&self) -> Result<RecordBatch, SystemError> {
        let entries = self.scan_entries(None)?;
        self.build_batch_from_entries(entries)
    }

    /// Collect manifest entries with optional early limit.
    fn scan_entries(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(ManifestId, ManifestCacheEntry)>, SystemError> {
        let iter = self
            .store
            .scan_iterator(None, None)
            .map_err(|e| SystemError::Storage(e.to_string()))?;

        let mut entries = Vec::with_capacity(limit.unwrap_or(256));
        let mut count = 0usize;
        for row in iter {
            if limit.is_some_and(|lim| count >= lim) {
                break;
            }
            let (key, raw_row) = row.map_err(|e| SystemError::Storage(e.to_string()))?;
            let entry = Self::decode_manifest_row(&raw_row)?;
            entries.push((key, entry));
            count += 1;
        }
        Ok(entries)
    }

    /// Build a RecordBatch from materialized manifest entries.
    fn build_batch_from_entries(
        &self,
        entries: Vec<(ManifestId, ManifestCacheEntry)>,
    ) -> Result<RecordBatch, SystemError> {
        crate::build_record_batch!(
            schema: manifest_arrow_schema(),
            entries: entries,
            columns: [
                cache_keys => OptionalString(|entry| Some(entry.0.as_str())),
                namespace_ids => OptionalString(|entry| Some(entry.0.table_id().namespace_id().as_str())),
                table_names => OptionalString(|entry| Some(entry.0.table_id().table_name().as_str())),
                scopes => OptionalString(|entry| Some(entry.0.scope_str())),
                etags => OptionalString(|entry| entry.1.etag.as_deref()),
                last_refreshed_vals => Timestamp(|entry| Some(entry.1.last_refreshed_millis())),
                // last_accessed = last_refreshed (moka manages TTI internally).
                last_accessed_vals => Timestamp(|entry| Some(entry.1.last_refreshed_millis())),
                in_memory_vals => OptionalBoolean(|entry| Some(self.is_in_memory(&entry.0.as_str()))),
                sync_states => OptionalString(|entry| Some(entry.1.sync_state.to_string())),
                manifest_jsons => OptionalString(|entry| Some(entry.1.manifest_json()))
            ]
        )
        .into_arrow_error("Failed to create RecordBatch")
    }

    /// Build a RecordBatch from a single manifest entry (point lookup result).
    fn build_single_entry_batch(
        &self,
        manifest_id: &ManifestId,
        entry: &ManifestCacheEntry,
    ) -> Result<RecordBatch, SystemError> {
        self.build_batch_from_entries(vec![(manifest_id.clone(), entry.clone())])
    }

    /// Scan manifest entries with a limit (early termination).
    fn scan_to_record_batch_limited(&self, limit: usize) -> Result<RecordBatch, SystemError> {
        let entries = self.scan_entries(Some(limit))?;
        self.build_batch_from_entries(entries)
    }

    /// Iterator over pending manifest IDs (from the pending-write index).
    pub fn pending_manifest_ids_iter(
        &self,
        prefix: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Box<dyn Iterator<Item = Result<ManifestId, SystemError>> + Send + '_>, SystemError>
    {
        let iter = self
            .store
            .scan_index_keys_iter(0, prefix, limit)
            .map_err(|e| SystemError::Storage(e.to_string()))?;

        let mapped = iter.map(|res| res.map_err(|e| SystemError::Storage(e.to_string())));
        Ok(Box::new(mapped))
    }

    /// Return all pending manifest IDs (collected).
    pub fn pending_manifest_ids(&self) -> Result<Vec<ManifestId>, SystemError> {
        let iter = self.pending_manifest_ids_iter(None, None)?;
        let mut results = Vec::new();
        for item in iter {
            results.push(item?);
        }

        Ok(results)
    }

    /// Return pending manifest IDs for a specific table.
    pub fn pending_manifest_ids_for_table(
        &self,
        table_id: &TableId,
    ) -> Result<Vec<ManifestId>, SystemError> {
        let prefix_bytes = ManifestId::table_prefix(table_id);
        let iter = self.pending_manifest_ids_iter(Some(&prefix_bytes), None)?;
        let mut results = Vec::new();
        for item in iter {
            results.push(item?);
        }

        Ok(results)
    }

    /// Check if a specific manifest is pending (by key).
    pub fn pending_exists(&self, manifest_id: &ManifestId) -> Result<bool, SystemError> {
        let prefix_bytes = manifest_id.storage_key();
        self.store
            .exists_by_index(0, &prefix_bytes)
            .map_err(|e| SystemError::Storage(e.to_string()))
    }

    /// Count pending manifests via index scan.
    pub fn pending_count(&self) -> Result<usize, SystemError> {
        let iter = self.pending_manifest_ids_iter(None, None)?;
        let mut count = 0usize;
        for item in iter {
            item?;
            count += 1;
        }

        Ok(count)
    }
    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        // Check for primary key equality filter → O(1) point lookup
        if let Some(cache_key_str) = extract_filter_value(filters, "cache_key") {
            let manifest_id = ManifestId::from(cache_key_str.as_str());
            if let Ok(Some(row)) = self.store.get(&manifest_id) {
                let entry = Self::decode_manifest_row(&row)?;
                return self.build_single_entry_batch(&manifest_id, &entry);
            }
            // Empty result
            return Ok(RecordBatch::new_empty(manifest_arrow_schema()));
        }

        // With limit: use iterator with early termination
        if let Some(lim) = limit {
            return self.scan_to_record_batch_limited(lim);
        }

        // No filters/limit: full scan
        self.scan_to_record_batch()
    }

    fn encode_manifest_row(entry: &ManifestCacheEntry) -> Result<SystemTableRow, SystemError> {
        let manifest_id =
            ManifestId::new(entry.manifest.table_id.clone(), entry.manifest.user_id.clone());
        let last_refreshed = entry.last_refreshed_millis();
        let storage_row = ManifestStorageRow {
            cache_key: manifest_id.as_str().to_string(),
            namespace_id: entry.manifest.table_id.namespace_id().as_str().to_string(),
            table_name: entry.manifest.table_id.table_name().as_str().to_string(),
            scope: manifest_id.scope_str().to_string(),
            etag: entry.etag.clone(),
            last_refreshed,
            last_accessed: last_refreshed,
            in_memory: false,
            sync_state: entry.sync_state.to_string(),
            manifest_json: serde_json::to_value(&entry.manifest).map_err(|error| {
                SystemError::SerializationError(format!("manifest serialize failed: {error}"))
            })?,
        };

        model_to_system_row(&storage_row, &manifest_table_definition())
    }

    fn decode_manifest_row(row: &SystemTableRow) -> Result<ManifestCacheEntry, SystemError> {
        let storage_row: ManifestStorageRow =
            system_row_to_model(row, &manifest_table_definition())?;

        let manifest: Manifest = match storage_row.manifest_json {
            Value::String(json_text) => serde_json::from_str(&json_text).map_err(|error| {
                SystemError::SerializationError(format!("manifest deserialize failed: {error}"))
            })?,
            json_value => serde_json::from_value(json_value).map_err(|error| {
                SystemError::SerializationError(format!("manifest deserialize failed: {error}"))
            })?,
        };

        let sync_state = match storage_row.sync_state.as_str() {
            "in_sync" => SyncState::InSync,
            "pending_write" => SyncState::PendingWrite,
            "syncing" => SyncState::Syncing,
            "stale" => SyncState::Stale,
            "error" => SyncState::Error,
            value => {
                return Err(SystemError::SerializationError(format!(
                    "invalid sync_state value: {value}"
                )));
            },
        };

        Ok(ManifestCacheEntry::new(
            manifest,
            storage_row.etag,
            storage_row.last_refreshed,
            sync_state,
        ))
    }

    fn to_storage_error(err: SystemError) -> StorageError {
        StorageError::SerializationError(err.to_string())
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = ManifestTableProvider,
    table_name = kalamdb_commons::SystemTable::Manifest.table_name(),
    schema = manifest_arrow_schema()
);

crate::impl_simple_system_table_provider!(
    provider = ManifestTableProvider,
    key = ManifestId,
    value = SystemTableRow,
    definition = provider_definition,
    scan_all = scan_to_record_batch,
    scan_filtered = scan_to_batch_filtered
);

#[cfg(test)]
mod tests {
    use kalamdb_commons::{NamespaceId, TableId, TableName};
    use kalamdb_store::{entity_store::EntityStore, test_utils::InMemoryBackend};

    use super::*;
    use crate::providers::manifest::{Manifest, ManifestCacheEntry, SyncState};

    #[tokio::test]
    async fn test_empty_manifest_table() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let provider = ManifestTableProvider::new(backend);

        let batch = provider.scan_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 10); // Updated column count
    }

    #[tokio::test]
    async fn test_manifest_table_with_entries() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let provider = ManifestTableProvider::new(backend.clone());

        // Insert test entries directly via store
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("tbl1"));
        let manifest = Manifest::new(table_id, None);
        let entry =
            ManifestCacheEntry::new(manifest, Some("etag123".to_string()), 1000, SyncState::InSync);

        let key = ManifestId::from("ns1:tbl1:shared");
        let row = ManifestTableProvider::encode_manifest_row(&entry).unwrap();
        provider.store.put(&key, &row).unwrap();

        let batch = provider.scan_to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);

        // Verify column names match schema
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "cache_key");
        assert_eq!(schema.field(1).name(), "namespace_id");
        assert_eq!(schema.field(2).name(), "table_name");
        assert_eq!(schema.field(3).name(), "scope");
        assert_eq!(schema.field(4).name(), "etag");
        assert_eq!(schema.field(7).name(), "in_memory");
        assert_eq!(schema.field(9).name(), "manifest_json");
    }
}
