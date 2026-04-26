//! Storage Registry for managing storage backends
//!
//! Provides centralized access to storage configurations and path template validation.
//! Includes an in-memory cache to avoid repeated RocksDB lookups when multiple tables
//! share the same storage.

use std::sync::Arc;

use dashmap::DashMap;
use kalamdb_commons::models::StorageId;
use kalamdb_configs::config::types::RemoteStorageTimeouts;
use kalamdb_system::{Storage, StoragesTableProvider};

use crate::{
    error::{FilestoreError, Result},
    registry::storage_cached::StorageCached,
};

/// Registry for managing storage backends
///
/// Provides methods to:
/// - Retrieve storage configurations by ID (with caching)
/// - List all available storages
/// - Validate path templates for correctness
/// - Invalidate cache entries on storage updates
/// - Get ObjectStore instances (cached per storage, not per table)
pub struct StorageRegistry {
    storages_provider: Arc<StoragesTableProvider>,
    /// Default base path for local filesystem storage when base_directory is empty
    /// Comes from server config: storage.default_storage_path (e.g., "/data/storage")
    _default_storage_path: String,
    /// Remote storage timeout configuration
    timeouts: RemoteStorageTimeouts,
    /// In-memory cache for StorageCached objects keyed by StorageId
    /// Avoids repeated RocksDB lookups and ensures one ObjectStore per storage
    /// (not one per table - 100 tables using same storage = 1 ObjectStore)
    cache: DashMap<StorageId, Arc<StorageCached>>,
}

impl StorageRegistry {
    /// Create a new StorageRegistry
    pub fn new(
        storages_provider: Arc<StoragesTableProvider>,
        default_storage_path: String,
        timeouts: RemoteStorageTimeouts,
    ) -> Self {
        use std::path::{Path, PathBuf};
        // Normalize default path: if relative, resolve against current working directory
        let normalized = if Path::new(&default_storage_path).is_absolute() {
            default_storage_path
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(default_storage_path)
                .to_string_lossy()
                .into_owned()
        };
        Self {
            storages_provider,
            _default_storage_path: normalized,
            timeouts,
            cache: DashMap::new(),
        }
    }

    /// Get a cached storage entry by ID
    ///
    /// First checks the in-memory cache, then falls back to RocksDB on cache miss.
    /// Results are cached for subsequent lookups, including lazy ObjectStore.
    ///
    /// # Arguments
    /// * `storage_id` - The unique storage identifier
    ///
    /// # Returns
    /// * `Ok(Some(Arc<StorageCached>))` - Storage found (cached or freshly loaded)
    /// * `Ok(None)` - Storage not found
    /// * `Err` - Database error
    pub fn get_cached(&self, storage_id: &StorageId) -> Result<Option<Arc<StorageCached>>> {
        // Fast path: check cache
        if let Some(cached) = self.cache.get(storage_id) {
            return Ok(Some(Arc::clone(&cached)));
        }

        // Slow path: fetch from RocksDB
        let storage = self
            .storages_provider
            .get_storage(storage_id)
            .map_err(|e| FilestoreError::StorageError(e.to_string()))?;

        if let Some(s) = storage {
            let cached = Arc::new(StorageCached::new(s, self.timeouts.clone()));
            self.cache.insert(storage_id.clone(), Arc::clone(&cached));
            Ok(Some(cached))
        } else {
            Ok(None)
        }
    }

    /// Get a storage configuration by ID (cached)
    ///
    /// Convenience method that returns just the Storage, not the full StorageCached.
    /// Use `get_cached()` if you also need the ObjectStore.
    ///
    /// # Arguments
    /// * `storage_id` - The unique storage identifier
    ///
    /// # Returns
    /// * `Ok(Some(Arc<Storage>))` - Storage found
    /// * `Ok(None)` - Storage not found
    /// * `Err` - Database error
    pub fn get_storage(&self, storage_id: &StorageId) -> Result<Option<Arc<Storage>>> {
        Ok(self.get_cached(storage_id)?.map(|c| Arc::clone(&c.storage)))
    }

    /// Invalidate a storage entry from the cache
    ///
    /// Call this when a storage is updated or deleted to ensure
    /// subsequent reads fetch fresh data from RocksDB.
    /// This also invalidates the cached ObjectStore for that storage.
    ///
    /// # Arguments
    /// * `storage_id` - The storage ID to invalidate
    pub fn invalidate(&self, storage_id: &StorageId) {
        self.cache.remove(storage_id);
    }

    /// Invalidate just the ObjectStore for a storage (keep Storage config cached)
    ///
    /// Use this when storage credentials change but the config structure is the same.
    /// Less disruptive than full invalidate() as it doesn't require re-fetching from RocksDB.
    ///
    /// # Arguments
    /// * `storage_id` - The storage ID whose ObjectStore to invalidate
    pub fn invalidate_object_store(&self, storage_id: &StorageId) {
        if let Some(cached) = self.cache.get(storage_id) {
            cached.invalidate_object_store();
        }
    }

    /// Invalidate all storage entries from the cache
    ///
    /// Call this on server restart or when bulk storage changes occur.
    pub fn invalidate_all(&self) {
        self.cache.clear();
    }

    /// Get the number of cached storage entries (for metrics/debugging)
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// List all storage configurations
    ///
    /// Returns storages ordered with 'local' first, then alphabetically by storage_id.
    ///
    /// # Returns
    /// * `Ok(Vec<Storage>)` - List of all storages
    /// * `Err` - Database error
    ///
    /// # Example
    /// ```no_run
    /// # use kalamdb_filestore::registry::StorageRegistry;
    /// # fn example(registry: &StorageRegistry) -> Result<(), kalamdb_filestore::error::FilestoreError> {
    /// let storages = registry.list_storages()?;
    /// for storage in storages {
    ///     println!("Storage: {} ({})", storage.storage_name, storage.storage_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_storages(&self) -> Result<Vec<Storage>> {
        let mut storages = self
            .storages_provider
            .list_storages()
            .map_err(|e| FilestoreError::StorageError(e.to_string()))?;

        // Sort: 'local' first, then alphabetically
        storages.sort_by(|a, b| {
            if a.storage_id.is_local() {
                std::cmp::Ordering::Less
            } else if b.storage_id.is_local() {
                std::cmp::Ordering::Greater
            } else {
                a.storage_id.as_str().cmp(b.storage_id.as_str())
            }
        });

        Ok(storages)
    }

    /// Validate a path template for correctness
    ///
    /// # Template Variables
    /// - `{namespace}` - Namespace ID
    /// - `{tableName}` - Table name
    /// - `{shard}` - Shard identifier (optional for user tables)
    /// - `{userId}` - User ID (required for user tables)
    ///
    /// # Validation Rules
    /// 1. **Shared tables**: `{namespace}` must appear before `{tableName}`
    /// 2. **User tables**: Enforce ordering `{namespace}` → `{tableName}` → `{shard}` → `{userId}`
    /// 3. **User tables**: `{userId}` is required
    ///
    /// # Arguments
    /// * `template` - The path template string
    /// * `is_user_table` - True for user table templates, false for shared table templates
    ///
    /// # Returns
    /// * `Ok(())` - Template is valid
    /// * `Err` - Template validation failed with specific error message
    ///
    /// # Examples
    /// ```no_run
    /// # use kalamdb_filestore::registry::StorageRegistry;
    /// # fn example(registry: &StorageRegistry) -> Result<(), kalamdb_filestore::error::FilestoreError> {
    /// // Valid shared table template
    /// registry.validate_template("{namespace}/{tableName}", false)?;
    ///
    /// // Valid user table template
    /// registry.validate_template("{namespace}/{tableName}/{userId}", true)?;
    ///
    /// // Invalid: {userId} missing for user table
    /// // registry.validate_template("{namespace}/{tableName}", true)?; // Error!
    /// # Ok(())
    /// # }
    /// ```
    pub fn validate_template(&self, template: &str, is_user_table: bool) -> Result<()> {
        if is_user_table {
            // User table template validation
            self.validate_user_table_template(template)
        } else {
            // Shared table template validation
            self.validate_shared_table_template(template)
        }
    }

    /// Validate shared table template
    ///
    /// Rule: {namespace} must appear before {tableName}
    fn validate_shared_table_template(&self, template: &str) -> Result<()> {
        let namespace_pos = template.find("{namespace}");
        let table_name_pos = template.find("{tableName}");

        match (namespace_pos, table_name_pos) {
            (Some(ns_pos), Some(tn_pos)) => {
                if ns_pos < tn_pos {
                    Ok(())
                } else {
                    Err(FilestoreError::InvalidTemplate(
                        "Shared table template: {namespace} must appear before {tableName}"
                            .to_string(),
                    ))
                }
            },
            (None, Some(_)) => Err(FilestoreError::InvalidTemplate(
                "Shared table template: {namespace} is required".to_string(),
            )),
            (Some(_), None) => Err(FilestoreError::InvalidTemplate(
                "Shared table template: {tableName} is required".to_string(),
            )),
            (None, None) => Err(FilestoreError::InvalidTemplate(
                "Shared table template: Both {namespace} and {tableName} are required".to_string(),
            )),
        }
    }

    /// Validate user table template
    ///
    /// Rules:
    /// 1. {userId} is required
    /// 2. Enforce ordering: {namespace} → {tableName} → {shard} → {userId}
    fn validate_user_table_template(&self, template: &str) -> Result<()> {
        let namespace_pos = template.find("{namespace}");
        let table_name_pos = template.find("{tableName}");
        let shard_pos = template.find("{shard}");
        let user_id_pos = template.find("{userId}");

        // Rule 1: {userId} is required
        let user_id_pos = match user_id_pos {
            Some(pos) => pos,
            None => {
                return Err(FilestoreError::InvalidTemplate(
                    "User table template: {userId} is required".to_string(),
                ))
            },
        };

        // Rule 2: Enforce ordering
        if let Some(ns_pos) = namespace_pos {
            if let Some(tn_pos) = table_name_pos {
                if ns_pos >= tn_pos {
                    return Err(FilestoreError::InvalidTemplate(
                        "User table template: {namespace} must appear before {tableName}"
                            .to_string(),
                    ));
                }
            }
        }

        if let (Some(tn_pos), Some(shard_pos)) = (table_name_pos, shard_pos) {
            if tn_pos >= shard_pos {
                return Err(FilestoreError::InvalidTemplate(
                    "User table template: {tableName} must appear before {shard}".to_string(),
                ));
            }
        }

        if let (Some(shard_pos), user_id_pos) = (shard_pos, user_id_pos) {
            if shard_pos >= user_id_pos {
                return Err(FilestoreError::InvalidTemplate(
                    "User table template: {shard} must appear before {userId}".to_string(),
                ));
            }
        }

        if let (Some(tn_pos), None) = (table_name_pos, shard_pos) {
            // No {shard}, check {tableName} → {userId} ordering
            if tn_pos >= user_id_pos {
                return Err(FilestoreError::InvalidTemplate(
                    "User table template: {tableName} must appear before {userId}".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Get the default storage path configured for the server
    ///
    /// Used when a storage's `base_directory` is empty to construct
    /// absolute paths for Parquet outputs.
    pub fn default_storage_path(&self) -> &str {
        &self._default_storage_path
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kalamdb_store::test_utils::InMemoryBackend;

    use super::*;

    // These are unit tests for template validation logic.
    // Use an in-memory backend so validation tests do not depend on RocksDB lifecycle.

    #[test]
    fn test_validate_shared_template_valid() {
        // Mock registry (validation doesn't need DB access)
        let registry = create_mock_registry();

        // Valid shared table templates
        assert!(registry.validate_template("{namespace}/{tableName}", false).is_ok());
        assert!(registry.validate_template("{namespace}/tables/{tableName}", false).is_ok());
    }

    #[test]
    fn test_validate_shared_template_wrong_order() {
        let registry = create_mock_registry();

        // Invalid: {tableName} before {namespace}
        assert!(registry.validate_template("{tableName}/{namespace}", false).is_err());
    }

    #[test]
    fn test_validate_user_template_valid() {
        let registry = create_mock_registry();

        // Valid user table templates
        assert!(registry.validate_template("{namespace}/{tableName}/{userId}", true).is_ok());
        assert!(registry
            .validate_template("{namespace}/{tableName}/{shard}/{userId}", true)
            .is_ok());
    }

    #[test]
    fn test_validate_user_template_missing_user_id() {
        let registry = create_mock_registry();

        // Invalid: {userId} missing
        assert!(registry.validate_template("{namespace}/{tableName}", true).is_err());
    }

    #[test]
    fn test_validate_user_template_wrong_order() {
        let registry = create_mock_registry();

        // Invalid: {userId} before {tableName}
        assert!(registry.validate_template("{namespace}/{userId}/{tableName}", true).is_err());

        // Invalid: {shard} before {tableName}
        assert!(registry
            .validate_template("{namespace}/{shard}/{tableName}/{userId}", true)
            .is_err());
    }

    // Helper to create a mock registry for tests that don't need DB
    fn create_mock_registry() -> StorageRegistry {
        let backend: Arc<dyn kalamdb_store::StorageBackend> = Arc::new(InMemoryBackend::new());

        // Create StoragesTableProvider for tests
        let storages_provider =
            Arc::new(kalamdb_system::providers::storages::StoragesTableProvider::new(backend));

        StorageRegistry::new(
            storages_provider,
            "/tmp/kalamdb-storage-registry-tests".to_string(),
            kalamdb_configs::config::types::RemoteStorageTimeouts::default(),
        )
    }
}
