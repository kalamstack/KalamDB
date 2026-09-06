//! Primary Key Existence Checker Implementation
//!
//! Provides optimized PK uniqueness validation using manifest-based segment pruning.

use std::sync::Arc;

use kalamdb_commons::{models::TableId, schemas::TableType, UserId};
use kalamdb_filestore::StorageRegistry;
use kalamdb_store::StorageError;
use kalamdb_system::{
    Manifest, ManifestService as ManifestServiceTrait, SchemaRegistry as SchemaRegistryTrait,
};

use crate::{
    error::KalamDbError, error_extensions::KalamDbResultExt, manifest::ManifestAccessPlanner,
};

/// Result of a primary key existence check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkCheckResult {
    /// PK column is AUTO_INCREMENT - no check needed, system generates unique value
    AutoIncrement,

    /// PK was not found in any storage layer
    NotFound,

    /// PK exists in hot storage (RocksDB memtable/SST)
    FoundInHot,

    /// PK exists in cold storage (Parquet files)
    FoundInCold {
        /// Segment file that contains the PK
        segment_path: String,
    },

    /// PK was pruned by manifest min/max stats - definitely not present
    /// This is a fast path that avoids reading any Parquet files
    PrunedByManifest,
}

impl PkCheckResult {
    /// Returns true if the PK exists in any storage layer
    pub fn exists(&self) -> bool {
        matches!(self, PkCheckResult::FoundInHot | PkCheckResult::FoundInCold { .. })
    }
}

/// Primary Key Existence Checker
///
/// Optimized for INSERT/UPDATE operations that need to validate PK uniqueness.
/// Uses a tiered approach:
/// 1. Check if PK is auto-increment (skip)
/// 2. Use ManifestService to load segment metadata from memory, RocksDB, or storage
/// 3. Apply min/max pruning to skip irrelevant segments
/// 4. Scan only necessary Parquet files
///
/// The hot-storage PK index is checked by the caller before this checker runs.
pub struct PkExistenceChecker {
    schema_registry:  Arc<dyn SchemaRegistryTrait<Error = KalamDbError>>,
    storage_registry: Arc<StorageRegistry>,
    manifest_service: Arc<dyn ManifestServiceTrait>,
}

impl PkExistenceChecker {
    /// Create a new PK existence checker
    pub fn new(
        schema_registry: Arc<dyn SchemaRegistryTrait<Error = KalamDbError>>,
        storage_registry: Arc<StorageRegistry>,
        manifest_service: Arc<dyn ManifestServiceTrait>,
    ) -> Self {
        Self {
            schema_registry,
            storage_registry,
            manifest_service,
        }
    }

    /// Check if a PK value exists, using optimized manifest-based pruning
    ///
    /// ## Flow:
    /// 1. Check if PK is AUTO_INCREMENT → return AutoIncrement (skip check)
    /// 2. Load manifest from ManifestService (memory → RocksDB → storage)
    /// 3. Use column_stats min/max to prune segments
    /// 4. Scan only matching Parquet files
    ///
    /// ## Arguments
    /// * `core` - TableProviderCore with cached PK info
    /// * `user_id` - Optional user ID for scoping (User tables)
    /// * `pk_value` - The PK value to check (as string)
    ///
    /// ## Returns
    /// * `PkCheckResult` indicating where (or if) the PK was found
    pub async fn check_pk_exists(
        &self,
        core: &crate::utils::base::TableProviderCore,
        user_id: Option<&UserId>,
        pk_value: &str,
    ) -> Result<PkCheckResult, KalamDbError> {
        // Step 1: Check if PK is auto-increment (O(1) cached value)
        if core.is_auto_increment_pk() {
            log::trace!(
                "[PkExistenceChecker] PK is auto-increment for {}, skipping check",
                core.table_id()
            );
            return Ok(PkCheckResult::AutoIncrement);
        }

        // Step 2: Get PK info from core (O(1) cached values)
        let table_id = core.table_id();
        let table_type = core.table_type();
        let pk_column = core.primary_key_field_name();
        let pk_column_id = core.primary_key_column_id();

        // Step 2-4: Use the optimized cold storage check
        self.check_cold_storage(table_id, table_type, user_id, pk_column, pk_column_id, pk_value)
            .await
    }

    /// Check cold storage for PK existence using manifest-based pruning (async)
    ///
    /// Returns the segment path if found, None otherwise.
    async fn check_cold_storage(
        &self,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<&UserId>,
        pk_column: &str,
        pk_column_id: u64,
        pk_value: &str,
    ) -> Result<PkCheckResult, KalamDbError> {
        let namespace = table_id.namespace_id();
        let table = table_id.table_name();
        let scope_label = user_id
            .map(|uid| format!("user={}", uid.as_str()))
            .unwrap_or_else(|| format!("scope={}", table_type.as_str()));

        // 1. Load manifest through the centralized memory -> RocksDB -> storage path.
        let manifest: Option<Manifest> =
            match self.manifest_service.get_or_load_async(table_id, user_id).await {
                Ok(Some(entry)) => {
                    log::trace!(
                        "[PkExistenceChecker] Manifest loaded from cache for {}.{} {}",
                        namespace.as_str(),
                        table.as_str(),
                        scope_label
                    );
                    Some(entry.manifest.clone())
                },
                Ok(None) => {
                    log::trace!(
                        "[PkExistenceChecker] No manifest for {}.{} {} - will check all files",
                        namespace.as_str(),
                        table.as_str(),
                        scope_label
                    );
                    None
                },
                Err(StorageError::SerializationError(e)) => {
                    return Err(KalamDbError::InvalidOperation(format!(
                        "Failed to load manifest for {} {}: {}",
                        table_id, scope_label, e
                    )));
                },
                Err(e) => {
                    log::warn!(
                        "[PkExistenceChecker] Manifest cache error for {}.{} {}: {}",
                        namespace.as_str(),
                        table.as_str(),
                        scope_label,
                        e
                    );
                    None
                },
            };

        if let Some(ref m) = manifest {
            if m.segments.is_empty() {
                log::trace!(
                    "[PkExistenceChecker] Cached manifest has no segments for {}.{} {}",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(PkCheckResult::NotFound);
            }
        }

        // 2. Use manifest to prune segments by min/max, or list files if no manifest exists.
        let planner = ManifestAccessPlanner::new();
        let mut storage_cached_for_scan: Option<Arc<kalamdb_filestore::StorageCached>> = None;
        let files_to_scan: Vec<String> = if let Some(ref m) = manifest {
            let pruned_paths = planner.plan_by_pk_value(m, pk_column_id, pk_value);
            if pruned_paths.is_empty() {
                log::trace!(
                    "[PkExistenceChecker] Manifest pruning returned no candidate segments for PK \
                     {} on {}.{} {} - PK not in cold",
                    pk_value,
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(PkCheckResult::PrunedByManifest);
            } else {
                log::trace!(
                    "[PkExistenceChecker] Manifest pruning: {} of {} segments may contain PK {} \
                     for {}.{} {}",
                    pruned_paths.len(),
                    m.segments.len(),
                    pk_value,
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                pruned_paths
            }
        } else {
            let storage_cached = match self.storage_cached_for_table(table_id, &scope_label)? {
                Some(cached) => cached,
                None => return Ok(PkCheckResult::NotFound),
            };
            let all_parquet_files =
                match storage_cached.list_parquet_files(table_type, table_id, user_id).await {
                    Ok(files) => files,
                    Err(_) => {
                        log::trace!(
                            "[PkExistenceChecker] No storage dir for {}.{} {}",
                            namespace.as_str(),
                            table.as_str(),
                            scope_label
                        );
                        return Ok(PkCheckResult::NotFound);
                    },
                };

            if all_parquet_files.is_empty() {
                log::trace!(
                    "[PkExistenceChecker] No files in storage for {}.{} {}",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(PkCheckResult::NotFound);
            }

            storage_cached_for_scan = Some(storage_cached);
            all_parquet_files
        };

        if files_to_scan.is_empty() {
            return Ok(PkCheckResult::NotFound);
        }

        let storage_cached = match storage_cached_for_scan {
            Some(cached) => cached,
            None => match self.storage_cached_for_table(table_id, &scope_label)? {
                Some(cached) => cached,
                None => return Ok(PkCheckResult::NotFound),
            },
        };

        // 3. Scan pruned Parquet files for the PK.
        for file_name in files_to_scan {
            if crate::utils::pk::pk_exists_in_parquet_file(
                storage_cached.as_ref(),
                table_type,
                table_id,
                user_id,
                &file_name,
                pk_column,
                pk_value,
            )
            .await?
            {
                log::trace!(
                    "[PkExistenceChecker] Found PK {} in {} for {}.{} {}",
                    pk_value,
                    file_name,
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(PkCheckResult::FoundInCold {
                    segment_path: file_name,
                });
            }
        }

        Ok(PkCheckResult::NotFound)
    }

    fn storage_cached_for_table(
        &self,
        table_id: &TableId,
        scope_label: &str,
    ) -> Result<Option<Arc<kalamdb_filestore::StorageCached>>, KalamDbError> {
        let namespace = table_id.namespace_id();
        let table = table_id.table_name();
        let storage_id = match self.schema_registry.get_storage_id(table_id) {
            Ok(id) => id,
            Err(e) => {
                log::trace!(
                    "[PkExistenceChecker] No storage id for {}.{} {} - PK not in cold: {}",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label,
                    e
                );
                return Ok(None);
            },
        };

        match self
            .storage_registry
            .get_cached(&storage_id)
            .into_kalamdb_error("Failed to get storage cache")?
        {
            Some(cached) => Ok(Some(cached)),
            None => {
                log::trace!(
                    "[PkExistenceChecker] Storage not found for {}.{} {}",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                Ok(None)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::Path};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use kalamdb_commons::{
        ids::SeqId,
        models::{
            datatypes::KalamDataType,
            rows::StoredScalarValue,
            schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableType},
        },
        NamespaceId, StorageId, TableId, TableName, UserId,
    };
    use kalamdb_filestore::StorageRegistry;
    use kalamdb_store::{test_utils::InMemoryBackend, StorageBackend, StorageError};
    use kalamdb_system::{
        ManifestCacheEntry, ManifestService, SegmentMetadata, Storage, StorageType,
        StoragesTableProvider, SyncState,
    };
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Clone)]
    struct TestSchemaRegistry {
        table_id:   TableId,
        table_def:  Arc<TableDefinition>,
        schema:     SchemaRef,
        storage_id: StorageId,
    }

    impl SchemaRegistryTrait for TestSchemaRegistry {
        type Error = KalamDbError;

        fn get_arrow_schema(&self, table_id: &TableId) -> Result<SchemaRef, Self::Error> {
            if &self.table_id == table_id {
                Ok(Arc::clone(&self.schema))
            } else {
                Err(KalamDbError::TableNotFound(table_id.to_string()))
            }
        }

        fn get_table_if_exists(
            &self,
            table_id: &TableId,
        ) -> Result<Option<Arc<TableDefinition>>, Self::Error> {
            if &self.table_id == table_id {
                Ok(Some(Arc::clone(&self.table_def)))
            } else {
                Ok(None)
            }
        }

        fn get_arrow_schema_for_version(
            &self,
            table_id: &TableId,
            _schema_version: u32,
        ) -> Result<SchemaRef, Self::Error> {
            self.get_arrow_schema(table_id)
        }

        fn get_storage_id(&self, table_id: &TableId) -> Result<StorageId, Self::Error> {
            if &self.table_id == table_id {
                Ok(self.storage_id.clone())
            } else {
                Err(KalamDbError::TableNotFound(table_id.to_string()))
            }
        }
    }

    #[derive(Debug)]
    struct FixedManifestService {
        entry: Arc<ManifestCacheEntry>,
    }

    #[async_trait]
    impl ManifestService for FixedManifestService {
        fn get_or_load(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
        ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
            Ok(Some(Arc::clone(&self.entry)))
        }

        async fn get_or_load_async(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
        ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
            Ok(Some(Arc::clone(&self.entry)))
        }

        fn validate_manifest(&self, _manifest: &Manifest) -> Result<(), StorageError> {
            Ok(())
        }

        fn mark_as_stale(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        fn rebuild_manifest(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
        ) -> Result<Manifest, StorageError> {
            panic!("rebuild_manifest is unused in PK pruning tests")
        }

        fn mark_pending_write(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        fn ensure_manifest_initialized(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
        ) -> Result<Manifest, StorageError> {
            panic!("ensure_manifest_initialized is unused in PK pruning tests")
        }

        fn stage_before_flush(
            &self,
            _table_id: &TableId,
            _user_id: Option<&UserId>,
            _manifest: &Manifest,
        ) -> Result<(), StorageError> {
            panic!("stage_before_flush is unused in PK pruning tests")
        }

        fn get_manifest_user_ids(&self, _table_id: &TableId) -> Result<Vec<UserId>, StorageError> {
            Ok(Vec::new())
        }
    }

    fn build_storage_registry(temp_dir: &TempDir) -> Arc<StorageRegistry> {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let storages_provider = Arc::new(StoragesTableProvider::new(backend));
        let base_directory = temp_dir.path().to_string_lossy().into_owned();

        storages_provider
            .create_storage(Storage {
                storage_id:             StorageId::local(),
                storage_name:           "Local Storage".to_string(),
                description:            Some("PK pruning test storage".to_string()),
                storage_type:           StorageType::Filesystem,
                base_directory:         base_directory.clone(),
                credentials:            None,
                config_json:            None,
                shared_tables_template: "shared/{namespace}/{tableName}".to_string(),
                user_tables_template:   "user/{namespace}/{tableName}/{userId}".to_string(),
                created_at:             1_000,
                updated_at:             1_000,
            })
            .expect("seed local storage");

        Arc::new(StorageRegistry::new(
            storages_provider,
            base_directory,
            Default::default(),
            Default::default(),
        ))
    }

    fn numeric_stats(min: i64, max: i64) -> kalamdb_system::ColumnStats {
        kalamdb_system::ColumnStats::new(
            Some(StoredScalarValue::Int64(Some(min.to_string()))),
            Some(StoredScalarValue::Int64(Some(max.to_string()))),
            Some(0),
        )
    }

    #[allow(dead_code)]
    fn create_test_table_def(pk_default: ColumnDefault) -> TableDefinition {
        TableDefinition {
            namespace_id:   NamespaceId::new("test"),
            table_name:     TableName::new("users"),
            table_type:     TableType::User,
            table_options:  kalamdb_commons::schemas::TableOptions::User(Default::default()),
            columns:        vec![
                ColumnDefinition::new(
                    1,
                    "id",
                    1,
                    KalamDataType::BigInt,
                    false,
                    true, // is_primary_key
                    false,
                    pk_default,
                    None,
                ),
                ColumnDefinition::new(
                    2,
                    "name",
                    2,
                    KalamDataType::Text,
                    true,
                    false,
                    false,
                    ColumnDefault::None,
                    None,
                ),
            ],
            next_column_id: 3,
            schema_version: 1,
            table_comment:  None,
            scalar_indexes: Vec::new(),
            created_at:     chrono::Utc::now(),
            updated_at:     chrono::Utc::now(),
        }
    }

    #[test]
    fn test_pk_check_result_exists() {
        assert!(!PkCheckResult::NotFound.exists());
        assert!(!PkCheckResult::AutoIncrement.exists());
        assert!(!PkCheckResult::PrunedByManifest.exists());
        assert!(PkCheckResult::FoundInHot.exists());
        assert!(PkCheckResult::FoundInCold {
            segment_path: "batch-0.parquet".to_string(),
        }
        .exists());
    }

    #[tokio::test]
    #[ntest::timeout(2000)]
    async fn manifest_prune_returns_without_opening_parquet() {
        let table_def = Arc::new(create_test_table_def(ColumnDefault::None));
        let table_id = TableId::new(table_def.namespace_id.clone(), table_def.table_name.clone());
        let user_id = UserId::from("u_123");
        let schema = table_def.to_arrow_schema().expect("build arrow schema");
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage_registry = build_storage_registry(&temp_dir);
        let storage_cached = storage_registry
            .get_cached(&StorageId::local())
            .expect("lookup storage")
            .expect("local storage exists");

        let parquet_path = storage_cached.get_file_path(
            TableType::User,
            &table_id,
            Some(&user_id),
            "batch-0.parquet",
        );
        fs::create_dir_all(
            Path::new(&parquet_path.full_path).parent().expect("parquet parent exists"),
        )
        .expect("create parquet dir");
        fs::write(&parquet_path.full_path, b"not a parquet file")
            .expect("seed invalid parquet file");

        let mut manifest = Manifest::new(table_id.clone(), Some(user_id.clone()));
        let mut column_stats = HashMap::new();
        column_stats.insert(1, numeric_stats(1, 10));
        manifest.add_segment(SegmentMetadata::with_schema_version(
            "batch-0.parquet".to_string(),
            "batch-0.parquet".to_string(),
            column_stats,
            SeqId::from(1i64),
            SeqId::from(10i64),
            1,
            16,
            1,
        ));

        let manifest_service: Arc<dyn ManifestServiceTrait> = Arc::new(FixedManifestService {
            entry: Arc::new(ManifestCacheEntry::new(
                manifest,
                None,
                chrono::Utc::now().timestamp_millis(),
                SyncState::InSync,
            )),
        });

        let schema_registry: Arc<dyn SchemaRegistryTrait<Error = KalamDbError>> =
            Arc::new(TestSchemaRegistry {
                table_id: table_id.clone(),
                table_def,
                schema,
                storage_id: StorageId::local(),
            });

        let checker = PkExistenceChecker::new(schema_registry, storage_registry, manifest_service);

        let result = checker
            .check_cold_storage(&table_id, TableType::User, Some(&user_id), "id", 1, "999")
            .await
            .expect("manifest-pruned lookup should not open parquet");

        assert_eq!(result, PkCheckResult::PrunedByManifest);
    }
}
