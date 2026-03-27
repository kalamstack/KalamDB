//! Primary Key Existence Checker Implementation
//!
//! Provides optimized PK uniqueness validation using manifest-based segment pruning.

use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::UserId;
use kalamdb_system::Manifest;
use std::sync::Arc;

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::manifest::ManifestAccessPlanner;
use kalamdb_filestore::StorageRegistry;
use kalamdb_system::ManifestService as ManifestServiceTrait;
use kalamdb_system::SchemaRegistry as SchemaRegistryTrait;

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

    /// Returns true if the check was skipped due to auto-increment
    pub fn is_auto_increment(&self) -> bool {
        matches!(self, PkCheckResult::AutoIncrement)
    }
}

/// Primary Key Existence Checker
///
/// Optimized for INSERT/UPDATE operations that need to validate PK uniqueness.
/// Uses a tiered approach:
/// 1. Check if PK is auto-increment (skip)
/// 2. Check hot storage (RocksDB) via provider's PK index
/// 3. Use manifest cache (L1/L2) to load segment metadata
/// 4. Apply min/max pruning to skip irrelevant segments
/// 5. Scan only necessary Parquet files
pub struct PkExistenceChecker {
    schema_registry: Arc<dyn SchemaRegistryTrait<Error = KalamDbError>>,
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
    /// 2. Check if PK value is provided (not null)
    /// 3. Load manifest from cache (L1 hot cache → L2 RocksDB → storage)
    /// 4. Use column_stats min/max to prune segments
    /// 5. Scan only matching Parquet files
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

        // Step 3-5: Use the optimized cold storage check
        let exists_in_cold = self
            .check_cold_storage(table_id, table_type, user_id, pk_column, pk_column_id, pk_value)
            .await?;

        if let Some(segment_path) = exists_in_cold {
            return Ok(PkCheckResult::FoundInCold { segment_path });
        }

        Ok(PkCheckResult::NotFound)
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
    ) -> Result<Option<String>, KalamDbError> {
        let namespace = table_id.namespace_id();
        let table = table_id.table_name();
        let scope_label = user_id
            .map(|uid| format!("user={}", uid.as_str()))
            .unwrap_or_else(|| format!("scope={}", table_type.as_str()));

        // 1. Get CachedTableData for storage access
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

        // 2. Get StorageCached from registry
        let storage_cached = match self
            .storage_registry
            .get_cached(&storage_id)
            .into_kalamdb_error("Failed to get storage cache")?
        {
            Some(cached) => cached,
            None => {
                log::trace!(
                    "[PkExistenceChecker] Storage not found for {}.{} {}",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(None);
            },
        };

        // 3. List parquet files using async method
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
                    return Ok(None);
                },
            };

        if all_parquet_files.is_empty() {
            log::trace!(
                "[PkExistenceChecker] No files in storage for {}.{} {}",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        }

        // 4. Load manifest from cache (L1 → L2 → storage)
        let manifest: Option<Manifest> = match self.manifest_service.get_or_load(table_id, user_id)
        {
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
                    "[PkExistenceChecker] No manifest in cache for {}.{} {} - will check all files",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                // Try to load from storage (manifest.json file)
                self.load_manifest_from_storage_async(
                    &storage_cached,
                    table_type,
                    table_id,
                    user_id,
                )
                .await?
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

        // 5. Use manifest to prune segments by min/max or check all Parquet files
        let planner = ManifestAccessPlanner::new();
        let files_to_scan: Vec<String> = if let Some(ref m) = manifest {
            let pruned_paths = planner.plan_by_pk_value(m, pk_column_id, pk_value);
            if pruned_paths.is_empty() {
                log::trace!(
                    "[PkExistenceChecker] Manifest pruning returned no candidate segments for PK {} on {}.{} {} - falling back to full parquet scan",
                    pk_value,
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                all_parquet_files
            } else {
                log::trace!(
                    "[PkExistenceChecker] Manifest pruning: {} of {} segments may contain PK {} for {}.{} {}",
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
            // No manifest - check all Parquet files
            all_parquet_files
        };

        if files_to_scan.is_empty() {
            return Ok(None);
        }

        // 6. Scan pruned Parquet files for the PK
        for file_name in files_to_scan {
            if self
                .pk_exists_in_parquet_async(
                    &storage_cached,
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
                return Ok(Some(file_name));
            }
        }

        Ok(None)
    }

    /// Extract PK value as string from an Arrow array (now uses shared utility)
    fn extract_pk_as_string(
        col: &dyn datafusion::arrow::array::Array,
        idx: usize,
    ) -> Option<String> {
        crate::utils::pk_utils::extract_pk_as_string(col, idx)
    }

    /// Async load manifest.json from storage
    async fn load_manifest_from_storage_async(
        &self,
        storage_cached: &kalamdb_filestore::StorageCached,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Option<Manifest>, KalamDbError> {
        match storage_cached.get(table_type, table_id, user_id, "manifest.json").await {
            Ok(result) => {
                let manifest: Manifest = serde_json::from_slice(&result.data).map_err(|e| {
                    KalamDbError::InvalidOperation(format!("Failed to parse manifest.json: {}", e))
                })?;
                log::trace!(
                    "[PkExistenceChecker] Loaded manifest.json from storage: {} segments",
                    manifest.segments.len()
                );
                Ok(Some(manifest))
            },
            Err(_) => {
                log::trace!("[PkExistenceChecker] No manifest.json found in storage");
                Ok(None)
            },
        }
    }

    /// Async check if a PK exists in a specific Parquet file via streaming.
    ///
    /// Uses column-projected streaming — only reads pk/_seq/_deleted column chunks,
    /// never loads the entire file into memory.
    async fn pk_exists_in_parquet_async(
        &self,
        storage_cached: &kalamdb_filestore::StorageCached,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        parquet_filename: &str,
        pk_column: &str,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        use datafusion::arrow::array::{Array, BooleanArray, Int64Array, UInt64Array};
        use futures_util::TryStreamExt;
        use kalamdb_commons::constants::SystemColumnNames;

        let columns_to_read: Vec<&str> = vec![
            pk_column,
            SystemColumnNames::SEQ,
            SystemColumnNames::DELETED,
        ];
        let mut stream = storage_cached
            .read_parquet_file_stream(
                table_type,
                table_id,
                user_id,
                parquet_filename,
                &columns_to_read,
            )
            .await
            .into_kalamdb_error("Failed to open Parquet stream")?;

        // Track latest version: (max_seq, is_deleted)
        let mut latest: Option<(i64, bool)> = None;

        while let Some(batch) = stream
            .try_next()
            .await
            .into_kalamdb_error("Failed to read Parquet batch")?
        {
            let pk_idx = batch.schema().index_of(pk_column).ok();
            let seq_idx = batch.schema().index_of(SystemColumnNames::SEQ).ok();
            let deleted_idx = batch.schema().index_of(SystemColumnNames::DELETED).ok();

            let (Some(pk_i), Some(seq_i)) = (pk_idx, seq_idx) else {
                continue;
            };

            let pk_col = batch.column(pk_i);
            let seq_col = batch.column(seq_i);
            let deleted_col = deleted_idx.map(|i| batch.column(i));

            for row_idx in 0..batch.num_rows() {
                let row_pk = Self::extract_pk_as_string(pk_col.as_ref(), row_idx);
                let Some(row_pk_str) = row_pk else { continue };

                if row_pk_str != pk_value {
                    continue;
                }

                let seq = if let Some(arr) = seq_col.as_any().downcast_ref::<Int64Array>() {
                    arr.value(row_idx)
                } else if let Some(arr) = seq_col.as_any().downcast_ref::<UInt64Array>() {
                    arr.value(row_idx) as i64
                } else {
                    continue;
                };

                let deleted = if let Some(del_col) = &deleted_col {
                    if let Some(arr) = del_col.as_any().downcast_ref::<BooleanArray>() {
                        if arr.is_null(row_idx) {
                            false
                        } else {
                            arr.value(row_idx)
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                match &mut latest {
                    Some((max_seq, del)) => {
                        if seq > *max_seq {
                            *max_seq = seq;
                            *del = deleted;
                        }
                    }
                    None => latest = Some((seq, deleted)),
                }
            }
        }

        if let Some((_, is_deleted)) = latest {
            Ok(!is_deleted)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::datatypes::KalamDataType;
    use kalamdb_commons::models::schemas::{
        ColumnDefault, ColumnDefinition, TableDefinition, TableType,
    };
    use kalamdb_commons::{NamespaceId, TableName};

    #[allow(dead_code)]
    fn create_test_table_def(pk_default: ColumnDefault) -> TableDefinition {
        TableDefinition {
            namespace_id: NamespaceId::new("test"),
            table_name: TableName::new("users"),
            table_type: TableType::User,
            table_options: kalamdb_commons::schemas::TableOptions::User(Default::default()),
            columns: vec![
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
            table_comment: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_pk_check_result_exists() {
        assert!(!PkCheckResult::NotFound.exists());
        assert!(!PkCheckResult::AutoIncrement.exists());
        assert!(!PkCheckResult::PrunedByManifest.exists());
        assert!(PkCheckResult::FoundInHot.exists());
        assert!(PkCheckResult::FoundInCold {
            segment_path: "batch-0.parquet".to_string()
        }
        .exists());
    }
}
