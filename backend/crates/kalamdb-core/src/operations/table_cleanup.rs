//! Reusable table cleanup helpers.
//!
//! These functions are shared by the `DropTableHandler` (in `kalamdb-handlers`)
//! and the `CleanupExecutor` (in `kalamdb-jobs`).

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::schema_registry::SchemaRegistry;
use kalamdb_commons::models::{StorageId, TableId};
use kalamdb_commons::schemas::TableType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Cleanup operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupOperation {
    /// Drop table (delete all data and metadata)
    DropTable,
    /// Truncate table (delete all data, keep schema)
    Truncate,
    /// Remove orphaned files
    RemoveOrphaned,
}

/// Storage details needed to delete Parquet trees after metadata removal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCleanupDetails {
    /// Storage identifier
    pub storage_id: StorageId,
    /// Base directory resolved for this storage
    pub base_directory: String,
    /// Relative path template with static placeholders substituted
    pub relative_path_template: String,
}

/// Delete table data from RocksDB partitions.
///
/// Returns the number of rows deleted (currently always 0 — partitions are
/// dropped wholesale, not row-by-row).
pub async fn cleanup_table_data_internal(
    app_context: &Arc<AppContext>,
    table_id: &TableId,
    table_type: TableType,
) -> Result<usize, KalamDbError> {
    log::debug!(
        "[CleanupHelper] Cleaning up table data for {:?} (type: {:?})",
        table_id,
        table_type
    );

    let rows_deleted = match table_type {
        TableType::User => {
            use kalamdb_tables::new_indexed_user_table_store;

            new_indexed_user_table_store(app_context.storage_backend(), table_id, "_pk")
                .drop_all_partitions()
                .map_err(|e| {
                    KalamDbError::Other(format!(
                        "Failed to drop user table partitions for {}: {}",
                        table_id, e
                    ))
                })?;

            log::debug!("[CleanupHelper] Dropped all partitions for user table {:?}", table_id);
            0usize
        },
        TableType::Shared => {
            use kalamdb_tables::new_indexed_shared_table_store;

            new_indexed_shared_table_store(app_context.storage_backend(), table_id, "_pk")
                .drop_all_partitions()
                .map_err(|e| {
                    KalamDbError::Other(format!(
                        "Failed to drop shared table partitions for {}: {}",
                        table_id, e
                    ))
                })?;

            log::debug!("[CleanupHelper] Dropped all partitions for shared table {:?}", table_id);
            0usize
        },
        TableType::Stream => {
            use kalamdb_commons::constants::ColumnFamilyNames;
            use kalamdb_store::storage_trait::Partition as StorePartition;

            let partition_name = format!("{}{}", ColumnFamilyNames::STREAM_TABLE_PREFIX, table_id);

            let backend = app_context.storage_backend();
            let partition = StorePartition::new(partition_name.clone());

            match backend.drop_partition(&partition) {
                Ok(_) => {
                    log::debug!(
                        "[CleanupHelper] Dropped partition '{}' for stream table {:?}",
                        partition_name,
                        table_id
                    );
                    0usize
                },
                Err(e) => {
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("not found") {
                        log::debug!(
                            "[CleanupHelper] Stream partition '{}' not found (likely in-memory)",
                            partition_name
                        );
                        0usize
                    } else {
                        return Err(KalamDbError::Other(format!(
                            "Failed to drop partition '{}' for stream table {}: {}",
                            partition_name, table_id, e
                        )));
                    }
                },
            }
        },
        TableType::System => {
            return Err(KalamDbError::InvalidOperation(
                "Cannot cleanup system table data".to_string(),
            ));
        },
    };

    log::debug!("[CleanupHelper] Deleted {} rows from table data", rows_deleted);
    Ok(rows_deleted)
}

/// Delete Parquet files from the storage backend for a given table.
///
/// Returns the number of bytes freed (currently 0 — `delete_prefix` does not
/// report byte counts).
pub async fn cleanup_parquet_files_internal(
    app_context: &Arc<AppContext>,
    table_id: &TableId,
    table_type: TableType,
    storage: &StorageCleanupDetails,
) -> Result<u64, KalamDbError> {
    log::debug!(
        "[CleanupHelper] Cleaning up Parquet files for {:?} using storage {}",
        table_id,
        storage.storage_id.as_str()
    );

    let storage_cached =
        app_context.storage_registry().get_cached(&storage.storage_id)?.ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "Storage '{}' not found during cleanup",
                storage.storage_id.as_str()
            ))
        })?;

    let files_deleted = storage_cached
        .delete_prefix(table_type, table_id, None)
        .await
        .into_kalamdb_error("Failed to delete Parquet tree")?
        .files_deleted;

    log::debug!("[CleanupHelper] Freed {} files from Parquet storage", files_deleted);
    Ok(0)
}

/// Remove table metadata from system tables (schema registry).
///
/// If the table has been re-created since the drop, the cleanup is skipped to
/// avoid deleting the new definition.
pub async fn cleanup_metadata_internal(
    _app_ctx: &AppContext,
    schema_registry: &Arc<SchemaRegistry>,
    table_id: &TableId,
) -> Result<(), KalamDbError> {
    log::debug!("[CleanupHelper] Cleaning up metadata for {:?}", table_id);

    if schema_registry.get_table_if_exists(table_id)?.is_some() {
        log::debug!(
            "[CleanupHelper] Metadata present for {:?} (table re-created) - skipping cleanup",
            table_id
        );
        return Ok(());
    }

    schema_registry.delete_table_definition(table_id)?;

    log::debug!("[CleanupHelper] Metadata cleanup complete");
    Ok(())
}
