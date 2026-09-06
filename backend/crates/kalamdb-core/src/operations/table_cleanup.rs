//! Reusable table cleanup helpers.
//!
//! These functions are shared by the `DropTableHandler` (in `kalamdb-handlers`)
//! and the `CleanupExecutor` (in `kalamdb-jobs`).

use std::{fs, sync::Arc};

use kalamdb_commons::{
    models::{
        schemas::{ColumnDefinition, ScalarIndexDefinition},
        StorageId, TableId,
    },
    schemas::TableType,
};
use serde::{Deserialize, Serialize};

use crate::{
    app_context::AppContext, error::KalamDbError, error_extensions::KalamDbResultExt,
    schema_registry::SchemaRegistry,
};

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
    pub storage_id:             StorageId,
    /// Base directory resolved for this storage
    pub base_directory:         String,
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
            use kalamdb_tables::{empty_storage_schema, new_indexed_user_table_store};

            let (pk_field, scalar_indexes, columns) =
                catalog_open_args(app_context, table_id, "_pk");
            new_indexed_user_table_store(
                app_context.storage_backend(),
                table_id,
                &pk_field,
                empty_storage_schema(),
                &scalar_indexes,
                &columns,
            )
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
            use kalamdb_tables::{empty_storage_schema, new_indexed_shared_table_store};

            let (pk_field, scalar_indexes, columns) =
                catalog_open_args(app_context, table_id, "_pk");
            new_indexed_shared_table_store(
                app_context.storage_backend(),
                table_id,
                &pk_field,
                empty_storage_schema(),
                &scalar_indexes,
                &columns,
            )
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

fn catalog_open_args(
    app_context: &Arc<AppContext>,
    table_id: &TableId,
    fallback_pk: &str,
) -> (String, Vec<ScalarIndexDefinition>, Vec<ColumnDefinition>) {
    match app_context.schema_registry().get_table_if_exists(table_id) {
        Ok(Some(def)) => {
            let pk = def
                .columns
                .iter()
                .find(|column| column.is_primary_key)
                .map(|column| column.column_name.clone())
                .unwrap_or_else(|| fallback_pk.to_string());
            (pk, def.scalar_indexes.clone(), def.columns.clone())
        },
        _ => (fallback_pk.to_string(), Vec::new(), Vec::new()),
    }
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

    let mut files_deleted: usize = 0;

    if table_type == TableType::User {
        // User-table cold files are user-scoped by template. Delete every known user scope.
        let manifest_user_ids = app_context
            .manifest_service()
            .get_manifest_user_ids(table_id)
            .into_kalamdb_error("Failed to enumerate manifest user scopes for cleanup")?;

        for user_id in &manifest_user_ids {
            let deleted = storage_cached
                .delete_prefix(table_type, table_id, Some(user_id))
                .await
                .into_kalamdb_error("Failed to delete user-scoped Parquet tree")?
                .files_deleted;
            files_deleted += deleted;
        }

        // Fallback cleanup for legacy/non-user-scoped layouts and any unresolved residue.
        let deleted = storage_cached
            .delete_prefix(table_type, table_id, None)
            .await
            .into_kalamdb_error("Failed to delete fallback Parquet tree")?
            .files_deleted;
        files_deleted += deleted;
    } else {
        files_deleted = storage_cached
            .delete_prefix(table_type, table_id, None)
            .await
            .into_kalamdb_error("Failed to delete Parquet tree")?
            .files_deleted;
    }

    if table_type == TableType::Stream {
        let stream_table_dir = app_context
            .config()
            .storage
            .streams_dir()
            .join(table_id.namespace_id().as_str())
            .join(table_id.table_name().as_str());

        if stream_table_dir.exists() {
            fs::remove_dir_all(&stream_table_dir).map_err(|e| {
                KalamDbError::Other(format!(
                    "Failed to delete stream log directory '{}' for {}: {}",
                    stream_table_dir.display(),
                    table_id,
                    e
                ))
            })?;
        }
    }

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

#[cfg(test)]
mod tests {
    use std::fs;

    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;
    use crate::test_helpers::test_app_context_simple;

    #[tokio::test]
    async fn cleanup_stream_drop_removes_stream_log_directory() {
        let app_ctx = test_app_context_simple();

        let unique = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let table_id = TableId::new(
            NamespaceId::new(format!("cleanup_stream_ns_{}", unique)),
            TableName::new(format!("cleanup_stream_tbl_{}", unique)),
        );

        let stream_table_dir = app_ctx
            .config()
            .storage
            .streams_dir()
            .join(table_id.namespace_id().as_str())
            .join(table_id.table_name().as_str());
        fs::create_dir_all(&stream_table_dir).expect("create stream table dir");
        fs::write(stream_table_dir.join("orphan.log"), b"orphan").expect("create stream file");

        let storage = StorageCleanupDetails {
            storage_id:             StorageId::local(),
            base_directory:         ".".to_string(),
            relative_path_template: "{namespace}/{tableName}".to_string(),
        };

        cleanup_parquet_files_internal(&app_ctx, &table_id, TableType::Stream, &storage)
            .await
            .expect("cleanup stream drop");

        assert!(
            !stream_table_dir.exists(),
            "stream log directory should be removed when stream table is dropped"
        );
    }

    #[tokio::test]
    async fn cleanup_stream_drop_succeeds_when_stream_directory_missing() {
        let app_ctx = test_app_context_simple();

        let unique = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let table_id = TableId::new(
            NamespaceId::new(format!("cleanup_stream_missing_ns_{}", unique)),
            TableName::new(format!("cleanup_stream_missing_tbl_{}", unique)),
        );

        let stream_table_dir = app_ctx
            .config()
            .storage
            .streams_dir()
            .join(table_id.namespace_id().as_str())
            .join(table_id.table_name().as_str());
        let _ = fs::remove_dir_all(&stream_table_dir);

        let storage = StorageCleanupDetails {
            storage_id:             StorageId::local(),
            base_directory:         ".".to_string(),
            relative_path_template: "{namespace}/{tableName}".to_string(),
        };

        cleanup_parquet_files_internal(&app_ctx, &table_id, TableType::Stream, &storage)
            .await
            .expect("cleanup stream drop without dir");
    }
}
