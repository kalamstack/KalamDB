//! File cleanup helpers

use kalamdb_commons::models::ids::StorageId;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_system::FileRef;
use std::collections::HashMap;

/// Cleanup files after SQL error
pub async fn cleanup_files(
    file_refs: &HashMap<String, FileRef>,
    storage_id: &StorageId,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    app_context: &kalamdb_core::app_context::AppContext,
) {
    let file_service = app_context.file_storage_service();
    for file_ref in file_refs.values() {
        if let Err(err) = file_service
            .delete_file(file_ref, storage_id, table_type, table_id, user_id)
            .await
        {
            log::warn!("Failed to cleanup file {} after SQL error: {}", file_ref.id, err);
        }
    }
}
