//! Append version function for MVCC DML operations
//!
//! This module implements append_version(), the core function used by INSERT/UPDATE/DELETE
//! to create new versions in the hot storage layer.

use crate::error::KalamDbError;
use crate::{SharedTableRow, SharedTableStore, UserTableRow, UserTableStore};
use kalamdb_commons::conversions::arrow_json_conversion::json_to_row;
use kalamdb_commons::ids::{SeqId, UserTableRowId};
use kalamdb_commons::models::schemas::TableType;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_store::EntityStore;
use kalamdb_system::SystemColumnsService;
use std::sync::Arc;

/// Append a new version to the table's hot storage (synchronous)
///
/// **MVCC Architecture**: This function is used by INSERT, UPDATE, and DELETE operations.
/// - INSERT: Appends new row with _deleted=false
/// - UPDATE: Appends modified row with new SeqId and _deleted=false
/// - DELETE: Appends tombstone row with _deleted=true
///
/// # Arguments
/// * `system_columns` - System columns service for SeqId generation
/// * `user_table_store` - User table store (for user tables)
/// * `shared_table_store` - Shared table store (for shared tables)
/// * `_table_id` - Table identifier (unused, for future extensions)
/// * `table_type` - User or Shared table type
/// * `user_id` - User ID (required for user tables, ignored for shared tables)
/// * `fields` - JSON object with all user-defined columns (including PK)
/// * `deleted` - Tombstone flag (false for INSERT/UPDATE, true for DELETE)
///
/// # Returns
/// * `Ok(SeqId)` - The sequence ID of the newly appended version
/// * `Err(KalamDbError)` - If append fails
pub fn append_version_sync_with_deps(
    system_columns: Arc<SystemColumnsService>,
    user_table_store: Arc<UserTableStore>,
    shared_table_store: Arc<SharedTableStore>,
    _table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    fields: serde_json::Value,
    deleted: bool,
) -> Result<SeqId, KalamDbError> {
    // T060: Validate PRIMARY KEY (basic check - full uniqueness validation in provider)
    // This is a safety check; actual uniqueness validation happens in the provider layer
    // to avoid scanning all rows on every insert

    // Validate table type
    match table_type {
        TableType::System => {
            return Err(KalamDbError::InvalidOperation(
                "System tables cannot use append_version()".to_string(),
            ));
        },
        TableType::Stream => {
            return Err(KalamDbError::InvalidOperation(
                "Stream tables cannot use append_version()".to_string(),
            ));
        },
        _ => {},
    }

    // Generate new SeqId via SystemColumnsService
    let seq_id = system_columns
        .generate_seq_id()
        .map_err(|e| KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e)))?;

    match table_type {
        TableType::User => {
            let user_id = user_id.ok_or_else(|| {
                KalamDbError::InvalidOperation("user_id required for user table".to_string())
            })?;

            // Create UserTableRow
            let entity = UserTableRow {
                user_id: user_id.clone(),
                _seq: seq_id,
                _commit_seq: 0,
                _deleted: deleted,
                fields: json_to_row(&fields).ok_or_else(|| {
                    KalamDbError::InvalidOperation(
                        "Invalid JSON fields: must be an object".to_string(),
                    )
                })?,
            };

            // Create composite key
            let row_key = UserTableRowId::new(user_id.clone(), seq_id);

            // Store the entity
            user_table_store.put(&row_key, &entity).map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to append version to user table: {}",
                    e
                ))
            })?;

            log::debug!(
                "Appended version to user table for user {} with _seq {} (deleted: {})",
                user_id.as_str(),
                seq_id,
                deleted
            );

            Ok(seq_id)
        },
        TableType::Shared => {
            // Create SharedTableRow
            let entity = SharedTableRow {
                _seq: seq_id,
                _commit_seq: 0,
                _deleted: deleted,
                fields: json_to_row(&fields).ok_or_else(|| {
                    KalamDbError::InvalidOperation(
                        "Invalid JSON fields: must be an object".to_string(),
                    )
                })?,
            };

            // Key is just the SeqId (SharedTableRowId is a type alias for SeqId)
            let row_key = seq_id;

            // Store the entity
            shared_table_store.put(&row_key, &entity).map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to append version to shared table: {}",
                    e
                ))
            })?;

            log::debug!(
                "Appended version to shared table with _seq {} (deleted: {})",
                seq_id,
                deleted
            );

            Ok(seq_id)
        },
        TableType::System | TableType::Stream => {
            // The early validation above returns Err for these types;
            // this arm is a defensive fallback in case the guard is ever bypassed.
            Err(KalamDbError::InvalidOperation(format!(
                "{:?} tables cannot be modified via append_version()",
                table_type
            )))
        },
    }
}

/// Append a new version to the table's hot storage (async wrapper)
///
/// **MVCC Architecture**: This function is used by INSERT, UPDATE, and DELETE operations.
/// - INSERT: Appends new row with _deleted=false
/// - UPDATE: Appends modified row with new SeqId and _deleted=false
/// - DELETE: Appends tombstone row with _deleted=true
///
/// # Arguments
/// * `system_columns` - System columns service for SeqId generation
/// * `user_table_store` - User table store (for user tables)
/// * `shared_table_store` - Shared table store (for shared tables)
/// * `table_id` - Table identifier
/// * `table_type` - User or Shared table type
/// * `user_id` - User ID (required for user tables, ignored for shared tables)
/// * `fields` - JSON object with all user-defined columns (including PK)
/// * `deleted` - Tombstone flag (false for INSERT/UPDATE, true for DELETE)
///
/// # Returns
/// * `Ok(SeqId)` - The sequence ID of the newly appended version
/// * `Err(KalamDbError)` - If append fails
pub async fn append_version(
    system_columns: Arc<SystemColumnsService>,
    user_table_store: Arc<UserTableStore>,
    shared_table_store: Arc<SharedTableStore>,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    fields: serde_json::Value,
    deleted: bool,
) -> Result<SeqId, KalamDbError> {
    // Delegate to synchronous implementation
    append_version_sync_with_deps(
        system_columns,
        user_table_store,
        shared_table_store,
        table_id,
        table_type,
        user_id,
        fields,
        deleted,
    )
}
