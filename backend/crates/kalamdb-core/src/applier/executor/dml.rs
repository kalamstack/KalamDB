//! DML Executor - Handles data manipulation (INSERT/UPDATE/DELETE)
//!
//! This module contains the logic for executing data operations against
//! specific table providers. It is designed to be stateless and thread-safe,
//! allowing concurrent execution from multiple Data Raft groups.
//!
//! ## Design
//!
//! - Stateless: No internal locks, safe for parallel execution across shards
//! - Unified: Same code path for standalone and cluster modes
//! - Provider-agnostic: Handles User, Stream, and Shared table types

use std::sync::Arc;

use kalamdb_commons::ids::{StreamTableRowId, UserTableRowId};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::TableId;

use crate::app_context::AppContext;
use crate::applier::error::ApplierError;
use crate::applier::executor::utils::fileref_util::{
    collect_file_refs_from_row, collect_replaced_file_refs_for_update, delete_file_refs_best_effort,
};
use crate::providers::base::{find_row_by_pk, BaseTableProvider};
use crate::providers::{SharedTableProvider, StreamTableProvider, UserTableProvider};
use kalamdb_tables::{StreamTableRow, UserTableRow};

/// Executor for DML operations (Data Plane)
///
/// This executor is stateless and designed for concurrent access from
/// multiple Data Raft groups. Each method accesses the schema registry
/// to get the appropriate provider and performs the operation.
pub struct DmlExecutor {
    app_context: Arc<AppContext>,
}

impl DmlExecutor {
    /// Create a new DmlExecutor
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    async fn load_provider(
        &self,
        table_id: &TableId,
        provider_label: &'static str,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider + Send + Sync>, ApplierError> {
        let schema_registry = self.app_context.schema_registry();

        if let Some(provider) = schema_registry.get_provider(table_id) {
            return Ok(provider);
        }

        schema_registry.get_table_if_exists_async(table_id).await.map_err(|e| {
            ApplierError::Execution(format!(
                "Failed to reload {} metadata for {}: {}",
                provider_label, table_id, e
            ))
        })?;

        schema_registry
            .get_provider(table_id)
            .ok_or_else(|| ApplierError::not_found(provider_label, table_id))
    }

    // =========================================================================
    // User Table Operations (per-user sharded data)
    // =========================================================================

    /// Insert rows into a user table
    pub async fn insert_user_data(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        rows: &[Row],
    ) -> Result<usize, ApplierError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let provider_arc = self.load_provider(table_id, "Table provider").await?;

        // Try UserTableProvider first, then StreamTableProvider
        if let Some(provider) = provider_arc.as_any().downcast_ref::<UserTableProvider>() {
            let row_ids = provider
                .insert_batch(user_id, rows.to_vec())
                .await
                .map_err(|e| ApplierError::Execution(format!("Failed to insert batch: {}", e)))?;
            log::debug!("DmlExecutor: Inserted {} rows into {}", row_ids.len(), table_id);
            Ok(row_ids.len())
        } else if let Some(provider) = provider_arc.as_any().downcast_ref::<StreamTableProvider>() {
            let row_ids = provider.insert_batch(user_id, rows.to_vec()).await.map_err(|e| {
                ApplierError::Execution(format!("Failed to insert stream batch: {}", e))
            })?;
            log::debug!("DmlExecutor: Inserted {} stream rows into {}", row_ids.len(), table_id);
            Ok(row_ids.len())
        } else {
            Err(ApplierError::Execution(format!(
                "Provider type mismatch for user table {}",
                table_id
            )))
        }
    }

    /// Update rows in a user table
    pub async fn update_user_data(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        updates: &[Row],
        filter: Option<&str>,
    ) -> Result<usize, ApplierError> {
        let pk_value = filter.ok_or_else(|| {
            ApplierError::Validation("Update requires filter with PK value".to_string())
        })?;

        let update_row = updates.first().ok_or_else(|| {
            ApplierError::Validation("Update requires at least one update row".to_string())
        })?;

        let provider_arc = self.load_provider(table_id, "Table provider").await?;

        if let Some(provider) = provider_arc.as_any().downcast_ref::<UserTableProvider>() {
            let prior_row =
                self.load_user_row_for_cleanup(provider, table_id, user_id, pk_value).await;

            let replaced_refs = prior_row.as_ref().map_or_else(Vec::new, |row| {
                collect_replaced_file_refs_for_update(
                    self.app_context.as_ref(),
                    table_id,
                    row,
                    update_row,
                )
            });

            let updated = self
                .update_user_provider(provider, user_id, pk_value, update_row.clone())
                .await?;
            if updated > 0 {
                delete_file_refs_best_effort(
                    self.app_context.as_ref(),
                    table_id,
                    TableType::User,
                    Some(user_id),
                    &replaced_refs,
                )
                .await;
            }
            Ok(updated)
        } else if let Some(provider) = provider_arc.as_any().downcast_ref::<StreamTableProvider>() {
            self.update_stream_provider(provider, user_id, pk_value, update_row.clone())
                .await
        } else {
            Err(ApplierError::Execution(format!(
                "Provider type mismatch for user table {}",
                table_id
            )))
        }
    }

    /// Delete rows from a user table
    pub async fn delete_user_data(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        pk_values: Option<&[String]>,
    ) -> Result<usize, ApplierError> {
        let pk_values = pk_values.ok_or_else(|| {
            ApplierError::Validation("Delete requires pk_values list".to_string())
        })?;

        if pk_values.is_empty() {
            return Ok(0);
        }

        let provider_arc = self.load_provider(table_id, "Table provider").await?;

        if let Some(provider) = provider_arc.as_any().downcast_ref::<UserTableProvider>() {
            let mut deleted_count = 0;
            for pk_value in pk_values {
                let file_refs = self
                    .load_user_row_for_cleanup(provider, table_id, user_id, pk_value)
                    .await
                    .map_or_else(Vec::new, |row| {
                        collect_file_refs_from_row(self.app_context.as_ref(), table_id, &row)
                    });

                if provider
                    .delete_by_id_field(user_id, pk_value)
                    .await
                    .map_err(|e| ApplierError::Execution(format!("Failed to delete row: {}", e)))?
                {
                    deleted_count += 1;
                    delete_file_refs_best_effort(
                        self.app_context.as_ref(),
                        table_id,
                        TableType::User,
                        Some(user_id),
                        &file_refs,
                    )
                    .await;
                }
            }
            log::debug!("DmlExecutor: Deleted {} rows from {}", deleted_count, table_id);
            Ok(deleted_count)
        } else if let Some(provider) = provider_arc.as_any().downcast_ref::<StreamTableProvider>() {
            let mut deleted_count = 0;
            for pk_value in pk_values {
                if provider
                    .delete_by_id_field(user_id, pk_value)
                    .await
                    .map_err(|e| ApplierError::Execution(format!("Failed to delete row: {}", e)))?
                {
                    deleted_count += 1;
                }
            }
            log::debug!("DmlExecutor: Deleted {} stream rows from {}", deleted_count, table_id);
            Ok(deleted_count)
        } else {
            Err(ApplierError::Execution(format!(
                "Provider type mismatch for user table {}",
                table_id
            )))
        }
    }

    // =========================================================================
    // Shared Table Operations (global data, not per-user)
    // =========================================================================

    /// Insert rows into a shared table
    pub async fn insert_shared_data(
        &self,
        table_id: &TableId,
        rows: &[Row],
    ) -> Result<usize, ApplierError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let provider_arc = self.load_provider(table_id, "Shared table provider").await?;

        if let Some(provider) = provider_arc.as_any().downcast_ref::<SharedTableProvider>() {
            let system_user = UserId::system();
            let row_ids = provider
                .insert_batch(&system_user, rows.to_vec())
                .await
                .map_err(|e| ApplierError::Execution(format!("Failed to insert batch: {}", e)))?;
            log::debug!("DmlExecutor: Inserted {} shared rows into {}", row_ids.len(), table_id);
            Ok(row_ids.len())
        } else {
            Err(ApplierError::Execution(format!(
                "Provider type mismatch for shared table {}",
                table_id
            )))
        }
    }

    /// Update rows in a shared table
    pub async fn update_shared_data(
        &self,
        table_id: &TableId,
        updates: &[Row],
        filter: Option<&str>,
    ) -> Result<usize, ApplierError> {
        if updates.is_empty() {
            return Ok(0);
        }

        let pk_value = filter.ok_or_else(|| {
            ApplierError::Validation("Update requires filter with PK value".to_string())
        })?;

        let provider_arc = self.load_provider(table_id, "Shared table provider").await?;

        if let Some(provider) = provider_arc.as_any().downcast_ref::<SharedTableProvider>() {
            let system_user = UserId::system();
            let update_row = updates[0].clone();

            let prior_row = self.load_shared_row_for_cleanup(provider, table_id, pk_value).await;

            let replaced_refs = prior_row.as_ref().map_or_else(Vec::new, |row| {
                collect_replaced_file_refs_for_update(
                    self.app_context.as_ref(),
                    table_id,
                    row,
                    &update_row,
                )
            });

            let updated = provider
                .update_by_id_field(&system_user, pk_value, update_row)
                .await
                .map_err(|e| ApplierError::Execution(format!("Failed to update row: {}", e)))?;

            let affected_rows = usize::from(updated.is_some());
            if affected_rows > 0 {
                delete_file_refs_best_effort(
                    self.app_context.as_ref(),
                    table_id,
                    TableType::Shared,
                    None,
                    &replaced_refs,
                )
                .await;
            }

            log::debug!(
                "DmlExecutor: Updated {} shared row(s) in {} (pk={})",
                affected_rows,
                table_id,
                pk_value
            );
            Ok(affected_rows)
        } else {
            Err(ApplierError::Execution(format!(
                "Provider type mismatch for shared table {}",
                table_id
            )))
        }
    }

    /// Delete rows from a shared table
    pub async fn delete_shared_data(
        &self,
        table_id: &TableId,
        pk_values: Option<&[String]>,
    ) -> Result<usize, ApplierError> {
        let pk_values = pk_values.ok_or_else(|| {
            ApplierError::Validation("Delete requires pk_values list".to_string())
        })?;

        if pk_values.is_empty() {
            return Ok(0);
        }

        let provider_arc = self.load_provider(table_id, "Shared table provider").await?;

        if let Some(provider) = provider_arc.as_any().downcast_ref::<SharedTableProvider>() {
            let system_user = UserId::system();
            let mut deleted_count = 0;

            for pk_value in pk_values {
                let file_refs = self
                    .load_shared_row_for_cleanup(provider, table_id, pk_value)
                    .await
                    .map_or_else(Vec::new, |row| {
                        collect_file_refs_from_row(self.app_context.as_ref(), table_id, &row)
                    });

                if provider
                    .delete_by_id_field(&system_user, pk_value)
                    .await
                    .map_err(|e| ApplierError::Execution(format!("Failed to delete row: {}", e)))?
                {
                    deleted_count += 1;
                    delete_file_refs_best_effort(
                        self.app_context.as_ref(),
                        table_id,
                        TableType::Shared,
                        None,
                        &file_refs,
                    )
                    .await;
                }
            }

            log::debug!("DmlExecutor: Deleted {} shared rows from {}", deleted_count, table_id);
            Ok(deleted_count)
        } else {
            Err(ApplierError::Execution(format!(
                "Provider type mismatch for shared table {}",
                table_id
            )))
        }
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    async fn load_user_row_for_cleanup(
        &self,
        provider: &UserTableProvider,
        table_id: &TableId,
        user_id: &UserId,
        pk_value: &str,
    ) -> Option<Row> {
        let pk_name = provider.primary_key_field_name();
        let schema = provider.schema_ref();
        let pk_field = match schema.field_with_name(pk_name) {
            Ok(field) => field,
            Err(err) => {
                log::warn!(
                    "Failed to resolve PK field for file cleanup in {} (pk={}): {}",
                    table_id,
                    pk_value,
                    err
                );
                return None;
            },
        };

        let pk_scalar = match kalamdb_commons::conversions::parse_string_as_scalar(
            pk_value,
            pk_field.data_type(),
        ) {
            Ok(value) => value,
            Err(err) => {
                log::warn!(
                    "Failed to parse PK value for file cleanup in {} (pk={}): {}",
                    table_id,
                    pk_value,
                    err
                );
                return None;
            },
        };

        match provider.find_by_pk(user_id, &pk_scalar).await {
            Ok(Some((_key, row))) => Some(row.fields),
            Ok(None) => match find_row_by_pk(provider, Some(user_id), pk_value).await {
                Ok(Some((_key, row))) => Some(row.fields),
                Ok(None) => None,
                Err(err) => {
                    log::warn!(
                        "Failed to load cold row for file cleanup in {} (pk={}): {}",
                        table_id,
                        pk_value,
                        err
                    );
                    None
                },
            },
            Err(err) => {
                log::warn!(
                    "Failed to load hot row for file cleanup in {} (pk={}): {}",
                    table_id,
                    pk_value,
                    err
                );
                None
            },
        }
    }

    async fn load_shared_row_for_cleanup(
        &self,
        provider: &SharedTableProvider,
        table_id: &TableId,
        pk_value: &str,
    ) -> Option<Row> {
        let pk_name = provider.primary_key_field_name();
        let schema = provider.schema_ref();
        let pk_field = match schema.field_with_name(pk_name) {
            Ok(field) => field,
            Err(err) => {
                log::warn!(
                    "Failed to resolve PK field for file cleanup in {} (pk={}): {}",
                    table_id,
                    pk_value,
                    err
                );
                return None;
            },
        };

        let pk_scalar = match kalamdb_commons::conversions::parse_string_as_scalar(
            pk_value,
            pk_field.data_type(),
        ) {
            Ok(value) => value,
            Err(err) => {
                log::warn!(
                    "Failed to parse PK value for file cleanup in {} (pk={}): {}",
                    table_id,
                    pk_value,
                    err
                );
                return None;
            },
        };

        match provider.find_by_pk(&pk_scalar).await {
            Ok(Some((_key, row))) => Some(row.fields),
            Ok(None) => match find_row_by_pk(provider, None, pk_value).await {
                Ok(Some((_key, row))) => Some(row.fields),
                Ok(None) => None,
                Err(err) => {
                    log::warn!(
                        "Failed to load cold row for file cleanup in {} (pk={}): {}",
                        table_id,
                        pk_value,
                        err
                    );
                    None
                },
            },
            Err(err) => {
                log::warn!(
                    "Failed to load hot row for file cleanup in {} (pk={}): {}",
                    table_id,
                    pk_value,
                    err
                );
                None
            },
        }
    }

    /// Update with fallback for UserTableProvider
    async fn update_user_provider(
        &self,
        provider: &UserTableProvider,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<usize, ApplierError> {
        match provider.update_by_id_field(user_id, pk_value, updates.clone()).await {
            Ok(result) => Ok(usize::from(result.is_some())),
            Err(kalamdb_tables::TableError::NotFound(_)) => {
                if let Some(key) =
                    provider.find_row_key_by_id_field(user_id, pk_value).await.map_err(|e| {
                        ApplierError::Execution(format!("Failed to find row key: {}", e))
                    })?
                {
                    let updated = <UserTableProvider as BaseTableProvider<
                        UserTableRowId,
                        UserTableRow,
                    >>::update(provider, user_id, &key, updates)
                    .await
                    .map_err(|e| ApplierError::Execution(format!("Failed to update row: {}", e)))?;
                    Ok(usize::from(updated.is_some()))
                } else {
                    Ok(0)
                }
            },
            Err(e) => Err(ApplierError::Execution(format!("Failed to update row: {}", e))),
        }
    }

    /// Update with fallback for StreamTableProvider
    async fn update_stream_provider(
        &self,
        provider: &StreamTableProvider,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<usize, ApplierError> {
        match provider.update_by_id_field(user_id, pk_value, updates.clone()).await {
            Ok(result) => Ok(usize::from(result.is_some())),
            Err(kalamdb_tables::TableError::NotFound(_)) => {
                if let Some(key) =
                    provider.find_row_key_by_id_field(user_id, pk_value).await.map_err(|e| {
                        ApplierError::Execution(format!("Failed to find row key: {}", e))
                    })?
                {
                    let updated = <StreamTableProvider as BaseTableProvider<
                        StreamTableRowId,
                        StreamTableRow,
                    >>::update(provider, user_id, &key, updates)
                    .await
                    .map_err(|e| {
                        ApplierError::Execution(format!("Failed to update row: {}", e))
                    })?;
                    Ok(usize::from(updated.is_some()))
                } else {
                    Ok(0)
                }
            },
            Err(e) => Err(ApplierError::Execution(format!("Failed to update row: {}", e))),
        }
    }
}
