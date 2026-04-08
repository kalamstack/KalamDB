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
use kalamdb_commons::models::{OperationKind, TopicOp, TransactionId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::websocket::{ChangeNotification, ChangeType};
use kalamdb_commons::TableId;
use kalamdb_raft::TransactionApplyResult;
use kalamdb_system::{NotificationService as NotificationServiceTrait, TopicPublisher};
use kalamdb_transactions::StagedMutation;

use crate::app_context::AppContext;
use crate::applier::error::ApplierError;
use crate::applier::executor::utils::fileref_util::{
    collect_file_refs_from_row, collect_replaced_file_refs_for_update, delete_file_refs_best_effort,
};
use crate::providers::base::{find_row_by_pk, BaseTableProvider};
use crate::providers::{SharedTableProvider, StreamTableProvider, UserTableProvider};
use crate::transactions::{CommitSideEffectPlan, FanoutOwnerScope};
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
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        self.insert_user_data_with_commit_seq(table_id, user_id, rows, commit_seq).await
    }

    pub async fn insert_user_data_with_commit_seq(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        rows: &[Row],
        commit_seq: u64,
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
            for row_id in &row_ids {
                provider.patch_commit_seq_for_row_key(row_id, commit_seq).await.map_err(|e| {
                    ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                })?;
            }
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
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        self.update_user_data_with_commit_seq(table_id, user_id, updates, filter, commit_seq)
            .await
    }

    pub async fn update_user_data_with_commit_seq(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        updates: &[Row],
        filter: Option<&str>,
        commit_seq: u64,
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
                provider
                    .patch_latest_commit_seq_by_pk(user_id, pk_value, commit_seq)
                    .await
                    .map_err(|e| {
                        ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                    })?;
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
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        self.delete_user_data_with_commit_seq(table_id, user_id, pk_values, commit_seq)
            .await
    }

    pub async fn delete_user_data_with_commit_seq(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        pk_values: Option<&[String]>,
        commit_seq: u64,
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
                    provider
                        .patch_latest_commit_seq_by_pk(user_id, pk_value, commit_seq)
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                        })?;
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
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        self.insert_shared_data_with_commit_seq(table_id, rows, commit_seq).await
    }

    pub async fn insert_shared_data_with_commit_seq(
        &self,
        table_id: &TableId,
        rows: &[Row],
        commit_seq: u64,
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
            for row_id in &row_ids {
                provider.patch_commit_seq_for_row_key(row_id, commit_seq).await.map_err(|e| {
                    ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                })?;
            }
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
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        self.update_shared_data_with_commit_seq(table_id, updates, filter, commit_seq)
            .await
    }

    pub async fn update_shared_data_with_commit_seq(
        &self,
        table_id: &TableId,
        updates: &[Row],
        filter: Option<&str>,
        commit_seq: u64,
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
                provider
                    .patch_latest_commit_seq_by_pk(pk_value, commit_seq)
                    .await
                    .map_err(|e| {
                        ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                    })?;
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
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        self.delete_shared_data_with_commit_seq(table_id, pk_values, commit_seq)
            .await
    }

    pub async fn delete_shared_data_with_commit_seq(
        &self,
        table_id: &TableId,
        pk_values: Option<&[String]>,
        commit_seq: u64,
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
                    provider
                        .patch_latest_commit_seq_by_pk(pk_value, commit_seq)
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                        })?;
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

    pub async fn apply_user_transaction_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: &[StagedMutation],
    ) -> Result<TransactionApplyResult, ApplierError> {
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        let mut affected_rows = 0;
        let mut side_effect_plan = CommitSideEffectPlan::new(transaction_id.clone());

        for mutation in mutations {
            if &mutation.transaction_id != transaction_id {
                return Err(ApplierError::Validation(format!(
                    "staged mutation transaction mismatch: expected '{}', got '{}'",
                    transaction_id, mutation.transaction_id
                )));
            }

            let user_id = mutation.user_id.clone().ok_or_else(|| {
                ApplierError::Validation("user transaction batch mutation missing user_id".to_string())
            })?;

            let provider_arc = self.load_provider(&mutation.table_id, "Table provider").await?;
            let provider = provider_arc
                .as_any()
                .downcast_ref::<UserTableProvider>()
                .ok_or_else(|| {
                    ApplierError::Execution(format!(
                        "Provider type mismatch for user table {}",
                        mutation.table_id
                    ))
                })?;

            let applied = match mutation.operation_kind {
                OperationKind::Insert => {
                    let (row_key, notification) = provider
                        .insert_deferred(&user_id, mutation.payload.clone())
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to insert batch row: {}", e))
                        })?;
                    provider.patch_commit_seq_for_row_key(&row_key, commit_seq).await.map_err(
                        |e| ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e)),
                    )?;
                    Some((row_key, notification))
                },
                OperationKind::Update => {
                    provider
                        .update_by_pk_value_deferred(
                            &user_id,
                            mutation.primary_key.as_str(),
                            mutation.payload.clone(),
                        )
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to update row: {}", e))
                        })?
                        .map(|(row_key, notification)| {
                            (row_key, notification)
                        })
                },
                OperationKind::Delete => {
                    provider
                        .delete_by_pk_value_deferred(&user_id, mutation.primary_key.as_str())
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to delete row: {}", e))
                        })?
                },
            };

            let Some((row_key, notification)) = applied else {
                continue;
            };

            if !matches!(mutation.operation_kind, OperationKind::Insert) {
                provider.patch_commit_seq_for_row_key(&row_key, commit_seq).await.map_err(|e| {
                    ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                })?;
            }

            affected_rows += 1;
            side_effect_plan.record_manifest_update();

            if let Some(notification) = notification {
                if self
                    .publish_transaction_notification(Some(&user_id), &notification)
                    .await
                {
                    side_effect_plan.record_publisher_event();
                }

                if NotificationServiceTrait::has_subscribers(
                    self.app_context.notification_service().as_ref(),
                    Some(&user_id),
                    &mutation.table_id,
                ) {
                    side_effect_plan.push_notification(
                        FanoutOwnerScope::User(user_id.clone()),
                        notification,
                    );
                }
            }
        }

        let notifications_sent = self
            .app_context
            .notification_service()
            .dispatch_commit_plan(&side_effect_plan);

        Ok(TransactionApplyResult {
            rows_affected: affected_rows,
            commit_seq,
            notifications_sent,
            manifest_updates: side_effect_plan.manifest_updates,
            publisher_events: side_effect_plan.publisher_events,
        })
    }

    pub async fn apply_shared_transaction_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: &[StagedMutation],
    ) -> Result<TransactionApplyResult, ApplierError> {
        let commit_seq = self.app_context.commit_sequence_tracker().allocate_next();
        let mut affected_rows = 0;
        let mut side_effect_plan = CommitSideEffectPlan::new(transaction_id.clone());

        for mutation in mutations {
            if &mutation.transaction_id != transaction_id {
                return Err(ApplierError::Validation(format!(
                    "staged mutation transaction mismatch: expected '{}', got '{}'",
                    transaction_id, mutation.transaction_id
                )));
            }

            let provider_arc = self.load_provider(&mutation.table_id, "Shared table provider").await?;
            let provider = provider_arc
                .as_any()
                .downcast_ref::<SharedTableProvider>()
                .ok_or_else(|| {
                    ApplierError::Execution(format!(
                        "Provider type mismatch for shared table {}",
                        mutation.table_id
                    ))
                })?;

            let applied = match mutation.operation_kind {
                OperationKind::Insert => {
                    let (row_key, notification) = provider
                        .insert_deferred(mutation.payload.clone())
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to insert batch row: {}", e))
                        })?;
                    provider.patch_commit_seq_for_row_key(&row_key, commit_seq).await.map_err(
                        |e| ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e)),
                    )?;
                    Some((row_key, notification))
                },
                OperationKind::Update => {
                    provider
                        .update_by_pk_value_deferred(
                            mutation.primary_key.as_str(),
                            mutation.payload.clone(),
                        )
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to update row: {}", e))
                        })?
                },
                OperationKind::Delete => {
                    provider
                        .delete_by_pk_value_deferred(mutation.primary_key.as_str())
                        .await
                        .map_err(|e| {
                            ApplierError::Execution(format!("Failed to delete row: {}", e))
                        })?
                },
            };

            let Some((row_key, notification)) = applied else {
                continue;
            };

            if !matches!(mutation.operation_kind, OperationKind::Insert) {
                provider.patch_commit_seq_for_row_key(&row_key, commit_seq).await.map_err(|e| {
                    ApplierError::Execution(format!("Failed to stamp commit_seq: {}", e))
                })?;
            }

            affected_rows += 1;
            side_effect_plan.record_manifest_update();

            if let Some(notification) = notification {
                if self.publish_transaction_notification(None, &notification).await {
                    side_effect_plan.record_publisher_event();
                }

                if NotificationServiceTrait::has_subscribers(
                    self.app_context.notification_service().as_ref(),
                    None,
                    &mutation.table_id,
                ) {
                    side_effect_plan.push_notification(FanoutOwnerScope::Shared, notification);
                }
            }
        }

        let notifications_sent = self
            .app_context
            .notification_service()
            .dispatch_commit_plan(&side_effect_plan);

        Ok(TransactionApplyResult {
            rows_affected: affected_rows,
            commit_seq,
            notifications_sent,
            manifest_updates: side_effect_plan.manifest_updates,
            publisher_events: side_effect_plan.publisher_events,
        })
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    #[inline]
    fn topic_op_for_change(change_type: &ChangeType) -> TopicOp {
        match change_type {
            ChangeType::Insert => TopicOp::Insert,
            ChangeType::Update => TopicOp::Update,
            ChangeType::Delete => TopicOp::Delete,
        }
    }

    async fn publish_transaction_notification(
        &self,
        user_id: Option<&UserId>,
        notification: &ChangeNotification,
    ) -> bool {
        let topic_publisher = self.app_context.topic_publisher();
        if !topic_publisher.has_topics_for_table(&notification.table_id) {
            return false;
        }

        let is_leader = match user_id {
            Some(user_id) => self.app_context.is_leader_for_user(user_id).await,
            None => self.app_context.is_leader_for_shared().await,
        };
        if !is_leader {
            return false;
        }

        let op = Self::topic_op_for_change(&notification.change_type);
        if let Err(error) =
            topic_publisher.publish_for_table(&notification.table_id, op, &notification.row_data, user_id)
        {
            log::warn!(
                "Topic publish failed for transaction change on table {}: {}",
                notification.table_id,
                error
            );
            return false;
        }

        true
    }

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
