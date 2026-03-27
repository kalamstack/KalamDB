//! Implementation of UserDataApplier for provider persistence
//!
//! This module provides the concrete implementation of `kalamdb_raft::UserDataApplier`
//! that persists user table data to the actual providers (RocksDB-backed stores).
//!
//! Called by UserDataStateMachine after Raft consensus on all nodes.

use async_trait::async_trait;
use std::sync::Arc;

use crate::app_context::AppContext;
use crate::applier::executor::CommandExecutorImpl;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{ConnectionId, LiveQueryId, NodeId, UserId};
use kalamdb_commons::TableId;
use kalamdb_raft::{RaftError, UserDataApplier};
use kalamdb_system::providers::live_queries::models::LiveQuery;

/// UserDataApplier implementation using Unified Command Executor
///
/// This is called by the Raft state machine when applying committed commands.
/// It delegates to `CommandExecutorImpl::dml()` for actual logic.
pub struct ProviderUserDataApplier {
    executor: CommandExecutorImpl,
}

impl ProviderUserDataApplier {
    /// Create a new ProviderUserDataApplier
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            executor: CommandExecutorImpl::new(app_context),
        }
    }
}

#[async_trait]
impl UserDataApplier for ProviderUserDataApplier {
    async fn insert(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        rows: &[Row],
    ) -> Result<usize, RaftError> {
        log::debug!(
            "ProviderUserDataApplier: Inserting into {} for user {} ({} rows)",
            table_id,
            user_id,
            rows.len()
        );

        self.executor
            .dml()
            .insert_user_data(table_id, user_id, rows)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn update(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        updates: &[Row],
        filter: Option<&str>,
    ) -> Result<usize, RaftError> {
        log::debug!(
            "ProviderUserDataApplier: Updating {} for user {} ({} rows)",
            table_id,
            user_id,
            updates.len()
        );

        self.executor
            .dml()
            .update_user_data(table_id, user_id, updates, filter)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn delete(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        pk_values: Option<&[String]>,
    ) -> Result<usize, RaftError> {
        log::debug!("ProviderUserDataApplier: Deleting from {} for user {}", table_id, user_id);

        self.executor
            .dml()
            .delete_user_data(table_id, user_id, pk_values)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    // =========================================================================
    // Live Query Operations - Persist to system.live_queries
    // =========================================================================

    async fn create_live_query(&self, live_query: &LiveQuery) -> Result<String, RaftError> {
        log::info!(
            "ProviderUserDataApplier: Creating live query {} on node {} for user {}",
            live_query.live_id,
            live_query.node_id,
            live_query.user_id
        );

        let app_context = self.executor.app_context().clone();
        let live_query = live_query.clone();
        tokio::task::spawn_blocking(move || {
            app_context
                .system_tables()
                .live_queries()
                .create_live_query(live_query.clone())
                .map_err(|e| RaftError::Internal(format!("Failed to create live query: {}", e)))?;

            Ok(format!(
                "Live query {} created for table {}.{}",
                live_query.live_id, live_query.namespace_id, live_query.table_name
            ))
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn update_live_query_stats(
        &self,
        live_id: &LiveQueryId,
        last_update: i64,
        changes: i64,
    ) -> Result<(), RaftError> {
        log::trace!(
            "ProviderUserDataApplier: Updating live query stats {} (changes={})",
            live_id,
            changes
        );

        let app_context = self.executor.app_context().clone();
        let live_id = live_id.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(mut lq) = app_context
                .system_tables()
                .live_queries()
                .get_live_query_by_id(&live_id)
                .map_err(|e| RaftError::Internal(format!("Failed to get live query: {}", e)))?
            {
                lq.last_update = last_update;
                lq.changes = changes;

                app_context
                    .system_tables()
                    .live_queries()
                    .update_live_query(lq)
                    .map_err(|e| {
                        RaftError::Internal(format!("Failed to update live query: {}", e))
                    })?;
            }
            Ok(())
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn delete_live_query(
        &self,
        live_id: &LiveQueryId,
        _deleted_at: i64,
    ) -> Result<(), RaftError> {
        log::info!("ProviderUserDataApplier: Deleting live query {}", live_id);

        let app_context = self.executor.app_context().clone();
        let live_id = live_id.clone();
        tokio::task::spawn_blocking(move || {
            app_context
                .system_tables()
                .live_queries()
                .delete_live_query(&live_id)
                .map_err(|e| RaftError::Internal(format!("Failed to delete live query: {}", e)))?;
            Ok(())
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn delete_live_queries_by_connection(
        &self,
        connection_id: &ConnectionId,
        _deleted_at: i64,
    ) -> Result<usize, RaftError> {
        log::debug!(
            "ProviderUserDataApplier: Deleting live queries for connection {}",
            connection_id
        );

        let app_context = self.executor.app_context().clone();
        let connection_id = connection_id.clone();
        tokio::task::spawn_blocking(move || {
            // Get all live queries for this connection and delete them
            let live_queries = app_context
                .system_tables()
                .live_queries()
                .list_live_queries()
                .map_err(|e| RaftError::Internal(format!("Failed to list live queries: {}", e)))?;

            let mut deleted_count = 0;
            for lq in live_queries {
                if lq.connection_id == connection_id.as_str() {
                    app_context
                        .system_tables()
                        .live_queries()
                        .delete_live_query(&lq.live_id)
                        .map_err(|e| {
                            RaftError::Internal(format!(
                                "Failed to delete live query {}: {}",
                                lq.live_id, e
                            ))
                        })?;
                    deleted_count += 1;
                }
            }

            Ok(deleted_count)
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }

    async fn cleanup_node_subscriptions(&self, failed_node_id: NodeId) -> Result<usize, RaftError> {
        log::info!(
            "ProviderUserDataApplier: Cleaning up subscriptions for failed node {}",
            failed_node_id
        );

        let app_context = self.executor.app_context().clone();
        tokio::task::spawn_blocking(move || {
            // Get all live queries and delete those on the failed node
            let live_queries = app_context
                .system_tables()
                .live_queries()
                .list_live_queries()
                .map_err(|e| RaftError::Internal(format!("Failed to list live queries: {}", e)))?;

            let mut removed_count = 0;
            for lq in live_queries {
                if lq.node_id == failed_node_id {
                    app_context
                        .system_tables()
                        .live_queries()
                        .delete_live_query(&lq.live_id)
                        .map_err(|e| {
                            RaftError::Internal(format!(
                                "Failed to delete live query {}: {}",
                                lq.live_id, e
                            ))
                        })?;
                    removed_count += 1;
                }
            }

            Ok(removed_count)
        })
        .await
        .map_err(|e| RaftError::Internal(format!("Task join error: {}", e)))?
    }
}
