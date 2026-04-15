//! Implementation of SharedDataApplier for provider persistence
//!
//! This module provides the concrete implementation of `kalamdb_raft::SharedDataApplier`
//! that persists shared table data to the actual providers (RocksDB-backed stores).
//!
//! Called by SharedDataStateMachine after Raft consensus on all nodes.

use async_trait::async_trait;
use std::sync::Arc;

use crate::app_context::AppContext;
use crate::applier::executor::CommandExecutorImpl;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::TransactionId;
use kalamdb_commons::TableId;
use kalamdb_raft::{RaftError, SharedDataApplier, TransactionApplyResult};
use kalamdb_transactions::StagedMutation;

/// SharedDataApplier implementation using Unified Command Executor
///
/// This is called by the Raft state machine when applying committed commands.
/// It delegates to `CommandExecutorImpl::dml()` for actual logic.
pub struct ProviderSharedDataApplier {
    executor: CommandExecutorImpl,
}

impl ProviderSharedDataApplier {
    /// Create a new ProviderSharedDataApplier
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            executor: CommandExecutorImpl::new(app_context),
        }
    }
}

#[async_trait]
impl SharedDataApplier for ProviderSharedDataApplier {
    async fn insert(&self, table_id: &TableId, rows: &[Row]) -> Result<usize, RaftError> {
        log::debug!("ProviderSharedDataApplier: Inserting into {} ({} rows)", table_id, rows.len());

        self.executor
            .dml()
            .insert_shared_data(table_id, rows)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn update(
        &self,
        table_id: &TableId,
        updates: &[Row],
        filter: Option<&str>,
    ) -> Result<usize, RaftError> {
        log::debug!("ProviderSharedDataApplier: Updating {} ({} rows)", table_id, updates.len());

        self.executor
            .dml()
            .update_shared_data(table_id, updates, filter)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn delete(
        &self,
        table_id: &TableId,
        pk_values: Option<&[String]>,
    ) -> Result<usize, RaftError> {
        log::debug!("ProviderSharedDataApplier: Deleting from {}", table_id);

        self.executor
            .dml()
            .delete_shared_data(table_id, pk_values)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn apply_transaction_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: &[StagedMutation],
    ) -> Result<TransactionApplyResult, RaftError> {
        self.executor
            .dml()
            .apply_shared_transaction_batch(transaction_id, mutations)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }
}
