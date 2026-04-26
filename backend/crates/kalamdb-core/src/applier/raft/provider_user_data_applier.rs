//! Implementation of UserDataApplier for provider persistence
//!
//! This module provides the concrete implementation of `kalamdb_raft::UserDataApplier`
//! that persists user table data to the actual providers (RocksDB-backed stores).
//!
//! Called by UserDataStateMachine after Raft consensus on all nodes.

use std::sync::Arc;

use async_trait::async_trait;
use kalamdb_commons::{
    models::{rows::Row, TransactionId, UserId},
    TableId,
};
use kalamdb_raft::{RaftError, TransactionApplyResult, UserDataApplier};
use kalamdb_transactions::StagedMutation;

use crate::{app_context::AppContext, applier::executor::CommandExecutorImpl};

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
        commit_seq: u64,
    ) -> Result<usize, RaftError> {
        log::debug!(
            "ProviderUserDataApplier: Inserting into {} for user {} ({} rows)",
            table_id,
            user_id,
            rows.len()
        );

        self.executor
            .dml()
            .insert_user_data_with_commit_seq(table_id, user_id, rows, commit_seq)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn update(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        updates: &[Row],
        filter: Option<&str>,
        commit_seq: u64,
    ) -> Result<usize, RaftError> {
        log::debug!(
            "ProviderUserDataApplier: Updating {} for user {} ({} rows)",
            table_id,
            user_id,
            updates.len()
        );

        self.executor
            .dml()
            .update_user_data_with_commit_seq(table_id, user_id, updates, filter, commit_seq)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn delete(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        pk_values: Option<&[String]>,
        commit_seq: u64,
    ) -> Result<usize, RaftError> {
        log::debug!("ProviderUserDataApplier: Deleting from {} for user {}", table_id, user_id);

        self.executor
            .dml()
            .delete_user_data_with_commit_seq(table_id, user_id, pk_values, commit_seq)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }

    async fn apply_transaction_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: &[StagedMutation],
        commit_seq: u64,
    ) -> Result<TransactionApplyResult, RaftError> {
        self.executor
            .dml()
            .apply_user_transaction_batch_with_commit_seq(transaction_id, mutations, commit_seq)
            .await
            .map_err(|e| RaftError::provider(e.to_string()))
    }
}
