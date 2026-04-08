//! Shared Data Applier trait for persisting shared table data
//!
//! This trait is called by SharedDataStateMachine after Raft consensus commits
//! a command. All nodes (leader and followers) call this.
//!
//! The implementation lives in kalamdb-core using provider infrastructure.

use async_trait::async_trait;
use kalamdb_commons::models::TransactionId;
use kalamdb_commons::TableId;
use kalamdb_transactions::StagedMutation;

use crate::{RaftError, TransactionApplyResult};

/// Applier callback for shared table data operations
///
/// This trait is called by SharedDataStateMachine after Raft consensus commits
/// a data command. All nodes (leader and followers) call this.
#[async_trait]
pub trait SharedDataApplier: Send + Sync {
    /// Insert rows into a shared table
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `rows` - Row data to insert
    ///
    /// # Returns
    /// Number of rows inserted
    async fn insert(
        &self,
        table_id: &TableId,
        rows: &[kalamdb_commons::models::rows::Row],
    ) -> Result<usize, RaftError>;

    /// Update rows in a shared table
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `updates` - Update row data
    /// * `filter` - Optional filter string (e.g., primary key value)
    ///
    /// # Returns
    /// Number of rows updated
    async fn update(
        &self,
        table_id: &TableId,
        updates: &[kalamdb_commons::models::rows::Row],
        filter: Option<&str>,
    ) -> Result<usize, RaftError>;

    /// Delete rows from a shared table
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `pk_values` - Optional list of primary key values to delete
    ///
    /// # Returns
    /// Number of rows deleted
    async fn delete(
        &self,
        table_id: &TableId,
        pk_values: Option<&[String]>,
    ) -> Result<usize, RaftError>;

    /// Apply an explicit-transaction write set inside one state-machine cycle.
    async fn apply_transaction_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: &[StagedMutation],
    ) -> Result<TransactionApplyResult, RaftError>;
}

/// No-op applier for testing or standalone scenarios
pub struct NoOpSharedDataApplier;

#[async_trait]
impl SharedDataApplier for NoOpSharedDataApplier {
    async fn insert(
        &self,
        _table_id: &TableId,
        _rows: &[kalamdb_commons::models::rows::Row],
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn update(
        &self,
        _table_id: &TableId,
        _updates: &[kalamdb_commons::models::rows::Row],
        _filter: Option<&str>,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn delete(
        &self,
        _table_id: &TableId,
        _pk_values: Option<&[String]>,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn apply_transaction_batch(
        &self,
        _transaction_id: &TransactionId,
        _mutations: &[StagedMutation],
    ) -> Result<TransactionApplyResult, RaftError> {
        Ok(TransactionApplyResult::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::{NamespaceId, TableName};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Mock shared applier for testing
    struct MockSharedDataApplier {
        insert_count: Arc<AtomicUsize>,
    }

    impl MockSharedDataApplier {
        fn new() -> Self {
            Self {
                insert_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl SharedDataApplier for MockSharedDataApplier {
        async fn insert(
            &self,
            _table_id: &TableId,
            rows: &[kalamdb_commons::models::rows::Row],
        ) -> Result<usize, RaftError> {
            self.insert_count.fetch_add(1, Ordering::SeqCst);
            Ok(rows.len())
        }

        async fn update(
            &self,
            _table_id: &TableId,
            _updates: &[kalamdb_commons::models::rows::Row],
            _filter: Option<&str>,
        ) -> Result<usize, RaftError> {
            Ok(1)
        }

        async fn delete(
            &self,
            _table_id: &TableId,
            _pk_values: Option<&[String]>,
        ) -> Result<usize, RaftError> {
            Ok(1)
        }

        async fn apply_transaction_batch(
            &self,
            _transaction_id: &TransactionId,
            mutations: &[StagedMutation],
        ) -> Result<TransactionApplyResult, RaftError> {
            Ok(TransactionApplyResult {
                rows_affected: mutations.len(),
                commit_seq: 1,
                notifications_sent: 0,
                manifest_updates: 0,
                publisher_events: 0,
            })
        }
    }

    #[tokio::test]
    async fn test_shared_data_applier_insert() {
        let applier = MockSharedDataApplier::new();
        let table_id =
            TableId::new(NamespaceId::from("shared_ns"), TableName::from("shared_table"));

        let result = applier.insert(&table_id, &[]).await;
        assert!(result.is_ok());
        assert_eq!(applier.insert_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_noop_shared_applier_returns_zero() {
        let applier = NoOpSharedDataApplier;
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));

        assert_eq!(applier.insert(&table_id, &[]).await.unwrap(), 0);
        assert_eq!(applier.update(&table_id, &[], None).await.unwrap(), 0);
        assert_eq!(applier.delete(&table_id, None).await.unwrap(), 0);
    }
}
