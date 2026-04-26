//! User Data Applier trait for persisting user table data
//!
//! This trait is called by UserDataStateMachine after Raft consensus commits
//! a command. All nodes (leader and followers) call this, ensuring that all
//! replicas persist the same data.
//!
//! The implementation lives in kalamdb-core using provider infrastructure.

use async_trait::async_trait;
use kalamdb_commons::{
    models::{TransactionId, UserId},
    TableId,
};
use kalamdb_transactions::StagedMutation;

use crate::{RaftError, TransactionApplyResult};

/// Applier callback for user table data operations
///
/// This trait is called by UserDataStateMachine after Raft consensus commits
/// a data command. All nodes (leader and followers) call this, ensuring that
/// all replicas persist the same data to their local storage.
///
/// # Implementation
///
/// The implementation lives in kalamdb-core and uses table providers
/// to persist data to RocksDB.
#[async_trait]
pub trait UserDataApplier: Send + Sync {
    /// Insert rows into a user table
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `user_id` - The user who owns this data
    /// * `rows` - Row data to insert
    ///
    /// # Returns
    /// Number of rows inserted
    async fn insert(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        rows: &[kalamdb_commons::models::rows::Row],
        commit_seq: u64,
    ) -> Result<usize, RaftError>;

    /// Update rows in a user table
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `user_id` - The user who owns this data
    /// * `updates` - Update row data
    /// * `filter` - Optional filter string (e.g., primary key value)
    ///
    /// # Returns
    /// Number of rows updated
    async fn update(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        updates: &[kalamdb_commons::models::rows::Row],
        filter: Option<&str>,
        commit_seq: u64,
    ) -> Result<usize, RaftError>;

    /// Delete rows from a user table
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `user_id` - The user who owns this data
    /// * `pk_values` - Optional list of primary key values to delete
    ///
    /// # Returns
    /// Number of rows deleted
    async fn delete(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        pk_values: Option<&[String]>,
        commit_seq: u64,
    ) -> Result<usize, RaftError>;

    /// Apply an explicit-transaction write set inside one state-machine cycle.
    async fn apply_transaction_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: &[StagedMutation],
        commit_seq: u64,
    ) -> Result<TransactionApplyResult, RaftError>;
}

/// No-op applier for testing or standalone scenarios
pub struct NoOpUserDataApplier;

#[async_trait]
impl UserDataApplier for NoOpUserDataApplier {
    async fn insert(
        &self,
        _table_id: &TableId,
        _user_id: &UserId,
        _rows: &[kalamdb_commons::models::rows::Row],
        _commit_seq: u64,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn update(
        &self,
        _table_id: &TableId,
        _user_id: &UserId,
        _updates: &[kalamdb_commons::models::rows::Row],
        _filter: Option<&str>,
        _commit_seq: u64,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn delete(
        &self,
        _table_id: &TableId,
        _user_id: &UserId,
        _pk_values: Option<&[String]>,
        _commit_seq: u64,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn apply_transaction_batch(
        &self,
        _transaction_id: &TransactionId,
        _mutations: &[StagedMutation],
        _commit_seq: u64,
    ) -> Result<TransactionApplyResult, RaftError> {
        Ok(TransactionApplyResult::default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;

    /// Mock applier that tracks calls for testing
    struct MockUserDataApplier {
        insert_count: Arc<AtomicUsize>,
        update_count: Arc<AtomicUsize>,
        delete_count: Arc<AtomicUsize>,
    }

    impl MockUserDataApplier {
        fn new() -> Self {
            Self {
                insert_count: Arc::new(AtomicUsize::new(0)),
                update_count: Arc::new(AtomicUsize::new(0)),
                delete_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_counts(&self) -> (usize, usize, usize) {
            (
                self.insert_count.load(Ordering::SeqCst),
                self.update_count.load(Ordering::SeqCst),
                self.delete_count.load(Ordering::SeqCst),
            )
        }
    }

    #[async_trait]
    impl UserDataApplier for MockUserDataApplier {
        async fn insert(
            &self,
            _table_id: &TableId,
            _user_id: &UserId,
            rows: &[kalamdb_commons::models::rows::Row],
            _commit_seq: u64,
        ) -> Result<usize, RaftError> {
            self.insert_count.fetch_add(1, Ordering::SeqCst);
            Ok(rows.len())
        }

        async fn update(
            &self,
            _table_id: &TableId,
            _user_id: &UserId,
            _updates: &[kalamdb_commons::models::rows::Row],
            _filter: Option<&str>,
            _commit_seq: u64,
        ) -> Result<usize, RaftError> {
            self.update_count.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }

        async fn delete(
            &self,
            _table_id: &TableId,
            _user_id: &UserId,
            _pk_values: Option<&[String]>,
            _commit_seq: u64,
        ) -> Result<usize, RaftError> {
            self.delete_count.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }

        async fn apply_transaction_batch(
            &self,
            _transaction_id: &TransactionId,
            mutations: &[StagedMutation],
            _commit_seq: u64,
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
    async fn test_user_data_applier_insert() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        let result = applier.insert(&table_id, &user_id, &[], 1).await;
        assert!(result.is_ok());
        assert_eq!(applier.get_counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn test_user_data_applier_update() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        let result = applier.update(&table_id, &user_id, &[], None, 1).await;
        assert!(result.is_ok());
        assert_eq!(applier.get_counts(), (0, 1, 0));
    }

    #[tokio::test]
    async fn test_user_data_applier_delete() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        let result = applier.delete(&table_id, &user_id, None, 1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(applier.get_counts(), (0, 0, 1));
    }

    #[tokio::test]
    async fn test_noop_user_applier_returns_zero() {
        let applier = NoOpUserDataApplier;
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        assert_eq!(applier.insert(&table_id, &user_id, &[], 1).await.unwrap(), 0);
        assert_eq!(applier.update(&table_id, &user_id, &[], None, 1).await.unwrap(), 0);
        assert_eq!(applier.delete(&table_id, &user_id, None, 1).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_user_applier_with_filter() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        // Update with filter
        let result = applier.update(&table_id, &user_id, &[], Some("filter_value"), 1).await;
        assert!(result.is_ok());

        // Delete with filter
        let result = applier.delete(&table_id, &user_id, Some(&["pk_1".to_string()]), 1).await;
        assert!(result.is_ok());

        assert_eq!(applier.get_counts(), (0, 1, 1));
    }

    #[tokio::test]
    async fn test_multiple_operations_sequential() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        applier.insert(&table_id, &user_id, &[], 1).await.unwrap();
        applier.insert(&table_id, &user_id, &[], 2).await.unwrap();
        applier.update(&table_id, &user_id, &[], None, 3).await.unwrap();
        applier.delete(&table_id, &user_id, None, 4).await.unwrap();

        assert_eq!(applier.get_counts(), (2, 1, 1));
    }
}
