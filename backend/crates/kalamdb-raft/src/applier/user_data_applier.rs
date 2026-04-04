//! User Data Applier trait for persisting user table data
//!
//! This trait is called by UserDataStateMachine after Raft consensus commits
//! a command. All nodes (leader and followers) call this, ensuring that all
//! replicas persist the same data.
//!
//! The implementation lives in kalamdb-core using provider infrastructure.

use async_trait::async_trait;
use kalamdb_commons::models::UserId;
use kalamdb_commons::TableId;

use crate::RaftError;

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
    ) -> Result<usize, RaftError>;
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
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn update(
        &self,
        _table_id: &TableId,
        _user_id: &UserId,
        _updates: &[kalamdb_commons::models::rows::Row],
        _filter: Option<&str>,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }

    async fn delete(
        &self,
        _table_id: &TableId,
        _user_id: &UserId,
        _pk_values: Option<&[String]>,
    ) -> Result<usize, RaftError> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::{NamespaceId, TableName};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
        ) -> Result<usize, RaftError> {
            self.update_count.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }

        async fn delete(
            &self,
            _table_id: &TableId,
            _user_id: &UserId,
            _pk_values: Option<&[String]>,
        ) -> Result<usize, RaftError> {
            self.delete_count.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }
    }

    #[tokio::test]
    async fn test_user_data_applier_insert() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        let result = applier.insert(&table_id, &user_id, &[]).await;
        assert!(result.is_ok());
        assert_eq!(applier.get_counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn test_user_data_applier_update() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        let result = applier.update(&table_id, &user_id, &[], None).await;
        assert!(result.is_ok());
        assert_eq!(applier.get_counts(), (0, 1, 0));
    }

    #[tokio::test]
    async fn test_user_data_applier_delete() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        let result = applier.delete(&table_id, &user_id, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(applier.get_counts(), (0, 0, 1));
    }

    #[tokio::test]
    async fn test_noop_user_applier_returns_zero() {
        let applier = NoOpUserDataApplier;
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        assert_eq!(applier.insert(&table_id, &user_id, &[]).await.unwrap(), 0);
        assert_eq!(applier.update(&table_id, &user_id, &[], None).await.unwrap(), 0);
        assert_eq!(applier.delete(&table_id, &user_id, None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_user_applier_with_filter() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        // Update with filter
        let result = applier.update(&table_id, &user_id, &[], Some("filter_value")).await;
        assert!(result.is_ok());

        // Delete with filter
        let result = applier.delete(&table_id, &user_id, Some(&["pk_1".to_string()])).await;
        assert!(result.is_ok());

        assert_eq!(applier.get_counts(), (0, 1, 1));
    }

    #[tokio::test]
    async fn test_multiple_operations_sequential() {
        let applier = MockUserDataApplier::new();
        let table_id = TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table"));
        let user_id = UserId::from("user_123");

        applier.insert(&table_id, &user_id, &[]).await.unwrap();
        applier.insert(&table_id, &user_id, &[]).await.unwrap();
        applier.update(&table_id, &user_id, &[], None).await.unwrap();
        applier.delete(&table_id, &user_id, None).await.unwrap();

        assert_eq!(applier.get_counts(), (2, 1, 1));
    }
}
