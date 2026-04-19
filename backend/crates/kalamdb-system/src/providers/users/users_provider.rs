//! System.users table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.users table.
//! Uses `IndexedEntityStore` for automatic secondary index management.
//!
//! ## Indexes
//!
//! The users table has one secondary index (managed automatically):
//!
//! 1. **UserRoleIndex** - Query users by role
//!    - Key: `{role}:{user_id}`
//!    - Enables: "All users with role 'admin'"

use super::users_indexes::create_users_indexes;
use crate::error::{SystemError, SystemResultExt};
use crate::providers::base::{system_rows_to_batch, IndexedProviderDefinition};
use crate::providers::users::models::User;
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};
use crate::SystemTable;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::UserId;
use kalamdb_store::entity_store::EntityStore;
use kalamdb_store::{IndexedEntityStore, StorageBackend};
use std::sync::{Arc, OnceLock};

/// Type alias for the indexed users store
pub type UsersStore = IndexedEntityStore<UserId, SystemTableRow>;

/// System.users table provider using IndexedEntityStore for automatic index management.
///
/// All insert/update/delete operations automatically maintain secondary indexes
/// using RocksDB's atomic WriteBatch - no manual index management needed.
pub struct UsersTableProvider {
    store: UsersStore,
}

impl std::fmt::Debug for UsersTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsersTableProvider").finish()
    }
}

impl UsersTableProvider {
    /// Create a new users table provider with automatic index management.
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new UsersTableProvider instance with indexes configured
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            SystemTable::Users.column_family_name().expect("Users is a table, not a view"),
            create_users_indexes(),
        );
        Self { store }
    }

    /// Create a new user.
    ///
    /// Indexes are automatically maintained via `IndexedEntityStore`.
    ///
    /// # Arguments
    /// * `user` - The user to create
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn create_user(&self, user: User) -> Result<(), SystemError> {
        // Insert user - indexes are managed automatically
        let row = Self::encode_user_row(&user)?;
        self.store.insert(&user.user_id, &row).into_system_error("insert user error")
    }

    /// Update an existing user.
    ///
    /// Indexes are automatically maintained via `IndexedEntityStore`.
    /// Stale index entries are removed and new ones added atomically.
    ///
    /// # Arguments
    /// * `user` - The updated user data
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn update_user(&self, user: User) -> Result<(), SystemError> {
        // Check if user exists
        let existing = self.store.get(&user.user_id)?;
        if existing.is_none() {
            return Err(SystemError::NotFound(format!("User not found: {}", user.user_id)));
        }

        let existing_row = existing.unwrap();

        // Use update_with_old for efficiency (we already have old entity)
        let new_row = Self::encode_user_row(&user)?;
        self.store
            .update_with_old(&user.user_id, Some(&existing_row), &new_row)
            .into_system_error("update user error")
    }

    /// Soft delete a user (sets deleted_at timestamp).
    ///
    /// # Arguments
    /// * `user_id` - The ID of the user to delete
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn delete_user(&self, user_id: &UserId) -> Result<(), SystemError> {
        // Get existing user
        let mut user = self
            .store
            .get(user_id)?
            .map(|row| Self::decode_user_row(&row))
            .transpose()?
            .ok_or_else(|| SystemError::NotFound(format!("User not found: {}", user_id)))?;

        // Set deleted_at timestamp (soft delete)
        user.deleted_at = Some(chrono::Utc::now().timestamp_millis());

        // Update user with deleted_at
        let row = Self::encode_user_row(&user)?;
        self.store.update(user_id, &row).into_system_error("update user error")
    }

    /// Get a user by ID.
    ///
    /// # Arguments
    /// * `user_id` - The user ID to lookup
    ///
    /// # Returns
    /// Option<User> if found, None otherwise
    pub fn get_user_by_id(&self, user_id: &UserId) -> Result<Option<User>, SystemError> {
        let row = self.store.get(user_id)?;
        row.map(|value| Self::decode_user_row(&value)).transpose()
    }

    /// Helper to create RecordBatch from users
    fn create_batch(
        &self,
        users: Vec<(UserId, SystemTableRow)>,
    ) -> Result<RecordBatch, SystemError> {
        let rows = users.into_iter().map(|(_, row)| row).collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    /// Scan all users and return as RecordBatch
    pub fn scan_all_users(&self) -> Result<RecordBatch, SystemError> {
        let users = self.store.scan_all_typed(None, None, None)?;
        self.create_batch(users)
    }

    fn encode_user_row(user: &User) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(user, &User::definition())
    }

    fn decode_user_row(row: &SystemTableRow) -> Result<User, SystemError> {
        system_row_to_model(row, &User::definition())
    }
    fn provider_definition() -> IndexedProviderDefinition<UserId> {
        IndexedProviderDefinition {
            table_name: SystemTable::Users.table_name(),
            primary_key_column: "user_id",
            schema: Self::schema,
            parse_key: |value| Some(UserId::new(value)),
        }
    }

    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                User::definition().to_arrow_schema().expect("failed to build users schema")
            })
            .clone()
    }

    fn filter_pushdown(filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Inexact pushdown: we may use filters for index/prefix scans,
        // but DataFusion must still apply them for correctness.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

crate::impl_indexed_system_table_provider!(
    provider = UsersTableProvider,
    key = UserId,
    value = SystemTableRow,
    store = store,
    definition = provider_definition,
    build_batch = create_batch,
    load_batch = scan_all_users,
    pushdown = UsersTableProvider::filter_pushdown
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::TableProvider;
    use kalamdb_commons::{AuthType, Role, StorageId};
    use kalamdb_store::test_utils::InMemoryBackend;

    fn create_test_provider() -> UsersTableProvider {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        UsersTableProvider::new(backend)
    }

    fn create_test_user(id: &str) -> User {
        User {
            user_id: UserId::new(id),
            password_hash: "hashed_password".to_string(),
            role: Role::User,
            email: Some(format!("{}@example.com", id)),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: crate::providers::storages::models::StorageMode::Table,
            storage_id: Some(StorageId::local()),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: 1000,
            updated_at: 1000,
            last_seen: None,
            deleted_at: None,
        }
    }

    #[test]
    fn test_create_and_get_user() {
        let provider = create_test_provider();
        let user = create_test_user("user1");

        // Create user
        provider.create_user(user.clone()).unwrap();

        // Get by ID
        let retrieved = provider.get_user_by_id(&UserId::new("user1")).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.email, Some("user1@example.com".to_string()));
    }

    #[test]
    fn test_update_user() {
        let provider = create_test_provider();
        let user = create_test_user("user1");

        provider.create_user(user).unwrap();

        // Update user
        let mut updated = provider.get_user_by_id(&UserId::new("user1")).unwrap().unwrap();
        updated.email = Some("newemail@example.com".to_string());
        updated.updated_at = 2000;

        provider.update_user(updated).unwrap();

        // Verify update
        let retrieved = provider.get_user_by_id(&UserId::new("user1")).unwrap().unwrap();
        assert_eq!(retrieved.email, Some("newemail@example.com".to_string()));
        assert_eq!(retrieved.updated_at, 2000);
    }

    #[test]
    fn test_delete_user() {
        let provider = create_test_provider();
        let user = create_test_user("user1");

        provider.create_user(user).unwrap();
        provider.delete_user(&UserId::new("user1")).unwrap();

        // Verify deleted_at is set
        let retrieved = provider.get_user_by_id(&UserId::new("user1")).unwrap().unwrap();
        assert!(retrieved.deleted_at.is_some());
    }

    #[test]
    fn test_scan_all_users() {
        let provider = create_test_provider();

        // Create multiple users
        for i in 1..=3 {
            let user = create_test_user(&format!("user{}", i));
            provider.create_user(user).unwrap();
        }

        // Scan all
        let batch = provider.scan_all_users().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 15);
    }

    #[test]
    fn test_table_provider_schema() {
        let provider = create_test_provider();
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(0).name(), "user_id");
    }
}
