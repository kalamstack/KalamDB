//! Users table index definitions
//!
//! This module defines secondary indexes for the system.users table.

use crate::providers::users::models::User;
use crate::system_row_mapper::system_row_to_model;
use crate::StoragePartition;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::storage::Partition;
use kalamdb_commons::UserId;
use kalamdb_store::IndexDefinition;
use std::sync::Arc;

/// Index for querying users by role.
///
/// Key format: `{role}:{user_id}`
///
/// This index allows efficient queries like:
/// - "All users with role 'admin'"
/// - "All service accounts"
pub struct UserRoleIndex;

impl IndexDefinition<UserId, SystemTableRow> for UserRoleIndex {
    fn partition(&self) -> Partition {
        Partition::new(StoragePartition::SystemUsersRoleIdx.name())
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec!["role"]
    }

    fn extract_key(&self, _primary_key: &UserId, row: &SystemTableRow) -> Option<Vec<u8>> {
        let user: User = system_row_to_model(row, &User::definition()).ok()?;
        let key = format!("{}:{}", user.role.as_str(), user.user_id.as_str());
        Some(key.into_bytes())
    }

    fn filter_to_prefix(&self, filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        use kalamdb_store::extract_string_equality;

        if let Some((col, val)) = extract_string_equality(filter) {
            if col == "role" {
                return Some(format!("{}:", val).into_bytes());
            }
        }
        None
    }
}

/// Create the default set of indexes for the users table.
pub fn create_users_indexes() -> Vec<Arc<dyn IndexDefinition<UserId, SystemTableRow>>> {
    vec![Arc::new(UserRoleIndex)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_row_mapper::model_to_system_row;
    use kalamdb_commons::{AuthType, Role, StorageId};

    fn create_test_user(id: &str, role: Role) -> User {
        User {
            user_id: UserId::new(id),
            password_hash: "hashed_password".to_string(),
            role,
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
    fn test_role_index_key_format() {
        let user = create_test_user("user1", Role::Dba);
        let user_id = user.user_id.clone();
        let row = model_to_system_row(&user, &User::definition()).unwrap();

        let index = UserRoleIndex;
        let key = index.extract_key(&user_id, &row).unwrap();

        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "dba:user1");
    }

    #[test]
    fn test_create_users_indexes() {
        let indexes = create_users_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].partition(), StoragePartition::SystemUsersRoleIdx.name().into());
    }
}
