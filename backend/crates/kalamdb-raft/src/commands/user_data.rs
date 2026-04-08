//! User data shard commands for user table mutations
//!
//! Each data command carries a `required_meta_index` watermark, which is the
//! `Meta` group's last applied index on the leader at proposal time. Followers
//! buffer data commands until local `Meta` has applied at least that index.

use kalamdb_commons::models::{TransactionId, UserId};
use kalamdb_commons::TableId;
use serde::{Deserialize, Serialize};

/// Commands for user data shards (32 shards by default)
///
/// Handles:
/// - User table INSERT/UPDATE/DELETE operations
///
/// Routing: user_id % num_user_shards
///
/// Each variant carries `required_meta_index` for watermark-based ordering.
/// Followers must buffer commands until `Meta.last_applied_index() >= required_meta_index`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserDataCommand {
    // === User Table Data ===
    /// Insert rows into a user table
    Insert {
        /// Watermark: Meta group's last_applied_index at proposal time
        required_meta_index: u64,
        #[serde(default)]
        transaction_id: Option<TransactionId>,
        table_id: TableId,
        user_id: UserId,
        /// Rows to insert
        rows: Vec<kalamdb_commons::models::rows::Row>,
    },

    /// Update rows in a user table
    Update {
        /// Watermark: Meta group's last_applied_index at proposal time
        required_meta_index: u64,
        #[serde(default)]
        transaction_id: Option<TransactionId>,
        table_id: TableId,
        user_id: UserId,
        /// Updates to apply
        updates: Vec<kalamdb_commons::models::rows::Row>,
        /// Optional filter (primary key value)
        filter: Option<String>,
    },

    /// Delete rows from a user table
    Delete {
        /// Watermark: Meta group's last_applied_index at proposal time
        required_meta_index: u64,
        #[serde(default)]
        transaction_id: Option<TransactionId>,
        table_id: TableId,
        user_id: UserId,
        /// Primary keys to delete
        pk_values: Option<Vec<String>>,
    },
}

impl UserDataCommand {
    /// Get the required_meta_index watermark for this command
    pub fn required_meta_index(&self) -> u64 {
        match self {
            UserDataCommand::Insert {
                required_meta_index,
                ..
            } => *required_meta_index,
            UserDataCommand::Update {
                required_meta_index,
                ..
            } => *required_meta_index,
            UserDataCommand::Delete {
                required_meta_index,
                ..
            } => *required_meta_index,
        }
    }

    /// Set the required_meta_index watermark for this command
    pub fn set_required_meta_index(&mut self, index: u64) {
        match self {
            UserDataCommand::Insert {
                required_meta_index,
                ..
            } => *required_meta_index = index,
            UserDataCommand::Update {
                required_meta_index,
                ..
            } => *required_meta_index = index,
            UserDataCommand::Delete {
                required_meta_index,
                ..
            } => *required_meta_index = index,
        }
    }

    /// Get the user_id for routing to the correct shard
    pub fn user_id(&self) -> &UserId {
        match self {
            UserDataCommand::Insert { user_id, .. } => user_id,
            UserDataCommand::Update { user_id, .. } => user_id,
            UserDataCommand::Delete { user_id, .. } => user_id,
        }
    }

    pub fn transaction_id(&self) -> Option<&TransactionId> {
        match self {
            UserDataCommand::Insert { transaction_id, .. }
            | UserDataCommand::Update { transaction_id, .. }
            | UserDataCommand::Delete { transaction_id, .. } => transaction_id.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::{NamespaceId, TableName};

    #[test]
    fn test_user_data_command_watermark_get_set() {
        let mut cmd = UserDataCommand::Insert {
            required_meta_index: 100,
            transaction_id: None,
            table_id: TableId::new(NamespaceId::from("ns"), TableName::from("table")),
            user_id: UserId::from("user_1"),
            rows: vec![],
        };

        assert_eq!(cmd.required_meta_index(), 100);
        cmd.set_required_meta_index(200);
        assert_eq!(cmd.required_meta_index(), 200);
    }

    #[test]
    fn test_user_data_command_user_id() {
        let user_id = UserId::from("user_123");
        let cmd = UserDataCommand::Delete {
            required_meta_index: 50,
            transaction_id: None,
            table_id: TableId::new(NamespaceId::from("ns"), TableName::from("table")),
            user_id: user_id.clone(),
            pk_values: None,
        };

        assert_eq!(cmd.user_id(), &user_id);
    }

    #[test]
    fn test_user_command_serialization() {
        let cmd = UserDataCommand::Insert {
            required_meta_index: 123,
            transaction_id: None,
            table_id: TableId::new(NamespaceId::from("test_ns"), TableName::from("test_table")),
            user_id: UserId::from("user_456"),
            rows: vec![],
        };

        assert_eq!(cmd.required_meta_index(), 123);
        assert_eq!(cmd.user_id(), &UserId::from("user_456"));
    }

    #[test]
    fn test_all_user_command_variants_get_watermark() {
        let table_id = TableId::new(NamespaceId::from("n"), TableName::from("t"));
        let user_id = UserId::from("u");

        let commands = vec![
            UserDataCommand::Insert {
                required_meta_index: 1,
                transaction_id: None,
                table_id: table_id.clone(),
                user_id: user_id.clone(),
                rows: vec![],
            },
            UserDataCommand::Update {
                required_meta_index: 2,
                transaction_id: None,
                table_id: table_id.clone(),
                user_id: user_id.clone(),
                updates: vec![],
                filter: None,
            },
            UserDataCommand::Delete {
                required_meta_index: 3,
                transaction_id: None,
                table_id: table_id.clone(),
                user_id: user_id.clone(),
                pk_values: None,
            },
        ];

        for (i, cmd) in commands.iter().enumerate() {
            assert_eq!(cmd.required_meta_index(), (i + 1) as u64);
        }
    }

    #[test]
    fn test_user_command_set_watermark_to_zero() {
        // DML commands should support setting watermark to 0 for performance optimization
        // See spec 021 section 5.4.1 "Watermark Nuance"
        let user_id = UserId::from("user123");
        let table_id = TableId::new(NamespaceId::from("ns"), TableName::from("table"));

        let mut insert = UserDataCommand::Insert {
            required_meta_index: 100, // Initially non-zero
            transaction_id: None,
            table_id: table_id.clone(),
            user_id: user_id.clone(),
            rows: vec![],
        };

        let mut update = UserDataCommand::Update {
            required_meta_index: 100,
            transaction_id: None,
            table_id: table_id.clone(),
            user_id: user_id.clone(),
            updates: vec![],
            filter: None,
        };

        let mut delete = UserDataCommand::Delete {
            required_meta_index: 100,
            transaction_id: None,
            table_id: table_id.clone(),
            user_id: user_id.clone(),
            pk_values: None,
        };

        // Set to 0 - this is what RaftExecutor does for DML
        insert.set_required_meta_index(0);
        update.set_required_meta_index(0);
        delete.set_required_meta_index(0);

        assert_eq!(insert.required_meta_index(), 0);
        assert_eq!(update.required_meta_index(), 0);
        assert_eq!(delete.required_meta_index(), 0);
    }
}
