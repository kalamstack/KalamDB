//! Shared data shard commands (shared tables)
//!
//! Each data command carries a `required_meta_index` watermark, which is the
//! `Meta` group's last applied index on the leader at proposal time. Followers
//! buffer data commands until local `Meta` has applied at least that index.

use kalamdb_commons::{models::TransactionId, TableId};
use serde::{Deserialize, Serialize};

/// Commands for shared data shards (1 shard by default)
///
/// Handles: shared table INSERT/UPDATE/DELETE operations
///
/// Routing: Phase 1 uses single shard; future may shard by table_id
///
/// Each variant carries `required_meta_index` for watermark-based ordering.
/// Followers must buffer commands until `Meta.last_applied_index() >= required_meta_index`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SharedDataCommand {
    /// Insert rows into a shared table
    Insert {
        /// Watermark: Meta group's last_applied_index at proposal time
        required_meta_index: u64,
        #[serde(default)]
        transaction_id: Option<TransactionId>,
        table_id: TableId,
        /// Rows to insert
        rows: Vec<kalamdb_commons::models::rows::Row>,
    },

    /// Update rows in a shared table
    Update {
        /// Watermark: Meta group's last_applied_index at proposal time
        required_meta_index: u64,
        #[serde(default)]
        transaction_id: Option<TransactionId>,
        table_id: TableId,
        /// Updates to apply
        updates: Vec<kalamdb_commons::models::rows::Row>,
        /// Optional filter (primary key value)
        filter: Option<String>,
    },

    /// Delete rows from a shared table
    Delete {
        /// Watermark: Meta group's last_applied_index at proposal time
        required_meta_index: u64,
        #[serde(default)]
        transaction_id: Option<TransactionId>,
        table_id: TableId,
        /// Primary keys to delete
        pk_values: Option<Vec<String>>,
    },
}

impl SharedDataCommand {
    /// Get the required_meta_index watermark for this command
    pub fn required_meta_index(&self) -> u64 {
        match self {
            SharedDataCommand::Insert {
                required_meta_index,
                ..
            } => *required_meta_index,
            SharedDataCommand::Update {
                required_meta_index,
                ..
            } => *required_meta_index,
            SharedDataCommand::Delete {
                required_meta_index,
                ..
            } => *required_meta_index,
        }
    }

    /// Set the required_meta_index watermark for this command
    pub fn set_required_meta_index(&mut self, index: u64) {
        match self {
            SharedDataCommand::Insert {
                required_meta_index,
                ..
            } => *required_meta_index = index,
            SharedDataCommand::Update {
                required_meta_index,
                ..
            } => *required_meta_index = index,
            SharedDataCommand::Delete {
                required_meta_index,
                ..
            } => *required_meta_index = index,
        }
    }

    /// Get the table_id for this command
    pub fn table_id(&self) -> &TableId {
        match self {
            SharedDataCommand::Insert { table_id, .. } => table_id,
            SharedDataCommand::Update { table_id, .. } => table_id,
            SharedDataCommand::Delete { table_id, .. } => table_id,
        }
    }

    pub fn transaction_id(&self) -> Option<&TransactionId> {
        match self {
            SharedDataCommand::Insert { transaction_id, .. }
            | SharedDataCommand::Update { transaction_id, .. }
            | SharedDataCommand::Delete { transaction_id, .. } => transaction_id.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;

    #[test]
    fn test_shared_data_command_watermark() {
        let mut cmd = SharedDataCommand::Update {
            required_meta_index: 300,
            transaction_id: None,
            table_id: TableId::new(NamespaceId::from("ns"), TableName::from("shared_table")),
            updates: vec![],
            filter: None,
        };

        assert_eq!(cmd.required_meta_index(), 300);
        cmd.set_required_meta_index(400);
        assert_eq!(cmd.required_meta_index(), 400);
    }

    #[test]
    fn test_shared_data_command_table_id() {
        let table_id = TableId::new(NamespaceId::from("shared_ns"), TableName::from("shared_t"));
        let cmd = SharedDataCommand::Insert {
            required_meta_index: 10,
            transaction_id: None,
            table_id: table_id.clone(),
            rows: vec![],
        };

        assert_eq!(cmd.table_id(), &table_id);
    }

    #[test]
    fn test_shared_command_serialization() {
        let cmd = SharedDataCommand::Delete {
            required_meta_index: 999,
            transaction_id: None,
            table_id: TableId::new(NamespaceId::from("shared"), TableName::from("data")),
            pk_values: None,
        };

        assert_eq!(cmd.required_meta_index(), 999);
    }

    #[test]
    fn test_all_shared_command_variants_get_watermark() {
        let table_id = TableId::new(NamespaceId::from("n"), TableName::from("t"));

        let commands = vec![
            SharedDataCommand::Insert {
                required_meta_index: 10,
                transaction_id: None,
                table_id: table_id.clone(),
                rows: vec![],
            },
            SharedDataCommand::Update {
                required_meta_index: 20,
                transaction_id: None,
                table_id: table_id.clone(),
                updates: vec![],
                filter: None,
            },
            SharedDataCommand::Delete {
                required_meta_index: 30,
                transaction_id: None,
                table_id: table_id.clone(),
                pk_values: None,
            },
        ];

        assert_eq!(commands[0].required_meta_index(), 10);
        assert_eq!(commands[1].required_meta_index(), 20);
        assert_eq!(commands[2].required_meta_index(), 30);
    }

    #[test]
    fn test_shared_command_set_watermark_to_zero() {
        // Shared DML commands should also support watermark = 0
        let table_id = TableId::new(NamespaceId::from("ns"), TableName::from("shared_table"));

        let mut insert = SharedDataCommand::Insert {
            required_meta_index: 100,
            transaction_id: None,
            table_id: table_id.clone(),
            rows: vec![],
        };

        let mut update = SharedDataCommand::Update {
            required_meta_index: 100,
            transaction_id: None,
            table_id: table_id.clone(),
            updates: vec![],
            filter: None,
        };

        let mut delete = SharedDataCommand::Delete {
            required_meta_index: 100,
            transaction_id: None,
            table_id: table_id.clone(),
            pk_values: None,
        };

        insert.set_required_meta_index(0);
        update.set_required_meta_index(0);
        delete.set_required_meta_index(0);

        assert_eq!(insert.required_meta_index(), 0);
        assert_eq!(update.required_meta_index(), 0);
        assert_eq!(delete.required_meta_index(), 0);
    }
}
