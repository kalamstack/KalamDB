use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, TableId, TransactionId, UserId};
use kalamdb_commons::TableType;
use serde::{Deserialize, Serialize};

use crate::overlay::TransactionOverlayEntry;

/// Shared logical DML mutation buffered inside an explicit transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedMutation {
    pub transaction_id: TransactionId,
    pub mutation_order: u64,
    pub table_id: TableId,
    pub table_type: TableType,
    pub user_id: Option<UserId>,
    pub operation_kind: OperationKind,
    pub primary_key: String,
    pub payload: Row,
    pub tombstone: bool,
}

impl StagedMutation {
    pub fn new(
        transaction_id: TransactionId,
        table_id: TableId,
        table_type: TableType,
        user_id: Option<UserId>,
        operation_kind: OperationKind,
        primary_key: impl Into<String>,
        payload: Row,
        tombstone: bool,
    ) -> Self {
        Self {
            transaction_id,
            mutation_order: 0,
            table_id,
            table_type,
            user_id,
            operation_kind,
            primary_key: primary_key.into(),
            payload,
            tombstone,
        }
    }

    #[inline]
    pub fn approximate_size_bytes(&self) -> usize {
        let payload_bytes = serde_json::to_vec(&self.payload).map(|bytes| bytes.len()).unwrap_or(0);
        self.primary_key.len()
            + self.table_id.full_name().len()
            + self.user_id.as_ref().map(|user_id| user_id.as_str().len()).unwrap_or(0)
            + payload_bytes
            + std::mem::size_of::<u64>()
    }

    #[inline]
    pub fn overlay_entry(&self) -> TransactionOverlayEntry {
        TransactionOverlayEntry {
            transaction_id: self.transaction_id.clone(),
            mutation_order: self.mutation_order,
            table_id: self.table_id.clone(),
            table_type: self.table_type,
            user_id: self.user_id.clone(),
            operation_kind: self.operation_kind,
            primary_key: self.primary_key.clone(),
            payload: self.payload.clone(),
            tombstone: self.tombstone,
        }
    }
}