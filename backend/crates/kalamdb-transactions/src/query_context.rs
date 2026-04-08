use std::sync::Arc;

use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, TableId, TransactionId, UserId};
use kalamdb_commons::TableType;

use crate::access::{TransactionAccessError, TransactionAccessValidator};
use crate::overlay::TransactionOverlay;

/// Lightweight view trait exposed to query providers for transaction-local reads.
pub trait TransactionOverlayView: std::fmt::Debug + Send + Sync {
    fn overlay(&self) -> TransactionOverlay;

    fn overlay_for_table(&self, table_id: &TableId) -> Option<TransactionOverlay>;
}

pub trait TransactionMutationSink: std::fmt::Debug + Send + Sync {
    fn stage_mutation(
        &self,
        transaction_id: &TransactionId,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<UserId>,
        operation_kind: OperationKind,
        primary_key: String,
        row: Row,
        is_deleted: bool,
    ) -> Result<(), TransactionAccessError>;
}

/// Query-time transaction context shared between the coordinator and providers.
#[derive(Debug, Clone)]
pub struct TransactionQueryContext {
    pub transaction_id: TransactionId,
    pub snapshot_commit_seq: u64,
    pub overlay_view: Arc<dyn TransactionOverlayView>,
    pub mutation_sink: Arc<dyn TransactionMutationSink>,
    pub access_validator: Arc<dyn TransactionAccessValidator>,
}

impl TransactionQueryContext {
    #[inline]
    pub fn new(
        transaction_id: TransactionId,
        snapshot_commit_seq: u64,
        overlay_view: Arc<dyn TransactionOverlayView>,
        mutation_sink: Arc<dyn TransactionMutationSink>,
        access_validator: Arc<dyn TransactionAccessValidator>,
    ) -> Self {
        Self {
            transaction_id,
            snapshot_commit_seq,
            overlay_view,
            mutation_sink,
            access_validator,
        }
    }
}