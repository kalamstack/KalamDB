use std::sync::Arc;

use kalamdb_commons::models::{TransactionId, TransactionOrigin, TransactionState};

/// Lightweight observability projection for active in-memory transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTransactionMetric {
    pub transaction_id: TransactionId,
    pub owner_id: Arc<str>,
    pub state: TransactionState,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub write_count: usize,
    pub write_bytes: usize,
    pub touched_tables_count: usize,
    pub snapshot_commit_seq: u64,
    pub origin: TransactionOrigin,
}
