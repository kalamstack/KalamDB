use super::row::Row;
use crate::ids::SeqId;
use crate::models::UserId;
use serde::{Deserialize, Serialize};

/// Unified table row model for User and Stream tables
///
/// **Phase 13: Provider Consolidation**
/// - Unifies UserTableRow and StreamTableRow
/// - Used by BaseTableProvider::snapshot_rows
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KTableRow {
    pub user_id: UserId,
    pub _seq: SeqId,
    #[serde(default)]
    pub _commit_seq: u64,
    /// Soft delete flag (always false for stream tables)
    pub _deleted: bool,
    /// Row data (JSON)
    pub fields: Row,
}

impl KTableRow {
    pub fn new(user_id: UserId, _seq: SeqId, _commit_seq: u64, fields: Row, _deleted: bool) -> Self {
        Self {
            user_id,
            _seq,
            _commit_seq,
            _deleted,
            fields,
        }
    }
}
