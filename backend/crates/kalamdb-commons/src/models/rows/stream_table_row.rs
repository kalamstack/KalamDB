use serde::{Deserialize, Serialize};

use super::{KTableRow, Row};
use crate::{ids::SeqId, models::UserId};

/// Stream table row entity
///
/// **Design Notes**:
/// - Removed: event_id (redundant with _seq), timestamp (embedded in _seq Snowflake ID), row_id,
///   inserted_at, _updated
/// - Kept: user_id (event owner), _seq (unique version ID with embedded timestamp), fields (all
///   event data)
/// - Note: NO _deleted field (stream tables don't use soft deletes, only TTL eviction)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamTableRow {
    /// User who owns this event
    pub user_id: UserId,
    /// Monotonically increasing sequence ID (Snowflake ID with embedded timestamp)
    /// Maps to SQL column `_seq`
    pub _seq: SeqId,
    /// All event data (serialized as JSON map)
    pub fields: Row,
}

impl From<StreamTableRow> for KTableRow {
    fn from(row: StreamTableRow) -> Self {
        KTableRow {
            user_id: row.user_id,
            _seq: row._seq,
            _commit_seq: 0,
            _deleted: false,
            fields: row.fields,
        }
    }
}
