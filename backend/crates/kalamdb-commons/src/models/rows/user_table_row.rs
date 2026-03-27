use super::{KTableRow, Row};
use crate::ids::SeqId;
use crate::models::UserId;
use serde::{Deserialize, Serialize};

/// User table row data
///
/// **MVCC Architecture (Phase 12, User Story 5)**:
/// - Removed: row_id (redundant with _seq), _updated (timestamp embedded in _seq Snowflake ID)
/// - Kept: user_id (row owner), _seq (version identifier with embedded timestamp), _deleted (tombstone), fields (all user columns including PK)
///
/// **Note on System Column Naming**:
/// The underscore prefix (`_seq`, `_deleted`) follows SQL convention for system-managed columns.
/// These names match the SQL column names exactly for consistency across the codebase.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UserTableRow {
    /// User who owns this row
    pub user_id: UserId,
    /// Monotonically increasing sequence ID (Snowflake ID with embedded timestamp)
    /// Maps to SQL column `_seq`
    pub _seq: SeqId,
    /// Soft delete tombstone marker
    /// Maps to SQL column `_deleted`
    pub _deleted: bool,
    /// All user-defined columns including PK (serialized as JSON map)
    pub fields: Row,
}

impl From<UserTableRow> for KTableRow {
    fn from(row: UserTableRow) -> Self {
        KTableRow {
            user_id: row.user_id,
            _seq: row._seq,
            _deleted: row._deleted,
            fields: row.fields,
        }
    }
}

// KSerializable implementation for EntityStore support
#[cfg(feature = "serialization")]
impl crate::serialization::KSerializable for UserTableRow {
    fn encode(&self) -> Result<Vec<u8>, crate::storage::StorageError> {
        crate::serialization::row_codec::encode_user_table_row(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self, crate::storage::StorageError>
    where
        Self: Sized,
    {
        crate::serialization::row_codec::decode_user_table_row(bytes)
    }
}
