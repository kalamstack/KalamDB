use serde::{Deserialize, Serialize};

use super::Row;
use crate::ids::SeqId;

/// Shared table row data.
///
/// **MVCC Architecture**:
/// - Kept: `_seq` (version identifier with embedded timestamp), `_commit_seq` (commit-order
///   visibility), `_deleted` (tombstone), `fields` (all shared table columns including PK)
/// - Identity lives on the RocksDB key (`SeqId` / `SharedTableRowId`), not in the persisted value
///   payload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedTableRow {
    /// Monotonically increasing sequence ID (Snowflake ID with embedded timestamp).
    /// Maps to SQL column `_seq`.
    pub _seq:        SeqId,
    /// Commit-order visibility marker assigned by the durable apply path.
    /// Maps to SQL column `_commit_seq`.
    #[serde(default)]
    pub _commit_seq: u64,
    /// Soft delete tombstone marker.
    /// Maps to SQL column `_deleted`.
    pub _deleted:    bool,
    /// All user-defined columns including PK.
    pub fields:      Row,
}

#[cfg(feature = "serialization")]
impl crate::serialization::KSerializable for SharedTableRow {}
