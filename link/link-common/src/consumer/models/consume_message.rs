use serde::{Deserialize, Serialize};

use crate::auth::models::Username;
use crate::models::RowData;

/// A single consumed message from a topic.
///
/// Contains the message payload (decoded from base64), metadata about the
/// source operation, and positioning information for acknowledgment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeMessage {
    /// Unique message identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// Source table that produced this message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_table: Option<String>,

    /// Operation type: "insert", "update", or "delete"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,

    /// Message timestamp in milliseconds since epoch
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,

    /// Message offset in the partition (used for acknowledgment)
    pub offset: u64,

    /// Partition this message belongs to
    pub partition_id: u32,

    /// Topic this message belongs to
    pub topic: String,

    /// Consumer group ID
    pub group_id: String,

    /// Username of the user who produced this message/event
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<Username>,

    /// Decoded message payload as a named-column row (`column → value`).
    /// Mirrors the subscription row shape: `HashMap<String, KalamCellValue>`.
    pub value: RowData,
}
