use serde::{Deserialize, Serialize};

use crate::models::RowData;
use kalamdb_commons::UserId;

/// A single consumed message from a topic.
///
/// Contains the message payload (decoded from base64), metadata about the
/// source operation, and positioning information for acknowledgment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeMessage {
    /// Optional message key from the topic envelope.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "message_id")]
    pub key: Option<String>,

    /// Operation type from the topic envelope (`Insert`, `Update`, or `Delete`).
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

    /// Canonical user identifier of the user who produced this message/event
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<UserId>,

    /// Decoded payload from the HTTP API's base64 `payload` field.
    ///
    /// For `WITH (payload = 'full')` routes this is usually the changed row JSON,
    /// plus `_table` metadata identifying the source table.
    #[serde(alias = "value")]
    pub payload: RowData,
}
