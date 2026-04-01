//! Topic message model

use kalamdb_commons::models::TopicId;
use serde::Serialize;

/// Message in consume response
#[derive(Debug, Serialize)]
pub struct TopicMessage {
    /// Topic identifier (type-safe)
    pub topic_id: TopicId,
    /// Partition ID
    pub partition_id: u32,
    /// Message offset
    pub offset: u64,
    /// Base64-encoded payload bytes
    pub payload: String,
    /// Optional message key
    pub key: Option<String>,
    /// Timestamp in milliseconds since epoch
    pub timestamp_ms: i64,
    /// Username of the user who produced this message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>, //TODO: Use UserName type instead
    /// Operation type that triggered this message (Insert, Update, Delete)
    pub op: String, //TODO: Use TopicOp instead
}
