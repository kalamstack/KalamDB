//! Topic message models - ID and entity definitions
//!
//! This module defines the data structures for topic messages:
//! - TopicMessageId: Composite key for message identification
//! - TopicMessage: Message envelope with payload and metadata

use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::{TopicId, TopicOp, UserId};
use kalamdb_commons::{decode_key, encode_key, encode_prefix, KSerializable, StorageKey};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Composite key for topic messages: topic_id + partition_id + offset
///
/// Uses order-preserving composite key encoding for efficient range scans.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicMessageId {
    pub topic_id: TopicId,
    pub partition_id: u32,
    pub offset: u64,
}

impl TopicMessageId {
    /// Create a new topic message ID
    pub fn new(topic_id: TopicId, partition_id: u32, offset: u64) -> Self {
        Self {
            topic_id,
            partition_id,
            offset,
        }
    }

    /// Prefix for scanning all messages in a topic partition
    pub fn prefix_for_partition(topic_id: &TopicId, partition_id: u32) -> Vec<u8> {
        encode_prefix(&(topic_id.as_str(), partition_id))
    }

    /// Start key for scanning messages in a topic partition from an offset
    pub fn start_key_for_partition(topic_id: &TopicId, partition_id: u32, offset: u64) -> Vec<u8> {
        encode_key(&(topic_id.as_str(), partition_id, offset))
    }
}

impl StorageKey for TopicMessageId {
    fn storage_key(&self) -> Vec<u8> {
        // Use composite key encoding: (topic_id, partition_id, offset)
        // This enables efficient prefix scans by topic or topic+partition
        encode_key(&(self.topic_id.as_str(), self.partition_id, self.offset))
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let (topic_str, partition_id, offset): (String, u32, u64) = decode_key(bytes)?;
        Ok(Self::new(TopicId::from(topic_str.as_str()), partition_id, offset))
    }
}

/// Topic message envelope with metadata and payload
///
/// **Storage Format**:
/// - Serialized using binary codec for efficiency
/// - Contains full message envelope (not just payload)
/// - Immutable once written (append-only)
#[table(
    name = "topic_messages",
    namespace = "system",
    table_type = "system",
    comment = "Internal durable topic message envelopes"
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicMessage {
    /// Topic identifier
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Topic identifier"
    )]
    pub topic_id: TopicId,
    /// Partition ID (0-based)
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Partition identifier"
    )]
    pub partition_id: u32,
    /// Sequential offset within partition (0-based)
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Sequential offset within the partition"
    )]
    pub offset: u64,
    /// Message payload (serialized JSON, Avro, or raw bytes)
    #[column(
        id = 4,
        ordinal = 5,
        data_type(KalamDataType::Bytes),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Message payload bytes"
    )]
    pub payload: Vec<u8>,
    /// Optional message key for ordering/deduplication
    #[column(
        id = 5,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional message key for ordering or deduplication"
    )]
    pub key: Option<String>,
    /// Timestamp when message was published (milliseconds since epoch)
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Publish timestamp in milliseconds since epoch"
    )]
    pub timestamp_ms: i64,
    /// User who triggered the event that produced this message
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "User identifier that triggered the published event"
    )]
    #[serde(default)]
    pub user_id: Option<UserId>,
    /// Operation type that produced this message (INSERT, UPDATE, DELETE)
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Operation that produced this message"
    )]
    #[serde(default)]
    pub op: TopicOp,
}

impl TopicMessage {
    /// Create a new topic message
    pub fn new(
        topic_id: TopicId,
        partition_id: u32,
        offset: u64,
        payload: Vec<u8>,
        key: Option<String>,
        timestamp_ms: i64,
        op: TopicOp,
    ) -> Self {
        Self {
            topic_id,
            partition_id,
            offset,
            payload,
            key,
            timestamp_ms,
            user_id: None,
            op,
        }
    }

    /// Create a new topic message with an associated user
    pub fn new_with_user(
        topic_id: TopicId,
        partition_id: u32,
        offset: u64,
        payload: Vec<u8>,
        key: Option<String>,
        timestamp_ms: i64,
        user_id: Option<UserId>,
        op: TopicOp,
    ) -> Self {
        Self {
            topic_id,
            partition_id,
            offset,
            payload,
            key,
            timestamp_ms,
            user_id,
            op,
        }
    }

    /// Get the composite ID for this message
    pub fn id(&self) -> TopicMessageId {
        TopicMessageId::new(self.topic_id.clone(), self.partition_id, self.offset)
    }
}

impl KSerializable for TopicMessage {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_id_storage_key() {
        let msg_id = TopicMessageId::new(TopicId::from("my_topic"), 3, 42);
        let key = msg_id.storage_key();

        // Should be able to decode it back
        let decoded = TopicMessageId::from_storage_key(&key).unwrap();
        assert_eq!(decoded, msg_id);
    }

    #[test]
    fn test_message_id_ordering() {
        // Keys should be ordered by topic, then partition, then offset
        let key1 = TopicMessageId::new(TopicId::from("topic_a"), 0, 10).storage_key();
        let key2 = TopicMessageId::new(TopicId::from("topic_a"), 0, 20).storage_key();
        let key3 = TopicMessageId::new(TopicId::from("topic_a"), 1, 5).storage_key();
        let key4 = TopicMessageId::new(TopicId::from("topic_b"), 0, 0).storage_key();

        assert!(key1 < key2, "Offset ordering within same partition");
        assert!(key2 < key3, "Partition ordering within same topic");
        assert!(key3 < key4, "Topic ordering");
    }

    #[test]
    fn test_topic_message_creation() {
        let msg = TopicMessage::new(
            TopicId::from("test_topic"),
            0,
            100,
            b"test payload".to_vec(),
            Some("key1".to_string()),
            1706745600000,
            TopicOp::Insert,
        );

        assert_eq!(msg.topic_id, TopicId::from("test_topic"));
        assert_eq!(msg.partition_id, 0);
        assert_eq!(msg.offset, 100);
        assert_eq!(msg.payload, b"test payload");
        assert_eq!(msg.key, Some("key1".to_string()));
    }

    #[test]
    fn test_message_id_extraction() {
        let msg = TopicMessage::new(TopicId::from("test"), 5, 99, vec![], None, 0, TopicOp::Insert);

        let id = msg.id();
        assert_eq!(id.topic_id, TopicId::from("test"));
        assert_eq!(id.partition_id, 5);
        assert_eq!(id.offset, 99);
    }
}
