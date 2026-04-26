//! Topic offset models - entity definitions

use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{ConsumerGroupId, TopicId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Topic offset tracking for consumer groups
///
/// **Offset Semantics**:
/// - last_acked_offset: Last successfully acknowledged message offset
/// - Consumers commit offsets after processing messages
#[table(
    name = "topic_offsets",
    comment = "Consumer group offset tracking for topics"
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicOffset {
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

    /// Consumer group identifier
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Consumer group identifier"
    )]
    pub group_id: ConsumerGroupId,

    /// Partition ID (0-based)
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Partition ID (0-based)"
    )]
    pub partition_id: u32,

    /// Last acknowledged offset (successfully processed)
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last acknowledged offset"
    )]
    pub last_acked_offset: u64,

    /// Timestamp of last update (milliseconds since epoch)
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last update timestamp (milliseconds since epoch)"
    )]
    pub updated_at: i64,
}

impl TopicOffset {
    /// Create a new topic offset
    pub fn new(
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        partition_id: u32,
        last_acked_offset: u64,
        updated_at: i64,
    ) -> Self {
        Self {
            topic_id,
            group_id,
            partition_id,
            last_acked_offset,
            updated_at,
        }
    }

    /// Acknowledge (commit) an offset after processing.
    ///
    /// Monotonic: the committed offset only advances, never regresses.
    /// This is critical when multiple consumers in the same group ack
    /// out-of-order (e.g., consumer B acks offset 399, then consumer A
    /// acks offset 199 — the committed offset must stay at 399).
    pub fn ack(&mut self, offset: u64, timestamp_ms: i64) {
        if offset > self.last_acked_offset {
            self.last_acked_offset = offset;
            self.updated_at = timestamp_ms;
        }
    }

    /// Get the next offset to fetch (last_acked + 1)
    pub fn next_offset(&self) -> u64 {
        self.last_acked_offset + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_offset_creation() {
        let offset = TopicOffset::new(
            TopicId::from("test_topic"),
            ConsumerGroupId::from("group1"),
            0,
            10,
            1706745600000,
        );

        assert_eq!(offset.topic_id, TopicId::from("test_topic"));
        assert_eq!(offset.group_id, ConsumerGroupId::from("group1"));
        assert_eq!(offset.partition_id, 0);
        assert_eq!(offset.last_acked_offset, 10);
        assert_eq!(offset.next_offset(), 11);
    }

    #[test]
    fn test_offset_ack_logic() {
        let mut offset =
            TopicOffset::new(TopicId::from("test"), ConsumerGroupId::from("group1"), 0, 0, 1000);

        // Ack offset 5
        offset.ack(5, 2000);
        assert_eq!(offset.last_acked_offset, 5);
        assert_eq!(offset.next_offset(), 6);

        // Ack offset 10 (newer)
        offset.ack(10, 3000);
        assert_eq!(offset.last_acked_offset, 10);
        assert_eq!(offset.next_offset(), 11);
    }

    #[test]
    fn test_ack_never_regresses() {
        let mut offset =
            TopicOffset::new(TopicId::from("test"), ConsumerGroupId::from("group1"), 0, 0, 1000);

        // Advance to offset 399
        offset.ack(399, 2000);
        assert_eq!(offset.last_acked_offset, 399);

        // Try to ack a lower offset (out-of-order from Consumer A)
        offset.ack(199, 3000);
        assert_eq!(
            offset.last_acked_offset, 399,
            "ack with lower offset must not regress committed position"
        );
        assert_eq!(offset.updated_at, 2000, "timestamp should not change on no-op ack");

        // Equal offset is also a no-op
        offset.ack(399, 4000);
        assert_eq!(offset.last_acked_offset, 399);
        assert_eq!(offset.updated_at, 2000);

        // Higher offset advances
        offset.ack(400, 5000);
        assert_eq!(offset.last_acked_offset, 400);
        assert_eq!(offset.updated_at, 5000);
    }
}
