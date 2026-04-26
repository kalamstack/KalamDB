//! Topic route configuration.
//!
//! Defines how a topic consumes change events from a source table.

use kalamdb_commons::models::{PayloadMode, TableId, TopicOp};
use serde::{Deserialize, Serialize};

/// Topic route configuration.
///
/// Each route defines a source table, operation type, and transformation rules
/// for publishing change events to a topic.
///
/// ## Fields
/// - `table_id`: Source table to capture events from
/// - `op`: Operation type to capture (Insert/Update/Delete)
/// - `payload_mode`: Payload format (Key/Full/Diff)
/// - `filter_expr`: Optional SQL WHERE clause for filtering events
/// - `partition_key_expr`: Optional expression for partitioning (future use)
///
/// ## Examples
/// ```rust,ignore
/// use kalamdb_system::providers::topics::models::TopicRoute;
/// use kalamdb_commons::models::{TableId, TopicOp, PayloadMode};
///
/// // Route that captures all inserts with full row payload
/// let route = TopicRoute {
///     table_id: TableId::new("default", "messages"),
///     op: TopicOp::Insert,
///     payload_mode: PayloadMode::Full,
///     filter_expr: None,
///     partition_key_expr: None,
/// };
///
/// // Route with filtering - only visible messages
/// let filtered_route = TopicRoute {
///     table_id: TableId::new("default", "messages"),
///     op: TopicOp::Insert,
///     payload_mode: PayloadMode::Key,
///     filter_expr: Some("is_visible = true AND priority >= 5".to_string()),
///     partition_key_expr: None,
/// };
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TopicRoute {
    /// Source table to capture events from
    pub table_id: TableId,

    /// Operation type to capture (Insert/Update/Delete)
    pub op: TopicOp,

    /// Payload format (Key/Full/Diff)
    pub payload_mode: PayloadMode,

    /// Optional SQL WHERE clause for filtering events.
    ///
    /// If provided, only rows matching this condition will be published.
    /// Expression is evaluated in the context of the source table schema.
    ///
    /// Examples:
    /// - `"is_visible = true"`
    /// - `"priority >= 5 AND status = 'active'"`
    /// - `"user_id IN (SELECT user_id FROM premium_users)"`
    pub filter_expr: Option<String>,

    /// Optional expression for computing partition key (future enhancement).
    ///
    /// When topics support multiple partitions, this expression determines
    /// which partition a message is published to. Messages with the same
    /// partition key are guaranteed to maintain order within a partition.
    ///
    /// Examples:
    /// - `"user_id"` - Partition by user (all user's messages in order)
    /// - `"channel_id"` - Partition by channel
    /// - `"hash(user_id, 4)"` - Distribute evenly across 4 partitions
    pub partition_key_expr: Option<String>,
}

impl TopicRoute {
    /// Creates a new topic route with key-only payload and no filtering.
    pub fn new(table_id: TableId, op: TopicOp) -> Self {
        Self {
            table_id,
            op,
            payload_mode: PayloadMode::Key,
            filter_expr: None,
            partition_key_expr: None,
        }
    }

    /// Sets the payload mode for this route.
    pub fn with_payload_mode(mut self, mode: PayloadMode) -> Self {
        self.payload_mode = mode;
        self
    }

    /// Adds a filter expression to this route.
    pub fn with_filter(mut self, filter_expr: String) -> Self {
        self.filter_expr = Some(filter_expr);
        self
    }

    /// Adds a partition key expression to this route.
    pub fn with_partition_key(mut self, partition_key_expr: String) -> Self {
        self.partition_key_expr = Some(partition_key_expr);
        self
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;

    #[test]
    fn test_topic_route_builder() {
        let route = TopicRoute::new(
            TableId::new(NamespaceId::new("default"), TableName::new("messages")),
            TopicOp::Insert,
        )
        .with_payload_mode(PayloadMode::Full)
        .with_filter("is_visible = true".to_string());

        assert_eq!(route.op, TopicOp::Insert);
        assert_eq!(route.payload_mode, PayloadMode::Full);
        assert_eq!(route.filter_expr, Some("is_visible = true".to_string()));
        assert_eq!(route.partition_key_expr, None);
    }

    #[test]
    fn test_topic_route_serialization() {
        let route = TopicRoute::new(
            TableId::new(NamespaceId::new("default"), TableName::new("events")),
            TopicOp::Update,
        );

        // Test binary round-trip
        let encoded = flexbuffers::to_vec(&route).unwrap();
        let decoded: TopicRoute = flexbuffers::from_slice(&encoded).unwrap();
        assert_eq!(route, decoded);

        // Test JSON round-trip
        let json = serde_json::to_string(&route).unwrap();
        let from_json: TopicRoute = serde_json::from_str(&json).unwrap();
        assert_eq!(route, from_json);
    }
}
