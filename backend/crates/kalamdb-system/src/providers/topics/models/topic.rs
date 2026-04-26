//! Topic entity for system.topics table.
//!
//! Represents a durable pub/sub topic backed by RocksDB.

use kalamdb_commons::{datatypes::KalamDataType, models::TopicId};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

use super::TopicRoute;

/// Topic entity for system.topics table.
///
/// Represents a durable pub/sub topic for consuming change events from source tables.
///
/// ## Fields
/// - `topic_id`: Unique topic identifier (primary key)
/// - `name`: Unique topic name (e.g., "app.notifications")
/// - `alias`: Optional unique alias for backward compatibility
/// - `partitions`: Number of partitions (default 1, for future horizontal scaling)
/// - `retention_seconds`: Optional retention period in seconds (null = infinite)
/// - `retention_max_bytes`: Optional max storage per partition in bytes (null = unlimited)
/// - `routes`: List of source table routes defining what events to capture
/// - `created_at`: Unix timestamp in milliseconds when topic was created
/// - `updated_at`: Unix timestamp in milliseconds when topic was last updated
///
/// ## Serialization
/// - **RocksDB**: FlatBuffers envelope + FlexBuffers payload
/// - **API**: JSON via Serde
///
/// ## Example
/// ```rust,ignore
/// use kalamdb_system::providers::topics::models::{Topic, TopicRoute};
/// use kalamdb_commons::models::{TopicId, TableId, TopicOp, PayloadMode};
///
/// let topic = Topic {
///     topic_id: TopicId::new("topic_123"),
///     name: "app.notifications".to_string(),
///     alias: None,
///     partitions: 1,
///     retention_seconds: Some(86400 * 7), // 7 days
///     retention_max_bytes: Some(1024 * 1024 * 1024), // 1GB
///     routes: vec![
///         TopicRoute {
///             table_id: TableId::new("default", "messages"),
///             op: TopicOp::Insert,
///             payload_mode: PayloadMode::Key,
///             filter_expr: Some("is_visible = true".to_string()),
///             partition_key_expr: None,
///         }
///     ],
///     created_at: 1730000000000,
///     updated_at: 1730000000000,
/// };
/// ```
#[table(name = "topics", comment = "Durable topics for pub/sub messaging")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Topic {
    // Primary key field first
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Unique topic identifier"
    )]
    pub topic_id: TopicId,

    // String fields (pointer-sized)
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Topic name (unique, e.g. 'app.notifications')"
    )]
    pub name: String,

    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional unique alias for backward compatibility"
    )]
    pub alias: Option<String>,

    // Numeric fields (8-byte aligned)
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unix timestamp in milliseconds when topic was created"
    )]
    pub created_at: i64,

    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unix timestamp in milliseconds when topic was last updated"
    )]
    pub updated_at: i64,

    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::BigInt),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Retention period in seconds (null = infinite)"
    )]
    pub retention_seconds: Option<i64>,

    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::BigInt),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Max storage per partition in bytes (null = unlimited)"
    )]
    pub retention_max_bytes: Option<i64>,

    // Smaller numeric fields
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Number of partitions (default 1)"
    )]
    pub partitions: u32,

    // Complex field last (Vec is heap-allocated)
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Json),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "List of source table routes (JSON array)"
    )]
    pub routes: Vec<TopicRoute>,
}

impl Topic {
    /// Creates a new topic with default settings.
    pub fn new(topic_id: TopicId, name: String) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            topic_id,
            name,
            alias: None,
            partitions: 1,
            retention_seconds: None,
            retention_max_bytes: None,
            routes: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Adds a route to this topic.
    pub fn add_route(&mut self, route: TopicRoute) {
        self.routes.push(route);
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Removes all routes matching the given table_id and operation.
    pub fn remove_route(
        &mut self,
        table_id: &kalamdb_commons::models::TableId,
        op: kalamdb_commons::models::TopicOp,
    ) -> usize {
        let before = self.routes.len();
        self.routes.retain(|r| r.table_id != *table_id || r.op != op);
        let removed = before - self.routes.len();
        if removed > 0 {
            self.updated_at = chrono::Utc::now().timestamp_millis();
        }
        removed
    }

    /// Returns whether this topic has any routes configured.
    pub fn has_routes(&self) -> bool {
        !self.routes.is_empty()
    }

    /// Returns the number of routes configured for this topic.
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{NamespaceId, TableId, TableName, TopicOp};

    use super::*;

    #[test]
    fn test_topic_creation() {
        let topic = Topic::new(TopicId::new("topic_123"), "app.notifications".to_string());

        assert_eq!(topic.name, "app.notifications");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.routes.len(), 0);
        assert!(topic.retention_seconds.is_none());
    }

    #[test]
    fn test_topic_add_route() {
        let mut topic = Topic::new(TopicId::new("topic_123"), "app.events".to_string());

        let route = TopicRoute::new(
            TableId::new(NamespaceId::new("default"), TableName::new("messages")),
            TopicOp::Insert,
        );

        topic.add_route(route);
        assert_eq!(topic.routes.len(), 1);
        assert!(topic.has_routes());
    }

    #[test]
    fn test_topic_remove_route() {
        let mut topic = Topic::new(TopicId::new("topic_123"), "app.events".to_string());

        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("messages"));
        topic.add_route(TopicRoute::new(table_id.clone(), TopicOp::Insert));
        topic.add_route(TopicRoute::new(table_id.clone(), TopicOp::Update));

        let removed = topic.remove_route(&table_id, TopicOp::Insert);
        assert_eq!(removed, 1);
        assert_eq!(topic.routes.len(), 1);
        assert_eq!(topic.routes[0].op, TopicOp::Update);
    }

    #[test]
    fn test_topic_serialization() {
        let topic = Topic::new(TopicId::new("topic_456"), "test.topic".to_string());

        let encoded = serde_json::to_vec(&topic).unwrap();
        let decoded: Topic = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(topic, decoded);
    }
}
