//! Live query subscription row model for the system.live view.

use super::LiveQueryStatus;
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::{
    ids::{LiveQueryId, NamespaceId, UserId},
    NodeId, TableName,
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Live query subscription row model for `system.live`.
///
/// Represents an active live query subscription (WebSocket connection).
///
/// ## Fields
/// - `live_id`: Unique live query ID (format: {user_id}-{conn_id}-{table_name}-{subscription_id})
/// - `connection_id`: WebSocket connection identifier
/// - `subscription_id`: Client-provided subscription identifier
/// - `namespace_id`: Namespace ID
/// - `table_name`: Table being queried
/// - `user_id`: User who created the subscription
/// - `query`: SQL query text
/// - `options`: Optional JSON configuration
/// - `status`: Current status (Active, Paused, Completed, Error)
/// - `created_at`: Unix timestamp in milliseconds when subscription was created
/// - `last_update`: Unix timestamp in milliseconds of last update notification
/// - `changes`: Number of changes sent
/// - `node_id`: Node/server handling this subscription
///
/// **Note**: `last_seq_id` is tracked in-memory only (in WebSocketSession.subscription_metadata),
/// not stored in the `system.live` view row.
///
/// ## Serialization
/// - **Command payloads / tests**: JSON via Serde
/// - **View schema**: generated from this type via `#[table]`
///
/// ## Example
///
/// ```rust
/// use kalamdb_commons::models::ids::{ConnectionId, LiveQueryId, NamespaceId, UserId};
/// use kalamdb_system::LiveQuery;
/// use kalamdb_system::LiveQueryStatus;
/// use kalamdb_commons::{NodeId, TableName};
///
/// let user_id = UserId::new("u_123");
/// let connection_id = ConnectionId::new("conn_456");
///
/// let live_query = LiveQuery {
///     live_id: LiveQueryId::new(user_id.clone(), connection_id.clone(), "sub_1"),
///     connection_id: connection_id.as_str().to_string(),
///     subscription_id: "sub_1".to_string(),
///     namespace_id: NamespaceId::default(),
///     table_name: TableName::new("events"),
///     user_id,
///     query: "SELECT * FROM events WHERE type = 'click'".to_string(),
///     options: Some(r#"{"include_initial": true}"#.to_string()),
///     status: LiveQueryStatus::Active,
///     created_at: 1730000000000,
///     last_update: 1730000300000,
///     last_ping_at: 1730000300000,
///     changes: 42,
///     node_id: NodeId::from(1u64),
/// };
/// ```
/// LiveQuery struct with fields ordered for optimal memory alignment.
/// 8-byte aligned fields first, then smaller types.
#[table(name = "live", comment = "Active in-memory live subscriptions")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LiveQuery {
    // 8-byte aligned fields first (i64, String/pointer types)
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Live query creation timestamp"
    )]
    pub created_at: i64,
    #[column(
        id = 11,
        ordinal = 11,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last update sent to client"
    )]
    pub last_update: i64,
    #[column(
        id = 14,
        ordinal = 14,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last ping timestamp for stale detection"
    )]
    pub last_ping_at: i64,
    #[column(
        id = 12,
        ordinal = 12,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Number of changes pushed to client"
    )]
    pub changes: i64,
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Live query identifier (format: {user_id}-{conn_id}-{table}-{subscription_id})"
    )]
    pub live_id: LiveQueryId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "WebSocket connection identifier"
    )]
    pub connection_id: String,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Client-provided subscription identifier"
    )]
    pub subscription_id: String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Namespace containing the table"
    )]
    pub namespace_id: NamespaceId,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Table being queried"
    )]
    pub table_name: TableName,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "User who created the live query"
    )]
    pub user_id: UserId,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "SQL query for real-time subscription"
    )]
    pub query: String,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Query options (JSON)"
    )]
    pub options: Option<String>,
    /// Node identifier that holds this subscription's WebSocket connection
    #[column(
        id = 13,
        ordinal = 13,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Server node ID handling this live query"
    )]
    pub node_id: NodeId,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Current status (active, paused, etc.)"
    )]
    pub status: LiveQueryStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_query_serialization() {
        let live_query = LiveQuery {
            live_id: "u_123-conn_456-events-sub_1".into(),
            connection_id: "conn_456".to_string(),
            subscription_id: "sub_1".to_string(),
            namespace_id: NamespaceId::default(),
            table_name: TableName::new("events"),
            user_id: UserId::new("u_123"),
            query: "SELECT * FROM events".to_string(),
            options: Some(r#"{"include_initial": true}"#.to_string()),
            status: LiveQueryStatus::Active,
            created_at: 1730000000000,
            last_update: 1730000300000,
            last_ping_at: 1730000300000,
            changes: 42,
            node_id: NodeId::from(1u64),
        };

        let bytes = serde_json::to_vec(&live_query).unwrap();
        let deserialized: LiveQuery = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(live_query, deserialized);
    }
}
