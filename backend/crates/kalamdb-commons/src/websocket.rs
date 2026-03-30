//! WebSocket message protocol for KalamDB live query subscriptions
//!
//! This module defines the complete WebSocket message protocol between clients and server
//! for real-time query subscriptions with batched initial data loading.
//!
//! # Protocol Flow
//!
//! ## 1. Client Subscription Request
//! ```json
//! {
//!   "type": "subscribe",
//!   "subscriptions": [{
//!     "id": "sub-1",
//!     "sql": "SELECT * FROM messages WHERE user_id = CURRENT_USER()",
//!     "options": {}
//!   }]
//! }
//! ```
//!
//! ## 2. Server Subscription Acknowledgement
//! ```json
//! {
//!   "type": "subscription_ack",
//!   "subscription_id": "sub-1",
//!   "total_rows": 5000,
//!   "batch_control": {
//!     "batch_num": 0,
//!     "has_more": true,
//!     "status": "loading"
//!   }
//! }
//! ```
//!
//! ## 3. Server Initial Data Batch (First Batch)
//! ```json
//! {
//!   "type": "initial_data_batch",
//!   "subscription_id": "sub-1",
//!   "rows": [{"id": 1, "message": "Hello"}, ...],
//!   "batch_control": {
//!     "batch_num": 0,
//!     "has_more": true,
//!     "status": "loading"
//!   }
//! }
//! ```
//!
//! ## 4. Client Next Batch Request
//! ```json
//! {
//!   "type": "next_batch",
//!   "subscription_id": "sub-1",
//!   "last_seq_id": 1000
//! }
//! ```
//!
//! ## 5. Server Subsequent Batch
//! ```json
//! {
//!   "type": "initial_data_batch",
//!   "subscription_id": "sub-1",
//!   "rows": [{"id": 1001, "message": "World"}, ...],
//!   "batch_control": {
//!     "batch_num": 1,
//!     "has_more": true,
//!     "status": "loading_batch"
//!   }
//! }
//! ```
//!
//! ## 6. Server Final Batch
//! ```json
//! {
//!   "type": "initial_data_batch",
//!   "subscription_id": "sub-1",
//!   "rows": [{"id": 4501, "message": "Done"}, ...],
//!   "batch_control": {
//!     "batch_num": 4,
//!     "has_more": false,
//!     "status": "ready"
//!   }
//! }
//! ```
//!
//! ## 7. Real-time Change Notifications (After Initial Load)
//! ```json
//! {
//!   "type": "change",
//!   "subscription_id": "sub-1",
//!   "change_type": "insert",
//!   "rows": [{"id": 5001, "message": "New"}]
//! }
//! ```

use crate::ids::SeqId;
use crate::models::rows::Row;
use crate::models::KalamCellValue;
use crate::models::UserId;
use crate::schemas::SchemaField;
pub use crate::websocket_auth::WsAuthCredentials;

// Simple Row type for WASM (JSON only)
#[cfg(feature = "wasm")]
pub type Row = serde_json::Map<String, serde_json::Value>;

use datafusion_common::ScalarValue;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Wire-format serialization type negotiated during authentication.
///
/// The client sends its preferred serialization in the `Authenticate` message.
/// After a successful auth response (always JSON), all subsequent frames use
/// the negotiated format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SerializationType {
    /// JSON text frames (default, backward-compatible).
    #[default]
    Json,
    /// MessagePack binary frames — compact and fast with the same Serde model.
    #[serde(rename = "msgpack")]
    MessagePack,
}

/// Wire-format compression negotiated during authentication.
///
/// Applied independently of serialization type. Large payloads may still
/// benefit from gzip even when using MessagePack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    /// No compression.
    None,
    /// Gzip compression for payloads above the server threshold (default).
    #[default]
    Gzip,
}

/// Protocol options negotiated once per connection during authentication.
///
/// Always sent in `ClientMessage::Authenticate` and echoed in `AuthSuccess`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolOptions {
    /// Serialization format for messages after auth.
    pub serialization: SerializationType,
    /// Compression policy.
    pub compression: CompressionType,
}

impl Default for ProtocolOptions {
    fn default() -> Self {
        Self {
            serialization: SerializationType::Json,
            compression: CompressionType::Gzip,
        }
    }
}

/// Type alias for row data in WebSocket messages (column_name -> cell value)
pub type RowData = HashMap<String, KalamCellValue>;

/// Batch size in bytes (8KB) for chunking large initial data payloads
pub const BATCH_SIZE_BYTES: usize = 8 * 1024;

/// Maximum rows per batch to prevent memory spikes (default 1000 rows)
pub const MAX_ROWS_PER_BATCH: usize = 1000;

/// WebSocket message types sent from server to client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebSocketMessage {
    /// Authentication successful response
    ///
    /// Sent after client sends Authenticate message with valid credentials.
    /// Client can now send Subscribe/Unsubscribe messages.
    /// Always sent as JSON text, even when msgpack was negotiated; the
    /// negotiated protocol takes effect for all *subsequent* frames.
    AuthSuccess {
        /// Authenticated user ID
        user_id: UserId,
        /// User role
        role: String,
        /// Negotiated protocol echoed back to the client.
        protocol: ProtocolOptions,
    },

    /// Authentication failed response
    ///
    /// Sent when authentication fails. Connection will be closed immediately.
    AuthError {
        /// Error message describing why authentication failed
        message: String,
    },

    /// Acknowledgement of successful subscription registration
    ///
    /// Sent immediately after client subscribes, includes total row count
    /// and batch control information for paginated initial data loading.
    SubscriptionAck {
        /// The subscription ID that was registered
        subscription_id: String,
        /// Total number of rows available for initial load
        total_rows: u32,
        /// Batch control information for paginated loading
        batch_control: BatchControl,
        /// Schema describing the columns in the subscription result
        /// Contains column name, data type (KalamDataType), and index for each field
        schema: Vec<SchemaField>,
    },

    /// Initial data batch sent after subscription or on client request
    ///
    /// Sent automatically for the first batch after subscription acknowledgement,
    /// then sent on-demand when client requests via ClientMessage::NextBatch.
    InitialDataBatch {
        /// The subscription ID this data is for
        subscription_id: String,
        /// The rows in this batch
        rows: Vec<RowData>,
        /// Batch control information
        batch_control: BatchControl,
    },

    /// Change notification (delegates to Notification enum)
    ///
    /// Sent when data changes (INSERT/UPDATE/DELETE) after initial load completes.
    /// Only sent when batch_control.status == Ready.
    #[serde(untagged)]
    Notification(Notification),
}

/// Client-to-server request messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Authenticate WebSocket connection
    ///
    /// Client sends this immediately after establishing WebSocket connection.
    /// Server must receive this within 3 seconds or connection will be closed.
    /// Server responds with AuthSuccess or AuthError.
    ///
    /// Supports token-based authentication via the `credentials` field.
    /// Optionally negotiates wire format via `protocol`.
    Authenticate {
        /// Authentication credentials (jwt or future token-based methods)
        #[serde(flatten)]
        credentials: WsAuthCredentials,
        /// Protocol negotiation (serialization + compression).
        protocol: ProtocolOptions,
    },

    /// Subscribe to live query updates
    ///
    /// Client sends this to register a single subscription.
    /// Server responds with SubscriptionAck followed by InitialDataBatch.
    Subscribe {
        /// Subscription to register
        subscription: SubscriptionRequest,
    },

    /// Request next batch of initial data
    ///
    /// Client sends this after processing a batch to request the next batch.
    /// Server responds with InitialDataBatch.
    NextBatch {
        /// The subscription ID to fetch the next batch for
        subscription_id: String,
        /// The SeqId of the last row received (for pagination)
        last_seq_id: Option<SeqId>,
    },

    /// Unsubscribe from live query
    ///
    /// Client sends this to stop receiving updates for a subscription.
    Unsubscribe {
        /// The subscription ID to unsubscribe from
        subscription_id: String,
    },

    /// Application-level keepalive ping.
    ///
    /// Browser WebSocket APIs do not expose protocol-level Ping frames, so
    /// the TypeScript SDK sends this JSON message periodically to prevent
    /// the server-side heartbeat timeout from firing on idle connections.
    /// The server updates its last-activity timestamp and discards the message.
    Ping,
}

/// Subscription request details
///
/// This is a client-only struct representing the subscription request.
/// Server-side metadata (table_id, filter_expr, projections, etc.) is stored
/// in SubscriptionState within ConnectionState.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    /// Unique subscription identifier (client-generated)
    pub id: String,
    /// SQL query for live updates (must be a SELECT statement)
    pub sql: String,
    /// Optional subscription options
    #[serde(default)]
    pub options: SubscriptionOptions,
}

/// Options for live query subscriptions
///
/// These options control individual subscription behavior including:
/// - Initial data loading (batch_size, last_rows)
/// - Data resumption after reconnection (from)
///
/// Used by both SQL SUBSCRIBE TO command and WebSocket subscribe messages.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SubscriptionOptions {
    /// Hint for server-side batch sizing during initial data load
    /// Default: server-configured (typically 1000 rows per batch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Number of last (newest) rows to fetch for initial data
    /// Default: None (fetch all matching rows)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rows: Option<u32>,

    /// Resume subscription from a specific sequence ID
    /// When set, the server will only send changes after this seq_id
    /// Typically set automatically during reconnection to resume from last received event
    #[serde(skip_serializing_if = "Option::is_none", alias = "from_seq_id")]
    pub from: Option<SeqId>,

    /// Preserve the original snapshot boundary across reconnects while the
    /// initial load is still in progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_end_seq: Option<SeqId>,
}

/// Batch control metadata for paginated initial data loading
///
/// Tracks the progress of batched initial data loading to prevent
/// overwhelming clients with large payloads (e.g., 1MB+).
///
/// Note: We don't include total_batches because we can't know it upfront
/// without counting all rows first (expensive). The `has_more` field is
/// sufficient for clients to know whether to request more batches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchControl {
    /// Current batch number (0-indexed)
    pub batch_num: u32,

    /// Whether more batches are available to fetch
    pub has_more: bool,

    /// Loading status for the subscription
    pub status: BatchStatus,

    /// The SeqId of the last row in this batch (used for next request)
    pub last_seq_id: Option<SeqId>,

    /// The snapshot boundary (max SeqId at start of load)
    pub snapshot_end_seq: Option<SeqId>,
}

/// Status of the initial data loading process
///
/// Transitions: Loading → LoadingBatch → Ready
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    /// Initial batch being loaded (batch_num == 0)
    Loading,

    /// Subsequent batches being loaded (batch_num > 0, has_more == true)
    LoadingBatch,

    /// All initial data has been loaded, live updates active (has_more == false)
    Ready,
}

/// Notification message sent to clients for live query updates
///
/// # Example Initial Data
/// ```json
/// {
///   "type": "initial_data",
///   "subscription_id": "sub-1",
///   "rows": [
///     {"id": 1, "message": "Hello"},
///     {"id": 2, "message": "World"}
///   ]
/// }
/// ```
///
/// # Example Change Notification (INSERT)
/// ```json
/// {
///   "type": "change",
///   "subscription_id": "sub-1",
///   "change_type": "insert",
///   "rows": [
///     {"id": 3, "message": "New message"}
///   ]
/// }
/// ```
///
/// # Example Change Notification (UPDATE)
/// ```json
/// {
///   "type": "change",
///   "subscription_id": "sub-1",
///   "change_type": "update",
///   "rows": [
///     {"id": 2, "message": "Updated message"}
///   ],
///   "old_values": [
///     {"id": 2, "message": "World"}
///   ]
/// }
/// ```
///
/// # Example Change Notification (DELETE)
/// ```json
/// {
///   "type": "change",
///   "subscription_id": "sub-1",
///   "change_type": "delete",
///   "old_values": [
///     {"id": 1, "message": "Hello"}
///   ]
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Notification {
    /// Change notification for INSERT/UPDATE/DELETE operations
    ///
    /// Sent only after initial data loading is complete (batch_control.status == Ready).
    ///
    /// For UPDATE notifications, `rows` contains **all non-null columns** from the
    /// updated row plus the primary key column(s) and `_seq` for identification.
    /// This means subscribers always receive a full snapshot of non-null values,
    /// not just the columns that changed — useful when the table is used as a
    /// change trigger.
    /// `old_values` contains the previous values of only the **changed** columns
    /// plus PK and `_seq`.
    Change {
        /// The subscription ID this notification is for
        subscription_id: String,

        /// Type of change that occurred
        change_type: ChangeType,

        /// For INSERT: full row data. For UPDATE: all non-null columns + PK/_seq.
        #[serde(skip_serializing_if = "Option::is_none")]
        rows: Option<Vec<RowData>>,

        /// Previous row values (for UPDATE: only changed columns + PK/_seq; for DELETE: full row)
        #[serde(skip_serializing_if = "Option::is_none")]
        old_values: Option<Vec<RowData>>,
    },

    /// Error notification (e.g., subscription query failed)
    Error {
        /// The subscription ID this error is for
        subscription_id: String,

        /// Error code
        code: String,

        /// Error message
        message: String,
    },
}

/// Type of change that occurred in the database
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeType {
    /// New row(s) inserted
    Insert,

    /// Existing row(s) updated
    Update,

    /// Row(s) deleted
    Delete,
}

/// Change notification for live query subscribers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeNotification {
    pub change_type: ChangeType,
    pub table_id: crate::models::TableId,
    pub row_data: Row,
    pub old_data: Option<Row>,   // For UPDATE notifications
    pub row_id: Option<String>,  // For DELETE notifications (hard delete)
    pub pk_columns: Vec<String>, // Primary key column name(s) for UPDATE delta
}

impl ChangeNotification {
    /// Create an INSERT notification
    pub fn insert(table_id: crate::models::TableId, row_data: Row) -> Self {
        Self {
            change_type: ChangeType::Insert,
            table_id,
            row_data,
            old_data: None,
            row_id: None,
            pk_columns: vec![],
        }
    }

    /// Create an UPDATE notification with old and new values.
    ///
    /// `pk_columns` lists the primary key column name(s) to always include in the
    /// delta payload alongside `_seq`, so clients can identify the updated row.
    pub fn update(
        table_id: crate::models::TableId,
        old_data: Row,
        new_data: Row,
        pk_columns: Vec<String>,
    ) -> Self {
        Self {
            change_type: ChangeType::Update,
            table_id,
            row_data: new_data,
            old_data: Some(old_data),
            row_id: None,
            pk_columns,
        }
    }

    /// Create a DELETE notification (soft delete with data)
    pub fn delete_soft(table_id: crate::models::TableId, row_data: Row) -> Self {
        Self {
            change_type: ChangeType::Delete,
            table_id,
            row_data,
            old_data: None,
            row_id: None,
            pk_columns: vec![],
        }
    }

    /// Create a DELETE notification (hard delete, row_id only)
    pub fn delete_hard(table_id: crate::models::TableId, row_id: String) -> Self {
        let mut values = BTreeMap::new();
        values.insert("row_id".to_string(), ScalarValue::Utf8(Some(row_id.clone())));

        Self {
            change_type: ChangeType::Delete,
            table_id,
            row_data: Row::new(values),
            old_data: None,
            row_id: Some(row_id),
            pk_columns: vec![],
        }
    }
}

impl WebSocketMessage {
    /// Create a subscription acknowledgement message with batch control and schema
    pub fn subscription_ack(
        subscription_id: String,
        total_rows: u32,
        batch_control: BatchControl,
        schema: Vec<SchemaField>,
    ) -> Self {
        Self::SubscriptionAck {
            subscription_id,
            total_rows,
            batch_control,
            schema,
        }
    }

    /// Create an initial data batch message
    pub fn initial_data_batch(
        subscription_id: String,
        rows: Vec<RowData>,
        batch_control: BatchControl,
    ) -> Self {
        Self::InitialDataBatch {
            subscription_id,
            rows,
            batch_control,
        }
    }
}

impl ClientMessage {
    /// Create a subscribe message for a single subscription
    pub fn subscribe(subscription: SubscriptionRequest) -> Self {
        Self::Subscribe { subscription }
    }

    /// Create a next batch request message
    pub fn next_batch(subscription_id: String, last_seq_id: Option<SeqId>) -> Self {
        Self::NextBatch {
            subscription_id,
            last_seq_id,
        }
    }

    /// Create an unsubscribe message
    pub fn unsubscribe(subscription_id: String) -> Self {
        Self::Unsubscribe { subscription_id }
    }
}

impl BatchControl {
    /// Create a batch control for the first batch (batch_num=0)
    ///
    /// # Arguments
    /// * `has_more` - Whether there are more batches to fetch after this one
    pub fn first(has_more: bool) -> Self {
        Self {
            batch_num: 0,
            has_more,
            status: if has_more {
                BatchStatus::Loading
            } else {
                BatchStatus::Ready
            },
            last_seq_id: None,
            snapshot_end_seq: None,
        }
    }

    /// Create a batch control for a subsequent batch (batch_num > 0)
    ///
    /// # Arguments
    /// * `batch_num` - The current batch number (0-indexed)
    /// * `has_more` - Whether there are more batches to fetch after this one
    pub fn subsequent(batch_num: u32, has_more: bool) -> Self {
        Self {
            batch_num,
            has_more,
            status: if has_more {
                BatchStatus::LoadingBatch
            } else {
                BatchStatus::Ready
            },
            last_seq_id: None,
            snapshot_end_seq: None,
        }
    }

    /// Create batch control with all fields specified
    pub fn new(
        batch_num: u32,
        has_more: bool,
        last_seq_id: Option<SeqId>,
        snapshot_end_seq: Option<SeqId>,
    ) -> Self {
        let status = if batch_num == 0 {
            if has_more {
                BatchStatus::Loading
            } else {
                BatchStatus::Ready
            }
        } else if has_more {
            BatchStatus::LoadingBatch
        } else {
            BatchStatus::Ready
        };

        Self {
            batch_num,
            has_more,
            status,
            last_seq_id,
            snapshot_end_seq,
        }
    }
}

impl Notification {
    /// Create an INSERT change notification
    pub fn insert(subscription_id: String, rows: Vec<RowData>) -> Self {
        Self::Change {
            subscription_id,
            change_type: ChangeType::Insert,
            rows: Some(rows),
            old_values: None,
        }
    }

    /// Create an UPDATE change notification with delta (only changed columns).
    ///
    /// `new_rows` and `old_rows` contain **only** the columns that changed plus
    /// the primary key column(s) and `_seq` for identification.
    /// Clients can derive which columns changed by inspecting the keys in `rows[0]`
    /// and filtering out those starting with `_` (system columns).
    pub fn update(subscription_id: String, new_rows: Vec<RowData>, old_rows: Vec<RowData>) -> Self {
        Self::Change {
            subscription_id,
            change_type: ChangeType::Update,
            rows: Some(new_rows),
            old_values: Some(old_rows),
        }
    }

    /// Create a DELETE change notification
    pub fn delete(subscription_id: String, old_rows: Vec<RowData>) -> Self {
        Self::Change {
            subscription_id,
            change_type: ChangeType::Delete,
            rows: None,
            old_values: Some(old_rows),
        }
    }

    /// Create an error notification
    pub fn error(subscription_id: String, code: String, message: String) -> Self {
        Self::Error {
            subscription_id,
            code,
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::rows::Row;
    use datafusion_common::ScalarValue;
    use std::collections::BTreeMap;

    fn create_test_row(id: i64, message: &str) -> Row {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(id)));
        values.insert("message".to_string(), ScalarValue::Utf8(Some(message.to_string())));
        Row::new(values)
    }

    fn row_to_test_json(row: &Row) -> HashMap<String, KalamCellValue> {
        // Simple conversion for tests - convert ScalarValue to JSON
        row.values
            .iter()
            .map(|(k, v)| {
                let json_val = match v {
                    ScalarValue::Int64(Some(i)) => KalamCellValue::text(i.to_string()),
                    ScalarValue::Utf8(Some(s)) => KalamCellValue::text(s.clone()),
                    _ => KalamCellValue::null(),
                };
                (k.clone(), json_val)
            })
            .collect()
    }

    #[test]
    fn test_client_message_serialization() {
        use crate::websocket::{ClientMessage, SubscriptionOptions, SubscriptionRequest};

        let msg = ClientMessage::subscribe(SubscriptionRequest {
            id: "sub-1".to_string(),
            sql: "SELECT * FROM messages".to_string(),
            options: SubscriptionOptions::default(),
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"subscribe\""));
        assert!(json.contains("sub-1"));
    }

    #[test]
    fn test_next_batch_request() {
        use crate::ids::SeqId;
        use crate::websocket::ClientMessage;

        let msg = ClientMessage::next_batch("sub-1".to_string(), Some(SeqId::new(100)));
        let json = serde_json::to_string(&msg).unwrap();

        assert!(json.contains("\"type\":\"next_batch\""));
        assert!(json.contains("\"last_seq_id\":100"));
    }

    #[test]
    fn test_batch_control_helpers() {
        use crate::websocket::{BatchControl, BatchStatus};

        // First batch with more to come
        let first = BatchControl::first(true);
        assert_eq!(first.batch_num, 0);
        assert!(first.has_more);
        assert_eq!(first.status, BatchStatus::Loading);

        // First batch with no more (single batch)
        let single = BatchControl::first(false);
        assert_eq!(single.batch_num, 0);
        assert!(!single.has_more);
        assert_eq!(single.status, BatchStatus::Ready);

        // Subsequent batch with more to come
        let middle = BatchControl::subsequent(2, true);
        assert_eq!(middle.batch_num, 2);
        assert!(middle.has_more);
        assert_eq!(middle.status, BatchStatus::LoadingBatch);

        // Final batch
        let last = BatchControl::subsequent(4, false);
        assert_eq!(last.batch_num, 4);
        assert!(!last.has_more);
        assert_eq!(last.status, BatchStatus::Ready);
    }

    #[test]
    fn test_insert_notification() {
        let row = create_test_row(1, "Hello");
        let row_json = row_to_test_json(&row);
        let notification = Notification::insert("sub-1".to_string(), vec![row_json]);

        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("change"));
        assert!(json.contains("insert"));
        assert!(json.contains("sub-1"));
        assert!(json.contains("Hello"));
    }

    #[test]
    fn test_update_notification() {
        let new_row = create_test_row(1, "Updated");
        let old_row = create_test_row(1, "Original");
        let new_row_json = row_to_test_json(&new_row);
        let old_row_json = row_to_test_json(&old_row);

        let notification =
            Notification::update("sub-1".to_string(), vec![new_row_json], vec![old_row_json]);

        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("update"));
        assert!(json.contains("Updated"));
        assert!(json.contains("Original"));
    }

    #[test]
    fn test_delete_notification() {
        let old_row = create_test_row(1, "Hello");
        let old_row_json = row_to_test_json(&old_row);

        let notification = Notification::delete("sub-1".to_string(), vec![old_row_json]);

        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("delete"));
        assert!(json.contains("old_values"));
        assert!(!json.contains("\"rows\""));
    }

    #[test]
    fn test_error_notification() {
        let notification = Notification::error(
            "sub-1".to_string(),
            "INVALID_QUERY".to_string(),
            "Query syntax error".to_string(),
        );

        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("error"));
        assert!(json.contains("INVALID_QUERY"));
    }

    #[test]
    fn test_serialization_type_default() {
        assert_eq!(SerializationType::default(), SerializationType::Json);
    }

    #[test]
    fn test_compression_type_default() {
        assert_eq!(CompressionType::default(), CompressionType::Gzip);
    }

    #[test]
    fn test_protocol_options_default() {
        let opts = ProtocolOptions::default();
        assert_eq!(opts.serialization, SerializationType::Json);
        assert_eq!(opts.compression, CompressionType::Gzip);
    }

    #[test]
    fn test_protocol_options_json_roundtrip() {
        let opts = ProtocolOptions {
            serialization: SerializationType::MessagePack,
            compression: CompressionType::None,
        };
        let json = serde_json::to_string(&opts).unwrap();
        assert!(json.contains("\"msgpack\""));
        assert!(json.contains("\"none\""));
        let parsed: ProtocolOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.serialization, SerializationType::MessagePack);
        assert_eq!(parsed.compression, CompressionType::None);
    }

    #[test]
    fn test_protocol_options_default_omitted() {
        // Default protocol should serialize cleanly and round-trip
        let opts = ProtocolOptions::default();
        let json = serde_json::to_string(&opts).unwrap();
        let parsed: ProtocolOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, opts);
    }

    #[test]
    fn test_authenticate_with_protocol() {
        let msg = ClientMessage::Authenticate {
            credentials: crate::websocket::WsAuthCredentials::Jwt {
                token: "test_token".to_string(),
            },
            protocol: ProtocolOptions {
                serialization: SerializationType::MessagePack,
                compression: CompressionType::Gzip,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"protocol\""));
        assert!(json.contains("\"msgpack\""));
        let parsed: ClientMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            ClientMessage::Authenticate { protocol, .. } => {
                assert_eq!(protocol.serialization, SerializationType::MessagePack);
            },
            _ => panic!("Expected Authenticate"),
        }
    }

    #[test]
    fn test_authenticate_default_protocol() {
        let msg = ClientMessage::Authenticate {
            credentials: crate::websocket::WsAuthCredentials::Jwt {
                token: "test_token".to_string(),
            },
            protocol: ProtocolOptions::default(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"protocol\""));
        let parsed: ClientMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            ClientMessage::Authenticate { protocol, .. } => {
                assert_eq!(protocol.serialization, SerializationType::Json);
                assert_eq!(protocol.compression, CompressionType::Gzip);
            },
            _ => panic!("Expected Authenticate"),
        }
    }

    #[test]
    fn test_auth_success_with_protocol() {
        let msg = WebSocketMessage::AuthSuccess {
            user_id: UserId::from("user-1"),
            role: "admin".to_string(),
            protocol: ProtocolOptions {
                serialization: SerializationType::MessagePack,
                compression: CompressionType::Gzip,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"protocol\""));
        let parsed: WebSocketMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            WebSocketMessage::AuthSuccess { protocol, .. } => {
                assert_eq!(protocol.serialization, SerializationType::MessagePack);
            },
            _ => panic!("Expected AuthSuccess"),
        }
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_client_message_msgpack_roundtrip() {
        let msg = ClientMessage::subscribe(SubscriptionRequest {
            id: "sub-1".to_string(),
            sql: "SELECT * FROM test".to_string(),
            options: SubscriptionOptions::default(),
        });
        let bytes = rmp_serde::to_vec_named(&msg).unwrap();
        let parsed: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
        match parsed {
            ClientMessage::Subscribe { subscription } => {
                assert_eq!(subscription.id, "sub-1");
                assert_eq!(subscription.sql, "SELECT * FROM test");
            },
            _ => panic!("Expected Subscribe"),
        }
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_websocket_message_msgpack_roundtrip() {
        let msg = WebSocketMessage::AuthSuccess {
            user_id: UserId::from("user-1"),
            role: "admin".to_string(),
            protocol: ProtocolOptions {
                serialization: SerializationType::MessagePack,
                compression: CompressionType::None,
            },
        };
        let bytes = rmp_serde::to_vec_named(&msg).unwrap();
        let parsed: WebSocketMessage = rmp_serde::from_slice(&bytes).unwrap();
        match parsed {
            WebSocketMessage::AuthSuccess { user_id, role, protocol } => {
                assert_eq!(user_id, UserId::from("user-1"));
                assert_eq!(role, "admin");
                assert_eq!(protocol.serialization, SerializationType::MessagePack);
            },
            _ => panic!("Expected AuthSuccess"),
        }
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn test_notification_msgpack_roundtrip() {
        let notification = Notification::insert("sub-1".to_string(), vec![]);
        let bytes = rmp_serde::to_vec_named(&notification).unwrap();
        let parsed: Notification = rmp_serde::from_slice(&bytes).unwrap();
        match parsed {
            Notification::Change { subscription_id, .. } => {
                assert_eq!(subscription_id, "sub-1");
            }
            _ => panic!("Expected Change"),
        }
    }
}
