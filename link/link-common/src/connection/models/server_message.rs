use std::collections::HashMap;

use kalamdb_commons::{Role, UserId};
use serde::{Deserialize, Serialize};

use super::ProtocolOptions;
use crate::{
    models::{KalamCellValue, SchemaField},
    subscription::models::{BatchControl, ChangeTypeRaw},
};

/// WebSocket message types sent from server to client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Authentication successful response (browser clients only)
    AuthSuccess {
        /// Authenticated canonical user identifier
        user: UserId,
        /// User role
        role: Role,
        /// Negotiated protocol echoed back from the server.
        protocol: ProtocolOptions,
    },

    /// Authentication failed response (browser clients only)
    AuthError {
        /// Error message
        message: String,
    },

    /// Acknowledgement of successful subscription registration
    SubscriptionAck {
        /// The subscription ID that was registered
        subscription_id: String,
        /// Total number of rows available for initial load
        total_rows: u32,
        /// Batch control information
        batch_control: BatchControl,
        /// Schema describing the columns in the subscription result
        schema: Vec<SchemaField>,
    },

    /// Initial data batch sent after subscription or on client request
    InitialDataBatch {
        /// The subscription ID this data is for
        subscription_id: String,
        /// The rows in this batch
        rows: Vec<HashMap<String, KalamCellValue>>,
        /// Batch control information
        batch_control: BatchControl,
    },

    /// Change notification for INSERT/UPDATE/DELETE operations
    Change {
        /// The subscription ID this notification is for
        subscription_id: String,

        /// Type of change: "insert", "update", or "delete"
        change_type: ChangeTypeRaw,

        /// For INSERT: full row data. For UPDATE: only changed columns + PK/_seq.
        #[serde(skip_serializing_if = "Option::is_none")]
        rows: Option<Vec<HashMap<String, KalamCellValue>>>,

        /// Previous row values (for UPDATE: only changed columns + PK/_seq; for DELETE: full row)
        #[serde(skip_serializing_if = "Option::is_none")]
        old_values: Option<Vec<HashMap<String, KalamCellValue>>>,
    },

    /// Error notification
    Error {
        /// The subscription ID this error is for
        subscription_id: String,

        /// Error code
        code: String,

        /// Error message
        message: String,
    },
}
