use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::batch::BatchControl;
use crate::connection::models::ServerMessage;
use crate::models::KalamCellValue;
use crate::models::SchemaField;

/// Type of change that occurred in the database
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeTypeRaw {
    /// New row(s) inserted
    Insert,

    /// Existing row(s) updated
    Update,

    /// Row(s) deleted
    Delete,
}

/// Change event received via WebSocket subscription.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// Acknowledgement of subscription registration with batch info
    Ack {
        /// Subscription ID
        subscription_id: String,
        /// Total rows available for initial load
        total_rows: u32,
        /// Batch control information
        batch_control: BatchControl,
        /// Schema describing the columns in the subscription result
        schema: Vec<SchemaField>,
    },

    /// Initial data batch (paginated loading)
    InitialDataBatch {
        /// Subscription ID the batch belongs to
        subscription_id: String,
        /// Rows in this batch (named columns)
        rows: Vec<HashMap<String, KalamCellValue>>,
        /// Batch control information
        batch_control: BatchControl,
    },

    /// Insert notification
    Insert {
        /// Subscription ID the change belongs to
        subscription_id: String,
        /// Inserted rows (named columns)
        rows: Vec<HashMap<String, KalamCellValue>>,
    },

    /// Update notification
    Update {
        /// Subscription ID the change belongs to
        subscription_id: String,
        /// Updated rows (only changed columns + PK/_seq).
        /// The changed user columns are exactly the non-system keys in each row:
        /// `row.keys().filter(|k| !k.starts_with('_'))`
        rows: Vec<HashMap<String, KalamCellValue>>,
        /// Previous row values (only changed columns + PK/_seq)
        old_rows: Vec<HashMap<String, KalamCellValue>>,
    },

    /// Delete notification
    Delete {
        /// Subscription ID the change belongs to
        subscription_id: String,
        /// Deleted rows (named columns)
        old_rows: Vec<HashMap<String, KalamCellValue>>,
    },

    /// Error notification from the server
    Error {
        /// Subscription ID related to the error
        subscription_id: String,
        /// Error code
        code: String,
        /// Human-readable error message
        message: String,
    },

    /// Unknown payload (kept for logging/diagnostics)
    Unknown {
        /// Raw JSON payload
        raw: JsonValue,
    },
}

impl ChangeEvent {
    /// Returns true if this is an error event
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Returns the subscription ID for this event, if any
    pub fn subscription_id(&self) -> Option<&str> {
        match self {
            Self::Ack {
                subscription_id, ..
            }
            | Self::InitialDataBatch {
                subscription_id, ..
            }
            | Self::Insert {
                subscription_id, ..
            }
            | Self::Update {
                subscription_id, ..
            }
            | Self::Delete {
                subscription_id, ..
            }
            | Self::Error {
                subscription_id, ..
            } => Some(subscription_id.as_str()),
            Self::Unknown { .. } => None,
        }
    }

    /// Convert a [`ServerMessage`] into a `ChangeEvent`.
    ///
    /// Returns `None` for auth-only messages (`AuthSuccess`, `AuthError`) that
    /// are not subscription events.
    pub fn from_server_message(msg: ServerMessage) -> Option<Self> {
        match msg {
            ServerMessage::AuthSuccess { .. } | ServerMessage::AuthError { .. } => None,
            ServerMessage::SubscriptionAck {
                subscription_id,
                total_rows,
                batch_control,
                schema,
            } => Some(Self::Ack {
                subscription_id,
                total_rows,
                batch_control,
                schema,
            }),
            ServerMessage::InitialDataBatch {
                subscription_id,
                rows,
                batch_control,
            } => Some(Self::InitialDataBatch {
                subscription_id,
                rows,
                batch_control,
            }),
            ServerMessage::Change {
                subscription_id,
                change_type,
                rows,
                old_values,
            } => Some(match change_type {
                ChangeTypeRaw::Insert => Self::Insert {
                    subscription_id,
                    rows: rows.unwrap_or_default(),
                },
                ChangeTypeRaw::Update => Self::Update {
                    subscription_id,
                    rows: rows.unwrap_or_default(),
                    old_rows: old_values.unwrap_or_default(),
                },
                ChangeTypeRaw::Delete => Self::Delete {
                    subscription_id,
                    old_rows: old_values.unwrap_or_default(),
                },
            }),
            ServerMessage::Error {
                subscription_id,
                code,
                message,
            } => Some(Self::Error {
                subscription_id,
                code,
                message,
            }),
        }
    }

    /// Convert this event back to a [`ServerMessage`].
    pub fn to_server_message(&self) -> ServerMessage {
        match self {
            Self::Ack {
                subscription_id,
                total_rows,
                batch_control,
                schema,
            } => ServerMessage::SubscriptionAck {
                subscription_id: subscription_id.clone(),
                total_rows: *total_rows,
                batch_control: batch_control.clone(),
                schema: schema.clone(),
            },
            Self::InitialDataBatch {
                subscription_id,
                rows,
                batch_control,
            } => ServerMessage::InitialDataBatch {
                subscription_id: subscription_id.clone(),
                rows: rows.clone(),
                batch_control: batch_control.clone(),
            },
            Self::Insert {
                subscription_id,
                rows,
            } => ServerMessage::Change {
                subscription_id: subscription_id.clone(),
                change_type: ChangeTypeRaw::Insert,
                rows: Some(rows.clone()),
                old_values: None,
            },
            Self::Update {
                subscription_id,
                rows,
                old_rows,
            } => ServerMessage::Change {
                subscription_id: subscription_id.clone(),
                change_type: ChangeTypeRaw::Update,
                rows: Some(rows.clone()),
                old_values: Some(old_rows.clone()),
            },
            Self::Delete {
                subscription_id,
                old_rows,
            } => ServerMessage::Change {
                subscription_id: subscription_id.clone(),
                change_type: ChangeTypeRaw::Delete,
                rows: None,
                old_values: Some(old_rows.clone()),
            },
            Self::Error {
                subscription_id,
                code,
                message,
            } => ServerMessage::Error {
                subscription_id: subscription_id.clone(),
                code: code.clone(),
                message: message.clone(),
            },
            Self::Unknown { .. } => ServerMessage::Error {
                subscription_id: String::new(),
                code: "unknown".to_string(),
                message: "Unknown subscription event".to_string(),
            },
        }
    }
}
