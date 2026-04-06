use serde::{Deserialize, Serialize};

use crate::auth::models::WsAuthCredentials;
use crate::seq_id::SeqId;
use crate::subscription::models::SubscriptionRequest;

use super::ProtocolOptions;

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
    /// Supports multiple authentication methods via the credentials field.
    /// Optionally negotiates wire format via `protocol`.
    Authenticate {
        /// Authentication credentials (basic, jwt, or future methods)
        #[serde(flatten)]
        credentials: WsAuthCredentials,
        /// Protocol negotiation (serialization + compression).
        protocol: ProtocolOptions,
    },

    /// Subscribe to live query updates
    Subscribe {
        /// Subscription to register
        subscription: SubscriptionRequest,
    },

    /// Request next batch of initial data
    NextBatch {
        /// The subscription ID to fetch the next batch for
        subscription_id: String,
        /// The SeqId of the last row received (used for pagination)
        #[serde(skip_serializing_if = "Option::is_none")]
        last_seq_id: Option<SeqId>,
    },

    /// Unsubscribe from live query
    Unsubscribe {
        /// The subscription ID to unsubscribe from
        subscription_id: String,
    },

    /// Application-level keepalive ping.
    ///
    /// Browser WebSocket APIs do not expose protocol-level Ping frames, so
    /// the WASM client sends this periodically to prevent the server-side
    /// heartbeat timeout from firing on idle connections.
    Ping,
}
