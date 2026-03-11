use serde::{Deserialize, Serialize};

/// Authentication credentials for WebSocket connection.
///
/// This enum is the client-facing authentication model for WebSocket
/// connections. It maps 1:1 with `AuthRequest` variants in `kalamdb-auth`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum WsAuthCredentials {
    /// JWT token authentication.
    Jwt { token: String },
}