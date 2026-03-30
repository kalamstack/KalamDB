use base64::{engine::general_purpose, Engine as _};

use crate::models::{ClientMessage, ProtocolOptions, WsAuthCredentials};

/// Authentication provider for WASM clients
///
/// Supports three authentication modes:
/// - Basic: HTTP Basic Auth with username/password (HTTP only)
/// - Jwt: Bearer token authentication with JWT (required for WebSocket)
/// - None: No authentication (localhost bypass)
#[derive(Clone)]
pub(crate) enum WasmAuthProvider {
    /// HTTP Basic Authentication (username/password)
    Basic { username: String, password: String },
    /// JWT Token Authentication (Bearer token)
    Jwt { token: String },
    /// No authentication (for localhost bypass)
    None,
}

impl WasmAuthProvider {
    /// Get the Authorization header value for HTTP requests
    pub(crate) fn to_http_header(&self) -> Option<String> {
        match self {
            WasmAuthProvider::Basic { username, password } => {
                let credentials = format!("{}:{}", username, password);
                let encoded = general_purpose::STANDARD.encode(credentials.as_bytes());
                Some(format!("Basic {}", encoded))
            },
            WasmAuthProvider::Jwt { token } => Some(format!("Bearer {}", token)),
            WasmAuthProvider::None => None,
        }
    }

    /// Get the WebSocket authentication message using unified WsAuthCredentials
    pub(crate) fn to_ws_auth_message(
        &self,
        protocol: ProtocolOptions,
    ) -> Option<ClientMessage> {
        match self {
            WasmAuthProvider::Basic { .. } => None,
            WasmAuthProvider::Jwt { token } => Some(ClientMessage::Authenticate {
                credentials: WsAuthCredentials::Jwt {
                    token: token.clone(),
                },
                protocol,
            }),
            WasmAuthProvider::None => None,
        }
    }
}
