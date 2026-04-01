use crate::models::context::AuthenticatedUser;
pub use kalamdb_session::AuthMethod;

/// Authentication request that can come from HTTP or WebSocket.
#[derive(Debug, Clone)]
pub enum AuthRequest {
    /// HTTP Authorization header (Basic or Bearer)
    Header(String),

    /// Direct username/password (login flow)
    Credentials { username: String, password: String },

    /// Direct JWT token (WebSocket authenticate message)
    Jwt { token: String },
}

#[cfg(feature = "websocket")]
impl From<kalamdb_commons::websocket_auth::WsAuthCredentials> for AuthRequest {
    fn from(creds: kalamdb_commons::websocket_auth::WsAuthCredentials) -> Self {
        use kalamdb_commons::websocket_auth::WsAuthCredentials;
        match creds {
            WsAuthCredentials::Jwt { token } => AuthRequest::Jwt { token },
        }
    }
}

/// Result of authentication including method used.
#[derive(Debug, Clone)]
pub struct AuthenticationResult {
    /// The authenticated user
    pub user: AuthenticatedUser,
    /// The method used for authentication
    pub method: AuthMethod,
}
