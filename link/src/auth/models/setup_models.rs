//! Server setup request and response models.

use serde::{Deserialize, Serialize};

/// Server setup request body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSetupRequest {
    /// Username for the new DBA user
    pub username: String,
    /// Password for the new DBA user  
    pub password: String,
    /// Password for the root user
    pub root_password: String,
    /// Email for the new DBA user (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

impl ServerSetupRequest {
    /// Create a new server setup request
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
        root_password: impl Into<String>,
        email: Option<String>,
    ) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            root_password: root_password.into(),
            email,
        }
    }
}

/// Server setup response body
///
/// After successful setup, the user must login separately to obtain tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSetupResponse {
    /// The created DBA user info
    pub user: SetupUserInfo,
    /// Setup completion message
    pub message: String,
}

/// User info returned in setup response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupUserInfo {
    pub id: String,
    pub username: String,
    pub role: String,
    pub email: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Server setup status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupStatusResponse {
    /// Whether the server needs initial setup
    pub needs_setup: bool,
    /// Status message
    pub message: String,
}
