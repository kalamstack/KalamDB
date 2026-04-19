use kalamdb_commons::{Role, UserId};
use serde::{Deserialize, Serialize};

/// Login request body for authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    /// Canonical user identifier for authentication
    pub user: UserId,
    /// Password for authentication
    pub password: String,
}

/// User information returned in login response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginUserInfo {
    /// Canonical user identifier
    pub id: UserId,
    /// User role (user, service, dba, system)
    pub role: Role,
    /// User email (optional)
    pub email: Option<String>,
    /// Account creation time in RFC3339 format
    pub created_at: String,
    /// Account update time in RFC3339 format
    pub updated_at: String,
}

/// Login response from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    /// Authenticated user information
    pub user: LoginUserInfo,
    /// Whether this account can access the Admin UI
    pub admin_ui_access: bool,
    /// Token expiration time in RFC3339 format
    pub expires_at: String,
    /// JWT access token for subsequent API calls
    pub access_token: String,
    /// Refresh token for obtaining new access tokens (longer-lived)
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Refresh token expiration time in RFC3339 format
    #[serde(default)]
    pub refresh_expires_at: Option<String>,
}
