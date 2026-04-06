use serde::{Deserialize, Serialize};

/// Login request body for authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    /// Username for authentication
    pub username: String,
    /// Password for authentication
    pub password: String,
}

/// User information returned in login response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginUserInfo {
    /// User ID
    pub id: String,
    /// Username
    pub username: String,
    /// User role (user, service, dba, system)
    pub role: String,
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
