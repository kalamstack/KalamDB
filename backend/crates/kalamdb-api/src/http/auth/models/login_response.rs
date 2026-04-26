//! Login response model

use serde::Serialize;

use super::UserInfo;

/// Login response body
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    /// User information
    pub user: UserInfo,
    /// Whether this authenticated account is allowed to enter the Admin UI.
    pub admin_ui_access: bool,
    /// Access token expiration time in RFC3339 format
    pub expires_at: String,
    /// JWT access token for SDK usage (also set as HttpOnly cookie)
    pub access_token: String,
    /// Refresh token for obtaining new access tokens (longer-lived)
    pub refresh_token: String,
    /// Refresh token expiration time in RFC3339 format
    pub refresh_expires_at: String,
}
