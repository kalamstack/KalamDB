//! User info model

use kalamdb_commons::{models::UserId, Role};
use serde::Serialize;

/// User info returned in authentication responses
#[derive(Debug, Serialize)]
pub struct UserInfo {
    /// Unique user identifier
    pub id: UserId,
    /// User role (user, service, dba, system)
    pub role: Role,
    /// Email address (optional)
    pub email: Option<String>,
    /// Creation timestamp in RFC3339 format
    pub created_at: String,
    /// Last update timestamp in RFC3339 format
    pub updated_at: String,
}

/// Current-user response body.
#[derive(Debug, Serialize)]
pub struct CurrentUserResponse {
    /// User information
    pub user: UserInfo,
    /// Whether this authenticated account is allowed to enter the Admin UI.
    pub admin_ui_access: bool,
}
