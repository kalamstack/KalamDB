//! Server setup request model

use super::login_request::validate_password_length;
use kalamdb_commons::models::UserName;
use serde::Deserialize;

/// Server setup request body
#[derive(Debug, Deserialize)]
pub struct ServerSetupRequest {
    /// Username for the new DBA user
    pub username: UserName,
    /// Password for the new DBA user
    #[serde(deserialize_with = "validate_password_length")]
    pub password: String,
    /// Password for the root user
    #[serde(deserialize_with = "validate_password_length")]
    pub root_password: String,
    /// Email for the new DBA user (optional)
    pub email: Option<String>,
}
