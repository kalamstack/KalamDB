//! Server setup response model

use serde::Serialize;

use super::UserInfo;

/// Server setup response body
#[derive(Debug, Serialize)]
pub struct ServerSetupResponse {
    /// Created DBA user information
    pub user: UserInfo,
    /// Success message
    pub message: String,
}
