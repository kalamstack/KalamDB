//! Server setup response model

use super::UserInfo;
use serde::Serialize;

/// Server setup response body
#[derive(Debug, Serialize)]
pub struct ServerSetupResponse {
    /// Created DBA user information
    pub user: UserInfo,
    /// Success message
    pub message: String,
}
