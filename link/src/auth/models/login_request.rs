use serde::{Deserialize, Serialize};

/// Login request body for authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    /// Username for authentication
    pub username: String,
    /// Password for authentication
    pub password: String,
}
