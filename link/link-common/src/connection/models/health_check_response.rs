use serde::{Deserialize, Serialize};

/// Health check response from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    /// Health status (e.g., "healthy")
    pub status: String,

    /// Server version
    #[serde(default)]
    pub version: String,

    /// API version (e.g., "v1")
    pub api_version: String,

    /// Server build date
    #[serde(default)]
    pub build_date: Option<String>,
}
