use serde::{Deserialize, Serialize};

/// Health check response from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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
