use serde::{Deserialize, Serialize};

/// User information returned in login response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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
