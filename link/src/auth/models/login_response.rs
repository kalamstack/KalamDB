use serde::{Deserialize, Serialize};

use super::login_user_info::LoginUserInfo;

/// Login response from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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
