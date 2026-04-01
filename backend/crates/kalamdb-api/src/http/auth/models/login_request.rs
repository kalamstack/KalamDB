//! Login request model

use serde::{Deserialize, Serialize};

/// Maximum username length (prevent memory exhaustion)
const MAX_USERNAME_LENGTH: usize = 128;
/// Maximum password length (bcrypt limit is 72 bytes, but allow some headroom for encoding)
const MAX_PASSWORD_LENGTH: usize = 256;

/// Login request body
#[derive(Debug, Deserialize, Serialize)]
pub struct LoginRequest {
    /// Username for authentication
    #[serde(deserialize_with = "validate_username_length")]
    pub username: String,
    /// Password for authentication
    #[serde(deserialize_with = "validate_password_length")]
    pub password: String,
}

pub(crate) fn validate_username_length<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.len() > MAX_USERNAME_LENGTH {
        return Err(serde::de::Error::custom(format!(
            "username exceeds maximum length of {} characters",
            MAX_USERNAME_LENGTH
        )));
    }
    Ok(s)
}

pub(crate) fn validate_password_length<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.len() > MAX_PASSWORD_LENGTH {
        return Err(serde::de::Error::custom(format!(
            "password exceeds maximum length of {} characters",
            MAX_PASSWORD_LENGTH
        )));
    }
    Ok(s)
}
