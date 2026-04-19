//! Login request model

use serde::{Deserialize, Serialize};

/// Maximum user length (prevent memory exhaustion)
const MAX_USER_LENGTH: usize = 128;
/// Maximum password length (bcrypt limit is 72 bytes, but allow some headroom for encoding)
const MAX_PASSWORD_LENGTH: usize = 256;

/// Login request body
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    /// Canonical user identifier for authentication
    #[serde(deserialize_with = "validate_user_length")]
    pub user: String,
    /// Password for authentication
    #[serde(deserialize_with = "validate_password_length")]
    pub password: String,
}

pub(crate) fn validate_user_length<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.len() > MAX_USER_LENGTH {
        return Err(serde::de::Error::custom(format!(
            "user exceeds maximum length of {} characters",
            MAX_USER_LENGTH
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
