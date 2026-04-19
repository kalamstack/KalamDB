// HTTP Basic Authentication parser

use crate::errors::error::{AuthError, AuthResult};
use base64::prelude::*;

/// Parse HTTP Basic Auth header and extract credentials.
///
/// Expected format: `Authorization: Basic <base64-encoded-user:password>`
///
/// # Arguments
/// * `auth_header` - Value of the Authorization header
///
/// # Returns
/// Tuple of (user, password)
///
/// # Errors
/// - `AuthError::MalformedAuthorization` if header format is invalid
/// - `AuthError::MalformedAuthorization` if base64 decoding fails
/// - `AuthError::MalformedAuthorization` if credentials format is invalid
///
/// # Example
/// ```rust
/// use kalamdb_auth::helpers::basic_auth::parse_basic_auth_header;
///
/// let header = "Basic dXNlcjpwYXNz"; // base64("user:pass")
/// let (user, password) = parse_basic_auth_header(header).unwrap();
/// assert_eq!(user, "user");
/// assert_eq!(password, "pass");
/// ```
pub fn parse_basic_auth_header(auth_header: &str) -> AuthResult<(String, String)> {
    // Check if header starts with "Basic "
    let encoded = auth_header.strip_prefix("Basic ").ok_or_else(|| {
        AuthError::MalformedAuthorization(
            "Authorization header must start with 'Basic '".to_string(),
        )
    })?;

    // Decode base64
    let decoded_bytes = BASE64_STANDARD.decode(encoded.as_bytes()).map_err(|e| {
        AuthError::MalformedAuthorization(format!("Invalid base64 encoding: {}", e))
    })?;

    let decoded_str = String::from_utf8(decoded_bytes).map_err(|e| {
        AuthError::MalformedAuthorization(format!("Invalid UTF-8 in credentials: {}", e))
    })?;

    // Split into user:password
    extract_credentials(&decoded_str)
}

/// Extract user and password from decoded credentials string.
///
/// Expected format: `user:password`
///
/// # Arguments
/// * `credentials` - Decoded credentials string
///
/// # Returns
/// Tuple of (user, password)
///
/// # Errors
/// - `AuthError::MalformedAuthorization` if format is invalid (no colon found)
fn extract_credentials(credentials: &str) -> AuthResult<(String, String)> {
    let mut parts = credentials.splitn(2, ':');

    let user = parts.next().ok_or_else(|| {
        AuthError::MalformedAuthorization("Missing user in credentials".to_string())
    })?;

    let password = parts.next().ok_or_else(|| {
        AuthError::MalformedAuthorization(
            "Credentials must be in format 'user:password'".to_string(),
        )
    })?;

    Ok((user.to_string(), password.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_auth_valid() {
        // "user:pass" in base64 = "dXNlcjpwYXNz"
        let header = "Basic dXNlcjpwYXNz";
        let (user, password) = parse_basic_auth_header(header).unwrap();
        assert_eq!(user, "user");
        assert_eq!(password, "pass");
    }

    #[test]
    fn test_parse_basic_auth_with_colon_in_password() {
        // "admin:p@ss:word" in base64 = "YWRtaW46cEBzczp3b3Jk"
        let header = "Basic YWRtaW46cEBzczp3b3Jk";
        let (user, password) = parse_basic_auth_header(header).unwrap();
        assert_eq!(user, "admin");
        assert_eq!(password, "p@ss:word");
    }

    #[test]
    fn test_parse_basic_auth_missing_prefix() {
        let header = "dXNlcjpwYXNz"; // Missing "Basic "
        let result = parse_basic_auth_header(header);
        assert!(matches!(result, Err(AuthError::MalformedAuthorization(_))));
    }

    #[test]
    fn test_parse_basic_auth_invalid_base64() {
        let header = "Basic !!invalid!!";
        let result = parse_basic_auth_header(header);
        assert!(matches!(result, Err(AuthError::MalformedAuthorization(_))));
    }

    #[test]
    fn test_parse_basic_auth_no_colon() {
        // "userpass" (no colon) in base64 = "dXNlcnBhc3M="
        let header = "Basic dXNlcnBhc3M=";
        let result = parse_basic_auth_header(header);
        assert!(matches!(result, Err(AuthError::MalformedAuthorization(_))));
    }

    #[test]
    fn test_extract_credentials_valid() {
        let creds = "alice:SecurePassword123!";
        let (user, password) = extract_credentials(creds).unwrap();
        assert_eq!(user, "alice");
        assert_eq!(password, "SecurePassword123!");
    }

    #[test]
    fn test_extract_credentials_no_colon() {
        let creds = "alice";
        let result = extract_credentials(creds);
        assert!(matches!(result, Err(AuthError::MalformedAuthorization(_))));
    }
}
