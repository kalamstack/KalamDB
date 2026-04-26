// Password hashing and validation module

use std::{collections::HashSet, sync::OnceLock};

use bcrypt::{hash, verify, DEFAULT_COST};

use crate::errors::error::{AuthError, AuthResult};

/// Bcrypt cost factor for password hashing.
/// Higher values = more secure but slower.
/// Default: 12 (recommended for 2024)
/// Tests: 4 (much faster, sufficient for testing)
pub const BCRYPT_COST: u32 = DEFAULT_COST;

/// Test-only bcrypt cost factor (much faster)
/// Use this in test environments to speed up tests
#[cfg(test)]
pub const TEST_BCRYPT_COST: u32 = 4;

/// Get bcrypt cost - uses lower cost in test mode
pub fn get_bcrypt_cost() -> u32 {
    if cfg!(test) {
        4 // Fast cost for tests
    } else {
        BCRYPT_COST // Secure cost for production
    }
}

/// Minimum password length (default policy)
pub const MIN_PASSWORD_LENGTH: usize = 8;

/// Maximum password length (bcrypt has a 72-byte limit)
/// Note: Passwords longer than 72 bytes are truncated by bcrypt
/// Spec requests 1024, but bcrypt's cryptographic limit is 72
pub const MAX_PASSWORD_LENGTH: usize = 72;

/// Common passwords list (loaded once)
static COMMON_PASSWORDS: OnceLock<HashSet<String>> = OnceLock::new();

/// Hash a password using bcrypt.
///
/// Uses a configurable cost factor (default 12) and runs on a blocking thread pool
/// to avoid blocking the async runtime.
///
/// # Arguments
/// * `password` - Plain text password to hash
/// * `cost` - Optional bcrypt cost (defaults to BCRYPT_COST)
///
/// # Returns
/// Bcrypt hash string (includes salt)
///
/// # Errors
/// Returns `AuthError::HashingError` if bcrypt fails
pub async fn hash_password(password: &str, cost: Option<u32>) -> AuthResult<String> {
    let password = password.to_string();
    let cost = cost.unwrap_or_else(get_bcrypt_cost);

    // Run bcrypt on blocking thread pool (CPU-intensive)
    tokio::task::spawn_blocking(move || {
        hash(password, cost).map_err(|e| AuthError::HashingError(e.to_string()))
    })
    .await
    .map_err(|e| AuthError::HashingError(format!("Task join error: {}", e)))?
}

/// Verify a password against a bcrypt hash.
///
/// Runs on a blocking thread pool to avoid blocking the async runtime.
///
/// # Arguments
/// * `password` - Plain text password to verify
/// * `hash` - Bcrypt hash to check against
///
/// # Returns
/// `Ok(true)` if password matches, `Ok(false)` if not, `Err` on failure
///
/// # Errors
/// Returns `AuthError::HashingError` if bcrypt verification fails
pub async fn verify_password(password: &str, hash_str: &str) -> AuthResult<bool> {
    // Convert once for bcrypt (requires owned String)
    let password = password.to_string();
    let hash = hash_str.to_string();

    // Run bcrypt on blocking thread pool (CPU-intensive)
    let result = tokio::task::spawn_blocking(move || {
        verify(password, &hash).map_err(|e| AuthError::HashingError(e.to_string()))
    })
    .await
    .map_err(|e| AuthError::HashingError(format!("Task join error: {}", e)))??;

    Ok(result)
}

/// Password validation policy configuration.
#[derive(Debug, Clone)]
pub struct PasswordPolicy {
    pub min_length: usize,
    pub max_length: usize,
    pub enforce_complexity: bool,
    pub skip_common_check: bool,
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            min_length: MIN_PASSWORD_LENGTH,
            max_length: MAX_PASSWORD_LENGTH,
            enforce_complexity: false,
            skip_common_check: false,
        }
    }
}

impl PasswordPolicy {
    pub fn with_enforced_complexity(mut self, enforce: bool) -> Self {
        self.enforce_complexity = enforce;
        self
    }

    pub fn skip_common_password_check(mut self, skip: bool) -> Self {
        self.skip_common_check = skip;
        self
    }
}

/// Validate password meets security requirements.
pub fn validate_password_with_config(password: &str, skip_common_check: bool) -> AuthResult<()> {
    let policy = PasswordPolicy::default().skip_common_password_check(skip_common_check);
    validate_password_with_policy(password, &policy)
}

/// Validate password meets security requirements (with common password check enabled).
///
/// Convenience wrapper for `validate_password_with_config(password, false)`.
///
/// # Arguments
/// * `password` - Password to validate
///
/// # Returns
/// `Ok(())` if valid, `Err` with reason if invalid
///
/// # Errors
/// Returns `AuthError::WeakPassword` with specific reason
pub fn validate_password(password: &str) -> AuthResult<()> {
    validate_password_with_policy(password, &PasswordPolicy::default())
}

/// Validate password using custom policy.
pub fn validate_password_with_policy(password: &str, policy: &PasswordPolicy) -> AuthResult<()> {
    if password.len() < policy.min_length {
        return Err(AuthError::WeakPassword(format!(
            "Password must be at least {} characters (weak)",
            policy.min_length
        )));
    }

    if password.len() > policy.max_length {
        return Err(AuthError::WeakPassword(format!(
            "Password must be at most {} characters (bcrypt limit, weak)",
            policy.max_length
        )));
    }

    if !policy.skip_common_check && is_common_password(password) {
        return Err(AuthError::WeakPassword("Password is too common".to_string()));
    }

    if policy.enforce_complexity {
        let has_upper = password.chars().any(|c| c.is_ascii_uppercase());
        let has_lower = password.chars().any(|c| c.is_ascii_lowercase());
        let has_digit = password.chars().any(|c| c.is_ascii_digit());
        let has_special = password.chars().any(|c| !c.is_ascii_alphanumeric());

        if !has_upper {
            return Err(AuthError::WeakPassword(
                "Password must include at least one uppercase letter".to_string(),
            ));
        }
        if !has_lower {
            return Err(AuthError::WeakPassword(
                "Password must include at least one lowercase letter".to_string(),
            ));
        }
        if !has_digit {
            return Err(AuthError::WeakPassword(
                "Password must include at least one digit".to_string(),
            ));
        }
        if !has_special {
            return Err(AuthError::WeakPassword(
                "Password must include at least one special character".to_string(),
            ));
        }
    }

    Ok(())
}

/// Check if a password is in the common passwords list.
///
/// Loads the common passwords list on first call (lazy initialization).
///
/// # Arguments
/// * `password` - Password to check
///
/// # Returns
/// True if password is common, false otherwise
fn is_common_password(password: &str) -> bool {
    let common_passwords = COMMON_PASSWORDS.get_or_init(|| {
        // TODO: Load from file backend/crates/kalamdb-auth/data/common-passwords.txt
        // For now, use a small hardcoded list
        let passwords = vec![
            "password", "123456", "12345678", "qwerty", "abc123", "monkey", "1234567", "letmein",
            "trustno1", "dragon", "baseball", "iloveyou", "master", "sunshine", "ashley", "bailey",
            "passw0rd", "shadow", "123123", "654321", "superman", "qazwsx", "michael", "football",
        ];
        passwords.iter().map(|s| s.to_string()).collect()
    });

    common_passwords.contains(password)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hash_and_verify_password() {
        let password = "SecurePassword123!";
        let hash = hash_password(password, Some(4)).await.expect("Failed to hash");
        assert!(hash.starts_with("$2b$")); // Bcrypt hash format

        let verified = verify_password(password, &hash).await.expect("Failed to verify");
        assert!(verified);

        let wrong_verified =
            verify_password("WrongPassword", &hash).await.expect("Failed to verify");
        assert!(!wrong_verified);
    }

    #[test]
    fn test_validate_password_too_short() {
        let result = validate_password("short");
        assert!(matches!(result, Err(AuthError::WeakPassword(_))));
    }

    #[test]
    fn test_validate_password_common() {
        let result = validate_password("password");
        assert!(matches!(result, Err(AuthError::WeakPassword(_))));
    }

    #[test]
    fn test_validate_password_valid() {
        let result = validate_password("MySecurePassword123!");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_password_skip_common_check() {
        // Common password should fail with check enabled
        let result = validate_password("password");
        assert!(matches!(result, Err(AuthError::WeakPassword(_))));

        // Same password should pass with check disabled
        let result = validate_password_with_config("password", true);
        assert!(result.is_ok(), "Password should be accepted when common check disabled");
    }

    #[test]
    fn test_is_common_password() {
        assert!(is_common_password("password"));
        assert!(is_common_password("123456"));
        assert!(!is_common_password("MyUniquePassword2024!"));
    }

    #[test]
    fn test_password_complexity_policy() {
        let policy = PasswordPolicy::default().with_enforced_complexity(true);

        assert!(validate_password_with_policy("ValidPass1!", &policy).is_ok());
        assert!(validate_password_with_policy("noupper1!", &policy).is_err());
        assert!(validate_password_with_policy("NOLOWER1!", &policy).is_err());
        assert!(validate_password_with_policy("NoDigits!", &policy).is_err());
        assert!(validate_password_with_policy("NoSpecial1", &policy).is_err());
    }
}
