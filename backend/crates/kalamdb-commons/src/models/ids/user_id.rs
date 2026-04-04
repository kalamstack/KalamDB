//! Type-safe wrapper for user identifiers.

use std::fmt;
use std::sync::{Arc, OnceLock};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{constants::AuthConstants, StorageKey};

/// Type-safe wrapper for user identifiers.
///
/// Ensures user IDs cannot be accidentally used where namespace IDs or table names
/// are expected.
/// Statics for cheap singleton construction.
static ANON_USER_ID: OnceLock<Arc<str>> = OnceLock::new();
static ROOT_USER_ID: OnceLock<Arc<str>> = OnceLock::new();
static SYSTEM_USER_ID_STATIC: OnceLock<Arc<str>> = OnceLock::new();

/// Type-safe wrapper for user identifiers.
///
/// Stored as `Arc<str>` so `clone()` is a cheap atomic refcount increment
/// rather than a heap allocation — critical for high-concurrency hot paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct UserId(Arc<str>);

/// Error type for UserId validation failures
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserIdValidationError(pub String);

impl std::fmt::Display for UserIdValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UserIdValidationError {}

impl UserId {
    /// Creates a new UserId from a string.
    ///
    /// # Panics
    /// Panics if the ID contains path traversal characters. Use `try_new()` for fallible creation.
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self::try_new(id).expect("UserId contains invalid characters")
    }

    /// New anonymous UserId — cached singleton, clone is a free atomic increment.
    #[inline]
    pub fn anonymous() -> Self {
        Self(ANON_USER_ID.get_or_init(|| Arc::from(AuthConstants::ANONYMOUS_USER_ID)).clone())
    }

    /// Creates a new UserId from a string, returning an error if validation fails.
    ///
    /// # Security
    /// Validates that the ID does not contain path traversal characters:
    /// - `..` (parent directory)
    /// - `/` or `\` (directory separators)
    /// - Null bytes (`\0`)
    ///
    /// This prevents path traversal attacks when user IDs are used in storage paths.
    pub fn try_new(id: impl Into<String>) -> Result<Self, UserIdValidationError> {
        let id = id.into();
        Self::validate_id(&id)?;
        Ok(Self(Arc::<str>::from(id)))
    }

    /// Validates a user ID string for security.
    fn validate_id(id: &str) -> Result<(), UserIdValidationError> {
        // Check for path traversal patterns
        if id.contains("..") {
            return Err(UserIdValidationError(
                "User ID cannot contain '..' (path traversal)".to_string(),
            ));
        }
        if id.contains('/') {
            return Err(UserIdValidationError(
                "User ID cannot contain '/' (directory separator)".to_string(),
            ));
        }
        if id.contains('\\') {
            return Err(UserIdValidationError(
                "User ID cannot contain '\\' (directory separator)".to_string(),
            ));
        }
        if id.contains('\0') {
            return Err(UserIdValidationError("User ID cannot contain null bytes".to_string()));
        }
        // Check for empty ID
        if id.is_empty() {
            return Err(UserIdValidationError("User ID cannot be empty".to_string()));
        }
        Ok(())
    }

    /// Generates a new unique UserId using NanoID (21 URL-safe characters).
    ///
    /// Uses the default NanoID alphabet (`A-Za-z0-9_-`) which is safe for
    /// storage paths, URLs, and database keys.
    #[inline]
    #[cfg(feature = "full")]
    pub fn generate() -> Self {
        Self(Arc::<str>::from(nanoid::nanoid!()))
    }

    /// Creates a UserId without validation (for internal use only).
    ///
    /// # Safety
    /// This bypasses security validation. Only use for IDs that are known to be safe
    /// (e.g., loaded from database, generated internally).
    #[inline]
    #[allow(dead_code)] // Reserved for internal use when loading from trusted sources
    pub(crate) fn new_unchecked(id: impl Into<String>) -> Self {
        let s: String = id.into();
        Self(Arc::<str>::from(s))
    }

    /// Returns the user ID as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner String.
    #[inline]
    pub fn into_string(self) -> String {
        String::from(&*self.0)
    }

    /// Creates a default 'root' user ID — cached singleton.
    #[inline]
    pub fn root() -> Self {
        Self(
            ROOT_USER_ID
                .get_or_init(|| Arc::from(AuthConstants::DEFAULT_ROOT_USER_ID))
                .clone(),
        )
    }

    /// Creates a default 'system' user ID — cached singleton.
    #[inline]
    pub fn system() -> Self {
        Self(
            SYSTEM_USER_ID_STATIC
                .get_or_init(|| Arc::from(AuthConstants::DEFAULT_SYSTEM_USER_ID))
                .clone(),
        )
    }

    /// Is admin user?
    #[inline]
    pub fn is_admin(&self) -> bool {
        self.as_str() == AuthConstants::DEFAULT_ROOT_USER_ID
    }

    /// Is anonymous user?
    #[inline]
    pub fn is_anonymous(&self) -> bool {
        self.as_str() == AuthConstants::ANONYMOUS_USER_ID
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for UserId {
    /// Converts a String into UserId.
    ///
    /// # Panics
    /// Panics if the string contains path traversal characters.
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for UserId {
    /// Converts a &str into UserId.
    ///
    /// # Panics
    /// Panics if the string contains path traversal characters.
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

impl AsRef<str> for UserId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for UserId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl StorageKey for UserId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        String::from_utf8(bytes.to_vec())
            .map(|s| UserId(Arc::<str>::from(s)))
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_user_id() {
        let user = UserId::try_new("alice123");
        assert!(user.is_ok());
        assert_eq!(user.unwrap().as_str(), "alice123");
    }

    #[test]
    fn test_user_id_with_underscores_and_dashes() {
        let user = UserId::try_new("user_123-test");
        assert!(user.is_ok());
    }

    #[test]
    fn test_path_traversal_double_dot_blocked() {
        let user = UserId::try_new("../../../etc/passwd");
        assert!(user.is_err());
        assert!(user.unwrap_err().0.contains("path traversal"));
    }

    #[test]
    fn test_path_traversal_forward_slash_blocked() {
        let user = UserId::try_new("user/subdir");
        assert!(user.is_err());
        assert!(user.unwrap_err().0.contains("directory separator"));
    }

    #[test]
    fn test_path_traversal_backslash_blocked() {
        let user = UserId::try_new("user\\subdir");
        assert!(user.is_err());
        assert!(user.unwrap_err().0.contains("directory separator"));
    }

    #[test]
    fn test_null_byte_blocked() {
        let user = UserId::try_new("user\0hidden");
        assert!(user.is_err());
        assert!(user.unwrap_err().0.contains("null bytes"));
    }

    #[test]
    fn test_empty_user_id_blocked() {
        let user = UserId::try_new("");
        assert!(user.is_err());
        assert!(user.unwrap_err().0.contains("empty"));
    }

    #[test]
    #[should_panic(expected = "invalid characters")]
    fn test_new_panics_on_invalid() {
        let _ = UserId::new("../evil");
    }

    #[test]
    fn test_from_string_panics_on_invalid() {
        // This should panic on path traversal
        let result = std::panic::catch_unwind(|| {
            let _: UserId = "../etc/passwd".into();
        });
        assert!(result.is_err());
    }
}

// KSerializable implementation for EntityStore support
#[cfg(feature = "serialization")]
impl crate::serialization::KSerializable for UserId {}
