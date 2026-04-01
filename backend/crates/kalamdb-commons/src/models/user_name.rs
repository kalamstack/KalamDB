// File: backend/crates/kalamdb-commons/src/models/user_name.rs
// Type-safe wrapper for usernames (secondary index key)

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::models::oauth_provider::OAuthProvider;
use crate::StorageKey;

/// Static for cheap root username singleton.
static ROOT_USERNAME: OnceLock<Arc<str>> = OnceLock::new();

/// Type-safe wrapper for usernames used as secondary index keys.
///
/// Stored as `Arc<str>` so `clone()` is a cheap atomic refcount increment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserName(Arc<str>);

/// Error type for UserName validation failures
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserNameValidationError(pub String);

impl std::fmt::Display for UserNameValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UserNameValidationError {}

impl UserName {
    /// Validates a username for security issues.
    fn validate(name: &str) -> Result<(), UserNameValidationError> {
        // Check for empty name
        if name.is_empty() {
            return Err(UserNameValidationError("Username cannot be empty".to_string()));
        }

        // Check for SQL injection characters
        if name.contains('\'') || name.contains('"') || name.contains(';') {
            return Err(UserNameValidationError(
                "Username cannot contain quotes or semicolons".to_string(),
            ));
        }

        // Check for path traversal
        if name.contains("..") || name.contains('/') || name.contains('\\') {
            return Err(UserNameValidationError(
                "Username cannot contain path traversal characters".to_string(),
            ));
        }

        // Check for null bytes
        if name.contains('\0') {
            return Err(UserNameValidationError("Username cannot contain null bytes".to_string()));
        }

        Ok(())
    }

    /// Creates a new UserName from a string with validation.
    ///
    /// # Panics
    /// Panics if the name contains SQL injection or path traversal characters.
    pub fn new(name: impl Into<String>) -> Self {
        Self::try_new(name).expect("UserName contains invalid characters")
    }

    /// Creates a new UserName from a string, returning an error if validation fails.
    pub fn try_new(name: impl Into<String>) -> Result<Self, UserNameValidationError> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(Self(Arc::<str>::from(name)))
    }

    /// Returns the username as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner String.
    pub fn into_string(self) -> String {
        String::from(&*self.0)
    }

    /// Get the username as bytes for storage
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Convert to lowercase for case-insensitive comparisons
    pub fn to_lowercase(&self) -> UserName {
        UserName(Arc::<str>::from(self.as_str().to_lowercase()))
    }

    /// Root username helper — cached singleton.
    pub fn root() -> UserName {
        UserName(ROOT_USERNAME.get_or_init(|| Arc::from("root")).clone())
    }

    // ----- Provider username helpers -----

    /// The prefix that marks a username as provider-originated.
    const OIDC_PREFIX: &'static str = "oidc";

    /// Construct a provider username: `oidc:{provider_prefix}:{subject}`.
    ///
    /// This is the canonical way to create a username for an externally
    /// authenticated user.  The 3-char provider prefix comes from
    /// [`OAuthProvider::prefix()`].
    pub fn from_provider(provider: &OAuthProvider, subject: &str) -> Self {
        let prefix = provider.prefix();
        Self::new(format!("{}:{}:{}", Self::OIDC_PREFIX, prefix, subject))
    }

    /// Returns `true` if this username was created by [`from_provider`](Self::from_provider).
    pub fn is_provider_user(&self) -> bool {
        self.0.starts_with("oidc:")
    }

    /// Extract the [`OAuthProvider`] from a provider username.
    ///
    /// Returns `None` for non-provider usernames (those not starting with `oidc:`).
    /// For well-known providers the exact variant is returned; for custom
    /// providers [`OAuthProvider::Custom`] carries the 3-char prefix (the
    /// original issuer URL is not recoverable from the prefix alone — use
    /// [`AuthData`](kalamdb_system::AuthData) for that).
    pub fn get_provider(&self) -> Option<OAuthProvider> {
        if !self.is_provider_user() {
            return None;
        }
        // Format: oidc:{prefix}:{subject}
        let rest = &self.0[5..]; // skip "oidc:"
        let sep = rest.find(':')?;
        let prefix = &rest[..sep];
        Some(OAuthProvider::from_prefix(prefix))
    }

    /// Extract the provider subject (external user ID) from a provider username.
    ///
    /// Returns `None` for non-provider usernames.
    pub fn get_subject(&self) -> Option<&str> {
        if !self.is_provider_user() {
            return None;
        }
        let rest = &self.0[5..]; // skip "oidc:"
        let sep = rest.find(':')?;
        Some(&rest[sep + 1..])
    }
}

impl fmt::Display for UserName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for UserName {
    fn from(s: String) -> Self {
        Self(Arc::<str>::from(s))
    }
}

impl From<&str> for UserName {
    fn from(s: &str) -> Self {
        Self(Arc::<str>::from(s))
    }
}

impl AsRef<str> for UserName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for UserName {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl StorageKey for UserName {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        String::from_utf8(bytes.to_vec())
            .map(|s| UserName(Arc::<str>::from(s)))
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_name_new() {
        let name = UserName::new("john_doe");
        assert_eq!(name.as_str(), "john_doe");
    }

    #[test]
    fn test_user_name_from_string() {
        let name = UserName::from("jane_smith".to_string());
        assert_eq!(name.as_str(), "jane_smith");
    }

    #[test]
    fn test_user_name_from_str() {
        let name = UserName::from("alice");
        assert_eq!(name.as_str(), "alice");
    }

    #[test]
    fn test_user_name_as_ref_str() {
        let name = UserName::new("bob");
        let s: &str = name.as_ref();
        assert_eq!(s, "bob");
    }

    #[test]
    fn test_user_name_as_ref_bytes() {
        let name = UserName::new("charlie");
        let bytes: &[u8] = name.as_ref();
        assert_eq!(bytes, b"charlie");
    }

    #[test]
    fn test_user_name_as_bytes() {
        let name = UserName::new("david");
        assert_eq!(name.as_bytes(), b"david");
    }

    #[test]
    fn test_user_name_display() {
        let name = UserName::new("eve");
        assert_eq!(format!("{}", name), "eve");
    }

    #[test]
    fn test_user_name_into_string() {
        let name = UserName::new("frank");
        let s = name.into_string();
        assert_eq!(s, "frank");
    }

    #[test]
    fn test_user_name_clone() {
        let name1 = UserName::new("grace");
        let name2 = name1.clone();
        assert_eq!(name1, name2);
    }

    #[test]
    fn test_user_name_serialization() {
        let name = UserName::new("heidi");
        let json = serde_json::to_string(&name).unwrap();
        let deserialized: UserName = serde_json::from_str(&json).unwrap();
        assert_eq!(name, deserialized);
    }

    #[test]
    fn test_user_name_to_lowercase() {
        let name = UserName::new("IvAN");
        let lower = name.to_lowercase();
        assert_eq!(lower.as_str(), "ivan");
    }

    #[test]
    fn test_user_name_case_sensitivity() {
        let name1 = UserName::new("Alice");
        let name2 = UserName::new("alice");
        assert_ne!(name1, name2); // UserName is case-sensitive by default
        assert_eq!(name1.to_lowercase(), name2.to_lowercase()); // But can be compared case-insensitively
    }

    // -- Provider username tests -------------------------------------------

    #[test]
    fn test_from_provider_google() {
        let name = UserName::from_provider(&OAuthProvider::Google, "google_sub_123");
        assert_eq!(name.as_str(), "oidc:ggl:google_sub_123");
    }

    #[test]
    fn test_from_provider_keycloak() {
        let name = UserName::from_provider(&OAuthProvider::Keycloak, "kc-uuid-456");
        assert_eq!(name.as_str(), "oidc:kcl:kc-uuid-456");
    }

    #[test]
    fn test_from_provider_github() {
        let name = UserName::from_provider(&OAuthProvider::GitHub, "12345");
        assert_eq!(name.as_str(), "oidc:ghb:12345");
    }

    #[test]
    fn test_is_provider_user() {
        let provider_user = UserName::from_provider(&OAuthProvider::Google, "sub1");
        assert!(provider_user.is_provider_user());

        let regular_user = UserName::new("admin");
        assert!(!regular_user.is_provider_user());

        let root = UserName::root();
        assert!(!root.is_provider_user());
    }

    #[test]
    fn test_get_provider() {
        let name = UserName::from_provider(&OAuthProvider::GitHub, "sub123");
        assert_eq!(name.get_provider(), Some(OAuthProvider::GitHub));

        let name = UserName::from_provider(&OAuthProvider::AzureAd, "az-user");
        assert_eq!(name.get_provider(), Some(OAuthProvider::AzureAd));

        let regular = UserName::new("alice");
        assert_eq!(regular.get_provider(), None);
    }

    #[test]
    fn test_get_subject() {
        let name = UserName::from_provider(&OAuthProvider::Google, "google_sub_123");
        assert_eq!(name.get_subject(), Some("google_sub_123"));

        let regular = UserName::new("bob");
        assert_eq!(regular.get_subject(), None);
    }

    #[test]
    fn test_provider_roundtrip() {
        let provider = OAuthProvider::Okta;
        let subject = "okta-user-uuid";
        let name = UserName::from_provider(&provider, subject);

        assert_eq!(name.get_provider(), Some(provider));
        assert_eq!(name.get_subject(), Some(subject));
    }

    #[test]
    fn test_custom_provider_username() {
        let provider = OAuthProvider::Custom("https://my-idp.corp".to_string());
        let name = UserName::from_provider(&provider, "custom-sub");
        assert!(name.is_provider_user());
        assert_eq!(name.get_subject(), Some("custom-sub"));
        // Custom round-trip: get_provider returns Custom with the hash prefix, not the original URL
        let got = name.get_provider().unwrap();
        assert!(matches!(got, OAuthProvider::Custom(_)));
    }
}
