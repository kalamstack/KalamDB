//! Credential storage abstraction for KalamDB clients.
//!
//! Provides a trait-based system for storing and retrieving authentication
//! credentials across different storage backends (files, environment variables,
//! secure keychains, browser localStorage, etc.).
//!
//! This abstraction allows CLI tools, WASM clients, and other applications
//! to manage credentials in a platform-appropriate way.
//!
//! # Security Model
//!
//! Credentials stores **JWT tokens only**, never user/password pairs.
//! This provides better security because:
//! - JWT tokens can expire and be revoked
//! - No plaintext passwords stored on disk
//! - Tokens can have limited scopes

use kalamdb_commons::UserId;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Stored credentials for a KalamDB instance.
///
/// Contains a JWT token that can be persisted and reused across sessions.
/// The token is obtained by authenticating with user/password via the
/// `/v1/api/auth/login` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credentials {
    /// Database instance identifier (e.g., "local", "production", URL)
    pub instance: String,

    /// JWT access token for authentication
    /// Obtained from the login endpoint, expires after a configured period
    pub jwt_token: String,

    /// User associated with this token (for display purposes)
    #[serde(default)]
    pub user: Option<UserId>,

    /// Token expiration time in RFC3339 format (optional, for cache invalidation)
    #[serde(default)]
    pub expires_at: Option<String>,

    /// Optional: Server URL if different from instance name
    #[serde(default)]
    pub server_url: Option<String>,

    /// Refresh token for obtaining new access tokens (longer-lived)
    #[serde(default)]
    pub refresh_token: Option<String>,

    /// Refresh token expiration time in RFC3339 format
    #[serde(default)]
    pub refresh_expires_at: Option<String>,
}

impl Credentials {
    /// Create new credentials with a JWT token
    pub fn new(instance: String, jwt_token: String) -> Self {
        Self {
            instance,
            jwt_token,
            user: None,
            expires_at: None,
            server_url: None,
            refresh_token: None,
            refresh_expires_at: None,
        }
    }

    /// Create new credentials with full details
    pub fn with_details(
        instance: String,
        jwt_token: String,
        user: impl Into<UserId>,
        expires_at: String,
        server_url: Option<String>,
    ) -> Self {
        Self {
            instance,
            jwt_token,
            user: Some(user.into()),
            expires_at: Some(expires_at),
            server_url,
            refresh_token: None,
            refresh_expires_at: None,
        }
    }

    /// Create new credentials with full details including refresh token
    pub fn with_refresh_token(
        instance: String,
        jwt_token: String,
        user: impl Into<UserId>,
        expires_at: String,
        server_url: Option<String>,
        refresh_token: Option<String>,
        refresh_expires_at: Option<String>,
    ) -> Self {
        Self {
            instance,
            jwt_token,
            user: Some(user.into()),
            expires_at: Some(expires_at),
            server_url,
            refresh_token,
            refresh_expires_at,
        }
    }

    /// Get the server URL, defaulting to instance name if not set
    pub fn get_server_url(&self) -> &str {
        self.server_url.as_deref().unwrap_or(&self.instance)
    }

    /// Check if the access token has expired (if expiration is known)
    /// Returns false if expiration is unknown (assume valid)
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = &self.expires_at {
            if let Ok(exp_ms) = crate::timestamp::parse_iso8601(expires_at) {
                return exp_ms < crate::timestamp::now();
            }
        }
        false
    }

    /// Check if the refresh token has expired (if expiration is known)
    /// Returns true if no refresh token is available or if it has expired
    pub fn is_refresh_expired(&self) -> bool {
        match (&self.refresh_token, &self.refresh_expires_at) {
            (Some(_), Some(expires_at)) => {
                if let Ok(exp_ms) = crate::timestamp::parse_iso8601(expires_at) {
                    exp_ms < crate::timestamp::now()
                } else {
                    // Invalid format, assume expired
                    true
                }
            },
            // No refresh token available
            _ => true,
        }
    }

    /// Check if we can refresh the access token
    pub fn can_refresh(&self) -> bool {
        self.refresh_token.is_some() && !self.is_refresh_expired()
    }
}

/// Trait for credential storage backends.
///
/// Implementations can store credentials in files, environment variables,
/// secure keychains, browser localStorage, or any other storage mechanism.
///
/// # Security Note
///
/// Implementations MUST ensure credentials are stored securely:
/// - Files should use restrictive permissions (0600 on Unix)
/// - Passwords should never be logged
/// - Consider encryption for sensitive deployments
///
/// # Example Implementation
///
/// ```rust,ignore
/// use kalam_client::credentials::{CredentialStore, Credentials};
///
/// struct MyCredentialStore;
///
/// impl CredentialStore for MyCredentialStore {
///     fn get_credentials(&self, instance: &str) -> Result<Option<Credentials>> {
///         // Read from your storage backend
///         Ok(None)
///     }
///     
///     fn set_credentials(&mut self, credentials: &Credentials) -> Result<()> {
///         // Write to your storage backend
///         Ok(())
///     }
///     
///     fn delete_credentials(&mut self, instance: &str) -> Result<()> {
///         // Remove from your storage backend
///         Ok(())
///     }
///     
///     fn list_instances(&self) -> Result<Vec<String>> {
///         // List all stored instances
///         Ok(vec![])
///     }
/// }
/// ```
pub trait CredentialStore {
    /// Retrieve credentials for a specific database instance
    ///
    /// Returns `Ok(None)` if no credentials are stored for the instance.
    ///
    /// # Arguments
    /// * `instance` - Instance identifier (e.g., "local", "production")
    fn get_credentials(&self, instance: &str) -> Result<Option<Credentials>>;

    /// Store credentials for a database instance
    ///
    /// Overwrites existing credentials for the same instance.
    ///
    /// # Arguments
    /// * `credentials` - Credentials to store
    fn set_credentials(&mut self, credentials: &Credentials) -> Result<()>;

    /// Delete stored credentials for an instance
    ///
    /// Returns `Ok(())` even if no credentials were stored.
    ///
    /// # Arguments
    /// * `instance` - Instance identifier to delete
    fn delete_credentials(&mut self, instance: &str) -> Result<()>;

    /// List all stored instance identifiers
    ///
    /// Returns a vector of instance names that have stored credentials.
    fn list_instances(&self) -> Result<Vec<String>>;

    /// Check if credentials exist for an instance
    ///
    /// Default implementation calls `get_credentials()` and checks for Some.
    fn has_credentials(&self, instance: &str) -> Result<bool> {
        Ok(self.get_credentials(instance)?.is_some())
    }
}

/// In-memory credential store for testing and temporary use.
///
/// Does NOT persist credentials across restarts. Useful for:
/// - Unit tests
/// - Temporary sessions
/// - WASM applications without localStorage access
///
/// # Example
///
/// ```rust
/// use kalam_client::credentials::{CredentialStore, Credentials, MemoryCredentialStore};
///
/// let mut store = MemoryCredentialStore::new();
/// let creds = Credentials::new("local".to_string(), "jwt.token.value".to_string());
///
/// store.set_credentials(&creds).unwrap();
/// let retrieved = store.get_credentials("local").unwrap();
/// assert_eq!(retrieved, Some(creds));
/// ```
#[derive(Debug, Default, Clone)]
pub struct MemoryCredentialStore {
    credentials: std::collections::HashMap<String, Credentials>,
}

impl MemoryCredentialStore {
    /// Create a new empty in-memory credential store
    pub fn new() -> Self {
        Self {
            credentials: std::collections::HashMap::new(),
        }
    }
}

impl CredentialStore for MemoryCredentialStore {
    fn get_credentials(&self, instance: &str) -> Result<Option<Credentials>> {
        Ok(self.credentials.get(instance).cloned())
    }

    fn set_credentials(&mut self, credentials: &Credentials) -> Result<()> {
        self.credentials.insert(credentials.instance.clone(), credentials.clone());
        Ok(())
    }

    fn delete_credentials(&mut self, instance: &str) -> Result<()> {
        self.credentials.remove(instance);
        Ok(())
    }

    fn list_instances(&self) -> Result<Vec<String>> {
        Ok(self.credentials.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_creation() {
        let creds = Credentials::new(
            "local".to_string(),
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test".to_string(),
        );

        assert_eq!(creds.instance, "local");
        assert_eq!(creds.jwt_token, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test");
        assert_eq!(creds.user, None);
        assert_eq!(creds.expires_at, None);
        assert_eq!(creds.server_url, None);
        assert_eq!(creds.get_server_url(), "local");
    }

    #[test]
    fn test_credentials_with_details() {
        let creds = Credentials::with_details(
            "prod".to_string(),
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test".to_string(),
            "alice".to_string(),
            "2025-12-31T23:59:59Z".to_string(),
            Some("https://db.example.com".to_string()),
        );

        assert_eq!(creds.instance, "prod");
        assert_eq!(creds.user, Some(UserId::from("alice")));
        assert_eq!(creds.expires_at, Some("2025-12-31T23:59:59Z".to_string()));
        assert_eq!(creds.server_url, Some("https://db.example.com".to_string()));
        assert_eq!(creds.get_server_url(), "https://db.example.com");
    }

    #[test]
    fn test_credentials_expiry_check() {
        // Expired token
        let expired_creds = Credentials::with_details(
            "test".to_string(),
            "token".to_string(),
            "user".to_string(),
            "2020-01-01T00:00:00Z".to_string(),
            None,
        );
        assert!(expired_creds.is_expired());

        // Future token
        let valid_creds = Credentials::with_details(
            "test".to_string(),
            "token".to_string(),
            "user".to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            None,
        );
        assert!(!valid_creds.is_expired());

        // No expiry set (assume valid)
        let no_expiry = Credentials::new("test".to_string(), "token".to_string());
        assert!(!no_expiry.is_expired());
    }

    #[test]
    fn test_memory_store_basic_operations() {
        let mut store = MemoryCredentialStore::new();

        // Initially empty
        assert_eq!(store.get_credentials("local").unwrap(), None);
        assert!(!store.has_credentials("local").unwrap());

        // Store credentials
        let creds = Credentials::new("local".to_string(), "jwt_token_here".to_string());
        store.set_credentials(&creds).unwrap();

        // Retrieve credentials
        let retrieved = store.get_credentials("local").unwrap();
        assert_eq!(retrieved, Some(creds.clone()));
        assert!(store.has_credentials("local").unwrap());

        // Delete credentials
        store.delete_credentials("local").unwrap();
        assert_eq!(store.get_credentials("local").unwrap(), None);
    }

    #[test]
    fn test_memory_store_multiple_instances() {
        let mut store = MemoryCredentialStore::new();

        let creds1 = Credentials::with_details(
            "local".to_string(),
            "token1".to_string(),
            "alice".to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            None,
        );
        let creds2 = Credentials::with_details(
            "prod".to_string(),
            "token2".to_string(),
            "bob".to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            None,
        );
        let creds3 = Credentials::with_details(
            "dev".to_string(),
            "token3".to_string(),
            "carol".to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            None,
        );

        store.set_credentials(&creds1).unwrap();
        store.set_credentials(&creds2).unwrap();
        store.set_credentials(&creds3).unwrap();

        // List instances
        let instances = store.list_instances().unwrap();
        assert_eq!(instances.len(), 3);
        assert!(instances.contains(&"local".to_string()));
        assert!(instances.contains(&"prod".to_string()));
        assert!(instances.contains(&"dev".to_string()));

        // Retrieve specific instances
        assert_eq!(
            store.get_credentials("local").unwrap().unwrap().user,
            Some(UserId::from("alice"))
        );
        assert_eq!(store.get_credentials("prod").unwrap().unwrap().user, Some(UserId::from("bob")));
        assert_eq!(
            store.get_credentials("dev").unwrap().unwrap().user,
            Some(UserId::from("carol"))
        );
    }

    #[test]
    fn test_memory_store_overwrite() {
        let mut store = MemoryCredentialStore::new();

        let creds1 = Credentials::new("local".to_string(), "old_token".to_string());
        let creds2 = Credentials::new("local".to_string(), "new_token".to_string());

        store.set_credentials(&creds1).unwrap();
        store.set_credentials(&creds2).unwrap();

        let retrieved = store.get_credentials("local").unwrap().unwrap();
        assert_eq!(retrieved.jwt_token, "new_token");
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials::with_details(
            "prod".to_string(),
            "eyJhbGciOiJIUzI1NiJ9.test".to_string(),
            "alice".to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            Some("https://db.example.com".to_string()),
        );

        // Serialize to JSON
        let json = serde_json::to_string(&creds).unwrap();

        // Deserialize back
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized, creds);
    }

    #[test]
    fn test_credentials_with_refresh_token() {
        let creds = Credentials::with_refresh_token(
            "prod".to_string(),
            "access_token".to_string(),
            "alice".to_string(),
            "2099-01-01T00:00:00Z".to_string(),
            Some("https://db.example.com".to_string()),
            Some("refresh_token".to_string()),
            Some("2099-01-08T00:00:00Z".to_string()),
        );

        assert_eq!(creds.refresh_token, Some("refresh_token".to_string()));
        assert_eq!(creds.refresh_expires_at, Some("2099-01-08T00:00:00Z".to_string()));
        assert!(!creds.is_expired());
        assert!(!creds.is_refresh_expired());
        assert!(creds.can_refresh());
    }

    #[test]
    fn test_credentials_refresh_expired() {
        // Access token valid, but refresh token expired
        let creds = Credentials::with_refresh_token(
            "test".to_string(),
            "access_token".to_string(),
            "user".to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            None,
            Some("old_refresh_token".to_string()),
            Some("2020-01-01T00:00:00Z".to_string()), // Expired
        );

        assert!(!creds.is_expired());
        assert!(creds.is_refresh_expired());
        assert!(!creds.can_refresh());
    }

    #[test]
    fn test_credentials_no_refresh_token() {
        // No refresh token at all
        let creds = Credentials::with_details(
            "test".to_string(),
            "access_token".to_string(),
            "user".to_string(),
            "2020-01-01T00:00:00Z".to_string(), // Expired access token
            None,
        );

        assert!(creds.is_expired());
        assert!(creds.is_refresh_expired()); // No refresh token = expired
        assert!(!creds.can_refresh());
    }
}
