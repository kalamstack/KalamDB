//! Authentication provider for KalamDB client.
//!
//! Handles JWT tokens, login-only password credentials, and async dynamic auth providers.
//!
//! ## Dynamic Auth Provider
//!
//! Use [`DynamicAuthProvider`] to supply credentials lazily — called on every
//! connect or reconnect.  This is the right choice for:
//! - OAuth / OIDC token flows where tokens expire
//! - Credentials fetched from secure storage (e.g. keychain, secret manager)
//! - Automatic refresh-token rotation
//!
//! ```rust,no_run
//! use kalam_client::{AuthProvider, DynamicAuthProvider};
//! use std::sync::Arc;
//! use std::pin::Pin;
//! use std::future::Future;
//!
//! struct MyTokenStore { /* ... */ }
//!
//! impl DynamicAuthProvider for MyTokenStore {
//!     fn get_auth(&self) -> Pin<Box<dyn Future<Output = kalam_client::Result<AuthProvider>> + Send + '_>> {
//!         Box::pin(async {
//!             // fetch / refresh token here
//!             Ok(AuthProvider::jwt_token("fresh-token".into()))
//!         })
//!     }
//! }
//!
//! // Wrap in Arc and pass to the builder:
//! // .auth_provider(Arc::new(MyTokenStore { ... }))
//! ```

use crate::error::{KalamLinkError, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Authentication credentials for KalamDB server.
///
/// Supports JWT tokens and login credentials.
/// Password credentials are exchanged through `/v1/api/auth/login` before they
/// can be used for authenticated requests.
///
/// # Examples
///
/// ```rust
/// use kalam_client::AuthProvider;
///
/// // Login credentials (exchanged for JWTs automatically)
/// let auth = AuthProvider::basic_auth("user".to_string(), "password".to_string());
///
/// // JWT token authentication
/// let auth = AuthProvider::jwt_token("eyJhbGc...".to_string());
///
/// // No authentication (localhost bypass mode)
/// let auth = AuthProvider::none();
/// ```
#[derive(Debug, Clone)]
pub enum AuthProvider {
    /// HTTP Basic Auth (user, password)
    BasicAuth(String, String),

    /// JWT token authentication
    JwtToken(String),

    /// No authentication (localhost bypass)
    None,
}

impl AuthProvider {
    /// Create login credentials for `POST /v1/api/auth/login`.
    pub fn basic_auth(user: String, password: String) -> Self {
        Self::BasicAuth(user, password)
    }

    /// Create system user authentication (convenience for CLI and internal tools)
    ///
    /// Uses the default system user "root" with provided password.
    pub fn system_user_auth(password: String) -> Self {
        Self::BasicAuth("root".to_string(), password)
    }

    /// Create JWT token authentication
    pub fn jwt_token(token: String) -> Self {
        Self::JwtToken(token)
    }

    /// No authentication (for localhost bypass mode)
    pub fn none() -> Self {
        Self::None
    }

    /// Attach authentication headers to an HTTP request builder.
    ///
    /// Password credentials must be exchanged on `/v1/api/auth/login` before
    /// they can be used for authenticated requests. This method therefore only
    /// supports bearer tokens and anonymous requests.
    pub fn apply_to_request(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder> {
        match self {
            Self::BasicAuth(_, _) => Err(KalamLinkError::AuthenticationError(
                "User/password credentials can only be used with /v1/api/auth/login; exchange them for a JWT before sending authenticated requests.".to_string(),
            )),
            Self::JwtToken(token) => Ok(request.bearer_auth(token)),
            Self::None => Ok(request),
        }
    }

    /// Check if authentication is configured
    pub fn is_authenticated(&self) -> bool {
        !matches!(self, Self::None)
    }
}

// ── Dynamic (async) auth provider ────────────────────────────────────────────

/// Async authentication provider called on every connect or reconnect.
///
/// Implement this trait to supply credentials lazily from any source:
/// OAuth token refresh, secure storage, interactive login, etc.
pub trait DynamicAuthProvider: Send + Sync + 'static {
    /// Return the current (or freshly refreshed) credentials.
    fn get_auth(&self) -> Pin<Box<dyn Future<Output = Result<AuthProvider>> + Send + '_>>;
}

/// A boxed, reference-counted [`DynamicAuthProvider`].
pub type ArcDynAuthProvider = Arc<dyn DynamicAuthProvider>;

/// Resolves the effective [`AuthProvider`] for a connection.
///
/// Holds either a static provider or a dynamic one.  Call [`resolve`] before
/// each connect/reconnect to obtain a fresh [`AuthProvider`].
///
/// [`resolve`]: ResolvedAuth::resolve
#[derive(Clone)]
pub enum ResolvedAuth {
    /// Static credentials set at construction time.
    Static(AuthProvider),
    /// Dynamic provider called on every connect.
    Dynamic(ArcDynAuthProvider),
}

impl ResolvedAuth {
    /// Obtain effective credentials, calling the dynamic provider if present.
    pub async fn resolve(&self) -> Result<AuthProvider> {
        match self {
            Self::Static(p) => Ok(p.clone()),
            Self::Dynamic(provider) => provider.get_auth().await,
        }
    }

    /// `true` when no credentials of either kind are configured.
    pub fn is_none(&self) -> bool {
        matches!(self, Self::Static(AuthProvider::None))
    }
}

impl std::fmt::Debug for ResolvedAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(p) => write!(f, "ResolvedAuth::Static({:?})", p),
            Self::Dynamic(_) => write!(f, "ResolvedAuth::Dynamic(<fn>)"),
        }
    }
}

impl Default for ResolvedAuth {
    fn default() -> Self {
        Self::Static(AuthProvider::None)
    }
}

impl From<AuthProvider> for ResolvedAuth {
    fn from(p: AuthProvider) -> Self {
        Self::Static(p)
    }
}

impl From<ArcDynAuthProvider> for ResolvedAuth {
    fn from(p: ArcDynAuthProvider) -> Self {
        Self::Dynamic(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_provider_creation() {
        let basic = AuthProvider::basic_auth("alice".to_string(), "secret".to_string());
        assert!(basic.is_authenticated());

        let system = AuthProvider::system_user_auth("password123".to_string());
        assert!(system.is_authenticated());

        let jwt = AuthProvider::jwt_token("test_token".to_string());
        assert!(jwt.is_authenticated());

        let none = AuthProvider::none();
        assert!(!none.is_authenticated());
    }

    #[test]
    fn test_apply_to_request_rejects_basic_auth() {
        let auth = AuthProvider::basic_auth("alice".to_string(), "secret123".to_string());

        let client = reqwest::Client::new();
        let request = client.get("http://localhost:8080");
        let result = auth.apply_to_request(request);
        assert!(result.is_err());
    }

    #[test]
    fn test_system_user_auth_uses_root() {
        let auth = AuthProvider::system_user_auth("test_password".to_string());

        match auth {
            AuthProvider::BasicAuth(user, password) => {
                assert_eq!(user, "root");
                assert_eq!(password, "test_password");
            },
            _ => panic!("Expected BasicAuth variant"),
        }
    }
}
