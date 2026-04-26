// Authentication error types for KalamDB

use thiserror::Error;

use crate::oidc::OidcError;

/// Errors that can occur during authentication and authorization.
#[derive(Error, Debug)]
pub enum AuthError {
    /// Missing or empty Authorization header
    #[error("Missing authorization header: {0}")]
    MissingAuthorization(String),

    /// Invalid credentials
    #[error("Invalid credentials: {0}")]
    InvalidCredentials(String),

    /// Server requires initial setup (root password not configured)
    /// This error is returned when the root user has no password and requires initial setup.
    #[error("Server requires initial setup: {0}")]
    SetupRequired(String),

    /// Account is locked due to too many failed login attempts
    #[error("Account locked until {0}. Too many failed login attempts.")]
    AccountLocked(String),

    /// Malformed Authorization header (not properly formatted)
    #[error("Malformed authorization header: {0}")]
    MalformedAuthorization(String),

    /// JWT token has expired
    #[error("Token expired")]
    TokenExpired,

    /// JWT signature is invalid
    #[error("Invalid token signature")]
    InvalidSignature,

    /// JWT issuer is not in the trusted list
    #[error("Untrusted issuer: {0}")]
    UntrustedIssuer(String),

    /// Required JWT claim is missing
    #[error("Missing required claim: {0}")]
    MissingClaim(String),

    /// Password does not meet security requirements
    #[error("Weak password: {0}")]
    WeakPassword(String),

    /// User account has been deleted
    #[error("User account deleted")]
    UserDeleted,

    /// User not found in database
    #[error("User not found: {0}")]
    UserNotFound(String),

    /// Remote access denied
    #[error("Remote access denied: {0}")]
    RemoteAccessDenied(String),

    /// Insufficient permissions for requested action
    #[error("Insufficient permissions: {0}")]
    InsufficientPermissions(String),

    /// Internal database error
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Password hashing error
    #[error("Password hashing error: {0}")]
    HashingError(String),

    /// Generic authentication failure
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
}

/// Result type for authentication operations
pub type AuthResult<T> = Result<T, AuthError>;

/// Convert an `OidcError` into an `AuthError`.
///
/// Used by the `?` operator in bearer authentication when OIDC discovery,
/// JWKS lookup, or external token validation fails.
impl From<OidcError> for AuthError {
    fn from(e: OidcError) -> Self {
        match e {
            OidcError::JwtValidationFailed(ref msg) if msg.contains("expired") => {
                AuthError::TokenExpired
            },
            OidcError::JwtValidationFailed(ref msg) if msg.contains("signature") => {
                AuthError::InvalidSignature
            },
            _ => AuthError::MalformedAuthorization(e.to_string()),
        }
    }
}
