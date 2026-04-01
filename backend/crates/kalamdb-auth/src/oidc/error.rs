/// Errors produced by the OIDC validator.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OidcError {
    #[error("OIDC discovery failed: {0}")]
    DiscoveryFailed(String),

    #[error("JWKS fetch failed: {0}")]
    JwksFetchFailed(String),

    #[error("Token is missing the 'kid' header")]
    MissingKid,

    #[error("No key found for kid '{0}'")]
    KeyNotFound(String),

    #[error("Invalid JWK format: {0}")]
    InvalidKeyFormat(String),

    #[error("JWT validation failed: {0}")]
    JwtValidationFailed(String),
}

impl From<jsonwebtoken::errors::Error> for OidcError {
    fn from(error: jsonwebtoken::errors::Error) -> Self {
        use jsonwebtoken::errors::ErrorKind;

        match error.kind() {
            ErrorKind::ExpiredSignature => OidcError::JwtValidationFailed("Token expired".into()),
            ErrorKind::InvalidSignature => {
                OidcError::JwtValidationFailed("Invalid signature".into())
            },
            ErrorKind::InvalidToken => OidcError::JwtValidationFailed("Invalid token".into()),
            _ => OidcError::JwtValidationFailed(error.to_string()),
        }
    }
}
