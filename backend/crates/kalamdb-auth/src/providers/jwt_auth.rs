// JWT authentication and validation module

use crate::errors::error::{AuthError, AuthResult};
pub(crate) use crate::oidc::{extract_algorithm_unverified, extract_issuer_unverified};
pub use crate::oidc::{JwtClaims, TokenType, DEFAULT_JWT_EXPIRY_HOURS};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use kalamdb_commons::{Role, UserId};

/// Default issuer for KalamDB-issued tokens.
pub const KALAMDB_ISSUER: &str = "kalamdb";

/// Generate a new JWT token.
pub fn generate_jwt_token(claims: &JwtClaims, secret: &str) -> AuthResult<String> {
    let header = Header::new(Algorithm::HS256);
    let encoding_key = EncodingKey::from_secret(secret.as_bytes());

    encode(&header, claims, &encoding_key)
        .map_err(|e| AuthError::HashingError(format!("JWT encoding error: {}", e)))
}

/// Create and sign a new JWT access token.
pub fn create_and_sign_token(
    user_id: &UserId,
    role: &Role,
    email: Option<&str>,
    expiry_hours: Option<i64>,
    secret: &str,
) -> AuthResult<(String, JwtClaims)> {
    let claims = JwtClaims::with_token_type(
        user_id,
        role,
        email,
        expiry_hours,
        TokenType::Access,
        KALAMDB_ISSUER,
    );
    let token = generate_jwt_token(&claims, secret)?;
    Ok((token, claims))
}

/// Create and sign a new JWT refresh token.
pub fn create_and_sign_refresh_token(
    user_id: &UserId,
    role: &Role,
    email: Option<&str>,
    expiry_hours: Option<i64>,
    secret: &str,
) -> AuthResult<(String, JwtClaims)> {
    let claims = JwtClaims::with_token_type(
        user_id,
        role,
        email,
        expiry_hours,
        TokenType::Refresh,
        KALAMDB_ISSUER,
    );
    let token = generate_jwt_token(&claims, secret)?;
    Ok((token, claims))
}

/// Refresh a JWT token by generating a new token with extended expiration.
pub fn refresh_jwt_token(
    token: &str,
    secret: &str,
    expiry_hours: Option<i64>,
) -> AuthResult<(String, JwtClaims)> {
    let trusted_issuers = vec![KALAMDB_ISSUER.to_string()];
    let old_claims = validate_jwt_token(token, secret, &trusted_issuers)?;

    let user_id = UserId::new(&old_claims.sub);
    let role = old_claims.role.as_ref().cloned().unwrap_or(Role::User);

    create_and_sign_token(&user_id, &role, old_claims.email.as_deref(), expiry_hours, secret)
}

/// Validate a JWT token and extract claims.
pub fn validate_jwt_token(
    token: &str,
    secret: &str,
    trusted_issuers: &[String],
) -> AuthResult<JwtClaims> {
    let _header = decode_header(token)
        .map_err(|e| AuthError::MalformedAuthorization(format!("Invalid JWT header: {}", e)))?;

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.validate_aud = false;
    validation.leeway = 60;

    let decoding_key = DecodingKey::from_secret(secret.as_bytes());
    let token_data =
        decode::<JwtClaims>(token, &decoding_key, &validation).map_err(|e| match e.kind() {
            ErrorKind::ExpiredSignature => AuthError::TokenExpired,
            ErrorKind::InvalidSignature => AuthError::InvalidSignature,
            _ => AuthError::MalformedAuthorization(format!("JWT decode error: {}", e)),
        })?;

    let claims = token_data.claims;

    let now = chrono::Utc::now().timestamp() as usize;
    let leeway = validation.leeway as usize;
    if claims.iat > now + leeway {
        return Err(AuthError::MalformedAuthorization(format!(
            "Token issued in the future (iat: {}, now: {})",
            claims.iat, now
        )));
    }

    verify_issuer(&claims.iss, trusted_issuers)?;

    if claims.sub.is_empty() {
        return Err(AuthError::MissingClaim("sub".to_string()));
    }

    Ok(claims)
}

/// Verify JWT issuer is in the trusted list.
pub fn verify_issuer(issuer: &str, trusted_issuers: &[String]) -> AuthResult<()> {
    if trusted_issuers.is_empty() {
        return Err(AuthError::UntrustedIssuer(format!(
            "No trusted issuers configured. Rejecting issuer: {}",
            issuer
        )));
    }

    if trusted_issuers.iter().any(|i| i == issuer) {
        Ok(())
    } else {
        Err(AuthError::UntrustedIssuer(issuer.to_string()))
    }
}

/// Returns true if the issuer is the internal KalamDB issuer.
pub fn is_internal_issuer(issuer: &str) -> bool {
    issuer == KALAMDB_ISSUER
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn create_test_token(secret: &str, exp_offset_secs: i64) -> String {
        create_test_token_with_type(secret, exp_offset_secs, Some(TokenType::Access))
    }

    fn create_test_token_with_type(
        secret: &str,
        exp_offset_secs: i64,
        token_type: Option<TokenType>,
    ) -> String {
        let now = chrono::Utc::now().timestamp() as usize;
        let claims = JwtClaims {
            sub: "user_123".to_string(),
            iss: "kalamdb-test".to_string(),
            exp: ((now as i64) + exp_offset_secs) as usize,
            iat: now,
            email: Some("test@example.com".to_string()),
            role: Some(Role::User),
            token_type,
        };

        let header = Header::new(Algorithm::HS256);
        let encoding_key = EncodingKey::from_secret(secret.as_bytes());
        encode(&header, &claims, &encoding_key).unwrap()
    }

    #[test]
    fn test_validate_jwt_token_valid() {
        let secret = "test-secret-key";
        let token = create_test_token(secret, 3600);

        let trusted_issuers = vec!["kalamdb-test".to_string()];
        let result = validate_jwt_token(&token, secret, &trusted_issuers);
        assert!(result.is_ok());

        let claims = result.unwrap();
        assert_eq!(claims.sub, "user_123");
        assert_eq!(claims.iss, "kalamdb-test");
    }

    #[test]
    fn test_validate_jwt_token_wrong_secret() {
        let secret = "test-secret-key";
        let token = create_test_token(secret, 3600);

        let trusted_issuers = vec!["kalamdb-test".to_string()];
        let result = validate_jwt_token(&token, "wrong-secret", &trusted_issuers);
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }

    #[test]
    fn test_validate_jwt_token_expired() {
        let secret = "test-secret-key";
        let token = create_test_token(secret, -3600);

        let trusted_issuers = vec!["kalamdb-test".to_string()];
        let result = validate_jwt_token(&token, secret, &trusted_issuers);
        assert!(matches!(result, Err(AuthError::TokenExpired)));
    }

    #[test]
    fn test_verify_issuer_trusted() {
        let trusted = vec!["kalamdb.io".to_string(), "auth.kalamdb.io".to_string()];
        assert!(verify_issuer("kalamdb.io", &trusted).is_ok());
        assert!(verify_issuer("auth.kalamdb.io", &trusted).is_ok());
    }

    #[test]
    fn test_verify_issuer_untrusted() {
        let trusted = vec!["kalamdb.io".to_string()];
        let result = verify_issuer("evil.com", &trusted);
        assert!(matches!(result, Err(AuthError::UntrustedIssuer(_))));
    }

    #[test]
    fn test_verify_issuer_empty_list() {
        let trusted = vec![];
        let result = verify_issuer("any-issuer.com", &trusted);
        assert!(matches!(result, Err(AuthError::UntrustedIssuer(_))));
    }

    #[test]
    fn test_refresh_token_type_claim_is_preserved() {
        let secret = "test-secret-key";
        let trusted = vec!["kalamdb".to_string()];

        let user_id = UserId::new("u_refresh");
        let role = Role::User;

        let (refresh_token, _) = create_and_sign_refresh_token(&user_id, &role, None, None, secret)
            .expect("Failed to create refresh token");

        let claims =
            validate_jwt_token(&refresh_token, secret, &trusted).expect("Token validation failed");

        assert_eq!(
            claims.token_type,
            Some(TokenType::Refresh),
            "Refresh token must carry token_type=Refresh claim"
        );
    }

    #[test]
    fn test_access_token_type_claim_is_preserved() {
        let secret = "test-secret-key";
        let trusted = vec!["kalamdb".to_string()];

        let user_id = UserId::new("u_access");
        let role = Role::User;

        let (access_token, _) = create_and_sign_token(&user_id, &role, None, None, secret)
            .expect("Failed to create access token");

        let claims =
            validate_jwt_token(&access_token, secret, &trusted).expect("Token validation failed");

        assert_eq!(
            claims.token_type,
            Some(TokenType::Access),
            "Access token must carry token_type=Access claim"
        );
    }

    #[test]
    fn test_refresh_and_access_token_types_are_distinct() {
        let secret = "shared-secret";
        let trusted = vec!["kalamdb".to_string()];
        let user_id = UserId::new("u_distinct");
        let role = Role::User;

        let (access, _) = create_and_sign_token(&user_id, &role, None, None, secret).unwrap();

        let (refresh, _) =
            create_and_sign_refresh_token(&user_id, &role, None, None, secret).unwrap();

        let access_claims = validate_jwt_token(&access, secret, &trusted).unwrap();
        let refresh_claims = validate_jwt_token(&refresh, secret, &trusted).unwrap();

        assert_ne!(
            access_claims.token_type, refresh_claims.token_type,
            "Access and refresh tokens must have different token_type claims"
        );
    }

    #[test]
    fn test_validate_empty_string_returns_error() {
        let trusted = vec!["kalamdb.io".to_string()];
        let result = validate_jwt_token("", "any-secret", &trusted);
        assert!(result.is_err(), "Empty token string must be rejected");
    }

    #[test]
    fn test_validate_truncated_jwt_returns_error() {
        let trusted = vec!["kalamdb.io".to_string()];
        let result = validate_jwt_token("eyJhbGciOiJIUzI1NiJ9.e30", "any-secret", &trusted);
        assert!(result.is_err(), "Truncated JWT (missing signature) must be rejected");
    }

    #[test]
    fn test_validate_jwt_sql_injection_in_sub_is_safe() {
        let secret = "some-secret";
        let trusted = vec!["kalamdb-test".to_string()];

        let sqli_username = "'; DROP TABLE users; --";
        let now = chrono::Utc::now().timestamp() as usize;
        let claims = JwtClaims {
            sub: sqli_username.to_string(),
            iss: "kalamdb-test".to_string(),
            exp: now + 3600,
            iat: now,
            email: None,
            role: None,
            token_type: Some(TokenType::Access),
        };

        let token = generate_jwt_token(&claims, secret).unwrap();
        let parsed = validate_jwt_token(&token, secret, &trusted).unwrap();
        assert_eq!(parsed.sub, sqli_username, "JWT validator must preserve sub claims verbatim");
    }

    #[test]
    fn test_validate_jwt_role_claim_is_returned_for_caller_to_check() {
        let secret = "secure-secret";
        let trusted = vec!["kalamdb-test".to_string()];

        let token = create_test_token_with_type(secret, 3600, Some(TokenType::Access));
        let claims = validate_jwt_token(&token, secret, &trusted).unwrap();
        let _ = claims.role;
    }
}
