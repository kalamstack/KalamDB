use kalamdb_commons::{Role, UserId, UserName};
use serde::{Deserialize, Serialize};

/// Default JWT expiration time in hours.
pub const DEFAULT_JWT_EXPIRY_HOURS: i64 = 24;

/// Token type for distinguishing access from refresh tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenType {
    Access,
    Refresh,
}

impl std::fmt::Display for TokenType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenType::Access => write!(f, "access"),
            TokenType::Refresh => write!(f, "refresh"),
        }
    }
}

/// JWT claims shared across internal HS256 tokens and external OIDC tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
    pub iss: String,
    pub exp: usize,
    pub iat: usize,
    #[serde(alias = "preferred_username")]
    pub username: Option<UserName>,
    pub email: Option<String>,
    pub role: Option<Role>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_type: Option<TokenType>,
}

impl JwtClaims {
    pub fn new(
        user_id: &UserId,
        username: &UserName,
        role: &Role,
        email: Option<&str>,
        expiry_hours: Option<i64>,
        issuer: &str,
    ) -> Self {
        Self::with_token_type(
            user_id,
            username,
            role,
            email,
            expiry_hours,
            TokenType::Access,
            issuer,
        )
    }

    pub fn with_token_type(
        user_id: &UserId,
        username: &UserName,
        role: &Role,
        email: Option<&str>,
        expiry_hours: Option<i64>,
        token_type: TokenType,
        issuer: &str,
    ) -> Self {
        let now = chrono::Utc::now();
        let exp_hours = expiry_hours.unwrap_or(DEFAULT_JWT_EXPIRY_HOURS);
        let exp = now + chrono::Duration::hours(exp_hours);

        Self {
            sub: user_id.to_string(),
            iss: issuer.to_string(),
            exp: exp.timestamp() as usize,
            iat: now.timestamp() as usize,
            username: Some(username.clone()),
            email: email.map(|value| value.to_string()),
            role: Some(*role),
            token_type: Some(token_type),
        }
    }
}
