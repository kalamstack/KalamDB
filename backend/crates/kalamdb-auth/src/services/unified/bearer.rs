use crate::errors::error::{AuthError, AuthResult};
use crate::models::context::AuthenticatedUser;
use crate::oidc::OidcError;
use crate::providers::jwt_auth;
use crate::providers::jwt_config;
use crate::providers::jwt_config::JwtConfig;
use crate::repository::user_repo::UserRepository;
use jsonwebtoken::Algorithm;
use kalamdb_commons::models::{ConnectionInfo, UserId};
use kalamdb_commons::AuthType;
use std::sync::Arc;
use tracing::Instrument;

pub(super) async fn authenticate_bearer(
    token: &str,
    connection_info: &ConnectionInfo,
    repo: &Arc<dyn UserRepository>,
) -> AuthResult<AuthenticatedUser> {
    let span = tracing::info_span!(
        "auth.bearer",
        token_len = token.len(),
        is_localhost = connection_info.is_localhost()
    );

    async move {
        let config = jwt_config::get_jwt_config();

        let alg = jwt_auth::extract_algorithm_unverified(token)?;
        let issuer = jwt_auth::extract_issuer_unverified(token)?;

        jwt_auth::verify_issuer(&issuer, &config.trusted_issuers)?;

        let claims = validate_bearer_token(token, &alg, &issuer, config).await?;

        let is_internal = jwt_auth::is_internal_issuer(&claims.iss);

        if is_internal {
            match claims.token_type {
                Some(jwt_auth::TokenType::Access) => { /* OK */ },
                Some(jwt_auth::TokenType::Refresh) => {
                    log::warn!("Refresh token used as access token for user={}", claims.sub);
                    return Err(AuthError::InvalidCredentials(
                        "Refresh tokens cannot be used for API authentication".to_string(),
                    ));
                },
                None => {
                    log::warn!("Token missing token_type claim for user={}", claims.sub);
                    return Err(AuthError::InvalidCredentials(
                        "Token missing required token_type claim".to_string(),
                    ));
                },
            }
        } else {
            let header = jsonwebtoken::decode_header(token).map_err(|e| {
                AuthError::MalformedAuthorization(format!("Invalid JWT header: {}", e))
            })?;

            if let Some(typ) = header.typ {
                let typ_lower = typ.to_lowercase();
                if typ_lower.contains("refresh") {
                    log::warn!(
                        "External refresh token used as access token for user={}",
                        claims.sub
                    );
                    return Err(AuthError::InvalidCredentials(
                        "Refresh tokens cannot be used for API authentication".to_string(),
                    ));
                }
            }
        }

        // Internal and trusted external tokens both resolve directly by canonical sub.
        let token_user_id = UserId::try_new(claims.sub.clone()).map_err(|_| {
            AuthError::MalformedAuthorization("Token contains an invalid user claim".to_string())
        })?;
        let user = repo.get_user_by_id(&token_user_id).await?;

        if user.deleted_at.is_some() {
            return Err(AuthError::InvalidCredentials("Invalid credentials".to_string()));
        }

        if !is_internal && user.auth_type != AuthType::OAuth {
            return Err(AuthError::AuthenticationFailed(
                "Account is not configured for OAuth authentication".to_string(),
            ));
        }

        if let Some(claimed_role) = claims.role {
            if claimed_role != user.role {
                log::warn!(
                    "Bearer token role mismatch: claimed={:?}, actual={:?} for user={}",
                    claimed_role,
                    user.role,
                    user.user_id.as_str()
                );
                return Err(AuthError::InvalidCredentials(
                    "Token role does not match user role".to_string(),
                ));
            }
        }

        let role = user.role;

        tracing::trace!(user_id = %user.user_id, role = ?role, "Bearer authentication succeeded");

        Ok(AuthenticatedUser::new(
            user.user_id.clone(),
            role,
            user.email.clone(),
            user.created_at,
            user.updated_at,
            connection_info.clone(),
        ))
    }
    .instrument(span)
    .await
}

async fn validate_bearer_token(
    token: &str,
    alg: &Algorithm,
    issuer: &str,
    config: &'static JwtConfig,
) -> AuthResult<jwt_auth::JwtClaims> {
    match alg {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            if !jwt_auth::is_internal_issuer(issuer) {
                return Err(AuthError::MalformedAuthorization(format!(
                    "HS256 tokens are only accepted for internally-issued tokens \
                     (iss='{}'). External provider tokens must use an asymmetric \
                     algorithm (RS256 / ES256). Received iss='{}'.",
                    jwt_auth::KALAMDB_ISSUER,
                    issuer
                )));
            }
            jwt_auth::validate_jwt_token(token, &config.secret, &config.trusted_issuers)
        },
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512
        | Algorithm::ES256
        | Algorithm::ES384 => {
            if jwt_auth::is_internal_issuer(issuer) {
                return Err(AuthError::MalformedAuthorization(
                    "Internal 'kalamdb' tokens must use HS256, not an asymmetric algorithm."
                        .to_string(),
                ));
            }
            let validator = config.get_oidc_validator(issuer).await?;
            let claims: jwt_auth::JwtClaims =
                validator.validate(token).await.map_err(|e| match e {
                    OidcError::JwtValidationFailed(ref msg) if msg.contains("expired") => {
                        AuthError::TokenExpired
                    },
                    OidcError::JwtValidationFailed(ref msg) if msg.contains("signature") => {
                        AuthError::InvalidSignature
                    },
                    _ => AuthError::MalformedAuthorization(format!(
                        "External token validation failed: {}",
                        e
                    )),
                })?;
            jwt_auth::verify_issuer(&claims.iss, &config.trusted_issuers)?;
            if claims.sub.is_empty() {
                return Err(AuthError::MalformedAuthorization(
                    "Token is missing a required user claim".to_string(),
                ));
            }
            Ok(claims)
        },
        _ => Err(AuthError::MalformedAuthorization(format!(
            "Unsupported JWT algorithm: {:?}",
            alg
        ))),
    }
}
