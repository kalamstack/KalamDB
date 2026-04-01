use crate::errors::error::{AuthError, AuthResult};
use crate::models::context::AuthenticatedUser;
use crate::oidc::OidcError;
use crate::providers::jwt_auth;
use crate::providers::jwt_config;
use crate::providers::jwt_config::JwtConfig;
use crate::repository::user_repo::UserRepository;
use jsonwebtoken::Algorithm;
use kalamdb_commons::models::{ConnectionInfo, UserId, UserName};
use kalamdb_commons::{AuthType, OAuthProvider, Role};
use kalamdb_system::providers::storages::models::StorageMode;
use kalamdb_system::{AuthData, User};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::Instrument;

/// Compose a deterministic, index-friendly username for an OIDC provider user.
///
/// Format: `oidc:{3-char-provider-code}:{subject}`
///
/// Uses [`UserName::from_provider`] which derives the 3-char prefix from
/// [`OAuthProvider`].  This is stored as the user's `username` in the system,
/// so lookups use the existing username secondary index (O(1)) instead of
/// scanning all users.
pub(crate) fn compose_provider_username(issuer: &str, subject: &str) -> UserName {
    let provider = OAuthProvider::detect_from_issuer(issuer);
    UserName::from_provider(&provider, subject)
}

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

        // Route: read algorithm + issuer without verifying, then pick the right validator.
        let alg = jwt_auth::extract_algorithm_unverified(token)?;
        let issuer = jwt_auth::extract_issuer_unverified(token)?;

        // Fast-reject untrusted issuers before any crypto or network I/O
        jwt_auth::verify_issuer(&issuer, &config.trusted_issuers)?;

        let claims = validate_bearer_token(token, &alg, &issuer, config).await?;

        let is_internal = jwt_auth::is_internal_issuer(&claims.iss);

        if is_internal {
            // SECURITY: Only accept internal tokens explicitly marked as Access.
            // Tokens with missing token_type (legacy or hand-crafted) are rejected
            // to prevent refresh tokens or forged tokens from being used as access tokens.
            match claims.token_type {
                Some(jwt_auth::TokenType::Access) => { /* OK – expected type */ },
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
            // For external OIDC tokens, we validate the `typ` header if present.
            // Some providers use "at+jwt" for access tokens.
            // If it's an ID token, it might just be "JWT" or missing.
            // We don't strictly reject missing `typ` because many providers don't send it,
            // but we ensure it's not explicitly a refresh token if they use custom types.
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

        // ── Internal vs external user resolution ─────────────────────────

        let user = if is_internal {
            // Internal token: `claims.username` is the actual username set
            // by `create_and_sign_token`.
            let username = claims
                .username
                .clone()
                .ok_or_else(|| AuthError::MissingClaim("username".to_string()))?;

            repo.get_user_by_username(&username).await?
        } else {
            // External OIDC token: compose deterministic provider username
            let issuer = claims.iss.clone();
            let subject = claims.sub.clone();
            let provider_username = compose_provider_username(&issuer, &subject);

            resolve_or_provision_provider_user(
                &provider_username,
                &issuer,
                &subject,
                &claims,
                config.auto_create_users_from_provider,
                repo,
            )
            .await?
        };

        if user.deleted_at.is_some() {
            return Err(AuthError::InvalidCredentials("Invalid username or password".to_string()));
        }

        let role = if let Some(claimed_role) = &claims.role {
            if *claimed_role != user.role {
                log::warn!(
                    "JWT role mismatch: claimed={:?}, actual={:?} for user={}",
                    claimed_role,
                    user.role,
                    user.username
                );
                return Err(AuthError::InvalidCredentials(
                    "Token role does not match user role".to_string(),
                ));
            }
            *claimed_role
        } else {
            user.role
        };

        tracing::trace!(username = %user.username, role = ?role, "Bearer authentication succeeded");

        Ok(AuthenticatedUser::new(
            user.user_id.clone(),
            user.username.clone(),
            role,
            user.email.clone(),
            connection_info.clone(),
        ))
    }
    .instrument(span)
    .await
}

/// Look up a provider user by their composed username (index-backed, O(1)).
/// If not found and auto-provisioning is enabled, create the user.
async fn resolve_or_provision_provider_user(
    provider_username: &UserName,
    issuer: &str,
    subject: &str,
    claims: &jwt_auth::JwtClaims,
    auto_create_users_from_provider: bool,
    repo: &Arc<dyn UserRepository>,
) -> AuthResult<User> {
    match repo.get_user_by_username(provider_username).await {
        Ok(user) => Ok(user),
        Err(AuthError::UserNotFound(_)) => {
            maybe_auto_provision_provider_user(
                claims,
                issuer,
                subject,
                provider_username,
                auto_create_users_from_provider,
                repo,
            )
            .await
        },
        Err(e) => Err(e),
    }
}
/// TODO: Remove this and use the same logic we use for other userid generation to generate the provider user id. We can keep the deterministic username composition for lookups, but the user id can be generated in a more standard way instead of hashing the username.
fn build_provider_user_id(issuer: &str, subject: &str) -> UserId {
    let mut hasher = Sha256::new();
    hasher.update(issuer.as_bytes());
    hasher.update(b":");
    hasher.update(subject.as_bytes());
    let hash = hex::encode(hasher.finalize());
    UserId::new(format!("u_oidc_{}", &hash[..16]))
}

/// Auto-provision a new OIDC user when `auto_create_users_from_provider` is enabled.
///
/// The username is the deterministic `oidc:{code}:{subject}` composed earlier,
/// so subsequent logins resolve via the username index with zero scanning.
async fn maybe_auto_provision_provider_user(
    claims: &jwt_auth::JwtClaims,
    issuer: &str,
    subject: &str,
    provider_username: &UserName,
    auto_create_users_from_provider: bool,
    repo: &Arc<dyn UserRepository>,
) -> AuthResult<User> {
    if !auto_create_users_from_provider {
        return Err(AuthError::UserNotFound(format!(
            "User not found for provider and subject (username='{}')",
            provider_username
        )));
    }

    let user_id = build_provider_user_id(issuer, subject);

    let auth_data = AuthData::new(issuer, subject);

    let now = chrono::Utc::now().timestamp_millis();
    let user = User {
        created_at: now,
        updated_at: now,
        locked_until: None,
        last_login_at: Some(now),
        last_seen: Some(now),
        deleted_at: None,
        user_id,
        username: provider_username.clone(),
        password_hash: String::new(),
        email: claims.email.clone(),
        auth_data: Some(auth_data),
        storage_id: None,
        failed_login_attempts: 0,
        role: Role::User,
        auth_type: AuthType::OAuth,
        storage_mode: StorageMode::Table,
    };

    repo.create_user(user.clone()).await?;
    Ok(user)
}

// ---------------------------------------------------------------------------
// Token validation routing
// ---------------------------------------------------------------------------

/// Validate a bearer token, routing to the appropriate validator based on
/// algorithm and issuer.
///
/// ## Security model
///
/// | Algorithm      | Issuer       | Validation                           |
/// |----------------|--------------|--------------------------------------|
/// | HS256          | `kalamdb`    | Shared secret — self-issued by us    |
/// | HS256          | *external*   | **REJECTED** — symmetric alg cannot  |
/// |                |              | prove external origin                |
/// | RS256/ES256/…  | *external*   | OIDC provider's JWKS public key      |
/// | RS256/ES256/…  | `kalamdb`    | **REJECTED** — internal must be HS256|
async fn validate_bearer_token(
    token: &str,
    alg: &Algorithm,
    issuer: &str,
    config: &'static JwtConfig,
) -> AuthResult<jwt_auth::JwtClaims> {
    match alg {
        // ── Internal tokens (HS256) ──────────────────────────────────────
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

        // ── External provider tokens (RS256 / ES256 / PS256 / …) ────────
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

            // Get (or lazily create) the OidcValidator for this issuer.
            // The validator owns a per-issuer JWKS cache with auto-refresh on miss.
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

            // Post-decode issuer check: now cryptographically proven
            jwt_auth::verify_issuer(&claims.iss, &config.trusted_issuers)?;

            if claims.sub.is_empty() {
                return Err(AuthError::MissingClaim("sub".to_string()));
            }

            Ok(claims)
        },

        _ => Err(AuthError::MalformedAuthorization(format!(
            "Unsupported JWT algorithm: {:?}",
            alg
        ))),
    }
}
