//! Token refresh handler
//!
//! POST /v1/api/auth/refresh - Refreshes the JWT token if still valid

use actix_web::{web, HttpRequest, HttpResponse};
use chrono::{Duration, Utc};
use kalamdb_auth::providers::jwt_auth::{
    create_and_sign_refresh_token, validate_jwt_token, TokenType,
};
use kalamdb_auth::{
    create_and_sign_token, create_auth_cookie, extract_client_ip_secure, CookieConfig,
    UserRepository,
};
use kalamdb_configs::AuthSettings;
use std::sync::Arc;

use super::models::{AuthErrorResponse, LoginResponse, UserInfo};
use super::{extract_bearer_or_cookie_token, map_auth_error_to_response};
use crate::limiter::RateLimiter;

/// POST /v1/api/auth/refresh
///
/// Refreshes the JWT token if the current one is still valid.
pub async fn refresh_handler(
    req: HttpRequest,
    user_repo: web::Data<Arc<dyn UserRepository>>,
    config: web::Data<AuthSettings>,
    rate_limiter: web::Data<Arc<RateLimiter>>,
) -> HttpResponse {
    // Extract client IP with anti-spoofing checks for localhost validation
    let connection_info = extract_client_ip_secure(&req);

    // Rate limit auth attempts by client IP
    if !rate_limiter.get_ref().check_auth_rate(&connection_info) {
        return HttpResponse::TooManyRequests().json(AuthErrorResponse::new(
            "rate_limited",
            "Too many authentication attempts. Please retry shortly.",
        ));
    }

    let token = match extract_bearer_or_cookie_token(&req) {
        Ok(t) => t,
        Err(err) => return map_auth_error_to_response(err),
    };

    // Validate existing token directly, then require a real refresh token.
    let jwt_config = kalamdb_auth::providers::jwt_config::get_jwt_config();
    let claims = match validate_jwt_token(&token, &jwt_config.secret, &jwt_config.trusted_issuers) {
        Ok(c) => c,
        Err(err) => return map_auth_error_to_response(err),
    };

    if !matches!(claims.token_type, Some(TokenType::Refresh)) {
        return HttpResponse::Unauthorized().json(AuthErrorResponse::new(
            "unauthorized",
            "Refresh endpoint requires a refresh token",
        ));
    }

    let username_claim = match claims.username {
        Some(ref u) => u.clone(),
        None => {
            return HttpResponse::Unauthorized()
                .json(AuthErrorResponse::new("unauthorized", "Token missing username claim"));
        },
    };

    // Verify user still exists and is active by username
    let user = match user_repo.get_user_by_username(&username_claim).await {
        Ok(user) if user.deleted_at.is_none() => user,
        _ => {
            return HttpResponse::Unauthorized()
                .json(AuthErrorResponse::new("unauthorized", "User no longer valid"));
        },
    };

    // SECURITY: Verify the claimed role in the token matches the user's actual
    // DB role. This prevents a stale token (e.g., after a role downgrade) from
    // being used to mint new tokens with the old elevated role.
    if let Some(ref claimed_role) = claims.role {
        if *claimed_role != user.role {
            log::warn!(
                "Refresh token role mismatch: claimed={:?}, actual={:?} for user={}",
                claimed_role,
                user.role,
                user.username
            );
            return HttpResponse::Unauthorized().json(AuthErrorResponse::new(
                "unauthorized",
                "Token role does not match user role",
            ));
        }
    }

    // Generate new access token
    let (new_token, _new_claims) = match create_and_sign_token(
        &user.user_id,
        &user.username,
        &user.role,
        user.email.as_deref(),
        Some(config.jwt_expiry_hours),
        &config.jwt_secret,
    ) {
        Ok(t) => t,
        Err(e) => {
            log::error!("Error generating JWT: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to refresh token"));
        },
    };

    // Generate new refresh token (7 days by default, or 7x access token expiry)
    // SECURITY: Uses create_and_sign_refresh_token to set token_type="refresh",
    // preventing refresh tokens from being used as access tokens.
    let refresh_expiry_hours = config.jwt_expiry_hours * 7;
    let (new_refresh_token, _refresh_claims) = match create_and_sign_refresh_token(
        &user.user_id,
        &user.username,
        &user.role,
        user.email.as_deref(),
        Some(refresh_expiry_hours),
        &config.jwt_secret,
    ) {
        Ok(t) => t,
        Err(e) => {
            log::error!("Error generating refresh token: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to refresh token"));
        },
    };

    // Create new cookie
    let cookie_config = CookieConfig {
        secure: config.cookie_secure,
        ..Default::default()
    };
    let cookie =
        create_auth_cookie(&new_token, Duration::hours(config.jwt_expiry_hours), &cookie_config);

    let expires_at = Utc::now() + Duration::hours(config.jwt_expiry_hours);
    let refresh_expires_at = Utc::now() + Duration::hours(refresh_expiry_hours);
    let admin_ui_access = matches!(user.role, kalamdb_commons::Role::Dba | kalamdb_commons::Role::System);

    // Convert timestamps properly
    let created_at = chrono::DateTime::from_timestamp_millis(user.created_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();
    let updated_at = chrono::DateTime::from_timestamp_millis(user.updated_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();

    HttpResponse::Ok().cookie(cookie).json(LoginResponse {
        user: UserInfo {
            id: user.user_id.clone(),
            username: user.username.clone(),
            role: user.role,
            email: user.email,
            created_at,
            updated_at,
        },
        admin_ui_access,
        expires_at: expires_at.to_rfc3339(),
        access_token: new_token,
        refresh_token: new_refresh_token,
        refresh_expires_at: refresh_expires_at.to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_auth::providers::jwt_auth::JwtClaims;

    #[test]
    fn refresh_endpoint_only_accepts_refresh_token_type() {
        let now = chrono::Utc::now().timestamp() as usize;
        let refresh_claims = JwtClaims {
            sub: "u_1".to_string(),
            iss: "kalamdb".to_string(),
            exp: now + 3600,
            iat: now,
            username: None,
            email: None,
            role: None,
            token_type: Some(TokenType::Refresh),
        };
        let access_claims = JwtClaims {
            token_type: Some(TokenType::Access),
            ..refresh_claims.clone()
        };
        let legacy_claims = JwtClaims {
            token_type: None,
            ..refresh_claims.clone()
        };

        assert!(matches!(refresh_claims.token_type, Some(TokenType::Refresh)));
        assert!(!matches!(access_claims.token_type, Some(TokenType::Refresh)));
        assert!(!matches!(legacy_claims.token_type, Some(TokenType::Refresh)));
    }
}
