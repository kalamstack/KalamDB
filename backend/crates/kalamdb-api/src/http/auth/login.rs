//! Login handler
//!
//! POST /v1/api/auth/login - Authenticates a user and returns JWT tokens

use std::sync::Arc;

use actix_web::{web, HttpRequest, HttpResponse};
use chrono::{Duration, Utc};
use kalamdb_auth::{
    authenticate, create_and_sign_token, create_auth_cookie, create_refresh_cookie,
    extract_client_ip_secure, providers::jwt_auth::create_and_sign_refresh_token, AuthRequest,
    CookieConfig, UserRepository,
};
use kalamdb_commons::Role;
use kalamdb_configs::AuthSettings;
use kalamdb_core::app_context::AppContext;
use kalamdb_jobs::health_monitor::record_activity_now;

use super::{
    audit, map_auth_error_to_response,
    models::{AuthErrorResponse, LoginRequest, LoginResponse, UserInfo},
};
use crate::limiter::RateLimiter;

/// POST /v1/api/auth/login
///
/// Authenticates a user and returns JWT tokens for API usage.
/// The response also includes `admin_ui_access` so browser clients can
/// distinguish normal API tokens from accounts allowed to enter the Admin UI.
pub async fn login_handler(
    req: HttpRequest,
    app_context: web::Data<Arc<AppContext>>,
    user_repo: web::Data<Arc<dyn UserRepository>>,
    config: web::Data<AuthSettings>,
    rate_limiter: web::Data<Arc<RateLimiter>>,
    body: web::Json<LoginRequest>,
) -> HttpResponse {
    record_activity_now();

    // Extract client IP with anti-spoofing checks for localhost validation
    let connection_info = extract_client_ip_secure(&req);

    // Rate limit auth attempts by client IP
    if !rate_limiter.get_ref().check_auth_rate(&connection_info) {
        return HttpResponse::TooManyRequests().json(AuthErrorResponse::new(
            "rate_limited",
            "Too many authentication attempts. Please retry shortly.",
        ));
    }

    // Authenticate using unified auth flow (includes localhost/empty password rules)
    let auth_request = AuthRequest::Credentials {
        user: body.user.clone(),
        password: body.password.clone(),
    };

    let auth_result = match authenticate(auth_request, &connection_info, user_repo.get_ref()).await
    {
        Ok(result) => result,
        Err(err) => return map_auth_error_to_response(err),
    };

    let user = auth_result.user;
    let admin_ui_access = matches!(user.role, Role::Dba | Role::System);

    // Generate JWT access token
    let (token, _claims) = match create_and_sign_token(
        &user.user_id,
        &user.role,
        user.email.as_deref(),
        Some(config.jwt_expiry_hours),
        &config.jwt_secret,
    ) {
        Ok(t) => t,
        Err(e) => {
            log::error!("Error generating JWT: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to generate token"));
        },
    };

    // Generate refresh token (7 days by default, or 7x access token expiry)
    // SECURITY: Uses create_and_sign_refresh_token to set token_type="refresh",
    // preventing refresh tokens from being used as access tokens.
    let refresh_expiry_hours = config.jwt_expiry_hours * 7;
    let (refresh_token, _refresh_claims) = match create_and_sign_refresh_token(
        &user.user_id,
        &user.role,
        user.email.as_deref(),
        Some(refresh_expiry_hours),
        &config.jwt_secret,
    ) {
        Ok(t) => t,
        Err(e) => {
            log::error!("Error generating refresh token: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to generate token"));
        },
    };

    // Create HttpOnly cookie
    let cookie_config = CookieConfig {
        secure: config.cookie_secure && req.connection_info().scheme() == "https",
        ..Default::default()
    };
    let auth_cookie =
        create_auth_cookie(&token, Duration::hours(config.jwt_expiry_hours), &cookie_config);
    let refresh_cookie = create_refresh_cookie(
        &refresh_token,
        Duration::hours(refresh_expiry_hours),
        &cookie_config,
    );

    let expires_at = Utc::now() + Duration::hours(config.jwt_expiry_hours);
    let refresh_expires_at = Utc::now() + Duration::hours(refresh_expiry_hours);

    // Convert timestamps properly
    let created_at = chrono::DateTime::from_timestamp_millis(user.created_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();
    let updated_at = chrono::DateTime::from_timestamp_millis(user.updated_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();

    if admin_ui_access {
        audit::record_admin_login(app_context.get_ref(), &user.user_id, &connection_info).await;
    }

    HttpResponse::Ok()
        .cookie(auth_cookie)
        .cookie(refresh_cookie)
        .json(LoginResponse {
            user: UserInfo {
                id: user.user_id,
                role: user.role,
                email: user.email,
                created_at,
                updated_at,
            },
            admin_ui_access,
            expires_at: expires_at.to_rfc3339(),
            access_token: token,
            refresh_token,
            refresh_expires_at: refresh_expires_at.to_rfc3339(),
        })
}
