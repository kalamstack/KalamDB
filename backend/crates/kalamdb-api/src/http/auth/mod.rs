//! Authentication handlers for Admin UI and API
//!
//! Provides login, logout, refresh, current user, and server-setup endpoints.
//!
//! ## Endpoints
//! - POST /v1/api/auth/login - Authenticate user and get JWT tokens
//! - POST /v1/api/auth/refresh - Refresh JWT tokens
//! - POST /v1/api/auth/logout - Clear authentication cookie
//! - GET /v1/api/auth/me - Get current user info
//! - POST /v1/api/auth/setup - Initial server setup (localhost only)
//! - GET /v1/api/auth/status - Check server setup status

pub mod models;

mod login;
mod logout;
mod me;
mod refresh;
mod setup;

pub(crate) use login::login_handler;
pub(crate) use logout::logout_handler;
pub(crate) use me::me_handler;
pub(crate) use refresh::refresh_handler;
pub(crate) use setup::{server_setup_handler, setup_status_handler};

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use kalamdb_auth::{extract_auth_token, extract_refresh_token, AuthError};
use models::AuthErrorResponse;

/// Map authentication errors to HTTP responses
///
/// Uses generic error messages to prevent user enumeration attacks.
/// Sensitive errors (user not found, wrong password) return the same
/// "Invalid username or password" message.
pub(crate) fn map_auth_error_to_response(err: AuthError) -> HttpResponse {
    match err {
        AuthError::InvalidCredentials(_)
        | AuthError::UserNotFound(_)
        | AuthError::UserDeleted
        | AuthError::AuthenticationFailed(_) => HttpResponse::Unauthorized()
            .json(AuthErrorResponse::new("unauthorized", "Invalid username or password")),
        AuthError::SetupRequired(message) => {
            // HTTP 428 Precondition Required - server requires initial setup
            HttpResponse::build(actix_web::http::StatusCode::PRECONDITION_REQUIRED)
                .json(AuthErrorResponse::new("setup_required", message))
        },
        AuthError::AccountLocked(message) => {
            HttpResponse::Unauthorized().json(AuthErrorResponse::new("account_locked", message))
        },
        AuthError::RemoteAccessDenied(message) | AuthError::InsufficientPermissions(message) => {
            HttpResponse::Forbidden().json(AuthErrorResponse::new("forbidden", message))
        },
        AuthError::MalformedAuthorization(message)
        | AuthError::MissingAuthorization(message)
        | AuthError::MissingClaim(message)
        | AuthError::WeakPassword(message) => {
            HttpResponse::Unauthorized().json(AuthErrorResponse::new("unauthorized", message))
        },
        AuthError::TokenExpired | AuthError::InvalidSignature | AuthError::UntrustedIssuer(_) => {
            HttpResponse::Unauthorized()
                .json(AuthErrorResponse::new("unauthorized", "Invalid username or password"))
        },
        AuthError::DatabaseError(_) | AuthError::HashingError(_) => {
            HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Authentication failed"))
        },
    }
}

/// Extract auth token from `Authorization: Bearer ...` or fallback to auth cookie.
///
/// Header token is preferred so API calls can stay in sync with the same JWT used by
/// `@kalamdb/client` in the admin UI.
pub(crate) fn extract_bearer_or_cookie_token(req: &HttpRequest) -> Result<String, AuthError> {
    if let Some(raw_header) = req.headers().get(actix_web::http::header::AUTHORIZATION) {
        let auth_header = raw_header.to_str().map_err(|_| {
            AuthError::MalformedAuthorization(
                "Authorization header contains invalid characters".to_string(),
            )
        })?;

        let mut parts = auth_header.splitn(2, ' ');
        let scheme = parts.next().unwrap_or_default().trim();
        let token = parts.next().unwrap_or_default().trim();

        if !scheme.eq_ignore_ascii_case("Bearer") {
            return Err(AuthError::MalformedAuthorization(
                "Authorization header must use Bearer token".to_string(),
            ));
        }

        if token.is_empty() {
            return Err(AuthError::MalformedAuthorization("Bearer token missing".to_string()));
        }

        return Ok(token.to_string());
    }

    extract_auth_token(req.cookies().ok().iter().flat_map(|c| c.iter().cloned()))
        .ok_or_else(|| AuthError::MissingAuthorization("No auth token found".to_string()))
}

/// Extract refresh token from refresh cookie, or fall back to bearer auth.
pub(crate) fn extract_refresh_or_bearer_token(req: &HttpRequest) -> Result<String, AuthError> {
    if let Some(token) =
        extract_refresh_token(req.cookies().ok().iter().flat_map(|c| c.iter().cloned()))
    {
        return Ok(token);
    }

    if let Some(raw_header) = req.headers().get(actix_web::http::header::AUTHORIZATION) {
        let auth_header = raw_header.to_str().map_err(|_| {
            AuthError::MalformedAuthorization(
                "Authorization header contains invalid characters".to_string(),
            )
        })?;

        let mut parts = auth_header.splitn(2, ' ');
        let scheme = parts.next().unwrap_or_default().trim();
        let token = parts.next().unwrap_or_default().trim();

        if !scheme.eq_ignore_ascii_case("Bearer") {
            return Err(AuthError::MalformedAuthorization(
                "Authorization header must use Bearer token".to_string(),
            ));
        }

        if token.is_empty() {
            return Err(AuthError::MalformedAuthorization("Bearer token missing".to_string()));
        }

        return Ok(token.to_string());
    }

    Err(AuthError::MissingAuthorization("No refresh token found".to_string()))
}
