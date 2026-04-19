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

mod audit;
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
/// "Invalid credentials" message.
pub(crate) fn map_auth_error_to_response(err: AuthError) -> HttpResponse {
    match err {
        AuthError::InvalidCredentials(_)
        | AuthError::UserNotFound(_)
        | AuthError::UserDeleted
        | AuthError::AuthenticationFailed(_) => HttpResponse::Unauthorized()
            .json(AuthErrorResponse::new("unauthorized", "Invalid credentials")),
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
        | AuthError::WeakPassword(message) => {
            HttpResponse::Unauthorized().json(AuthErrorResponse::new("unauthorized", message))
        },
        AuthError::MissingClaim(_) => HttpResponse::Unauthorized()
            .json(AuthErrorResponse::new("unauthorized", "Token is missing required claims")),
        AuthError::TokenExpired | AuthError::InvalidSignature | AuthError::UntrustedIssuer(_) => {
            HttpResponse::Unauthorized()
                .json(AuthErrorResponse::new("unauthorized", "Invalid credentials"))
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

#[cfg(test)]
mod tests {
    use super::map_auth_error_to_response;
    use actix_web::{body::to_bytes, http::StatusCode, HttpResponse};
    use kalamdb_auth::AuthError;
    use serde_json::Value;

    async fn response_json(response: HttpResponse) -> Value {
        let body = to_bytes(response.into_body())
            .await
            .expect("auth response body should be readable");
        serde_json::from_slice(&body).expect("auth response body should be valid JSON")
    }

    #[actix_rt::test]
    async fn sensitive_auth_failures_are_redacted() {
        let sensitive_errors = vec![
            AuthError::InvalidCredentials("wrong password".to_string()),
            AuthError::UserNotFound("missing-user".to_string()),
            AuthError::UserDeleted,
            AuthError::AuthenticationFailed("provider rejected token".to_string()),
            AuthError::TokenExpired,
            AuthError::InvalidSignature,
            AuthError::UntrustedIssuer("evil-issuer".to_string()),
        ];

        for error in sensitive_errors {
            let response = map_auth_error_to_response(error);
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let body = response_json(response).await;
            assert_eq!(body["error"], "unauthorized");
            assert_eq!(body["message"], "Invalid credentials");
        }
    }

    #[actix_rt::test]
    async fn internal_auth_failures_do_not_leak_backend_details() {
        let response = map_auth_error_to_response(AuthError::DatabaseError(
            "database connection reset by peer".to_string(),
        ));

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = response_json(response).await;
        assert_eq!(body["error"], "internal_error");
        assert_eq!(body["message"], "Authentication failed");
    }
}
