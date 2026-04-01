//! Current user handler
//!
//! GET /v1/api/auth/me - Returns information about the currently authenticated user

use actix_web::{web, HttpRequest, HttpResponse};
use kalamdb_auth::{authenticate, extract_client_ip_secure, AuthRequest, UserRepository};
use std::sync::Arc;

use super::models::{AuthErrorResponse, UserInfo};
use super::{extract_bearer_or_cookie_token, map_auth_error_to_response};

/// GET /v1/api/auth/me
///
/// Returns information about the currently authenticated user.
pub async fn me_handler(
    req: HttpRequest,
    user_repo: web::Data<Arc<dyn UserRepository>>,
) -> HttpResponse {
    let token = match extract_bearer_or_cookie_token(&req) {
        Ok(t) => t,
        Err(err) => return map_auth_error_to_response(err),
    };

    // Validate token via unified auth (uses configured trusted issuers)
    let connection_info = extract_client_ip_secure(&req);
    let auth_request = AuthRequest::Jwt { token };
    let auth_result = match authenticate(auth_request, &connection_info, user_repo.get_ref()).await
    {
        Ok(result) => result,
        Err(err) => return map_auth_error_to_response(err),
    };

    // Get current user info
    let username_typed = auth_result.user.username.clone();
    let user = match user_repo.get_user_by_username(&username_typed).await {
        Ok(user) if user.deleted_at.is_none() => user,
        _ => {
            return HttpResponse::Unauthorized()
                .json(AuthErrorResponse::new("unauthorized", "User not found"));
        },
    };

    // Convert timestamps properly
    let created_at = chrono::DateTime::from_timestamp_millis(user.created_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();
    let updated_at = chrono::DateTime::from_timestamp_millis(user.updated_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();

    HttpResponse::Ok().json(UserInfo {
        id: user.user_id.clone(),
        username: user.username.clone(),
        role: user.role,
        email: user.email,
        created_at,
        updated_at,
    })
}
