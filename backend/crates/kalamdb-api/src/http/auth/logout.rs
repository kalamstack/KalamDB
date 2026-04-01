//! Logout handler
//!
//! POST /v1/api/auth/logout - Clears authentication cookie

use actix_web::{web, HttpRequest, HttpResponse};
use kalamdb_auth::{create_logout_cookie, create_refresh_logout_cookie, CookieConfig};
use kalamdb_configs::AuthSettings;

/// POST /v1/api/auth/logout
///
/// Clears the authentication cookie.
pub async fn logout_handler(req: HttpRequest, config: web::Data<AuthSettings>) -> HttpResponse {
    let cookie_config = CookieConfig {
        secure: config.cookie_secure && req.connection_info().scheme() == "https",
        ..Default::default()
    };
    let auth_cookie = create_logout_cookie(&cookie_config);
    let refresh_cookie = create_refresh_logout_cookie(&cookie_config);

    HttpResponse::Ok()
        .cookie(auth_cookie)
        .cookie(refresh_cookie)
        .json(serde_json::json!({
            "message": "Logged out successfully"
        }))
}
