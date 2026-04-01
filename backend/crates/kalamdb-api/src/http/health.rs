use actix_web::{HttpRequest, HttpResponse};
use kalamdb_auth::extract_client_ip_secure;
use kalamdb_core::metrics::{BUILD_DATE, SERVER_VERSION};
use serde_json::json;

pub(crate) async fn healthcheck_handler(req: HttpRequest) -> HttpResponse {
    let connection_info = extract_client_ip_secure(&req);
    if !connection_info.is_localhost() {
        return HttpResponse::Forbidden().json(json!({
            "error": "Access denied. Health endpoint is localhost-only."
        }));
    }

    HttpResponse::Ok().json(json!({
        "status": "healthy",
        "version": SERVER_VERSION,
        "api_version": "v1",
        "build_date": BUILD_DATE,
    }))
}
