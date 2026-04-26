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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use actix_web::{body::to_bytes, http::StatusCode, test::TestRequest};
    use serde_json::Value;

    use super::healthcheck_handler;

    async fn execute_healthcheck(req: actix_web::HttpRequest) -> (StatusCode, Value) {
        let response = healthcheck_handler(req).await;
        let status = response.status();
        let body = to_bytes(response.into_body())
            .await
            .expect("healthcheck response body should be readable");
        let json = serde_json::from_slice(&body).expect("healthcheck response should be JSON");
        (status, json)
    }

    #[actix_rt::test]
    async fn healthcheck_allows_localhost_peer() {
        let request = TestRequest::default()
            .peer_addr(SocketAddr::from(([127, 0, 0, 1], 8080)))
            .to_http_request();

        let (status, body) = execute_healthcheck(request).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "healthy");
    }

    #[actix_rt::test]
    async fn healthcheck_rejects_remote_peer() {
        let request = TestRequest::default()
            .peer_addr(SocketAddr::from(([198, 51, 100, 8], 8080)))
            .to_http_request();

        let (status, body) = execute_healthcheck(request).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "Access denied. Health endpoint is localhost-only.");
    }

    #[actix_rt::test]
    async fn healthcheck_rejects_spoofed_localhost_proxy_header() {
        let request = TestRequest::default()
            .insert_header(("X-Forwarded-For", "127.0.0.1"))
            .peer_addr(SocketAddr::from(([198, 51, 100, 8], 8080)))
            .to_http_request();

        let (status, body) = execute_healthcheck(request).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "Access denied. Health endpoint is localhost-only.");
    }
}
