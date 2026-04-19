//! Server-wide middleware configuration helpers.
//!
//! Keeps the Actix application setup focused by providing
//! reusable constructors for CORS, logging, and connection protection layers.
//!
//! ## Protection Middleware Stack (applied in order)
//!
//! 1. **ConnectionProtection**: First line of defense - drops abusive IPs early
//! 2. **CORS**: Cross-origin resource sharing policy (via actix-cors)
//! 3. **Logger**: Request/response logging
//!
//! ## DoS Protection Features
//!
//! - Per-IP connection limits
//! - Per-IP request rate limits (pre-authentication)
//! - Request body size limits
//! - Automatic IP banning for persistent abusers

use crate::connection_guard::{ConnectionGuard, ConnectionGuardResult};
use actix_cors::Cors;
use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::{header::HeaderName, Method, StatusCode};
use actix_web::{Error, HttpResponse};
use futures_util::future::LocalBoxFuture;
use kalamdb_auth::extract_client_ip_addr_secure;
use kalamdb_configs::{RateLimitSettings, ServerConfig};
use log::warn;
use std::future::{ready, Ready};
use std::net::IpAddr;
use std::sync::Arc;

/// Build CORS middleware from server configuration using actix-cors.
///
/// Maps all CorsSettings options to actix-cors builder methods.
/// See: https://docs.rs/actix-cors/latest/actix_cors/struct.Cors.html
pub fn build_cors_from_config(config: &ServerConfig) -> Cors {
    let cors_config = &config.security.cors;

    let mut cors = Cors::default();

    // Configure allowed origins
    if cors_config.allowed_origins.is_empty()
        || cors_config.allowed_origins.contains(&"*".to_string())
    {
        cors = cors.allow_any_origin();
    } else {
        for origin in &cors_config.allowed_origins {
            cors = cors.allowed_origin(origin);
        }
    }

    // Configure allowed methods
    let methods: Vec<Method> =
        cors_config.allowed_methods.iter().filter_map(|m| m.parse().ok()).collect();
    if !methods.is_empty() {
        cors = cors.allowed_methods(methods);
    }

    // Configure allowed headers
    if cors_config.allowed_headers.contains(&"*".to_string()) {
        cors = cors.allow_any_header();
    } else {
        let headers: Vec<HeaderName> =
            cors_config.allowed_headers.iter().filter_map(|h| h.parse().ok()).collect();
        if !headers.is_empty() {
            cors = cors.allowed_headers(headers);
        }
    }

    // Configure exposed headers
    if !cors_config.expose_headers.is_empty() {
        let expose_headers: Vec<HeaderName> =
            cors_config.expose_headers.iter().filter_map(|h| h.parse().ok()).collect();
        cors = cors.expose_headers(expose_headers);
    }

    // Configure credentials
    if cors_config.allow_credentials {
        cors = cors.supports_credentials();
    }

    // Configure max age
    cors = cors.max_age(cors_config.max_age as usize);

    cors
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::{header, Method};
    use actix_web::{test, web, App, HttpResponse};

    #[actix_web::test]
    async fn preflight_login_request_allows_vite_chat_origin() {
        let mut config = ServerConfig::default();
        config.security.cors.allowed_origins = vec![
            "http://localhost:5174".to_string(),
            "http://127.0.0.1:5174".to_string(),
        ];
        config.security.cors.allowed_methods = vec!["POST".to_string(), "OPTIONS".to_string()];
        config.security.cors.allowed_headers =
            vec!["Authorization".to_string(), "Content-Type".to_string()];
        config.security.cors.allow_credentials = true;

        let app = test::init_service(
            App::new()
                .wrap(build_cors_from_config(&config))
                .route("/v1/api/auth/login", web::post().to(HttpResponse::Ok)),
        )
        .await;

        let request = test::TestRequest::default()
            .method(Method::OPTIONS)
            .uri("/v1/api/auth/login")
            .insert_header((header::ORIGIN, "http://localhost:5174"))
            .insert_header((header::ACCESS_CONTROL_REQUEST_METHOD, "POST"))
            .insert_header((header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type,authorization"))
            .to_request();

        let response = test::call_service(&app, request).await;

        assert!(response.status().is_success());
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:5174")
        );
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
    }
}

// ============================================================================
// Connection Protection Middleware
// ============================================================================

/// Connection protection middleware factory.
///
/// This middleware provides the first line of defense against DoS attacks
/// by checking requests BEFORE any expensive processing happens.
///
/// Protection includes:
/// - Per-IP connection limits
/// - Per-IP request rate limits
/// - Request body size limits
/// - Automatic IP banning for abusive clients
#[derive(Clone)]
pub struct ConnectionProtection {
    guard: Arc<ConnectionGuard>,
}

impl ConnectionProtection {
    /// Create connection protection with default settings
    pub fn new() -> Self {
        Self {
            guard: Arc::new(ConnectionGuard::new()),
        }
    }

    /// Create connection protection with custom configuration
    pub fn with_config(config: &RateLimitSettings) -> Self {
        Self {
            guard: Arc::new(ConnectionGuard::with_config(config)),
        }
    }

    /// Create connection protection from server config
    pub fn from_server_config(config: &ServerConfig) -> Self {
        Self::with_config(&config.rate_limit)
    }

    /// Get the underlying connection guard (for monitoring/management)
    pub fn guard(&self) -> &Arc<ConnectionGuard> {
        &self.guard
    }
}

impl Default for ConnectionProtection {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, B> Transform<S, ServiceRequest> for ConnectionProtection
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type InitError = ();
    type Transform = ConnectionProtectionMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ConnectionProtectionMiddleware {
            service,
            guard: self.guard.clone(),
        }))
    }
}

/// The actual middleware service that checks each request.
pub struct ConnectionProtectionMiddleware<S> {
    service: S,
    guard: Arc<ConnectionGuard>,
}

impl<S, B> Service<ServiceRequest> for ConnectionProtectionMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Extract client IP
        let client_ip = extract_client_ip(&req);

        // Get content-length for body size check
        let content_length = req
            .headers()
            .get("content-length")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());

        // Check the request against the connection guard
        let result = self.guard.check_request(client_ip, content_length);

        match result {
            ConnectionGuardResult::Allowed => {
                // Request is allowed, proceed with the service
                let fut = self.service.call(req);
                Box::pin(async move {
                    let res = fut.await?;
                    Ok(res.map_into_left_body())
                })
            },

            ConnectionGuardResult::Banned { until: _ } => {
                warn!("[CONN_PROTECT] Rejected banned IP: {} path={}", client_ip, req.path());

                let response = HttpResponse::build(StatusCode::TOO_MANY_REQUESTS)
                    .insert_header(("Retry-After", "300"))
                    .insert_header(("X-RateLimit-Reset", "300"))
                    .json(serde_json::json!({
                        "error": "IP_BANNED",
                        "message": "Your IP has been temporarily banned due to excessive requests",
                        "retry_after_seconds": 300
                    }));

                Box::pin(async move { Ok(req.into_response(response).map_into_right_body()) })
            },

            ConnectionGuardResult::TooManyConnections { current, max } => {
                warn!(
                    "[CONN_PROTECT] Connection limit exceeded: IP={} current={} max={} path={}",
                    client_ip,
                    current,
                    max,
                    req.path()
                );

                let response = HttpResponse::build(StatusCode::SERVICE_UNAVAILABLE)
                    .insert_header(("Retry-After", "5"))
                    .json(serde_json::json!({
                        "error": "TOO_MANY_CONNECTIONS",
                        "message": "Too many connections from your IP address",
                        "current": current,
                        "max": max
                    }));

                Box::pin(async move { Ok(req.into_response(response).map_into_right_body()) })
            },

            ConnectionGuardResult::RateLimitExceeded => {
                warn!("[CONN_PROTECT] Rate limit exceeded: IP={} path={}", client_ip, req.path());

                let response = HttpResponse::build(StatusCode::TOO_MANY_REQUESTS)
                    .insert_header(("Retry-After", "1"))
                    .insert_header(("X-RateLimit-Reset", "1"))
                    .json(serde_json::json!({
                        "error": "RATE_LIMIT_EXCEEDED",
                        "message": "Too many requests from your IP address",
                        "retry_after_seconds": 1
                    }));

                Box::pin(async move { Ok(req.into_response(response).map_into_right_body()) })
            },

            ConnectionGuardResult::BodyTooLarge { size, max } => {
                warn!(
                    "[CONN_PROTECT] Request body too large: IP={} size={} max={} path={}",
                    client_ip,
                    size,
                    max,
                    req.path()
                );

                let response =
                    HttpResponse::build(StatusCode::PAYLOAD_TOO_LARGE).json(serde_json::json!({
                        "error": "BODY_TOO_LARGE",
                        "message": "Request body exceeds maximum allowed size",
                        "size_bytes": size,
                        "max_bytes": max
                    }));

                Box::pin(async move { Ok(req.into_response(response).map_into_right_body()) })
            },
        }
    }
}

/// Extract client IP from request, handling proxies
///
/// Security: Rejects localhost values in proxy headers to prevent rate limit bypass.
/// Attackers cannot spoof X-Forwarded-For: 127.0.0.1 to bypass protections.
fn extract_client_ip(req: &ServiceRequest) -> IpAddr {
    extract_client_ip_addr_secure(req.peer_addr().map(|addr| addr.ip()), req.headers())
        .unwrap_or_else(|| "127.0.0.1".parse().unwrap())
}
