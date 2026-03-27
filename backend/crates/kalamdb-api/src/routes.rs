//! API routes configuration
//!
//! This module configures all HTTP and WebSocket routes for the KalamDB API.

#[cfg(feature = "embedded-ui")]
use crate::embedded_ui;
use crate::handlers;
use actix_web::{web, HttpRequest, HttpResponse};
use kalamdb_auth::extract_client_ip_secure;
use kalamdb_core::metrics::{BUILD_DATE, SERVER_VERSION};
use serde_json::json;

/// Configure API routes for KalamDB
///
/// Health check endpoints (both point to same handler, localhost-only):
/// - GET /health - Simple health check (root level, no version prefix)
/// - GET /v1/api/healthcheck - Health check endpoint (versioned API path)
/// - GET /v1/api/cluster/health - Cluster health with OpenRaft metrics (local/auth required)
///
/// Other endpoints use the /v1 version prefix:
/// - POST /v1/api/sql - Execute SQL statements (requires Authorization header)
/// - GET /v1/ws - WebSocket connection for live query subscriptions
/// - POST /v1/api/auth/login - Admin UI login
/// - POST /v1/api/auth/refresh - Refresh auth token
/// - POST /v1/api/auth/logout - Logout and clear cookie
/// - GET /v1/api/auth/me - Get current user info
/// - POST /v1/api/auth/setup - Initial server setup (localhost only, requires no password on root)
/// - GET /v1/api/auth/status - Check server setup status (localhost only)
/// - POST /v1/api/topics/consume - Consume messages from a topic (long polling)
/// - POST /v1/api/topics/ack - Acknowledge offset for consumer group
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        // Root-level health check endpoint (no version prefix)
        .route("/health", web::get().to(healthcheck_handler))
        // Versioned API routes
        .service(
            web::scope("/v1")
                .service(
                    web::scope("/api")
                        .service(handlers::execute_sql_v1)
                        // Also support health check at versioned path
                        .route("/healthcheck", web::get().to(healthcheck_handler))
                        // Cluster health endpoint (with OpenRaft metrics)
                        .service(
                            web::scope("/cluster")
                                .route("/health", web::get().to(handlers::cluster_health_handler)),
                        )
                        // Auth routes for Admin UI
                        .service(
                            web::scope("/auth")
                                .route("/login", web::post().to(handlers::login_handler))
                                .route("/refresh", web::post().to(handlers::refresh_handler))
                                .route("/logout", web::post().to(handlers::logout_handler))
                                .route("/me", web::get().to(handlers::me_handler))
                                .route("/setup", web::post().to(handlers::server_setup_handler))
                                .route("/status", web::get().to(handlers::setup_status_handler)),
                        )
                        // Topic pub/sub endpoints
                        .service(
                            web::scope("/topics")
                                .service(handlers::consume_handler)
                                .service(handlers::ack_handler),
                        ),
                )
                // File download endpoint (outside of /api scope for shorter URLs)
                .service(handlers::download_file)
                // Export download endpoint
                .service(handlers::download_export)
                .service(handlers::websocket_handler),
        );
}

/// Configure embedded UI routes (recommended - UI is compiled into binary)
///
/// Serves the Admin UI from embedded assets at /ui route.
/// The UI is compressed and included in the binary at compile time.
#[cfg(feature = "embedded-ui")]
pub fn configure_embedded_ui_routes(cfg: &mut web::ServiceConfig) {
    embedded_ui::configure_embedded_ui(cfg);
}

/// Check if embedded UI is available
#[cfg(feature = "embedded-ui")]
pub fn is_embedded_ui_available() -> bool {
    embedded_ui::is_ui_embedded()
}

/// Check if embedded UI is available (always false when feature is disabled)
#[cfg(not(feature = "embedded-ui"))]
pub fn is_embedded_ui_available() -> bool {
    false
}

/// Configure static file serving for Admin UI (filesystem fallback)
///
/// Serves the built React app from /ui route using filesystem.
/// Use this only if you need to serve UI from a custom path.
/// For most cases, use `configure_embedded_ui_routes` instead.
pub fn configure_ui_routes(cfg: &mut web::ServiceConfig, ui_path: &str) {
    use std::path::PathBuf;

    let ui_path = PathBuf::from(ui_path);
    let index_path = ui_path.join("index.html");

    // Store index content for SPA fallback
    let index_content = std::fs::read_to_string(&index_path).unwrap_or_else(|_| {
        "<html><body><h1>Admin UI not built</h1><p>Run 'pnpm build' in ui/ directory</p></body></html>".to_string()
    });
    let index_content = web::Data::new(index_content);

    cfg.app_data(index_content.clone())
        .service(
            actix_files::Files::new("/ui", ui_path)
                .index_file("index.html")
                .default_handler(web::to(move |data: web::Data<String>| {
                    let content = data.get_ref().clone();
                    async move {
                        HttpResponse::Ok()
                            .content_type("text/html; charset=utf-8")
                            .body(content)
                    }
                })),
        );
}

/// Health check endpoint handler (localhost-only)
async fn healthcheck_handler(req: HttpRequest) -> HttpResponse {
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
