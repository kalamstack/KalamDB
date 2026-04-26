//! API routes configuration
//!
//! This module configures all HTTP and WebSocket routes for the KalamDB API.

use actix_web::web;

use crate::{http, ui, ws};

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
                        .service(http::sql::execute_sql_v1)
                        // Also support health check at versioned path
                        .route("/healthcheck", web::get().to(healthcheck_handler))
                        // Cluster health endpoint (with OpenRaft metrics)
                        .service(
                            web::scope("/cluster")
                                .route(
                                    "/health",
                                    web::get().to(http::cluster::cluster_health_handler),
                                ),
                        )
                        // Auth routes for Admin UI
                        .service(
                            web::scope("/auth")
                                .route("/login", web::post().to(http::auth::login_handler))
                                .route(
                                    "/refresh",
                                    web::post().to(http::auth::refresh_handler),
                                )
                                .route("/logout", web::post().to(http::auth::logout_handler))
                                .route("/me", web::get().to(http::auth::me_handler))
                                .route(
                                    "/setup",
                                    web::post().to(http::auth::server_setup_handler),
                                )
                                .route(
                                    "/status",
                                    web::get().to(http::auth::setup_status_handler),
                                ),
                        )
                        // Topic pub/sub endpoints
                        .service(
                            web::scope("/topics")
                                .service(http::topics::consume_handler)
                                .service(http::topics::ack_handler),
                        ),
                )
                // File download endpoint (outside of /api scope for shorter URLs)
                .service(http::files::download_file)
                // Export download endpoint
                .service(http::files::download_export)
                .service(ws::websocket_handler),
        );
}

/// Configure embedded UI routes (recommended - UI is compiled into binary)
///
/// Serves the Admin UI from embedded assets at /ui route.
/// The UI is compressed and included in the binary at compile time.
#[cfg(feature = "embedded-ui")]
pub fn configure_embedded_ui_routes(
    cfg: &mut web::ServiceConfig,
    runtime_config: ui::UiRuntimeConfig,
) {
    ui::configure_embedded_ui_routes(cfg, runtime_config);
}

/// Check if embedded UI is available
#[cfg(feature = "embedded-ui")]
pub fn is_embedded_ui_available() -> bool {
    ui::is_embedded_ui_available()
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
pub fn configure_ui_routes(
    cfg: &mut web::ServiceConfig,
    ui_path: &str,
    runtime_config: ui::UiRuntimeConfig,
) {
    ui::configure_filesystem_ui_routes(cfg, ui_path, runtime_config);
}

/// Health check endpoint handler (localhost-only)
async fn healthcheck_handler(req: actix_web::HttpRequest) -> actix_web::HttpResponse {
    http::healthcheck_handler(req).await
}
