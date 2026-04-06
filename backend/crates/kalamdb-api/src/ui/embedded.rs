//! Embedded Admin UI assets
//!
//! This module embeds the compiled React Admin UI into the server binary.
//! Files are compressed at compile time and served with proper MIME types.
//!
//! To update the embedded UI:
//! 1. Run `npm run build` in the `ui/` directory
//! 2. Rebuild the server with `cargo build`

use actix_web::{web, HttpRequest, HttpResponse};
use log::debug;
use rust_embed::Embed;

use super::UiRuntimeConfig;

/// Embedded UI assets from ui/dist directory
/// Files are compressed at compile time using rust-embed's compression feature
#[derive(Embed)]
#[folder = "../../../ui/dist"]
#[prefix = ""]
#[exclude = "*.map"]
#[exclude = "kalam_client_bg.wasm"]
struct UiAssets;

/// Serve embedded UI assets
///
/// Handles requests to /ui/* and serves the appropriate static file.
/// Falls back to index.html for client-side routing (SPA behavior).
pub async fn serve_embedded_ui(req: HttpRequest) -> HttpResponse {
    let path = req.match_info().query("path");

    // Normalize path - remove leading slash if present
    let path = path.trim_start_matches('/');

    // Empty path means index.html
    let path = if path.is_empty() { "index.html" } else { path };

    debug!("[embedded_ui] Serving path: {}", path);

    // Try to find the exact file
    if let Some(content) = UiAssets::get(path) {
        let mime_type = mime_guess::from_path(path).first_or_octet_stream().to_string();

        debug!("[embedded_ui] Found file: {} (mime: {})", path, mime_type);

        return HttpResponse::Ok().content_type(mime_type).body(content.data.into_owned());
    }

    debug!("[embedded_ui] File not found: {}, falling back to index.html", path);

    // For non-existent paths, serve index.html (SPA fallback)
    // This allows client-side routing to work
    if let Some(index) = UiAssets::get("index.html") {
        return HttpResponse::Ok()
            .content_type("text/html; charset=utf-8")
            .body(index.data.into_owned());
    }

    // No UI built - show helpful message
    HttpResponse::NotFound().body(
        "<html><body>\
        <h1>Admin UI Not Built</h1>\
        <p>The Admin UI assets were not found in the binary.</p>\
        <p>To build the UI:</p>\
        <ol>\
        <li>Run <code>cd ui && npm install && npm run build</code></li>\
        <li>Rebuild the server with <code>cargo build</code></li>\
        </ol>\
        </body></html>",
    )
}

/// Configure embedded UI routes
///
/// Mounts the embedded UI at /ui with fallback for SPA routing.
pub fn configure_embedded_ui(cfg: &mut web::ServiceConfig, runtime_config: UiRuntimeConfig) {
    let runtime_config = web::Data::new(runtime_config);

    cfg.app_data(runtime_config).service(
        web::scope("/ui")
            .route("/runtime-config.js", web::get().to(super::serve_runtime_config_script))
            // Catch-all for UI routes
            .route("", web::get().to(serve_embedded_ui))
            .route("/", web::get().to(serve_embedded_ui))
            .route("/{path:.*}", web::get().to(serve_embedded_ui)),
    );
}

/// Check if UI assets are available in the binary
pub fn is_ui_embedded() -> bool {
    UiAssets::get("index.html").is_some()
}
