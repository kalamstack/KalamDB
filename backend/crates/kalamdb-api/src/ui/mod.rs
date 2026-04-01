//! UI transport helpers for embedded and filesystem-served admin assets.

#[cfg(feature = "embedded-ui")]
mod embedded;

use actix_web::{web, HttpResponse};
use std::path::PathBuf;

#[cfg(feature = "embedded-ui")]
pub fn configure_embedded_ui_routes(cfg: &mut web::ServiceConfig) {
    embedded::configure_embedded_ui(cfg);
}

#[cfg(feature = "embedded-ui")]
pub fn is_embedded_ui_available() -> bool {
    embedded::is_ui_embedded()
}

#[cfg(not(feature = "embedded-ui"))]
pub fn is_embedded_ui_available() -> bool {
    false
}

pub fn configure_filesystem_ui_routes(cfg: &mut web::ServiceConfig, ui_path: &str) {
    let ui_path = PathBuf::from(ui_path);
    let index_path = ui_path.join("index.html");

    let index_content = std::fs::read_to_string(&index_path).unwrap_or_else(|_| {
        "<html><body><h1>Admin UI not built</h1><p>Run 'pnpm build' in ui/ directory</p></body></html>"
            .to_string()
    });
    let index_content = web::Data::new(index_content);

    cfg.app_data(index_content.clone()).service(
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
