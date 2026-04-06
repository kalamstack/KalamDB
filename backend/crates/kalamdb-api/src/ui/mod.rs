//! UI transport helpers for embedded and filesystem-served admin assets.

#[cfg(feature = "embedded-ui")]
mod embedded;

use actix_web::{web, HttpResponse};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct UiRuntimeConfig {
    backend_origin: String,
}

impl UiRuntimeConfig {
    pub fn new(backend_origin: String) -> Self {
        Self { backend_origin }
    }

    fn script_body(&self) -> String {
        let payload = serde_json::json!({
            "backendOrigin": self.backend_origin,
        });
        format!(
            "window.__KALAMDB_RUNTIME_CONFIG__ = Object.freeze({payload});"
        )
    }
}

async fn serve_runtime_config_script(runtime_config: web::Data<UiRuntimeConfig>) -> HttpResponse {
    HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .content_type("application/javascript; charset=utf-8")
        .body(runtime_config.script_body())
}

#[cfg(feature = "embedded-ui")]
pub fn configure_embedded_ui_routes(cfg: &mut web::ServiceConfig, runtime_config: UiRuntimeConfig) {
    embedded::configure_embedded_ui(cfg, runtime_config);
}

#[cfg(feature = "embedded-ui")]
pub fn is_embedded_ui_available() -> bool {
    embedded::is_ui_embedded()
}

#[cfg(not(feature = "embedded-ui"))]
pub fn is_embedded_ui_available() -> bool {
    false
}

pub fn configure_filesystem_ui_routes(
    cfg: &mut web::ServiceConfig,
    ui_path: &str,
    runtime_config: UiRuntimeConfig,
) {
    let ui_path = PathBuf::from(ui_path);
    let index_path = ui_path.join("index.html");

    let index_content = std::fs::read_to_string(&index_path).unwrap_or_else(|_| {
        "<html><body><h1>Admin UI not built</h1><p>Run 'pnpm build' in ui/ directory</p></body></html>"
            .to_string()
    });
    let index_content = web::Data::new(index_content);
    let runtime_config = web::Data::new(runtime_config);

    cfg.app_data(index_content.clone())
        .app_data(runtime_config)
        .route("/ui/runtime-config.js", web::get().to(serve_runtime_config_script))
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
