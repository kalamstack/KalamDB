//! UI transport helpers for embedded and filesystem-served admin assets.

#[cfg(feature = "embedded-ui")]
mod embedded;

use actix_web::{web, HttpResponse};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct UiRuntimeConfig {
    backend_origin: Option<String>,
}

impl UiRuntimeConfig {
    pub fn new(backend_origin: Option<String>) -> Self {
        Self {
            backend_origin: backend_origin
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.trim_end_matches('/').to_string()),
        }
    }

    fn script_body(&self) -> String {
        let mut payload = serde_json::Map::new();
        if let Some(backend_origin) = &self.backend_origin {
            payload.insert(
                "backendOrigin".to_string(),
                serde_json::Value::String(backend_origin.clone()),
            );
        }

        let payload = serde_json::Value::Object(payload);
        format!("window.__KALAMDB_RUNTIME_CONFIG__ = Object.freeze({payload});")
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
                        HttpResponse::Ok().content_type("text/html; charset=utf-8").body(content)
                    }
                })),
        );
}

#[cfg(test)]
mod tests {
    use super::UiRuntimeConfig;

    #[test]
    fn test_runtime_config_script_includes_backend_origin_when_configured() {
        let script =
            UiRuntimeConfig::new(Some("https://kalamdb.masky.app/".to_string())).script_body();

        assert!(script.contains("backendOrigin"));
        assert!(script.contains("https://kalamdb.masky.app"));
    }

    #[test]
    fn test_runtime_config_script_omits_backend_origin_when_unset() {
        let script = UiRuntimeConfig::new(Some("   ".to_string())).script_body();

        assert_eq!(script, "window.__KALAMDB_RUNTIME_CONFIG__ = Object.freeze({});");
    }
}
