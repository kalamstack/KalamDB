//! Export file download handler.
//!
//! Serves completed user data export ZIP archives.
//!
//! ## Endpoint
//! GET /v1/exports/{user_id}/{export_id}
//!
//! The requesting user must be the owner of the export, or an admin.

use actix_web::{get, web, HttpResponse, Responder};
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_core::app_context::AppContext;
use kalamdb_session::{is_admin_role, AuthSession};
use std::sync::Arc;

use crate::http::sql::models::{ErrorCode, SqlResponse};

/// GET /v1/exports/{user_id}/{export_id} - Download a user data export ZIP
///
/// Requires Bearer token (JWT) authorization.
/// Only the owning user or an admin can download the export.
#[get("/exports/{user_id}/{export_id}")]
pub async fn download_export(
    extractor: AuthSessionExtractor,
    path: web::Path<(String, String)>,
    app_context: web::Data<Arc<AppContext>>,
) -> impl Responder {
    let session: AuthSession = extractor.into();
    let (user_id, export_id) = path.into_inner();

    // Authorization: only the owning user or admins can download
    if session.user_id().as_str() != user_id && !is_admin_role(session.role()) {
        return HttpResponse::Forbidden().json(SqlResponse::error(
            ErrorCode::PermissionDenied,
            "You can only download your own exports",
            0.0,
        ));
    }

    // Security: validate path components
    if user_id.contains("..")
        || user_id.contains('/')
        || user_id.contains('\\')
        || user_id.contains('\0')
        || export_id.contains("..")
        || export_id.contains('/')
        || export_id.contains('\\')
        || export_id.contains('\0')
    {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::InvalidInput,
            "Invalid export path",
            0.0,
        ));
    }

    // Build file path
    let exports_dir = app_context.config().storage.exports_dir();
    let zip_path = exports_dir.join(&user_id).join(format!("{}.zip", export_id));

    if !zip_path.exists() {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Export not found",
            "code": "EXPORT_NOT_FOUND",
        }));
    }

    // Read and serve the file
    match tokio::fs::read(&zip_path).await {
        Ok(data) => {
            let filename = format!("{}.zip", export_id);
            HttpResponse::Ok()
                .content_type("application/zip")
                .append_header((
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename),
                ))
                .body(data)
        },
        Err(e) => {
            log::warn!("Export download failed: path={}, error={}", zip_path.display(), e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to read export file",
                "code": "INTERNAL_ERROR",
            }))
        },
    }
}
