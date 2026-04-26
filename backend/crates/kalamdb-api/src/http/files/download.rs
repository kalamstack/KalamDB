//! File download handler

use std::sync::Arc;

use actix_web::{get, web, HttpResponse, Responder};
use kalamdb_auth::AuthSessionExtractor;
use kalamdb_commons::{models::TableId, schemas::TableType, TableAccess};
use kalamdb_core::app_context::AppContext;
use kalamdb_session::{can_access_shared_table, can_impersonate_role, AuthSession};
use kalamdb_system::FileRef;

use super::models::DownloadQuery;
use crate::http::sql::models::{ErrorCode, SqlResponse};

/// GET /v1/files/{namespace}/{table_name}/{subfolder}/{file_id} - Download a file
///
/// Requires Bearer token (JWT) authorization and table access permissions.
/// For user tables, downloads from current user's table unless ?user_id is specified.
#[get("/files/{namespace}/{table_name}/{subfolder}/{file_id}")]
pub async fn download_file(
    extractor: AuthSessionExtractor,
    path: web::Path<(String, String, String, String)>,
    query: web::Query<DownloadQuery>,
    app_context: web::Data<Arc<AppContext>>,
) -> impl Responder {
    // Convert extractor to AuthSession
    let session: AuthSession = extractor.into();

    let (namespace, table_name, subfolder, file_id) = path.into_inner();
    let table_id = TableId::from_strings(&namespace, &table_name);

    // Look up table definition from schema registry
    let schema_registry = app_context.schema_registry();
    let table_entry = match schema_registry.get(&table_id) {
        Some(cached) => cached.table_entry(),
        None => {
            return HttpResponse::NotFound().json(serde_json::json!({
                "error": format!("Table '{}' not found", table_id),
            }));
        },
    };

    let storage_id = table_entry.storage_id.clone();
    let table_type = table_entry.table_type;

    // Check impersonation permissions
    if let Some(ref requested_user_id) = query.user_id {
        if requested_user_id != session.user_id() {
            // Offload sync RocksDB read to blocking thread
            let app_ctx = app_context.get_ref().clone();
            let req_uid = requested_user_id.clone();
            let target_user_result = tokio::task::spawn_blocking(move || {
                app_ctx.system_tables().users().get_user_by_id(&req_uid)
            })
            .await;

            let target_user = match target_user_result {
                Ok(Ok(Some(user))) if user.deleted_at.is_none() => user,
                Ok(Ok(_)) => {
                    return HttpResponse::NotFound().json(SqlResponse::error(
                        ErrorCode::InvalidInput,
                        "Requested user was not found",
                        0.0,
                    ));
                },
                Ok(Err(e)) => {
                    log::warn!(
                        "Failed to resolve impersonation target for file download: user_id={}, \
                         error={}",
                        requested_user_id,
                        e
                    );
                    return HttpResponse::InternalServerError().json(SqlResponse::error(
                        ErrorCode::InternalError,
                        "Failed to validate impersonation target",
                        0.0,
                    ));
                },
                Err(e) => {
                    log::warn!(
                        "Failed to resolve impersonation target for file download: user_id={}, \
                         error={}",
                        requested_user_id,
                        e
                    );
                    return HttpResponse::InternalServerError().json(SqlResponse::error(
                        ErrorCode::InternalError,
                        "Failed to validate impersonation target",
                        0.0,
                    ));
                },
            };

            if !can_impersonate_role(session.role(), target_user.role) {
                return HttpResponse::Forbidden().json(SqlResponse::error(
                    ErrorCode::PermissionDenied,
                    "Impersonation target is not allowed for the current role",
                    0.0,
                ));
            }
        }
    }

    let effective_user_id = query.user_id.clone().unwrap_or_else(|| session.user_id().clone());

    let user_id = match table_type {
        TableType::User => Some(effective_user_id),
        TableType::Shared => {
            let access_level = table_entry.access_level.unwrap_or(TableAccess::Private);
            if !can_access_shared_table(access_level, session.role()) {
                return HttpResponse::Forbidden().json(SqlResponse::error(
                    ErrorCode::PermissionDenied,
                    &format!("Shared table access denied (access_level={:?})", access_level),
                    0.0,
                ));
            }
            if query.user_id.is_some() {
                return HttpResponse::BadRequest().json(SqlResponse::error(
                    ErrorCode::InvalidInput,
                    "user_id is only valid for user tables",
                    0.0,
                ));
            }
            None
        },
        TableType::Stream | TableType::System => {
            // Stream and system tables don't support file storage
            return HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidInput,
                "File storage is not supported for stream or system tables",
                0.0,
            ));
        },
    };

    // Validate path components for security
    let subfolder_is_valid = FileRef::is_valid_subfolder(&subfolder);

    if !subfolder_is_valid
        || subfolder.contains("..")
        || subfolder.contains('/')
        || subfolder.contains('\\')
        || subfolder.contains('\0')
        || file_id.contains("..")
        || file_id.contains('/')
        || file_id.contains('\\')
        || file_id.contains('\0')
    {
        return HttpResponse::BadRequest().json(SqlResponse::error(
            ErrorCode::InvalidInput,
            "Invalid file path",
            0.0,
        ));
    }
    let relative_path = format!("{}/{}", subfolder, file_id);

    // Fetch file from storage
    let file_service = app_context.file_storage_service();
    match file_service
        .get_file_by_path(&storage_id, table_type, &table_id, user_id.as_ref(), &relative_path)
        .await
    {
        Ok(data) => {
            // TODO: Get content type from the stored file metadata
            // Guess content type from file extension in file_id
            let content_type = guess_content_type(&file_id);

            // SECURITY: Sanitize file_id for Content-Disposition header to prevent
            // HTTP response header injection (CRLF injection) via crafted filenames.
            let safe_file_id: String = file_id
                .chars()
                .filter(|c| *c != '"' && *c != '\r' && *c != '\n' && *c != '\0')
                .collect();
            HttpResponse::Ok()
                .content_type(content_type)
                .append_header((
                    "Content-Disposition",
                    format!("inline; filename=\"{}\"", safe_file_id),
                ))
                .body(data)
        },
        Err(e) => {
            log::warn!("File download failed: table={}, file={}: {}", table_id, file_id, e);
            HttpResponse::NotFound().json(serde_json::json!({
                "error": "File not found",
                "code": "FILE_NOT_FOUND",
            }))
        },
    }
}

fn guess_content_type(file_id: &str) -> String {
    mime_guess::from_path(file_id).first_or_octet_stream().to_string()
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::Role;

    use super::*;

    #[test]
    fn download_impersonation_respects_target_role_matrix() {
        assert!(can_impersonate_role(Role::System, Role::System));
        assert!(can_impersonate_role(Role::Dba, Role::Service));
        assert!(can_impersonate_role(Role::Service, Role::User));

        assert!(!can_impersonate_role(Role::Dba, Role::System));
        assert!(!can_impersonate_role(Role::Service, Role::Dba));
        assert!(!can_impersonate_role(Role::User, Role::User));
    }
}
