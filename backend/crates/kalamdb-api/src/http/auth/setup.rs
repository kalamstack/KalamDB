//! Server setup handlers
//!
//! POST /v1/api/auth/setup - Initial server setup (localhost only)
//! GET /v1/api/auth/status - Check server setup status

use std::sync::Arc;

use actix_web::{web, HttpRequest, HttpResponse};
use kalamdb_auth::{
    errors::error::AuthError,
    extract_client_ip_secure,
    security::password::{hash_password, validate_password},
    UserRepository,
};
use kalamdb_commons::{
    models::{StorageId, UserId},
    AuthType, Role,
};
use kalamdb_configs::AuthSettings;
use kalamdb_system::{providers::storages::models::StorageMode, User};

use super::models::{AuthErrorResponse, ServerSetupRequest, ServerSetupResponse, UserInfo};
use crate::limiter::RateLimiter;

fn build_setup_response(user: &User, message: String) -> ServerSetupResponse {
    let created_at_str = chrono::DateTime::from_timestamp_millis(user.created_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();
    let updated_at_str = chrono::DateTime::from_timestamp_millis(user.updated_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();

    ServerSetupResponse {
        user: UserInfo {
            id: user.user_id.clone(),
            role: user.role,
            email: user.email.clone(),
            created_at: created_at_str,
            updated_at: updated_at_str,
        },
        message,
    }
}

/// POST /v1/api/auth/setup
///
/// Initial server setup endpoint. Only works when:
/// 1. Root user has no password set (empty password_hash)
/// 2. Called from localhost only
///
/// This endpoint:
/// 1. Sets the root user password
/// 2. Creates a new DBA user with the provided credentials
/// 3. Returns user info (user must login separately to get tokens)
pub async fn server_setup_handler(
    req: HttpRequest,
    user_repo: web::Data<Arc<dyn UserRepository>>,
    config: web::Data<AuthSettings>,
    rate_limiter: web::Data<Arc<RateLimiter>>,
    body: web::Json<ServerSetupRequest>,
) -> HttpResponse {
    // Only allow setup from localhost
    let connection_info = extract_client_ip_secure(&req);

    // Rate limit setup attempts by client IP
    if !rate_limiter.get_ref().check_auth_rate(&connection_info) {
        return HttpResponse::TooManyRequests().json(AuthErrorResponse::new(
            "rate_limited",
            "Too many setup attempts. Please retry shortly.",
        ));
    }

    if !connection_info.is_localhost() && !config.allow_remote_setup {
        return HttpResponse::Forbidden().json(AuthErrorResponse::new(
            "forbidden",
            "Server setup can only be performed from localhost",
        ));
    }

    // Check if root user exists and has empty password
    let root_user_id = UserId::root();
    let root_user = match user_repo.get_user_by_id(&root_user_id).await {
        Ok(user) => user,
        Err(e) => {
            log::error!("Failed to get root user: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to verify setup status"));
        },
    };

    // Only allow setup if root has no password
    if !root_user.password_hash.is_empty() {
        return HttpResponse::Conflict().json(AuthErrorResponse::new(
            "already_configured",
            "Server has already been configured. Root password is set.",
        ));
    }

    // Validate passwords
    if let Err(e) = validate_password(&body.password) {
        return HttpResponse::BadRequest()
            .json(AuthErrorResponse::new("weak_password", format!("DBA user password: {}", e)));
    }
    if let Err(e) = validate_password(&body.root_password) {
        return HttpResponse::BadRequest()
            .json(AuthErrorResponse::new("weak_password", format!("Root password: {}", e)));
    }

    let dba_user_id = match UserId::try_new(body.user.clone()) {
        Ok(user_id) => user_id,
        Err(_) => {
            return HttpResponse::BadRequest()
                .json(AuthErrorResponse::new("invalid_user", "Invalid user value"));
        },
    };

    // Check user is not root
    if dba_user_id == UserId::root() {
        return HttpResponse::BadRequest().json(AuthErrorResponse::new(
            "invalid_user",
            "Cannot create a DBA user with user 'root'. Choose a different user.",
        ));
    }

    // Check if DBA user already exists
    if user_repo.get_user_by_id(&dba_user_id).await.is_ok() {
        return HttpResponse::Conflict().json(AuthErrorResponse::new(
            "user_exists",
            format!("User '{}' already exists", dba_user_id.as_str()),
        ));
    }

    // Hash passwords
    let root_password_hash = match hash_password(&body.root_password, None).await {
        Ok(hash) => hash,
        Err(e) => {
            log::error!("Failed to hash root password: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to hash password"));
        },
    };

    let dba_password_hash = match hash_password(&body.password, None).await {
        Ok(hash) => hash,
        Err(e) => {
            log::error!("Failed to hash DBA password: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to hash password"));
        },
    };

    // Update root user with password
    let mut updated_root = root_user.clone();
    updated_root.password_hash = root_password_hash;
    updated_root.updated_at = chrono::Utc::now().timestamp_millis();
    // No longer need auth_data with allow_remote - password is enough
    updated_root.auth_data = None;

    if let Err(e) = user_repo.update_user(&updated_root).await {
        log::error!("Failed to update root password: {}", e);
        return HttpResponse::InternalServerError()
            .json(AuthErrorResponse::new("internal_error", "Failed to configure root user"));
    }

    // Create new DBA user
    let created_at = chrono::Utc::now().timestamp_millis();
    let dba_user = User {
        user_id: dba_user_id.clone(),
        password_hash: dba_password_hash,
        role: Role::Dba,
        email: body.email.clone(),
        auth_type: AuthType::Password,
        auth_data: None,
        storage_mode: StorageMode::Table,
        storage_id: Some(StorageId::local()),
        failed_login_attempts: 0,
        locked_until: None,
        last_login_at: None,
        created_at,
        updated_at: created_at,
        last_seen: None,
        deleted_at: None,
    };

    if let Err(e) = user_repo.create_user(dba_user.clone()).await {
        match e {
            AuthError::DatabaseError(message) if message.contains("already exists") => {
                match user_repo.get_user_by_id(&dba_user_id).await {
                    Ok(existing_user) => {
                        log::info!(
                            "Server setup raced with another caller; reusing existing DBA user \
                             '{}'",
                            dba_user_id.as_str()
                        );
                        return HttpResponse::Ok().json(build_setup_response(
                            &existing_user,
                            format!(
                                "Server setup already completed for DBA user '{}'. Please login \
                                 to continue.",
                                existing_user.user_id.as_str()
                            ),
                        ));
                    },
                    Err(fetch_error) => {
                        log::error!(
                            "Failed to load existing DBA user '{}' after create race: {}",
                            dba_user_id.as_str(),
                            fetch_error
                        );
                    },
                }
            },
            other => {
                log::error!("Failed to create DBA user: {}", other);
            },
        }

        return HttpResponse::InternalServerError()
            .json(AuthErrorResponse::new("internal_error", "Failed to create DBA user"));
    }

    log::info!(
        "Server setup completed: root password set, DBA user '{}' created",
        dba_user_id.as_str()
    );

    // Return user info only - user must login separately to get tokens
    HttpResponse::Ok().json(build_setup_response(
        &dba_user,
        format!(
            "Server setup complete. Root password configured and DBA user '{}' created. Please \
             login to continue.",
            dba_user_id.as_str()
        ),
    ))
}

/// GET /v1/api/auth/status
///
/// Returns the current setup status of the server.
/// Returns whether setup is required or not.
pub async fn setup_status_handler(
    req: HttpRequest,
    user_repo: web::Data<Arc<dyn UserRepository>>,
    config: web::Data<AuthSettings>,
) -> HttpResponse {
    // Only allow status check from localhost
    let connection_info = extract_client_ip_secure(&req);
    if !connection_info.is_localhost() && !config.allow_remote_setup {
        return HttpResponse::Forbidden().json(AuthErrorResponse::new(
            "forbidden",
            "Setup status can only be checked from localhost",
        ));
    }

    let root_user_id = UserId::root();
    let root_user = match user_repo.get_user_by_id(&root_user_id).await {
        Ok(user) => user,
        Err(e) => {
            log::error!("Failed to get root user: {}", e);
            return HttpResponse::InternalServerError()
                .json(AuthErrorResponse::new("internal_error", "Failed to check setup status"));
        },
    };

    let needs_setup = root_user.password_hash.is_empty();

    HttpResponse::Ok().json(serde_json::json!({
        "needs_setup": needs_setup,
        "message": if needs_setup {
            "Server requires initial setup. Call POST /v1/api/auth/setup with user, password, root_password, and optional email."
        } else {
            "Server is configured and ready."
        }
    }))
}
