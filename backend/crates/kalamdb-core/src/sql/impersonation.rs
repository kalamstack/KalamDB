use crate::app_context::AppContext;
use crate::error::KalamDbError;
use kalamdb_commons::models::UserId;
use kalamdb_commons::Role;
use kalamdb_session::can_impersonate_role;
use std::sync::Arc;

/// Core service for SQL "execute as user" resolution and authorization.
pub struct SqlImpersonationService {
    app_context: Arc<AppContext>,
}

impl SqlImpersonationService {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Resolve a target username and authorize actor -> target impersonation.
    ///
    /// Returns the canonical target user_id on success.
    /// Offloads sync RocksDB user lookups to a blocking thread.
    pub async fn resolve_execute_as_user(
        &self,
        actor_user_id: &UserId,
        actor_role: Role,
        target_username: &str,
    ) -> Result<UserId, KalamDbError> {
        let app_ctx = self.app_context.clone();
        let target_name = target_username.to_string();

        let target_user = tokio::task::spawn_blocking(move || {
            let users_provider = app_ctx.system_tables().users();
            let user = if let Some(user) =
                users_provider.get_user_by_username(&target_name).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to resolve EXECUTE AS USER target '{}' by username: {}",
                        target_name, e
                    ))
                })? {
                user
            } else {
                users_provider
                    .get_user_by_id(&UserId::from(target_name.clone()))
                    .map_err(|e| {
                        KalamDbError::InvalidOperation(format!(
                            "Failed to resolve EXECUTE AS USER target '{}' by user_id: {}",
                            target_name, e
                        ))
                    })?
                    .ok_or_else(|| {
                        KalamDbError::NotFound(format!(
                            "EXECUTE AS USER target '{}' was not found by username or user_id",
                            target_name
                        ))
                    })?
            };
            Ok::<_, KalamDbError>(user)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        // No-op impersonation is always allowed.
        if &target_user.user_id == actor_user_id {
            return Ok(target_user.user_id);
        }

        if !can_impersonate_role(actor_role, target_user.role) {
            return Err(KalamDbError::Unauthorized(format!(
                "Role {:?} cannot execute as user '{}' with role {:?}",
                actor_role, target_username, target_user.role
            )));
        }

        Ok(target_user.user_id)
    }
}
