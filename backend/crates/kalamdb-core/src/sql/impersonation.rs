use crate::app_context::AppContext;
use crate::error::KalamDbError;
use chrono::Utc;
use kalamdb_commons::models::{AuditLogId, UserId};
use kalamdb_commons::Role;
use kalamdb_session::can_impersonate_role;
use kalamdb_system::AuditLogEntry;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

/// Core service for SQL "execute as user" resolution and authorization.
pub struct SqlImpersonationService {
    app_context: Arc<AppContext>,
}

impl SqlImpersonationService {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    async fn audit_impersonation_event(
        &self,
        actor_user_id: &UserId,
        actor_role: Role,
        target_user: &str,
        subject_user_id: Option<&UserId>,
        success: bool,
        reason: Option<&str>,
    ) {
        let timestamp = Utc::now().timestamp_millis();
        let audit_id = AuditLogId::from(format!("audit_{}_{}", timestamp, Uuid::new_v4().simple()));
        let details = match reason {
            Some(reason) => {
                json!({ "success": success, "actor_role": format!("{:?}", actor_role), "reason": reason })
                    .to_string()
            },
            None => {
                json!({ "success": success, "actor_role": format!("{:?}", actor_role) })
                    .to_string()
            },
        };
        let action = if success {
            "EXECUTE_AS_USER"
        } else {
            "EXECUTE_AS_USER_DENIED"
        };
        let entry = AuditLogEntry {
            audit_id,
            timestamp,
            actor_user_id: actor_user_id.clone(),
            action: action.to_string(),
            target: format!("user:{}", target_user),
            details: Some(details),
            ip_address: None,
            subject_user_id: subject_user_id.cloned(),
        };

        if let Err(error) = self.app_context.system_tables().audit_logs().append_async(entry).await
        {
            log::warn!(
                "Failed to persist EXECUTE AS USER audit entry for actor '{}' target '{}': {}",
                actor_user_id.as_str(),
                target_user,
                error
            );
        }
    }

    /// Resolve a target user identifier and authorize actor -> target impersonation.
    ///
    /// Returns the canonical target user_id on success.
    /// Offloads sync RocksDB user lookups to a blocking thread.
    pub async fn resolve_execute_as_user(
        &self,
        actor_user_id: &UserId,
        actor_role: Role,
        target_user: &str,
    ) -> Result<UserId, KalamDbError> {
        let app_ctx = self.app_context.clone();
        let target_name = target_user.to_string();
        let unresolved_target = target_name.clone();

        let resolved_user = match tokio::task::spawn_blocking(move || {
            let users_provider = app_ctx.system_tables().users();
            let target_user_id = UserId::from(target_name.clone());
            let user = users_provider
                .get_user_by_id(&target_user_id)
                .map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to resolve EXECUTE AS USER target '{}': {}",
                        target_name, e
                    ))
                })?
                .ok_or_else(|| {
                    KalamDbError::NotFound(format!(
                        "EXECUTE AS USER target '{}' was not found",
                        target_name
                    ))
                })?;
            Ok::<_, KalamDbError>(user)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))
        {
            Ok(Ok(user)) => user,
            Ok(Err(error)) => {
                self.audit_impersonation_event(
                    actor_user_id,
                    actor_role,
                    &unresolved_target,
                    None,
                    false,
                    Some("resolution_failed"),
                )
                .await;
                return Err(error);
            },
            Err(error) => {
                self.audit_impersonation_event(
                    actor_user_id,
                    actor_role,
                    &unresolved_target,
                    None,
                    false,
                    Some("resolution_failed"),
                )
                .await;
                return Err(error);
            },
        };

        // No-op impersonation is always allowed.
        if &resolved_user.user_id == actor_user_id {
            self.audit_impersonation_event(
                actor_user_id,
                actor_role,
                resolved_user.user_id.as_str(),
                Some(&resolved_user.user_id),
                true,
                None,
            )
            .await;
            return Ok(resolved_user.user_id);
        }

        if !can_impersonate_role(actor_role, resolved_user.role) {
            self.audit_impersonation_event(
                actor_user_id,
                actor_role,
                resolved_user.user_id.as_str(),
                Some(&resolved_user.user_id),
                false,
                Some("unauthorized"),
            )
            .await;
            return Err(KalamDbError::Unauthorized(format!(
                "Role {:?} is not authorized to use AS USER for '{}' with role {:?}",
                actor_role, target_user, resolved_user.role
            )));
        }

        self.audit_impersonation_event(
            actor_user_id,
            actor_role,
            resolved_user.user_id.as_str(),
            Some(&resolved_user.user_id),
            true,
            None,
        )
        .await;

        Ok(resolved_user.user_id)
    }
}
