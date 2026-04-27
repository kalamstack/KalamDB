//! Typed handler for ALTER USER statement

use std::sync::Arc;

use kalamdb_auth::security::password::{
    hash_password, validate_password_with_policy, PasswordPolicy,
};
use kalamdb_commons::UserId;
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::{AlterUserStatement, UserModification};
use kalamdb_system::Role;

/// Handler for ALTER USER
pub struct AlterUserHandler {
    app_context: Arc<AppContext>,
    enforce_complexity: bool,
}

impl AlterUserHandler {
    pub fn new(app_context: Arc<AppContext>, enforce_complexity: bool) -> Self {
        Self {
            app_context,
            enforce_complexity,
        }
    }
}

impl TypedStatementHandler<AlterUserStatement> for AlterUserHandler {
    async fn execute(
        &self,
        statement: AlterUserStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let app_ctx = self.app_context.clone();
        let user_id = UserId::new(&statement.username);
        let existing = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().users().get_user_by_id(&user_id)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??
        .ok_or_else(|| {
            KalamDbError::NotFound(format!("User '{}' not found", statement.username))
        })?;

        let mut updated = existing.clone();

        match statement.modification {
            UserModification::SetPassword(ref new_pw) => {
                // Self-service allowed: user modifying own password
                let is_self = context.user_id().as_str() == updated.user_id.as_str();
                if !is_self && !context.is_admin() {
                    return Err(KalamDbError::Unauthorized(
                        "Only admins can change other users' passwords".to_string(),
                    ));
                }
                // Enforce password complexity if enabled in config
                if self.enforce_complexity
                    || self.app_context.config().auth.enforce_password_complexity
                {
                    let policy = PasswordPolicy::default().with_enforced_complexity(true);
                    validate_password_with_policy(new_pw, &policy)
                        .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?;
                }
                updated.password_hash =
                    hash_password(new_pw, Some(self.app_context.config().auth.bcrypt_cost))
                        .await
                        .map_err(|e| {
                        KalamDbError::InvalidOperation(format!("Password hash error: {}", e))
                    })?;
            },
            UserModification::SetRole(new_role) => {
                if !context.is_admin() {
                    return Err(KalamDbError::Unauthorized(
                        "Only admins can change roles".to_string(),
                    ));
                }
                if context.user_role() == Role::Dba && new_role == Role::System {
                    return Err(KalamDbError::Unauthorized(
                        "DBA cannot grant System role".to_string(),
                    ));
                }
                updated.role = new_role;
            },
            UserModification::SetEmail(ref new_email) => {
                let is_self = context.user_id().as_str() == updated.user_id.as_str();
                if !is_self && !context.is_admin() {
                    return Err(KalamDbError::Unauthorized(
                        "Only admins can update other users' emails".to_string(),
                    ));
                }
                updated.email = Some(new_email.clone());
            },
        }

        updated.updated_at = chrono::Utc::now().timestamp_millis();

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .update_user(updated)
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("ALTER USER failed: {}", e)))?;

        // Log DDL operation (with password redaction)
        use crate::helpers::audit;
        let audit_entry = audit::log_ddl_operation(
            context,
            "ALTER",
            "USER",
            &statement.username,
            Some(format!("Modification: {}", statement.modification.display_for_audit())),
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Success {
            message: format!("User '{}' updated", statement.username),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &AlterUserStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        if !context.is_admin() {
            return Err(KalamDbError::Unauthorized(
                "ALTER USER requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
