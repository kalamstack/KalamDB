//! Typed handler for CREATE USER statement

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_auth::security::password::{
    hash_password, validate_password_with_policy, PasswordPolicy,
};
use kalamdb_commons::{AuthType, UserId};
use kalamdb_sql::ddl::CreateUserStatement;
use kalamdb_system::{AuthData, User};
use std::sync::Arc;

/// Handler for CREATE USER
pub struct CreateUserHandler {
    app_context: Arc<AppContext>,
    enforce_complexity: bool,
}

impl CreateUserHandler {
    pub fn new(app_context: Arc<AppContext>, enforce_complexity: bool) -> Self {
        Self {
            app_context,
            enforce_complexity,
        }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<CreateUserStatement> for CreateUserHandler {
    async fn execute(
        &self,
        statement: CreateUserStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Duplicate check (provider enforces via username index but we do early check for clearer error)
        let app_ctx = self.app_context.clone();
        let username = statement.username.clone();
        let existing = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().users().get_user_by_username(&username)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;
        if existing.is_some() {
            return Err(KalamDbError::AlreadyExists(format!(
                "User '{}' already exists",
                statement.username
            )));
        }

        // Hash password if auth_type = Password, or extract auth_data for OAuth
        let (password_hash, auth_data) = match statement.auth_type {
            AuthType::Password => {
                let raw = statement.password.clone().ok_or_else(|| {
                    KalamDbError::InvalidOperation(
                        "Password required for WITH PASSWORD".to_string(),
                    )
                })?;
                // Enforce password complexity if enabled in config
                if self.enforce_complexity
                    || self.app_context.config().auth.enforce_password_complexity
                {
                    let policy = PasswordPolicy::default().with_enforced_complexity(true);
                    validate_password_with_policy(&raw, &policy)
                        .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?;
                }
                let bcrypt_cost = self.app_context.config().auth.bcrypt_cost;
                let hash = hash_password(&raw, Some(bcrypt_cost)).await.map_err(|e| {
                    KalamDbError::InvalidOperation(format!("Password hash error: {}", e))
                })?;
                (hash, None)
            },
            AuthType::OAuth => {
                // For OAuth, the 'password' field contains the JSON payload
                let payload = statement.password.clone().ok_or_else(|| {
                    KalamDbError::InvalidOperation(
                        "OAuth user requires JSON payload with provider and subject".to_string(),
                    )
                })?;

                let json: serde_json::Value =
                    serde_json::from_str(&payload).into_invalid_operation("Invalid OAuth JSON")?;

                let provider = json
                    .get("provider")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        KalamDbError::InvalidOperation(
                            "OAuth user requires 'provider' field".to_string(),
                        )
                    })?
                    .to_string();

                let subject = json
                    .get("subject")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        KalamDbError::InvalidOperation(
                            "OAuth user requires 'subject' field".to_string(),
                        )
                    })?
                    .to_string();

                let auth_data = AuthData::new(provider, subject);
                ("".to_string(), Some(auth_data))
            },
            AuthType::Internal => ("".to_string(), None),
        };

        let now = chrono::Utc::now().timestamp_millis();
        let user = User {
            user_id: UserId::generate(),
            username: statement.username.clone().into(),
            password_hash,
            role: statement.role,
            email: statement.email.clone(),
            auth_type: statement.auth_type,
            auth_data,
            storage_mode: kalamdb_system::providers::storages::models::StorageMode::Table,
            storage_id: None,
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: now,
            updated_at: now,
            last_seen: None,
            deleted_at: None,
        };

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .create_user(user)
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("CREATE USER failed: {}", e)))?;

        // Log DDL operation
        use crate::helpers::audit;
        let audit_entry = audit::log_ddl_operation(
            context,
            "CREATE",
            "USER",
            &statement.username,
            Some(format!("Role: {:?}", statement.role)),
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Success {
            message: format!("User '{}' created", statement.username),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CreateUserStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        if !context.is_admin() {
            return Err(KalamDbError::Unauthorized(
                "CREATE USER requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
