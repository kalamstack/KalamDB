//! Typed handler for DROP USER statement

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::sql::executor::handlers::typed::TypedStatementHandler;
use kalamdb_sql::ddl::DropUserStatement;
use std::sync::Arc;
// No direct UserId usage, removing unused import

/// Handler for DROP USER
pub struct DropUserHandler {
    app_context: Arc<AppContext>,
}

impl DropUserHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<DropUserStatement> for DropUserHandler {
    async fn execute(
        &self,
        statement: DropUserStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let app_ctx = self.app_context.clone();
        let username = statement.username.clone();
        let existing = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().users().get_user_by_username(&username)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;
        if existing.is_none() {
            if statement.if_exists {
                return Ok(ExecutionResult::Success {
                    message: format!("User '{}' does not exist (skipped)", statement.username),
                });
            }
            return Err(KalamDbError::NotFound(format!("User '{}' not found", statement.username)));
        }
        let user = existing.unwrap();

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .delete_user(user.user_id.clone())
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("DROP USER failed: {}", e)))?;

        // Log DDL operation
        use crate::sql::executor::helpers::audit;
        let audit_entry =
            audit::log_ddl_operation(context, "DROP", "USER", &statement.username, None, None);
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Success {
            message: format!("User '{}' dropped (soft delete)", statement.username),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &DropUserStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        if !context.is_admin() {
            return Err(KalamDbError::Unauthorized(
                "DROP USER requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
