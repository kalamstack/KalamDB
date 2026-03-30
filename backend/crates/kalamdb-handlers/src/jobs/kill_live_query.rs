//! Typed handler for KILL LIVE QUERY statement

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::KillLiveQueryStatement;
use std::sync::Arc;

/// Handler for KILL LIVE QUERY
pub struct KillLiveQueryHandler {
    app_context: Arc<AppContext>,
}

impl KillLiveQueryHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<KillLiveQueryStatement> for KillLiveQueryHandler {
    async fn execute(
        &self,
        statement: KillLiveQueryStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let manager = self.app_context.live_query_manager();
        manager.unregister_subscription_by_id(&statement.live_id).await?;

        // Log DDL operation
        use crate::helpers::audit;
        let audit_entry = audit::log_ddl_operation(
            context,
            "KILL",
            "LIVE_QUERY",
            statement.live_id.as_ref(),
            None,
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Success {
            message: format!("Live query killed: {}", statement.live_id),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &KillLiveQueryStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        if !context.is_admin() {
            return Err(KalamDbError::Unauthorized(
                "KILL LIVE QUERY requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
