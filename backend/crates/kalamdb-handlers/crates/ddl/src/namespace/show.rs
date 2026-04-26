//! Typed DDL handler for SHOW NAMESPACES statements

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::ShowNamespacesStatement;

/// Typed handler for SHOW NAMESPACES statements
pub struct ShowNamespacesHandler {
    app_context: Arc<AppContext>,
}

impl ShowNamespacesHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<ShowNamespacesStatement> for ShowNamespacesHandler {
    async fn execute(
        &self,
        _statement: ShowNamespacesStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let start_time = std::time::Instant::now();

        // Query all namespaces via the table provider (offload sync RocksDB read)
        let app_ctx = self.app_context.clone();
        let batches = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().namespaces().scan_all_namespaces()
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        // Log query operation
        let duration = start_time.elapsed().as_secs_f64() * 1000.0;
        use crate::helpers::audit;
        let audit_entry = audit::log_query_operation(context, "SHOW", "NAMESPACES", duration, None);
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        // Return as query result
        let row_count = batches.num_rows();
        Ok(ExecutionResult::Rows {
            batches: vec![batches],
            row_count,
            schema: None,
        })
    }

    async fn check_authorization(
        &self,
        _statement: &ShowNamespacesStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // SHOW NAMESPACES allowed for all authenticated users
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kalamdb_commons::{models::UserId, Role};
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};

    use super::*;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), Role::User, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_show_namespaces_authorization() {
        let app_ctx = init_app_context();
        let handler = ShowNamespacesHandler::new(app_ctx);
        let stmt = ShowNamespacesStatement {};

        // All users can show namespaces
        let ctx = create_test_context();
        let result = handler.check_authorization(&stmt, &ctx).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_show_namespaces_success() {
        let app_ctx = init_app_context();
        let handler = ShowNamespacesHandler::new(app_ctx);
        let stmt = ShowNamespacesStatement {};
        let ctx = create_test_context();

        let result = handler.execute(stmt, vec![], &ctx).await;

        // Should return batches
        assert!(result.is_ok());
        if let Ok(ExecutionResult::Rows { batches, .. }) = result {
            assert!(!batches.is_empty());
        }
    }
}
