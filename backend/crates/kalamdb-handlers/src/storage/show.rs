//! Typed DDL handler for SHOW STORAGES statements

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::ShowStoragesStatement;
use std::sync::Arc;

/// Typed handler for SHOW STORAGES statements
pub struct ShowStoragesHandler {
    app_context: Arc<AppContext>,
}

impl ShowStoragesHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<ShowStoragesStatement> for ShowStoragesHandler {
    async fn execute(
        &self,
        _statement: ShowStoragesStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Query all storages (offload sync RocksDB read)
        let app_ctx = self.app_context.clone();
        let batches = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().storages().scan_all_storages()
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

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
        _statement: &ShowStoragesStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // SHOW STORAGES allowed for all authenticated users
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;
    use std::sync::Arc;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), Role::User, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_show_storages_authorization() {
        let app_ctx = init_app_context();
        let handler = ShowStoragesHandler::new(app_ctx);
        let stmt = ShowStoragesStatement {};

        // All users can show storages
        let ctx = create_test_context();
        let result = handler.check_authorization(&stmt, &ctx).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_show_storages_success() {
        let app_ctx = init_app_context();
        let handler = ShowStoragesHandler::new(app_ctx);
        let stmt = ShowStoragesStatement {};
        let ctx = create_test_context();

        let result = handler.execute(stmt, vec![], &ctx).await;

        // Should return batches
        assert!(result.is_ok());
        if let Ok(ExecutionResult::Rows { batches, .. }) = result {
            assert!(!batches.is_empty());
        }
    }
}
