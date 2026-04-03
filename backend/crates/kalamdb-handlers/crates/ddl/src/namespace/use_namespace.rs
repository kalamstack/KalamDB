//! USE NAMESPACE handler
//!
//! This handler sets the default schema for unqualified table names using
//! DataFusion's native configuration system.
//!
//! After executing `USE namespace1`, queries like `SELECT * FROM users`
//! will resolve to `kalam.namespace1.users`.

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::UseNamespaceStatement;
use std::sync::Arc;

/// Handler for USE NAMESPACE / USE / SET NAMESPACE statements
///
/// Uses DataFusion's native `datafusion.catalog.default_schema` configuration
/// to change the default schema for the current session.
pub struct UseNamespaceHandler {
    app_context: Arc<AppContext>,
}

impl UseNamespaceHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<UseNamespaceStatement> for UseNamespaceHandler {
    async fn execute(
        &self,
        statement: UseNamespaceStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let namespace_name = statement.namespace.as_str();

        // Verify namespace exists (offload sync RocksDB read)
        let app_ctx = self.app_context.clone();
        let ns = statement.namespace.clone();
        let exists = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().namespaces().get_namespace(&ns)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        if exists.is_none() {
            return Err(KalamDbError::NotFound(format!(
                "Namespace '{}' does not exist",
                namespace_name
            )));
        }

        // Use DataFusion's native SET command to change the default schema
        // This is the recommended way per DataFusion documentation
        let session = context.create_session_with_user();
        let set_sql = format!("SET datafusion.catalog.default_schema = '{}'", namespace_name);

        session.sql(&set_sql).await.map_err(|e| {
            KalamDbError::ExecutionError(format!("Failed to set default namespace: {}", e))
        })?;

        log::info!(
            "User '{}' switched to namespace '{}'",
            context.user_id().as_str(),
            namespace_name
        );

        Ok(ExecutionResult::Success {
            message: format!("Namespace switched to '{}'", namespace_name),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &UseNamespaceStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // All users can switch namespaces (they still need table-level permissions)
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::from("test_user"), Role::User, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_use_namespace_not_found() {
        let app_ctx = test_app_context_simple();
        let handler = UseNamespaceHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = UseNamespaceStatement {
            namespace: kalamdb_commons::models::NamespaceId::new("nonexistent"),
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(result.is_err());

        if let Err(KalamDbError::NotFound(msg)) = result {
            assert!(msg.contains("nonexistent"));
        } else {
            panic!("Expected NotFound error");
        }
    }
}
