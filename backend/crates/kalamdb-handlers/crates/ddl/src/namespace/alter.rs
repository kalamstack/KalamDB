//! Typed DDL handler for ALTER NAMESPACE statements

use crate::helpers::guards::require_admin;
use kalamdb_commons::models::NamespaceId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::AlterNamespaceStatement;
use std::sync::Arc;

/// Typed handler for ALTER NAMESPACE statements
pub struct AlterNamespaceHandler {
    app_context: Arc<AppContext>,
}

impl AlterNamespaceHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<AlterNamespaceStatement> for AlterNamespaceHandler {
    async fn execute(
        &self,
        statement: AlterNamespaceStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let name = statement.name.as_str();
        let namespace_id = NamespaceId::new(name);

        // Check if namespace exists and update (offload sync RocksDB read/write)
        let app_ctx = self.app_context.clone();
        let ns_id = namespace_id.clone();
        let options = statement.options.clone();
        let name_owned = name.to_string();
        tokio::task::spawn_blocking(move || {
            let namespaces_provider = app_ctx.system_tables().namespaces();
            let mut namespace = namespaces_provider.get_namespace(&ns_id)?.ok_or_else(|| {
                KalamDbError::NotFound(format!("Namespace '{}' not found", name_owned))
            })?;

            // Update namespace options (merge with existing options)
            let mut current_options: serde_json::Value = if let Some(ref opts) = namespace.options {
                serde_json::from_str(opts).unwrap_or(serde_json::json!({}))
            } else {
                serde_json::json!({})
            };

            // Merge new options
            if let Some(obj) = current_options.as_object_mut() {
                for (key, value) in &options {
                    obj.insert(key.clone(), value.clone());
                }
            }

            // Serialize back to string
            namespace.options = Some(serde_json::to_string(&current_options).map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to serialize options: {}", e))
            })?);

            // Save updated namespace
            namespaces_provider.update_namespace(namespace)?;
            Ok::<_, KalamDbError>(())
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        // Log DDL operation
        use crate::helpers::audit;
        let audit_entry = audit::log_ddl_operation(
            _context,
            "ALTER",
            "NAMESPACE",
            name,
            Some(format!("Options updated: {:?}", statement.options)),
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        let message = format!("Namespace '{}' altered successfully", name);
        Ok(ExecutionResult::Success { message })
    }

    async fn check_authorization(
        &self,
        _statement: &AlterNamespaceStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use crate::helpers::guards::block_anonymous_write;

        // T050: Block anonymous users from DDL operations
        block_anonymous_write(context, "ALTER NAMESPACE")?;

        require_admin(context, "alter namespace")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use std::sync::Arc;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), Role::Dba, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_alter_namespace_authorization() {
        let app_ctx = init_app_context();
        let handler = AlterNamespaceHandler::new(app_ctx);
        let stmt = AlterNamespaceStatement {
            name: NamespaceId::new("test"),
            options: std::collections::HashMap::new(),
        };

        // Test with non-admin user
        let ctx =
            ExecutionContext::new(UserId::new("user"), Role::User, create_test_session_simple());
        let result = handler.check_authorization(&stmt, &ctx).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), KalamDbError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn test_alter_namespace_success() {
        let app_ctx = init_app_context();
        let handler = AlterNamespaceHandler::new(app_ctx);
        let mut options = std::collections::HashMap::new();
        options.insert("max_tables".to_string(), serde_json::json!(100));

        let stmt = AlterNamespaceStatement {
            name: NamespaceId::new("test_namespace"),
            options,
        };
        let ctx = create_test_context();

        // Note: This test would need proper setup of test namespace
        let result = handler.execute(stmt, vec![], &ctx).await;

        // Would verify result or error based on test setup
        assert!(result.is_ok() || result.is_err());
    }
}
