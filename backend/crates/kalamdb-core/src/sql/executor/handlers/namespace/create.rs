//! Typed DDL handler for CREATE NAMESPACE statements
//!
//! This module demonstrates the TypedStatementHandler pattern where handlers
//! receive fully parsed AST structs instead of raw SQL strings.
//!
//! When a namespace is created, it is also registered as a DataFusion schema
//! so that queries like `SELECT * FROM namespace.table` work correctly.

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::sql::executor::handlers::typed::TypedStatementHandler;
use crate::sql::executor::helpers::guards::require_admin;
use datafusion::catalog::MemorySchemaProvider;
use kalamdb_commons::models::{NamespaceId, UserId};
use kalamdb_sql::ddl::CreateNamespaceStatement;
use std::sync::Arc;

/// Typed handler for CREATE NAMESPACE statements
pub struct CreateNamespaceHandler {
    app_context: Arc<AppContext>,
}

impl CreateNamespaceHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Register namespace as a DataFusion schema in the kalam catalog
    ///
    /// This enables queries like `SELECT * FROM namespace.table` to work.
    /// Uses DataFusion's native MemorySchemaProvider for dynamic schema registration.
    fn register_namespace_schema(&self, namespace_id: &NamespaceId) -> Result<(), KalamDbError> {
        let base_session = self.app_context.base_session_context();

        // Get the "kalam" catalog (default catalog configured in DataFusionSessionFactory)
        let catalog = base_session.catalog("kalam").ok_or_else(|| {
            KalamDbError::CatalogError("kalam catalog not found in session".to_string())
        })?;

        // Check if schema already registered (idempotent)
        if catalog.schema(namespace_id.as_str()).is_some() {
            log::debug!(
                "Schema '{}' already registered in DataFusion catalog",
                namespace_id.as_str()
            );
            return Ok(());
        }

        // Create a new schema provider for this namespace
        // Tables will be registered here when CREATE TABLE is executed
        let schema_provider = Arc::new(MemorySchemaProvider::new());

        catalog.register_schema(namespace_id.as_str(), schema_provider).map_err(|e| {
            KalamDbError::CatalogError(format!(
                "Failed to register schema '{}': {}",
                namespace_id.as_str(),
                e
            ))
        })?;

        log::debug!("Registered DataFusion schema for namespace '{}'", namespace_id.as_str());

        Ok(())
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<CreateNamespaceStatement> for CreateNamespaceHandler {
    async fn execute(
        &self,
        statement: CreateNamespaceStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let name = statement.name.as_str();

        // Validate namespace name
        kalamdb_sql::validation::validate_namespace_name(name).map_err(|e| e.to_string())?;

        // Check if namespace already exists
        let namespace_id = NamespaceId::new(name);
        let app_ctx = self.app_context.clone();
        let ns_id = namespace_id.clone();
        let existing = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().namespaces().get_namespace(&ns_id)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        if existing.is_some() {
            if statement.if_not_exists {
                let message = format!("Namespace '{}' already exists", name);
                return Ok(ExecutionResult::Success { message });
            }
            return Err(KalamDbError::AlreadyExists(format!(
                "Namespace '{}' already exists",
                name
            )));
        }

        // In cluster mode, route through executor for Raft replication
        // In standalone mode, the executor calls the provider directly
        let executor = self.app_context.executor();
        let created_by = Some(UserId::new(context.user_id().as_str()));
        let cmd = kalamdb_raft::MetaCommand::CreateNamespace {
            namespace_id: namespace_id.clone(),
            created_by,
        };

        executor.execute_meta(cmd).await.map_err(|e| {
            KalamDbError::ExecutionError(format!("Failed to create namespace via executor: {}", e))
        })?;

        // Register namespace as DataFusion schema for SQL queries
        self.register_namespace_schema(&namespace_id)?;

        // Log DDL operation
        use crate::sql::executor::helpers::audit;
        let audit_entry =
            audit::log_ddl_operation(context, "CREATE", "NAMESPACE", name, None, None);
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        let message = format!("Namespace '{}' created successfully", name);
        Ok(ExecutionResult::Success { message })
    }

    async fn check_authorization(
        &self,
        _statement: &CreateNamespaceStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use crate::sql::executor::helpers::guards::block_anonymous_write;

        // T050: Block anonymous users from DDL operations
        block_anonymous_write(context, "CREATE NAMESPACE")?;

        require_admin(context, "create namespace")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;
    use kalamdb_system::Namespace;

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::from("test_user"), Role::Dba, create_test_session_simple())
    }

    #[ignore = "Requires Raft for CREATE NAMESPACE"]
    #[tokio::test]
    async fn test_typed_create_namespace() {
        let app_ctx = test_app_context_simple();
        let handler = CreateNamespaceHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = CreateNamespaceStatement {
            name: NamespaceId::new("test_typed_ns"),
            if_not_exists: false,
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(result.is_ok());

        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("test_typed_ns"));
                assert!(message.contains("created successfully"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[ignore = "Requires Raft for CREATE NAMESPACE"]
    #[tokio::test]
    async fn test_typed_create_namespace_if_not_exists() {
        let app_ctx = test_app_context_simple();
        let handler = CreateNamespaceHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = CreateNamespaceStatement {
            name: NamespaceId::new("test_typed_ns_ine"),
            if_not_exists: true,
        };

        // First creation should succeed
        let result1 = handler.execute(stmt.clone(), vec![], &ctx).await;
        assert!(result1.is_ok());

        // Second creation with IF NOT EXISTS should also succeed
        let result2 = handler.execute(stmt, vec![], &ctx).await;
        assert!(result2.is_ok());

        match result2.unwrap() {
            ExecutionResult::Success { message } => assert!(message.contains("already exists")),
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_typed_authorization_check() {
        let app_ctx = test_app_context_simple();
        let handler = CreateNamespaceHandler::new(app_ctx);
        let user_ctx = ExecutionContext::new(
            UserId::from("regular_user"),
            Role::User,
            create_test_session_simple(),
        );

        let stmt = CreateNamespaceStatement {
            name: NamespaceId::new("unauthorized_ns"),
            if_not_exists: false,
        };

        let auth_result = handler.check_authorization(&stmt, &user_ctx).await;
        assert!(auth_result.is_err());

        if let Err(KalamDbError::Unauthorized(msg)) = auth_result {
            assert!(msg.contains("Insufficient privileges"));
        } else {
            panic!("Expected Unauthorized error");
        }
    }

    #[tokio::test]
    async fn test_typed_create_namespace_already_exists_returns_error_without_if_not_exists() {
        let app_ctx = test_app_context_simple();
        app_ctx
            .system_tables()
            .namespaces()
            .create_namespace(Namespace::new("existing_ns"))
            .expect("failed to seed namespace");

        let handler = CreateNamespaceHandler::new(app_ctx);
        let ctx = test_context();
        let stmt = CreateNamespaceStatement {
            name: NamespaceId::new("existing_ns"),
            if_not_exists: false,
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(matches!(result, Err(KalamDbError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_typed_create_namespace_already_exists_succeeds_with_if_not_exists() {
        let app_ctx = test_app_context_simple();
        app_ctx
            .system_tables()
            .namespaces()
            .create_namespace(Namespace::new("existing_ns_if"))
            .expect("failed to seed namespace");

        let handler = CreateNamespaceHandler::new(app_ctx);
        let ctx = test_context();
        let stmt = CreateNamespaceStatement {
            name: NamespaceId::new("existing_ns_if"),
            if_not_exists: true,
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(result.is_ok());
    }
}
