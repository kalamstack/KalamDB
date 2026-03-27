//! Generic adapter for TypedStatementHandler → SqlStatementHandler
//!
//! This module provides a zero-boilerplate way to register typed handlers
//! in the HandlerRegistry without writing custom adapters for each handler.

use crate::error::KalamDbError;
use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::sql::executor::handler_registry::SqlStatementHandler;
use crate::sql::executor::handlers::typed::TypedStatementHandler;
use kalamdb_sql::classifier::SqlStatement;
use kalamdb_sql::DdlAst;
use std::marker::PhantomData;

/// Generic adapter that bridges TypedStatementHandler<T> to SqlStatementHandler
///
/// This eliminates the need to write custom adapter implementations for each handler.
/// Instead, you can wrap any TypedStatementHandler in this adapter.
///
/// # Type Parameters
/// - `H`: The handler type implementing `TypedStatementHandler<T>`
/// - `T`: The concrete statement type (e.g., `CreateNamespaceStatement`)
/// - `F`: Extractor function that converts `SqlStatement` → `Option<T>`
///
/// # Example
/// ```ignore
/// let handler = CreateNamespaceHandler::new(app_context);
/// let adapter = TypedHandlerAdapter::new(
///     handler,
///     |stmt| match stmt {
///         SqlStatement::CreateNamespace(s) => Some(s),
///         _ => None,
///     }
/// );
/// registry.register(discriminant, Arc::new(adapter));
/// ```
pub struct TypedHandlerAdapter<H, T, F>
where
    H: TypedStatementHandler<T>,
    T: DdlAst,
    F: Fn(SqlStatement) -> Option<T> + Send + Sync,
{
    handler: H,
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<H, T, F> TypedHandlerAdapter<H, T, F>
where
    H: TypedStatementHandler<T>,
    T: DdlAst,
    F: Fn(SqlStatement) -> Option<T> + Send + Sync,
{
    pub fn new(handler: H, extractor: F) -> Self {
        Self {
            handler,
            extractor,
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<H, T, F> SqlStatementHandler for TypedHandlerAdapter<H, T, F>
where
    H: TypedStatementHandler<T> + Send + Sync,
    T: DdlAst + Send + 'static,
    F: Fn(SqlStatement) -> Option<T> + Send + Sync,
{
    async fn execute(
        &self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let stmt = (self.extractor)(statement.clone()).ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "Handler received wrong statement type: {}",
                statement.name()
            ))
        })?;

        self.handler.execute(stmt, params, context).await
    }

    async fn check_authorization(
        &self,
        statement: &SqlStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        let stmt = (self.extractor)(statement.clone()).ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "Handler received wrong statement type: {}",
                statement.name()
            ))
        })?;

        self.handler.check_authorization(&stmt, context).await
    }
}

/// Adapter that bridges StatementHandler to SqlStatementHandler
///
/// This adapter wraps handlers that work directly with SqlStatement variants
/// (like DML handlers for INSERT/UPDATE/DELETE) rather than typed AST.
///
/// # Type Parameters
/// - `H`: The handler type implementing `StatementHandler`
///
/// # Example
/// ```ignore
/// let handler = InsertHandler::new(app_context);
/// let adapter = DynamicHandlerAdapter::new(handler);
/// registry.register(discriminant, Arc::new(adapter));
/// ```
pub struct DynamicHandlerAdapter<H>
where
    H: crate::sql::executor::handlers::StatementHandler,
{
    handler: H,
}

impl<H> DynamicHandlerAdapter<H>
where
    H: crate::sql::executor::handlers::StatementHandler,
{
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl<H> SqlStatementHandler for DynamicHandlerAdapter<H>
where
    H: crate::sql::executor::handlers::StatementHandler + Send + Sync,
{
    async fn execute(
        &self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        self.handler.execute(statement, params, context).await
    }

    async fn check_authorization(
        &self,
        statement: &SqlStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        self.handler.check_authorization(statement, context).await
    }
}

/// Macro to create an extractor function for a statement variant
///
/// # Example
/// ```ignore
/// let extractor = extract_statement!(SqlStatement::CreateNamespace);
/// ```
#[macro_export]
macro_rules! extract_statement {
    ($variant:path) => {
        |stmt| match stmt {
            $variant(s) => Some(s),
            _ => None,
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::executor::handlers::namespace::CreateNamespaceHandler;
    use crate::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_commons::models::{NamespaceId, UserId};
    use kalamdb_commons::Role;
    use kalamdb_sql::ddl::CreateNamespaceStatement;

    #[ignore = "Requires Raft for CREATE NAMESPACE"]
    #[tokio::test]
    async fn test_generic_adapter() {
        let app_ctx = test_app_context_simple();
        let handler = CreateNamespaceHandler::new(app_ctx);

        let adapter = TypedHandlerAdapter::new(handler, |stmt| match stmt.kind() {
            kalamdb_sql::classifier::SqlStatementKind::CreateNamespace(s) => {
                Some(s.clone())
            },
            _ => None,
        });
        let ctx = ExecutionContext::new(
            UserId::from("test_user"),
            Role::Dba,
            create_test_session_simple(),
        );

        let stmt = kalamdb_sql::classifier::SqlStatement::new(
            "CREATE NAMESPACE test_adapter_ns".to_string(),
            kalamdb_sql::classifier::SqlStatementKind::CreateNamespace(
                CreateNamespaceStatement {
                    name: NamespaceId::new("test_adapter_ns"),
                    if_not_exists: false,
                },
            ),
        );

        let result = adapter.execute(stmt, vec![], &ctx).await;
        assert!(result.is_ok());
    }

    #[ignore = "Requires Raft for CREATE NAMESPACE"]
    #[tokio::test]
    async fn test_wrong_statement_type() {
        let app_ctx = test_app_context_simple();
        let handler = CreateNamespaceHandler::new(app_ctx);

        let adapter = TypedHandlerAdapter::new(handler, |stmt| match stmt.kind() {
            kalamdb_sql::classifier::SqlStatementKind::CreateNamespace(s) => {
                Some(s.clone())
            },
            _ => None,
        });
        let ctx = ExecutionContext::new(
            UserId::from("test_user"),
            Role::Dba,
            create_test_session_simple(),
        );

        // Pass wrong statement type (ShowNamespaces instead of CreateNamespace)
        let stmt = kalamdb_sql::classifier::SqlStatement::new(
            "SHOW NAMESPACES".to_string(),
            kalamdb_sql::classifier::SqlStatementKind::ShowNamespaces(
                kalamdb_sql::ddl::ShowNamespacesStatement,
            ),
        );

        let result = adapter.execute(stmt, vec![], &ctx).await;
        assert!(result.is_err());
        match result {
            Err(KalamDbError::InvalidOperation(msg)) => {
                assert!(msg.contains("wrong statement type"));
            },
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    // NOTE: DynamicHandlerAdapter test removed - DML handlers now use TypedStatementHandler pattern
    // and no longer implement StatementHandler trait. The test_generic_adapter test above demonstrates
    // the TypedHandlerAdapter pattern which is the new approach.
}
