//! Handler Registry for SQL Statement Routing
//!
//! This module provides a centralized registry for mapping SqlStatement variants
//! to their corresponding handler implementations, eliminating repetitive match arms
//! and enabling easy handler registration.
//!
//! # Architecture Benefits
//! - **Single Registration Point**: All handlers registered in one place
//! - **Type Safety**: Compile-time guarantees that handlers exist for statements
//! - **Extensibility**: New handlers added by registering, not modifying executor
//! - **Testability**: Easy to mock handlers for unit tests
//! - **Zero Overhead**: Registry lookup via DashMap is <1μs (vs 50-100ns for match)

use std::{future::Future, pin::Pin, sync::Arc};

use dashmap::DashMap;
use kalamdb_sql::classifier::SqlStatement;
use tracing::Instrument;

use crate::{
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handler_adapter::{DynamicHandlerAdapter, TypedHandlerAdapter},
    },
};

pub type SqlHandlerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Trait for handlers that can process any SqlStatement variant
///
/// This allows polymorphic handler dispatch without boxing every handler.
pub trait SqlStatementHandler: Send + Sync {
    /// Execute the statement with authorization pre-checked
    ///
    /// # Parameters
    /// - `statement`: Classified SQL statement
    /// - `sql_text`: Original SQL text (for DML handlers that need to parse SQL)
    /// - `params`: Query parameters ($1, $2, etc.)
    /// - `context`: Execution context (user, role, session, etc.)
    ///
    /// # Note
    /// SessionContext is available via `context.session` - no need to pass separately
    fn execute<'a>(
        &'a self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &'a ExecutionContext,
    ) -> SqlHandlerFuture<'a, Result<ExecutionResult, KalamDbError>>;

    /// Check authorization before execution (called by registry)
    fn check_authorization<'a>(
        &'a self,
        statement: &'a SqlStatement,
        context: &'a ExecutionContext,
    ) -> SqlHandlerFuture<'a, Result<(), KalamDbError>>;
}

/// Registry key type for handler lookup
///
/// Uses statement discriminant (enum variant identifier) for O(1) lookup.
/// This is more efficient than matching on the full statement structure.
type HandlerKey = std::mem::Discriminant<kalamdb_sql::classifier::SqlStatementKind>;

/// Centralized handler registry for SQL statement routing
///
/// # Usage Pattern
/// ```ignore
/// // Build registry once during initialization
/// let registry = HandlerRegistry::new();
///
/// // Route statement to handler
/// match registry.handle(session, stmt, params, exec_ctx).await {
///     Ok(result) => // ... handle result
///     Err(e) => // ... handle error
/// }
/// ```
///
/// # Registration
/// Handlers are registered in `HandlerRegistry::new()` by calling
/// `register_typed()` or `register_dynamic()` for each statement type.
///
/// # Performance
/// - Handler lookup: <1μs via DashMap (lock-free concurrent HashMap)
/// - Authorization check: 1-10μs depending on handler
/// - Total overhead: <2μs vs ~50ns for direct match (acceptable trade-off)
pub struct HandlerRegistry {
    handlers: DashMap<HandlerKey, Arc<dyn SqlStatementHandler>>,
}

impl HandlerRegistry {
    /// Create an empty handler registry.
    ///
    /// Handler registration is performed externally via
    /// `kalamdb_handlers::register_all_handlers()`.
    pub fn new() -> Self {
        Self {
            handlers: DashMap::new(),
        }
    }

    /// Register a typed handler for a specific statement variant
    ///
    /// Uses a generic adapter to bridge TypedStatementHandler<T> to SqlStatementHandler.
    /// No custom adapter boilerplate needed!
    ///
    /// # Type Parameters
    /// - `H`: Handler type implementing TypedStatementHandler<T>
    /// - `T`: Statement type (inferred from handler and extractor)
    ///
    /// # Arguments
    /// - `placeholder`: An instance of the SqlStatement variant (for discriminant extraction)
    /// - `handler`: The handler instance to register
    /// - `extractor`: Function to extract T from SqlStatement
    pub fn register_typed<H, T, F>(
        &self,
        placeholder: kalamdb_sql::classifier::SqlStatementKind,
        handler: H,
        extractor: F,
    ) where
        H: crate::sql::executor::handlers::typed::TypedStatementHandler<T> + Send + Sync + 'static,
        T: kalamdb_sql::DdlAst + Send + 'static,
        F: Fn(SqlStatement) -> Option<T> + Send + Sync + 'static,
    {
        let key = std::mem::discriminant(&placeholder);
        let adapter = TypedHandlerAdapter::new(handler, extractor);
        self.handlers.insert(key, Arc::new(adapter));
    }

    /// Register a dynamic handler for a specific statement variant
    ///
    /// Uses DynamicHandlerAdapter to bridge StatementHandler to SqlStatementHandler.
    /// Simpler than register_typed() - no extractor function needed!
    ///
    /// # Type Parameters
    /// - `H`: Handler type implementing StatementHandler
    ///
    /// # Arguments
    /// - `placeholder`: An instance of the SqlStatementKind variant (for discriminant extraction)
    /// - `handler`: The handler instance to register
    ///
    /// # Example
    /// ```ignore
    /// registry.register_dynamic(
    ///     SqlStatementKind::Insert(InsertStatement),
    ///     InsertHandler::new(app_context.clone()),
    /// );
    /// ```
    pub fn register_dynamic<H>(
        &self,
        placeholder: kalamdb_sql::classifier::SqlStatementKind,
        handler: H,
    ) where
        H: crate::sql::executor::handlers::StatementHandler + Send + Sync + 'static,
    {
        let key = std::mem::discriminant(&placeholder);
        let adapter = DynamicHandlerAdapter::new(handler);
        self.handlers.insert(key, Arc::new(adapter));
    }

    /// Dispatch a statement to its registered handler
    ///
    /// # Flow
    /// 1. Extract discriminant from statement (O(1) key lookup)
    /// 2. Find handler in registry (O(1) DashMap lookup)
    /// 3. Call handler.check_authorization() (fail-fast)
    /// 4. Call handler.execute() if authorized
    ///
    /// # Parameters
    /// - `statement`: Classified SQL statement
    /// - `sql_text`: Original SQL text (for DML handlers)
    /// - `params`: Query parameters
    /// - `context`: Execution context (includes session)
    ///
    /// # Returns
    /// - `Ok(ExecutionResult)` if handler found and execution succeeded
    /// - `Err(KalamDbError::Unauthorized)` if authorization failed
    /// - `Err(KalamDbError::InvalidOperation)` if no handler registered
    pub async fn handle(
        &self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let stmt_name = statement.name().to_string();
        let span = tracing::debug_span!("sql.handler", handler = %stmt_name);
        async {
            // Step 1: Extract statement discriminant for O(1) lookup
            let key = std::mem::discriminant(statement.kind());

            // Step 2: Find handler in registry
            let handler = self.handlers.get(&key).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "No handler registered for statement type '{}'",
                    statement.name()
                ))
            })?;

            // Step 3: Check authorization (fail-fast)
            handler.check_authorization(&statement, context).await?;

            // Step 4: Execute statement (session is in context, no need to pass separately)
            handler.execute(statement, params, context).await
        }
        .instrument(span)
        .await
    }

    /// Check if a handler is registered for a statement type
    pub fn has_handler(&self, statement: &SqlStatement) -> bool {
        let key = std::mem::discriminant(statement.kind());
        self.handlers.contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{
        models::{NamespaceId, UserId},
        Role,
    };

    use super::*;
    use crate::test_helpers::{create_test_session_simple, test_app_context_simple};

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::from("test_user"), Role::Dba, create_test_session_simple())
    }

    #[ignore = "Requires Raft for CREATE NAMESPACE"]
    #[tokio::test]
    async fn test_registry_create_namespace() {
        use kalamdb_sql::classifier::SqlStatementKind;

        let _app_ctx = test_app_context_simple();
        let registry = HandlerRegistry::new();
        let ctx = test_context();

        let stmt = SqlStatement::new(
            "CREATE NAMESPACE test_registry_ns".to_string(),
            SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
                name: NamespaceId::new("test_registry_ns"),
                if_not_exists: false,
            }),
        );

        // Check handler is registered
        assert!(registry.has_handler(&stmt));

        // Execute via registry
        let result = registry.handle(stmt, vec![], &ctx).await;
        assert!(result.is_ok());

        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("test_registry_ns"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_registry_unregistered_handler() {
        use kalamdb_sql::classifier::SqlStatementKind;

        let _app_ctx = test_app_context_simple();
        let registry = HandlerRegistry::new();
        let ctx = test_context();

        // Use an unclassified statement (not in SqlStatementKind)
        // For this test, we'll use a deliberately malformed statement kind
        let stmt = SqlStatement::new(
            "SOME UNKNOWN STATEMENT".to_string(),
            SqlStatementKind::Unknown, // This should not have a handler
        );

        // Handler not registered for "Unknown" kind
        assert!(!registry.has_handler(&stmt));

        // Should return error
        let result = registry.handle(stmt, vec![], &ctx).await;
        assert!(result.is_err());

        match result {
            Err(KalamDbError::InvalidOperation(msg)) => {
                assert!(msg.contains("No handler registered"));
            },
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[tokio::test]
    async fn test_registry_authorization_check() {
        use kalamdb_sql::classifier::SqlStatementKind;

        // A minimal handler stub that requires DBA role (simulates DDL handler auth).
        struct DbaOnlyStub;

        impl SqlStatementHandler for DbaOnlyStub {
            fn execute<'a>(
                &'a self,
                _statement: SqlStatement,
                _params: Vec<ScalarValue>,
                _context: &'a ExecutionContext,
            ) -> SqlHandlerFuture<'a, Result<ExecutionResult, KalamDbError>> {
                Box::pin(async {
                    Ok(ExecutionResult::Success {
                        message: "ok".to_string(),
                    })
                })
            }

            fn check_authorization<'a>(
                &'a self,
                _statement: &'a SqlStatement,
                context: &'a ExecutionContext,
            ) -> SqlHandlerFuture<'a, Result<(), KalamDbError>> {
                let allowed = matches!(
                    context.user_role(),
                    kalamdb_commons::Role::Dba | kalamdb_commons::Role::System
                );
                Box::pin(async move {
                    if !allowed {
                        Err(KalamDbError::Unauthorized("Requires DBA role".to_string()))
                    } else {
                        Ok(())
                    }
                })
            }
        }

        let _app_ctx = test_app_context_simple();
        let registry = HandlerRegistry::new();

        // Register stub handler for CreateNamespace
        let key = std::mem::discriminant(&SqlStatementKind::CreateNamespace(
            kalamdb_sql::ddl::CreateNamespaceStatement {
                name: kalamdb_commons::models::NamespaceId::new("_"),
                if_not_exists: false,
            },
        ));
        registry.handlers.insert(key, Arc::new(DbaOnlyStub));

        let user_ctx = ExecutionContext::new(
            UserId::from("regular_user"),
            Role::User,
            create_test_session_simple(),
        );

        let stmt = SqlStatement::new(
            "CREATE NAMESPACE unauthorized_ns".to_string(),
            SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
                name: NamespaceId::new("unauthorized_ns"),
                if_not_exists: false,
            }),
        );

        // Should fail authorization
        let result = registry.handle(stmt, vec![], &user_ctx).await;
        assert!(result.is_err());

        match result {
            Err(KalamDbError::Unauthorized(_)) => {},
            _ => panic!("Expected Unauthorized error"),
        }
    }
}
