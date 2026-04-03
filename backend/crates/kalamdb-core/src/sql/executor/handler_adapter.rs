//! Generic adapter for TypedStatementHandler → SqlStatementHandler
//!
//! This module provides a zero-boilerplate way to register typed handlers
//! in the HandlerRegistry without writing custom adapters for each handler.

use crate::error::KalamDbError;
use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::sql::executor::handler_registry::{SqlHandlerFuture, SqlStatementHandler};
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

impl<H, T, F> SqlStatementHandler for TypedHandlerAdapter<H, T, F>
where
    H: TypedStatementHandler<T> + Send + Sync,
    T: DdlAst + Send + 'static,
    F: Fn(SqlStatement) -> Option<T> + Send + Sync,
{
    fn execute<'a>(
        &'a self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &'a ExecutionContext,
    ) -> SqlHandlerFuture<'a, Result<ExecutionResult, KalamDbError>> {
        Box::pin(async move {
            let stmt = (self.extractor)(statement.clone()).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Handler received wrong statement type: {}",
                    statement.name()
                ))
            })?;

            self.handler.execute(stmt, params, context).await
        })
    }

    fn check_authorization<'a>(
        &'a self,
        statement: &'a SqlStatement,
        context: &'a ExecutionContext,
    ) -> SqlHandlerFuture<'a, Result<(), KalamDbError>> {
        Box::pin(async move {
            let stmt = (self.extractor)(statement.clone()).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Handler received wrong statement type: {}",
                    statement.name()
                ))
            })?;

            self.handler.check_authorization(&stmt, context).await
        })
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

impl<H> SqlStatementHandler for DynamicHandlerAdapter<H>
where
    H: crate::sql::executor::handlers::StatementHandler + Send + Sync,
{
    fn execute<'a>(
        &'a self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &'a ExecutionContext,
    ) -> SqlHandlerFuture<'a, Result<ExecutionResult, KalamDbError>> {
        Box::pin(async move { self.handler.execute(statement, params, context).await })
    }

    fn check_authorization<'a>(
        &'a self,
        statement: &'a SqlStatement,
        context: &'a ExecutionContext,
    ) -> SqlHandlerFuture<'a, Result<(), KalamDbError>> {
        Box::pin(async move { self.handler.check_authorization(statement, context).await })
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

// Tests for TypedHandlerAdapter live in kalamdb-handlers where the concrete handlers reside.
