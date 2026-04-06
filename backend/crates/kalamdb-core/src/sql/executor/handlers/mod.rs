//! SQL Execution Handlers
//!
//! This module provides modular handlers for different types of SQL operations:
//! - **models**: Core types (ExecutionContext, ScalarValue, ExecutionResult)
//! - **authorization**: Authorization gateway (COMPLETE - Phase 9.3)
//! - **transaction**: Transaction handling (COMPLETE - Phase 9.4)
//! - **ddl**: DDL operations (future)
//! - **dml**: DML operations (future)
//! - **query**: Query execution (future)
//! - **flush**: Flush operations (future)
//! - **subscription**: Live query subscriptions (future)
//! - **user_management**: User CRUD operations (future)
//! - **table_registry**: Table registration (REMOVED - deprecated REGISTER/UNREGISTER)
//! - **system_commands**: VACUUM, OPTIMIZE, ANALYZE (future)
//! - **helpers**: Shared helper functions (future)
//! - **audit**: Audit logging (future)

use crate::error::KalamDbError;
use kalamdb_sql::classifier::SqlStatement;
use std::future::Future;

// Typed handler trait (stays in core; handler impls are in kalamdb-handlers)
pub mod typed;

// Re-export core types from executor/models for convenience
pub use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};

// Re-export legacy placeholder handlers
pub use typed::TypedStatementHandler;

/// Common trait for SQL statement handlers
///
/// All statement handlers should implement this trait to provide a consistent
/// interface for executing SQL operations.
///
/// **Phase 2 Task T016**: Unified handler interface for all SQL statement types
///
/// # Example
///
/// ```ignore
/// use kalamdb_core::sql::executor::handlers::{StatementHandler, ExecutionContext, ExecutionResult};
/// use async_trait::async_trait;
///
/// struct MyHandler;
///
/// #[async_trait]
/// impl StatementHandler for MyHandler {
///     async fn execute(
///         &self,
///         session: &SessionContext,
///         statement: SqlStatement,
///         params: Vec<ScalarValue>,
///         context: &ExecutionContext,
///     ) -> Result<ExecutionResult, KalamDbError> {
///         // Handler implementation
///         Ok(ExecutionResult::Success("Completed".to_string()))
///     }
///
///     async fn check_authorization(
///         &self,
///         statement: &SqlStatement,
///         context: &ExecutionContext,
///     ) -> Result<(), KalamDbError> {
///         // Authorization checks
///         Ok(())
///     }
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait StatementHandler: Send + Sync {
    /// Execute a SQL statement with full context
    ///
    /// # Arguments
    /// * `statement` - Parsed SQL statement (from kalamdb_sql)
    /// * `params` - Parameter values for prepared statements ($1, $2, ... placeholders)
    /// * `context` - Execution context (user, role, namespace, audit info, session)
    ///
    /// # Returns
    /// * `Ok(ExecutionResult)` - Successful execution result
    /// * `Err(KalamDbError)` - Execution error
    ///
    /// # Note
    /// SessionContext is available via `context.session` - no need to pass separately
    fn execute<'a>(
        &'a self,
        statement: SqlStatement,
        params: Vec<ScalarValue>,
        context: &'a ExecutionContext,
    ) -> impl Future<Output = Result<ExecutionResult, KalamDbError>> + Send + 'a;

    /// Validate authorization before execution
    ///
    /// Called by the authorization gateway before routing to the handler.
    /// Handlers can implement statement-specific authorization logic here.
    ///
    /// # Arguments
    /// * `statement` - SQL statement to authorize
    /// * `context` - Execution context with user/role information
    ///
    /// # Returns
    /// * `Ok(())` - Authorization passed
    /// * `Err(KalamDbError::PermissionDenied)` - Authorization failed
    fn check_authorization<'a>(
        &'a self,
        statement: &'a SqlStatement,
        context: &'a ExecutionContext,
    ) -> impl Future<Output = Result<(), KalamDbError>> + Send + 'a {
        // Default implementation: delegate to AuthorizationHandler
        //AuthorizationHandler::check_authorization(context, statement)
        let result = statement
            .check_authorization(context.user_role())
            .map_err(KalamDbError::PermissionDenied);
        async move { result }
    }
}
