//! Typed statement handler trait over parsed AST statements

use super::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::error::KalamDbError;
use kalamdb_sql::DdlAst;
use std::future::Future;

#[allow(async_fn_in_trait)]
pub trait TypedStatementHandler<T: DdlAst>: Send + Sync {
    /// Execute a typed parsed statement with full context
    ///
    /// # Parameters
    /// * `statement` - Parsed statement AST
    /// * `params` - Query parameters ($1, $2, etc.)
    /// * `context` - Execution context (user, role, namespace, session, etc.)
    ///
    /// # Note
    /// SessionContext is available via `context.session` - no need to pass separately
    fn execute<'a>(
        &'a self,
        statement: T,
        params: Vec<ScalarValue>,
        context: &'a ExecutionContext,
    ) -> impl Future<Output = Result<ExecutionResult, KalamDbError>> + Send + 'a;

    /// Authorization hook for typed statements (optional override)
    fn check_authorization<'a>(
        &'a self,
        _statement: &'a T,
        _context: &'a ExecutionContext,
    ) -> impl Future<Output = Result<(), KalamDbError>> + Send + 'a {
        async move { Ok(()) }
    }
}
