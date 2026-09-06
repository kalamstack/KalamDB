//! Typed handler for DROP PROCEDURE.

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::DropProcedureStatement;

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct DropProcedureHandler {
    app_context: Arc<AppContext>,
}

impl DropProcedureHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<DropProcedureStatement> for DropProcedureHandler {
    async fn execute(
        &self,
        statement: DropProcedureStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "drop procedure")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || {
            let stores = app.system_tables().catalog_stores();
            let existing = stores
                .get_routine(&statement.routine_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            if existing.is_none() {
                if statement.if_exists {
                    return Ok(ExecutionResult::Success {
                        message: format!(
                            "Procedure {} does not exist, skipping",
                            statement.routine_id
                        ),
                    });
                }
                return Err(KalamDbError::NotFound(format!(
                    "procedure {} not found",
                    statement.routine_id
                )));
            }
            stores
                .drop_routine(&statement.routine_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok(ExecutionResult::Success {
                message: format!("Procedure {} dropped", statement.routine_id),
            })
        })
        .await
    }
}
