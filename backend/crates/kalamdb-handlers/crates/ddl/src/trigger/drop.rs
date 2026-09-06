//! Typed handler for DROP TRIGGER.

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::DropTriggerStatement;

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct DropTriggerHandler {
    app_context: Arc<AppContext>,
}

impl DropTriggerHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<DropTriggerStatement> for DropTriggerHandler {
    async fn execute(
        &self,
        statement: DropTriggerStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "drop trigger")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || {
            let stores = app.system_tables().catalog_stores();
            let existing = stores
                .get_trigger(&statement.trigger_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            if existing.is_none() {
                if statement.if_exists {
                    return Ok(ExecutionResult::Success {
                        message: format!(
                            "Trigger {} did not exist, skipping",
                            statement.trigger_id
                        ),
                    });
                }
                return Err(KalamDbError::NotFound(format!(
                    "trigger {} not found",
                    statement.trigger_id
                )));
            }
            stores
                .drop_trigger(&statement.trigger_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok(ExecutionResult::Success {
                message: format!("Trigger {} dropped", statement.trigger_id),
            })
        })
        .await
    }
}
