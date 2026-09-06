//! Typed handler for ALTER TRIGGER ENABLE/DISABLE.

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::AlterTriggerStatement;

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct AlterTriggerHandler {
    app_context: Arc<AppContext>,
}

impl AlterTriggerHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<AlterTriggerStatement> for AlterTriggerHandler {
    async fn execute(
        &self,
        statement: AlterTriggerStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "alter trigger")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || {
            let stores = app.system_tables().catalog_stores();
            let Some(mut trigger) = stores
                .get_trigger(&statement.trigger_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
            else {
                return Err(KalamDbError::NotFound(format!(
                    "trigger {} not found",
                    statement.trigger_id
                )));
            };
            trigger.enabled = statement.enabled;
            stores
                .upsert_trigger(trigger)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            let state = if statement.enabled {
                "enabled"
            } else {
                "disabled"
            };
            Ok(ExecutionResult::Success {
                message: format!("Trigger {} {state}", statement.trigger_id),
            })
        })
        .await
    }
}
