//! Typed handler for DROP TYPE.

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::DropTypeStatement;

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct DropTypeHandler {
    app_context: Arc<AppContext>,
}

impl DropTypeHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<DropTypeStatement> for DropTypeHandler {
    async fn execute(
        &self,
        statement: DropTypeStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "drop type")?;
        if statement.cascade {
            return Err(KalamDbError::InvalidSql(
                "DROP TYPE CASCADE is not supported; drop dependents first or use RESTRICT"
                    .to_string(),
            ));
        }
        let app = Arc::clone(&self.app_context);
        run_blocking(move || {
            let stores = app.system_tables().catalog_stores();
            let existing = stores
                .get_type(&statement.type_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            if existing.is_none() {
                if statement.if_exists {
                    return Ok(ExecutionResult::Success {
                        message: format!("Type {} does not exist, skipping", statement.type_id),
                    });
                }
                return Err(KalamDbError::NotFound(format!(
                    "type {} not found",
                    statement.type_id
                )));
            }
            if existing.as_ref().is_some_and(|catalog_type| {
                catalog_type.kind == kalamdb_commons::models::CatalogTypeKind::ImplicitTableRow
            }) {
                return Err(KalamDbError::InvalidSql(format!(
                    "cannot drop implicit table row type {}; drop the table instead",
                    statement.type_id
                )));
            }
            stores
                .drop_type(&statement.type_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok(ExecutionResult::Success {
                message: format!("Type {} dropped", statement.type_id),
            })
        })
        .await
    }
}
