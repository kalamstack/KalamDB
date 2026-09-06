//! GRANT / REVOKE EXECUTE handlers.

use std::sync::Arc;

use kalamdb_commons::models::{RoutineGrantId, RoutineGrantee};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::{ExecuteGrantee, GrantExecuteStatement, RevokeExecuteStatement};
use kalamdb_system::CatalogRoutineGrant;

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct GrantExecuteHandler {
    app_context: Arc<AppContext>,
}

impl GrantExecuteHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<GrantExecuteStatement> for GrantExecuteHandler {
    async fn execute(
        &self,
        statement: GrantExecuteStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "grant execute")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || {
            let stores = app.system_tables().catalog_stores();
            if stores
                .get_routine(&statement.routine_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
                .is_none()
            {
                return Err(KalamDbError::NotFound(format!(
                    "procedure {} not found",
                    statement.routine_id
                )));
            }
            let grantee = map_grantee(&statement.grantee);
            let grant = CatalogRoutineGrant {
                grant_id: RoutineGrantId::new(&statement.routine_id, &grantee),
                routine_id: statement.routine_id.clone(),
                grantee,
            };
            stores
                .upsert_grant(grant)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok::<ExecutionResult, KalamDbError>(ExecutionResult::Success {
                message: format!(
                    "Granted EXECUTE on {} to {}",
                    statement.routine_id,
                    statement.grantee.as_sql()
                ),
            })
        })
        .await
    }
}

pub struct RevokeExecuteHandler {
    app_context: Arc<AppContext>,
}

impl RevokeExecuteHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<RevokeExecuteStatement> for RevokeExecuteHandler {
    async fn execute(
        &self,
        statement: RevokeExecuteStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "revoke execute")?;
        let app = Arc::clone(&self.app_context);
        run_blocking(move || {
            let stores = app.system_tables().catalog_stores();
            let grantee = map_grantee(&statement.grantee);
            let grant_id = RoutineGrantId::new(&statement.routine_id, &grantee);
            stores
                .delete_grant(&grant_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
            Ok::<ExecutionResult, KalamDbError>(ExecutionResult::Success {
                message: format!(
                    "Revoked EXECUTE on {} from {}",
                    statement.routine_id,
                    statement.grantee.as_sql()
                ),
            })
        })
        .await
    }
}

fn map_grantee(grantee: &ExecuteGrantee) -> RoutineGrantee {
    match grantee {
        ExecuteGrantee::Public => RoutineGrantee::Public,
        ExecuteGrantee::User => RoutineGrantee::User,
        ExecuteGrantee::Service => RoutineGrantee::Service,
        ExecuteGrantee::Role(name) => RoutineGrantee::Role(name.clone()),
    }
}
