//! Typed handler for STORAGE COMPACT TABLE statement

use std::sync::Arc;

use kalamdb_commons::{models::TableId, schemas::TableType, JobId};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_jobs::{executors::compact::CompactParams, AppContextJobsExt};
use kalamdb_sql::ddl::CompactTableStatement;
use kalamdb_system::JobType;

/// Handler for STORAGE COMPACT TABLE
pub struct CompactTableHandler {
    app_context: Arc<AppContext>,
}

impl CompactTableHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<CompactTableStatement> for CompactTableHandler {
    async fn execute(
        &self,
        statement: CompactTableStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let registry = self.app_context.schema_registry();
        let table_id = TableId::new(statement.namespace.clone(), statement.table_name.clone());
        let table_def = registry.get_table_if_exists(&table_id)?.ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "Table {}.{} not found",
                statement.namespace.as_str(),
                statement.table_name.as_str()
            ))
        })?;

        match table_def.table_type {
            TableType::Stream => {
                return Err(KalamDbError::InvalidOperation(
                    "STORAGE COMPACT TABLE is not supported for STREAM tables".to_string(),
                ))
            },
            TableType::System => {
                return Err(KalamDbError::InvalidOperation(
                    "STORAGE COMPACT TABLE is not supported for SYSTEM tables".to_string(),
                ))
            },
            TableType::User | TableType::Shared => {},
        }

        let params = CompactParams {
            table_id: table_id.clone(),
            table_type: table_def.table_type,
            target_file_size_mb: 128,
        };

        let job_manager = self.app_context.job_manager();
        let idempotency_key =
            format!("compact-{}-{}", statement.namespace.as_str(), statement.table_name.as_str());
        let job_id: JobId = job_manager
            .create_job_typed(JobType::Compact, params, Some(idempotency_key), None)
            .await?;

        Ok(ExecutionResult::Success {
            message: format!(
                "Storage compaction started for table '{}.{}'. Job ID: {}",
                statement.namespace.as_str(),
                statement.table_name.as_str(),
                job_id.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CompactTableStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::can_execute_maintenance;
        if !can_execute_maintenance(context.user_role()) {
            return Err(KalamDbError::Unauthorized(
                "STORAGE COMPACT TABLE requires Service, DBA, or System role".to_string(),
            ));
        }
        Ok(())
    }
}
