//! Typed handler for STORAGE FLUSH TABLE statement

use std::sync::Arc;

use kalamdb_commons::{models::TableId, JobId, TableType};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_jobs::{executors::flush::FlushParams, AppContextJobsExt};
use kalamdb_sql::ddl::FlushTableStatement;
use kalamdb_system::JobType;

/// Handler for STORAGE FLUSH TABLE
pub struct FlushTableHandler {
    app_context: Arc<AppContext>,
}

impl FlushTableHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<FlushTableStatement> for FlushTableHandler {
    async fn execute(
        &self,
        statement: FlushTableStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Validate table exists via SchemaRegistry and fetch definition for table_type
        let registry = self.app_context.schema_registry();
        let manifest_service = self.app_context.manifest_service();
        let table_id = TableId::new(statement.namespace.clone(), statement.table_name.clone());
        let table_def = registry.get_table_if_exists(&table_id)?.ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "Table {}.{} not found",
                statement.namespace.as_str(),
                statement.table_name.as_str()
            ))
        })?;

        if table_def.table_type == TableType::Stream {
            return Err(KalamDbError::InvalidOperation(
                "STORAGE FLUSH TABLE is not supported for STREAM tables".to_string(),
            ));
        }

        let pending_manifests = manifest_service
            .get_pending_for_table(&table_id)
            .map_err(|e| KalamDbError::Other(format!("Pending manifest scan failed: {}", e)))?;

        if pending_manifests.is_empty() {
            return Ok(ExecutionResult::Success {
                message: format!(
                    "Storage flush skipped: no pending writes for table '{}.{}'",
                    statement.namespace.as_str(),
                    statement.table_name.as_str()
                ),
            });
        }

        // Create FlushParams with typed parameters
        let params = FlushParams {
            table_id: table_id.clone(),
            table_type: table_def.table_type,
            flush_threshold: None, // Use default from config
        };

        // Create a flush job via JobsManager (async execution handled in background loop)
        let job_manager = self.app_context.job_manager();
        let idempotency_key =
            format!("flush-{}-{}", statement.namespace.as_str(), statement.table_name.as_str());
        let job_id: JobId = match job_manager
            .create_job_typed(JobType::Flush, params, Some(idempotency_key), None)
            .await
        {
            Ok(job_id) => job_id,
            Err(KalamDbError::IdempotentConflict(_)) => {
                return Ok(ExecutionResult::Success {
                    message: format!(
                        "Storage flush skipped: a flush is already queued or running for table \
                         '{}.{}'",
                        statement.namespace.as_str(),
                        statement.table_name.as_str()
                    ),
                });
            },
            Err(err) => return Err(err),
        };

        Ok(ExecutionResult::Success {
            message: format!(
                "Storage flush started for table '{}.{}'. Job ID: {}",
                statement.namespace.as_str(),
                statement.table_name.as_str(),
                job_id.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &FlushTableStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::can_execute_maintenance;
        // Allow Service, DBA, and System roles to flush tables
        if !can_execute_maintenance(context.user_role()) {
            return Err(KalamDbError::Unauthorized(
                "STORAGE FLUSH TABLE requires Service, DBA, or System role".to_string(),
            ));
        }
        Ok(())
    }
}
