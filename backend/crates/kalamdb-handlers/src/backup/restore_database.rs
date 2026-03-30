//! Typed handler for RESTORE DATABASE statement

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_jobs::executors::restore::RestoreParams;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_commons::JobId;
use kalamdb_sql::ddl::RestoreDatabaseStatement;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Handler for RESTORE DATABASE FROM '<path>'
pub struct RestoreDatabaseHandler {
    app_context: Arc<AppContext>,
}

impl RestoreDatabaseHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<RestoreDatabaseStatement> for RestoreDatabaseHandler {
    async fn execute(
        &self,
        statement: RestoreDatabaseStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Verify backup directory exists before creating job
        let backup_path = std::path::Path::new(&statement.backup_path);
        if !backup_path.exists() {
            return Err(KalamDbError::NotFound(format!(
                "Backup directory '{}' not found",
                statement.backup_path
            )));
        }
        if !backup_path.is_dir() {
            return Err(KalamDbError::InvalidOperation(format!(
                "'{}' is not a directory. RESTORE DATABASE expects a backup directory, not a file.",
                statement.backup_path
            )));
        }

        let params = RestoreParams {
            backup_path: statement.backup_path.clone(),
        };

        // Create a restore job via JobsManager (async execution in background)
        let job_manager = self.app_context.job_manager();
        let idempotency_key = format!(
            "restore:{}:{}",
            statement.backup_path,
            chrono::Utc::now().format("%Y%m%d-%H%M%S")
        );
        let job_id: JobId = job_manager
            .create_job_typed(JobType::Restore, params, Some(idempotency_key), None)
            .await?;

        Ok(ExecutionResult::Success {
            message: format!(
                "Database restore started from '{}'. Job ID: {}. Server restart required after completion.",
                statement.backup_path,
                job_id.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &RestoreDatabaseStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::is_admin_role;
        if !is_admin_role(context.user_role()) {
            return Err(KalamDbError::Unauthorized(
                "RESTORE DATABASE requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
