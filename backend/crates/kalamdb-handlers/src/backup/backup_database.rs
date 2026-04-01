//! Typed handler for BACKUP DATABASE statement

use kalamdb_commons::JobId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_jobs::executors::backup::BackupParams;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::BackupDatabaseStatement;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Handler for BACKUP DATABASE TO '<path>'
pub struct BackupDatabaseHandler {
    app_context: Arc<AppContext>,
}

impl BackupDatabaseHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<BackupDatabaseStatement> for BackupDatabaseHandler {
    async fn execute(
        &self,
        statement: BackupDatabaseStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let params = BackupParams {
            backup_path: statement.backup_path.clone(),
        };

        // Create a backup job via JobsManager (async execution in background)
        let job_manager = self.app_context.job_manager();
        let idempotency_key = format!(
            "backup:{}:{}",
            statement.backup_path,
            chrono::Utc::now().format("%Y%m%d-%H%M%S")
        );
        let job_id: JobId = job_manager
            .create_job_typed(JobType::Backup, params, Some(idempotency_key), None)
            .await?;

        Ok(ExecutionResult::Success {
            message: format!(
                "Database backup started to '{}'. Job ID: {}",
                statement.backup_path,
                job_id.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &BackupDatabaseStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::is_admin_role;
        if !is_admin_role(context.user_role()) {
            return Err(KalamDbError::Unauthorized(
                "BACKUP DATABASE requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
