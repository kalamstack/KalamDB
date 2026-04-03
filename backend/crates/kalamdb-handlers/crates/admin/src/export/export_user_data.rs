//! Typed handler for EXPORT USER DATA statement

use kalamdb_commons::JobId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_jobs::executors::user_export::UserExportParams;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::ExportUserDataStatement;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Handler for EXPORT USER DATA
///
/// Creates a UserExport job that asynchronously exports all user tables
/// for the calling user and packages them as a downloadable ZIP archive.
pub struct ExportUserDataHandler {
    app_context: Arc<AppContext>,
}

impl ExportUserDataHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<ExportUserDataStatement> for ExportUserDataHandler {
    async fn execute(
        &self,
        _statement: ExportUserDataStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let user_id = context.user_id().to_string();

        // Generate a unique export ID
        let export_id =
            format!("export-{}-{}", &user_id, chrono::Utc::now().format("%Y%m%d-%H%M%S"));

        let params = UserExportParams {
            user_id: user_id.clone(),
            export_id: export_id.clone(),
        };

        // Idempotency: one export per user per day
        let idempotency_key =
            format!("user_export:{}:{}", &user_id, chrono::Utc::now().format("%Y%m%d"));

        let job_manager = self.app_context.job_manager();
        let job_id: JobId = job_manager
            .create_job_typed(JobType::UserExport, params, Some(idempotency_key), None)
            .await?;

        Ok(ExecutionResult::Success {
            message: format!(
                "User data export started. Job ID: {}. Use SHOW EXPORT to check status and get the download link.",
                job_id.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &ExportUserDataStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // Any authenticated user can export their own data
        Ok(())
    }
}
