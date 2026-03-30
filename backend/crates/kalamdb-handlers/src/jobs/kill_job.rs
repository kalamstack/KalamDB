//! Typed handler for KILL JOB statement

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_commons::JobId;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::JobCommand;
use std::sync::Arc;

/// Handler for KILL JOB
pub struct KillJobHandler {
    app_context: Arc<AppContext>,
}

impl KillJobHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<JobCommand> for KillJobHandler {
    async fn execute(
        &self,
        statement: JobCommand,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let job_manager = self.app_context.job_manager();
        match statement {
            JobCommand::Kill { job_id } => {
                let job_id_typed = JobId::new(job_id.clone());
                job_manager.cancel_job(&job_id_typed).await?;

                // Log DDL operation (treating KILL JOB as an admin operation)
                use crate::helpers::audit;
                let audit_entry =
                    audit::log_ddl_operation(context, "KILL", "JOB", &job_id, None, None);
                audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

                Ok(ExecutionResult::JobKilled {
                    job_id,
                    status: "cancelled".to_string(),
                })
            },
        }
    }

    async fn check_authorization(
        &self,
        _statement: &JobCommand,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        if !context.is_admin() {
            return Err(KalamDbError::Unauthorized(
                "KILL JOB requires DBA or System role".to_string(),
            ));
        }
        Ok(())
    }
}
