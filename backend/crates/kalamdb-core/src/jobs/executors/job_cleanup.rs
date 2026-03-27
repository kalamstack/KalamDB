//! Job Cleanup Executor
//!
//! **Phase 9**: JobExecutor implementation for cleaning up old job history
//!
//! Handles retention of system.jobs table to prevent infinite growth.
//!
//! ## Responsibilities
//! - Delete completed/failed/cancelled jobs older than retention period
//! - Track cleanup metrics
//!
//! ## Parameters Format
//! ```json
//! {
//!   "retention_days": 30
//! }
//! ```

use crate::error::KalamDbError;
use crate::jobs::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

/// Typed parameters for job cleanup operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCleanupParams {
    /// Retention period in days (required, must be > 0)
    pub retention_days: i64,
}

impl JobParams for JobCleanupParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.retention_days <= 0 {
            return Err(KalamDbError::InvalidOperation(
                "retention_days must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Job Cleanup Executor
///
/// Executes cleanup operations for system.jobs table.
pub struct JobCleanupExecutor;

impl JobCleanupExecutor {
    /// Create a new JobCleanupExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for JobCleanupExecutor {
    type Params = JobCleanupParams;

    fn job_type(&self) -> JobType {
        JobType::JobCleanup
    }

    fn name(&self) -> &'static str {
        "JobCleanupExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting job history cleanup operation");

        let params = ctx.params();
        let retention_days = params.retention_days;

        ctx.log_info(&format!("Cleaning up jobs older than {} days", retention_days));

        // Offload sync RocksDB scan+delete operations to blocking thread
        let app_ctx = ctx.app_ctx.clone();
        let (deleted_count, deleted_nodes) = tokio::task::spawn_blocking(move || {
            let jobs_provider = app_ctx.system_tables().jobs();
            let job_nodes_provider = app_ctx.system_tables().job_nodes();

            let deleted_count = jobs_provider
                .cleanup_old_jobs(retention_days)
                .map_err(|e| KalamDbError::ExecutionError(format!("Failed to cleanup old jobs: {}", e)))?;

            let deleted_nodes = job_nodes_provider
                .cleanup_old_job_nodes(retention_days)
                .map_err(|e| KalamDbError::ExecutionError(format!("Failed to cleanup old job_nodes: {}", e)))?;

            Ok::<_, KalamDbError>((deleted_count, deleted_nodes))
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        let message = format!(
            "Job history cleanup completed - {} jobs and {} job_nodes deleted (retention: {} days)",
            deleted_count, deleted_nodes, retention_days
        );

        ctx.log_info(&message);

        Ok(JobDecision::Completed {
            message: Some(message),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Job cleanup cancellation requested");
        Ok(())
    }
}

impl Default for JobCleanupExecutor {
    fn default() -> Self {
        Self::new()
    }
}
