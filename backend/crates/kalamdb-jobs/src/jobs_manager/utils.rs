use super::types::JobsManager;
use chrono::Utc;
use kalamdb_commons::{JobId, NodeId};
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_raft::commands::MetaCommand;
use kalamdb_raft::NodeStatus;
use kalamdb_system::providers::jobs::models::JobFilter;
use kalamdb_system::{JobStatus, JobType};
use log::Level;

/// Lazy-formatting version of log_job_event.
/// Avoids String allocation when the log level is disabled.
macro_rules! log_job {
    ($self:expr, $job_id:expr, $level:expr, $($arg:tt)+) => {
        if log::log_enabled!($level) {
            $self.log_job_event($job_id, &$level, &format!($($arg)+));
        }
    };
}
pub(crate) use log_job;

impl JobsManager {
    /// Generate typed JobId with prefix
    ///
    /// Prefixes:
    /// - FL: Flush jobs
    /// - CL: Cleanup jobs
    /// - RT: Retention jobs
    /// - SE: Stream eviction jobs
    /// - UC: User cleanup jobs
    /// - CO: Compaction jobs (future)
    /// - BK: Backup jobs (future)
    /// - RS: Restore jobs (future)
    pub(crate) fn generate_job_id(&self, job_type: &JobType) -> JobId {
        let prefix = job_type.short_prefix();

        // Generate UUID for uniqueness
        let uuid = uuid::Uuid::new_v4().to_string().replace("-", "");
        JobId::new(format!("{}-{}", prefix, &uuid[..12]))
    }

    /// Get active cluster node IDs for job fan-out.
    ///
    /// In cluster mode, returns nodes with status Active.
    /// In standalone mode (or if no nodes are reported), returns only this node.
    pub(crate) fn active_cluster_node_ids(&self) -> Vec<NodeId> {
        let app_ctx = self.get_attached_app_context();
        let cluster_info = app_ctx.executor().get_cluster_info();

        if !cluster_info.is_cluster_mode || cluster_info.nodes.is_empty() {
            return vec![self.node_id];
        }

        let mut nodes: Vec<NodeId> = cluster_info
            .nodes
            .iter()
            .filter(|n| !matches!(n.status, NodeStatus::Offline))
            .map(|n| n.node_id)
            .collect();

        if nodes.is_empty() {
            nodes.push(cluster_info.current_node_id);
        }

        nodes
    }

    /// Log job event to jobs.log file
    ///
    /// All log lines are prefixed with [JobId] for easy filtering.
    ///
    /// # Arguments
    /// * `job_id` - Job ID for log prefix
    /// * `level` - Log level (info, warn, error)
    /// * `message` - Log message
    pub(crate) fn log_job_event(&self, job_id: &JobId, level: &Level, message: &str) {
        // TODO: Implement dedicated jobs.log file appender (T137)
        // For now, use standard logging with [JobId] prefix
        match level {
            Level::Error => log::error!("[{}] {}", job_id.as_str(), message),
            Level::Warn => log::warn!("[{}] {}", job_id.as_str(), message),
            Level::Info => log::info!("[{}] {}", job_id.as_str(), message),
            Level::Debug => log::debug!("[{}] {}", job_id.as_str(), message),
            Level::Trace => log::trace!("[{}] {}", job_id.as_str(), message),
        }
    }

    /// Recover incomplete jobs on startup
    ///
    /// Marks all Running jobs as Failed with "Server restarted" error.
    /// Uses provider's async methods which handle spawn_blocking internally.
    pub(crate) async fn recover_incomplete_jobs(&self) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();

        if app_ctx.executor().is_cluster_mode() {
            let statuses = vec![JobStatus::Running, JobStatus::Retrying];
            let job_nodes = self
                .job_nodes_provider
                .list_for_node_with_statuses_async(&self.node_id, &statuses, 100000)
                .await
                .into_kalamdb_error("Failed to list job_nodes for recovery")?;

            if job_nodes.is_empty() {
                log::debug!("No incomplete job_nodes to recover for this node");
                return Ok(());
            }

            log::warn!("Recovering {} incomplete job_nodes from previous run", job_nodes.len());

            for node in job_nodes {
                let job_id = node.job_id.clone();
                let cmd = kalamdb_raft::commands::MetaCommand::UpdateJobNodeStatus {
                    job_id,
                    node_id: self.node_id,
                    status: JobStatus::Queued,
                    error_message: Some("Node restarted".to_string()),
                    updated_at: chrono::Utc::now(),
                };

                app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
                    KalamDbError::Other(format!("Failed to recover job_node via Raft: {}", e))
                })?;
            }

            return Ok(());
        }

        let filter = JobFilter {
            status: Some(JobStatus::Running),
            ..Default::default()
        };

        let running_jobs = self.list_jobs(filter).await?;

        if running_jobs.is_empty() {
            log::debug!("No incomplete jobs to recover");
            return Ok(());
        }

        log::warn!("Recovering {} incomplete jobs from previous run", running_jobs.len());

        let now_ms = chrono::Utc::now().timestamp_millis();

        // Update each job using provider's async method
        for mut job in running_jobs {
            let job_id = job.job_id.clone();
            job.status = JobStatus::Failed;
            job.message = Some("Server restarted".to_string());
            job.exception_trace = Some("Job was running when server shut down".to_string());
            job.updated_at = now_ms;
            job.finished_at = Some(now_ms);

            self.jobs_provider
                .update_job_async(job)
                .await
                .into_kalamdb_error("Failed to recover job")?;

            self.log_job_event(&job_id, &Level::Error, "Job marked as failed (server restart)");
        }

        Ok(())
    }

    /// Finalize job_nodes for a job once the job reaches a terminal status.
    ///
    /// Ensures queued/running job_nodes do not remain stuck after the job
    /// is completed, failed, or cancelled.
    pub(crate) async fn finalize_job_nodes(
        &self,
        job_id: &JobId,
        status: JobStatus,
        error_message: Option<String>,
    ) -> Result<(), KalamDbError> {
        let job_nodes = self
            .job_nodes_provider
            .list_for_job_id_async(job_id)
            .await
            .into_kalamdb_error("Failed to list job_nodes for job finalization")?;

        if job_nodes.is_empty() {
            return Ok(());
        }

        let app_ctx = self.get_attached_app_context();
        let now = Utc::now();

        for node in job_nodes {
            if matches!(
                node.status,
                JobStatus::Completed
                    | JobStatus::Failed
                    | JobStatus::Cancelled
                    | JobStatus::Skipped
            ) {
                continue;
            }

            let cmd = MetaCommand::UpdateJobNodeStatus {
                job_id: job_id.clone(),
                node_id: node.node_id,
                status,
                error_message: error_message.clone(),
                updated_at: now,
            };

            app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
                KalamDbError::Other(format!("Failed to finalize job_node via Raft: {}", e))
            })?;
        }

        Ok(())
    }
}
