use super::types::JobsManager;
use crate::executors::JobParams;
use chrono::Utc;
use kalamdb_commons::JobId;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_raft::commands::MetaCommand;
use kalamdb_system::providers::jobs::models::{Job, JobOptions};
use kalamdb_system::{JobStatus, JobType};
use log::Level;

impl JobsManager {
    /// Insert a job in the database asynchronously via Raft or direct write
    ///
    /// Uses Raft command when executor is available for cluster replication,
    /// otherwise falls back to direct provider write.
    async fn insert_job_async(&self, job: Job) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let executor = app_ctx.executor();

        // Use Raft command for cluster replication
        let cmd = MetaCommand::CreateJob { job };

        executor
            .execute_meta(cmd)
            .await
            .map_err(|e| KalamDbError::Other(format!("Failed to create job via Raft: {}", e)))?;

        Ok(())
    }

    /// Create a new job
    ///
    /// # Arguments
    /// * `job_type` - Type of job to create
    /// * `parameters` - Job parameters as JSON value (should contain namespace_id, table_name, etc.)
    /// * `idempotency_key` - Optional key to prevent duplicate jobs
    /// * `options` - Optional job creation options (retry, priority, queue)
    ///
    /// # Returns
    /// JobId for the created job
    ///
    /// # Errors
    /// - `IdempotentConflict` if a job with the same idempotency key is already running
    /// - `IoError` if job insertion fails
    pub async fn create_job(
        &self,
        job_type: JobType,
        parameters: serde_json::Value,
        idempotency_key: Option<String>,
        options: Option<JobOptions>,
    ) -> Result<JobId, KalamDbError> {
        // Only leader can create jobs (ensures single source of truth for job orchestration)
        // In standalone mode, this node is always the leader.
        // In cluster mode, admin operations (FLUSH, COMPACT, etc.) must be executed on the leader.
        if !self.is_cluster_leader().await {
            let app_ctx = self.get_attached_app_context();
            let cluster_info = app_ctx.executor().get_cluster_info();

            // Find leader's API address for error message
            let leader_addr =
                cluster_info.nodes.iter().find(|n| n.is_leader).map(|n| n.api_addr.clone());

            return Err(KalamDbError::NotLeader { leader_addr });
        }

        // Check idempotency: prevent duplicate jobs with same key
        if let Some(ref key) = idempotency_key {
            if self.has_active_job_with_key(key).await? {
                return Err(KalamDbError::IdempotentConflict(format!(
                    "Job with idempotency key '{}' is already running or queued",
                    key
                )));
            }
        }

        // Generate job ID with type-specific prefix
        let job_id = self.generate_job_id(&job_type);

        // Create job with Queued status
        // Note: namespace_id and table_name are now stored in the parameters JSON
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut job = Job {
            job_id: job_id.clone(),
            job_type,
            status: JobStatus::Queued,
            leader_status: None,
            parameters: Some(parameters),
            message: None,
            exception_trace: None,
            idempotency_key,
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now_ms,
            updated_at: now_ms,
            started_at: None,
            finished_at: None,
            node_id: self.node_id,
            leader_node_id: None,
            queue: None,
            priority: None,
        };

        // Apply options if provided
        if let Some(opts) = options {
            job.max_retries = opts.max_retries.unwrap_or(3);
            job.queue = opts.queue;
            job.priority = opts.priority;
        }

        // Persist job
        self.insert_job_async(job.clone()).await?;

        // Determine which nodes should execute this job
        let app_ctx = self.get_attached_app_context();
        let executor = app_ctx.executor();
        let is_leader_only = job_type.is_leader_only();
        let node_ids = if is_leader_only {
            // Leader-only jobs: only create job_node for this node (the leader)
            vec![self.node_id]
        } else {
            // All other jobs: create job_nodes for all active nodes
            self.active_cluster_node_ids()
        };
        let created_at = Utc::now();

        for node_id in node_ids {
            let cmd = MetaCommand::CreateJobNode {
                job_id: job_id.clone(),
                node_id,
                status: JobStatus::Queued,
                created_at,
            };

            executor.execute_meta(cmd).await.map_err(|e| {
                KalamDbError::Other(format!("Failed to create job_node via Raft: {}", e))
            })?;
        }

        // Log job creation
        self.log_job_event(&job_id, &Level::Debug, &format!("Job created: type={:?}", job_type));

        Ok(job_id)
    }

    /// Create a job with type-safe parameters
    ///
    /// **Type-Safe Alternative**: Accepts JobParams trait implementations for compile-time validation
    ///
    /// # Type Parameters
    /// * `T` - JobParams implementation (FlushParams, CleanupParams, etc.)
    ///
    /// # Arguments
    /// * `job_type` - Type of job to create
    /// * `params` - Typed parameters (automatically validated and serialized, should contain namespace_id, table_name)
    /// * `idempotency_key` - Optional key to prevent duplicate jobs
    /// * `options` - Optional job configuration (retries, priority, queue)
    ///
    /// # Returns
    /// Job ID if creation successful
    ///
    /// # Errors
    /// - `IdempotentConflict` if job with same idempotency key already exists
    /// - `KalamDbError` if parameter validation or persistence fails
    pub async fn create_job_typed<T: JobParams>(
        &self,
        job_type: JobType,
        params: T,
        idempotency_key: Option<String>,
        options: Option<JobOptions>,
    ) -> Result<JobId, KalamDbError> {
        // Validate parameters before serialization
        params.validate()?;

        // Serialize to JSON for storage and pre-validation
        let params_json = serde_json::to_string(&params)
            .into_kalamdb_error("Failed to serialize job parameters")?;

        // Call executor's pre_validate to check if job should be created
        let app_ctx = self.get_attached_app_context();
        let should_create =
            self.job_registry.pre_validate(&app_ctx, &job_type, &params_json).await?;

        if !should_create {
            // Log skip and return a special "skipped" job ID (or error)
            log::trace!(
                "Job pre-validation returned false; skipping job creation for {:?}",
                job_type
            );
            return Err(KalamDbError::Other(format!(
                "Job {:?} skipped: pre-validation returned false (nothing to do)",
                job_type
            )));
        }

        let parameters = serde_json::from_str(&params_json)
            .into_kalamdb_error("Failed to parse job parameters")?;

        // Delegate to existing create_job method
        self.create_job(job_type, parameters, idempotency_key, options).await
    }

    /// Cancel a running or queued job
    ///
    /// # Arguments
    /// * `job_id` - ID of job to cancel
    ///
    /// # Errors
    /// Returns error if job not found or cancellation fails
    pub async fn cancel_job(&self, job_id: &JobId) -> Result<(), KalamDbError> {
        // Get job using async method
        let job = self
            .get_job(job_id)
            .await?
            .ok_or_else(|| KalamDbError::NotFound(format!("Job {} not found", job_id)))?;

        // Can only cancel New, Queued, or Running jobs
        if !matches!(job.status, JobStatus::New | JobStatus::Queued | JobStatus::Running) {
            return Err(KalamDbError::InvalidOperation(format!(
                "Cannot cancel job in status {:?}",
                job.status
            )));
        }

        // Use Raft command to cancel job
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::CancelJob {
            job_id: job_id.clone(),
            reason: "Cancelled by user".to_string(),
            cancelled_at: Utc::now(),
        };

        app_ctx
            .executor()
            .execute_meta(cmd)
            .await
            .map_err(|e| KalamDbError::Other(format!("Failed to cancel job via Raft: {}", e)))?;

        self.finalize_job_nodes(
            job_id,
            JobStatus::Cancelled,
            Some("Cancelled by user".to_string()),
        )
        .await?;

        // Log cancellation
        self.log_job_event(job_id, &Level::Warn, "Job cancelled by user");

        Ok(())
    }

    /// Complete a job with success message (Phase 9, T165)
    ///
    /// # Arguments
    /// * `job_id` - ID of job to complete
    /// * `message` - Success message
    ///
    /// # Returns
    /// Ok if job completed successfully
    pub async fn complete_job(
        &self,
        job_id: &JobId,
        message: Option<String>,
    ) -> Result<(), KalamDbError> {
        let success_message = message.unwrap_or_else(|| "Job completed successfully".to_string());

        // Use Raft command to complete job
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::CompleteJob {
            job_id: job_id.clone(),
            result: Some(serde_json::json!({ "message": success_message.clone() }).to_string()),
            completed_at: Utc::now(),
        };

        app_ctx
            .executor()
            .execute_meta(cmd)
            .await
            .map_err(|e| KalamDbError::Other(format!("Failed to complete job via Raft: {}", e)))?;

        self.finalize_job_nodes(job_id, JobStatus::Completed, None).await?;

        self.log_job_event(job_id, &Level::Info, &success_message);
        Ok(())
    }

    /// Fail a job with error message (Phase 9, T165)
    ///
    /// # Arguments
    /// * `job_id` - ID of job to fail
    /// * `error_message` - Error message describing failure
    ///
    /// # Returns
    /// Ok if job marked as failed successfully
    pub async fn fail_job(
        &self,
        job_id: &JobId,
        error_message: String,
    ) -> Result<(), KalamDbError> {
        // Use Raft command to fail job
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::FailJob {
            job_id: job_id.clone(),
            error_message: error_message.clone(),
            failed_at: Utc::now(),
        };

        app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to mark job as failed via Raft: {}", e))
        })?;

        self.finalize_job_nodes(job_id, JobStatus::Failed, Some(error_message.clone()))
            .await?;

        self.log_job_event(job_id, &Level::Error, &format!("Job failed: {}", error_message));
        Ok(())
    }
}
