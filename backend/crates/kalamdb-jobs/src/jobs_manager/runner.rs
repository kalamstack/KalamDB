use super::types::JobsManager;
use super::utils::log_job;
use crate::executors::JobDecision;
use crate::AppContextJobsExt;
use crate::{FlushScheduler, HealthMonitor, StreamEvictionScheduler};
use kalamdb_commons::{JobId, NodeId};
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_raft::commands::MetaCommand;
use kalamdb_raft::GroupId;
use kalamdb_system::providers::jobs::models::{Job, JobFilter};
use kalamdb_system::JobNode;
use kalamdb_system::JobStatus;
use log::Level;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};
use tracing::Instrument;

const JOB_NODE_QUORUM_POLL_MS: u64 = 250;
const JOB_NODE_QUORUM_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Clone)]
enum JobNodeQuorumResult {
    QuorumReached { completed: usize, total: usize },
    TimedOut { completed: usize, total: usize },
    Failed { failed_nodes: Vec<NodeId> },
}

impl JobsManager {
    /// Claim job and mark it Running via Raft (sets `started_at`)
    async fn mark_job_running(&self, job_id: &JobId) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::ClaimJob {
            job_id: job_id.clone(),
            node_id: self.node_id,
            claimed_at: chrono::Utc::now(),
        };
        app_ctx
            .executor()
            .execute_meta(cmd)
            .await
            .map_err(|e| KalamDbError::Other(format!("Failed to claim job via Raft: {}", e)))?;
        Ok(())
    }

    /// Claim a per-node job entry via Raft
    async fn claim_job_node(&self, job_id: &JobId) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::ClaimJobNode {
            job_id: job_id.clone(),
            node_id: self.node_id,
            claimed_at: chrono::Utc::now(),
        };

        app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to claim job_node via Raft: {}", e))
        })?;
        Ok(())
    }

    /// Update per-node job status via Raft
    async fn update_job_node_status(
        &self,
        job_id: &JobId,
        status: JobStatus,
        error_message: Option<String>,
    ) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::UpdateJobNodeStatus {
            job_id: job_id.clone(),
            node_id: self.node_id,
            status,
            error_message,
            updated_at: chrono::Utc::now(),
        };

        app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to update job_node via Raft: {}", e))
        })?;
        Ok(())
    }

    /// Complete a job via Raft (for cluster replication)
    async fn mark_job_completed(
        &self,
        job_id: &kalamdb_commons::JobId,
        message: Option<String>,
    ) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let success_message = message.unwrap_or_else(|| "Job completed successfully".to_string());
        let cmd = MetaCommand::CompleteJob {
            job_id: job_id.clone(),
            result: Some(serde_json::json!({ "message": success_message }).to_string()),
            completed_at: chrono::Utc::now(),
        };
        app_ctx
            .executor()
            .execute_meta(cmd)
            .await
            .map_err(|e| KalamDbError::Other(format!("Failed to complete job via Raft: {}", e)))?;

        self.finalize_job_nodes(job_id, JobStatus::Completed, None).await?;
        Ok(())
    }

    /// Fail a job via Raft (for cluster replication)
    async fn mark_job_failed(
        &self,
        job_id: &kalamdb_commons::JobId,
        error_message: String,
    ) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::FailJob {
            job_id: job_id.clone(),
            error_message: error_message.clone(),
            failed_at: chrono::Utc::now(),
        };
        app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to mark job as failed via Raft: {}", e))
        })?;
        self.finalize_job_nodes(job_id, JobStatus::Failed, Some(error_message)).await?;
        Ok(())
    }

    /// Mark a job as skipped via Raft (for cluster replication)
    async fn mark_job_skipped(
        &self,
        job_id: &kalamdb_commons::JobId,
        skip_message: String,
    ) -> Result<(), KalamDbError> {
        let app_ctx = self.get_attached_app_context();
        let cmd = MetaCommand::CompleteJob {
            job_id: job_id.clone(),
            result: Some(
                serde_json::json!({ "message": skip_message, "skipped": true }).to_string(),
            ),
            completed_at: chrono::Utc::now(),
        };
        app_ctx.executor().execute_meta(cmd).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to mark job as skipped via Raft: {}", e))
        })?;

        // Mark job as skipped in the jobs table
        let cmd_status = MetaCommand::UpdateJobStatus {
            job_id: job_id.clone(),
            status: JobStatus::Skipped,
            updated_at: chrono::Utc::now(),
        };
        app_ctx.executor().execute_meta(cmd_status).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to update job status to Skipped via Raft: {}", e))
        })?;

        self.finalize_job_nodes(job_id, JobStatus::Skipped, Some(skip_message)).await?;
        Ok(())
    }

    /// Update a job in the database asynchronously (for retrying status only)
    ///
    /// Delegates to provider's async method which handles spawn_blocking internally.
    /// Note: For Completed/Failed states, use mark_job_completed/mark_job_failed instead.
    async fn update_job_async(&self, job: Job) -> Result<(), KalamDbError> {
        self.jobs_provider
            .update_job_async(job)
            .await
            .into_kalamdb_error("Failed to update job")
    }

    /// Check if this node is the cluster leader
    ///
    /// In standalone mode, always returns true.
    /// In cluster mode, checks if this node is the leader of the Meta group.
    pub async fn is_cluster_leader(&self) -> bool {
        let app_ctx = self.get_attached_app_context();

        // Check if we're in cluster mode
        if !app_ctx.executor().is_cluster_mode() {
            // Standalone mode - always act as leader
            return true;
        }

        // Cluster mode - check if we're the leader
        app_ctx.executor().is_leader(GroupId::Meta).await
    }

    /// Run job processing loop
    ///
    /// Continuously polls for queued jobs and executes them using registered executors.
    /// Implements idempotency checks, retry logic with exponential backoff, and crash recovery.
    /// Also periodically checks for stream tables requiring TTL eviction.
    ///
    /// **Two-Phase Distributed Execution**:
    /// - ALL nodes execute local work (RocksDB flush, cache eviction, compaction)
    /// - ONLY leader executes leader actions (Parquet upload, manifest updates)
    ///
    /// # Arguments
    /// * `max_concurrent` - Maximum number of concurrent jobs to run
    ///
    /// # Example
    /// ```rust
    /// // Run job loop with max 5 concurrent jobs
    /// job_manager.run_loop(5).await?;
    /// ```
    pub async fn run_loop(&self, max_concurrent: usize) -> Result<(), KalamDbError> {
        log::debug!("Starting job processing loop (max {} concurrent)", max_concurrent);

        // Take the awake receiver - can only run one loop per JobsManager
        let mut awake_receiver: mpsc::UnboundedReceiver<JobId> = self
            .awake_receiver
            .lock()
            .take()
            .expect("run_loop can only be called once per JobsManager");

        // Perform crash recovery on startup
        self.recover_incomplete_jobs().await?;

        // Health monitoring interval (log every 30 seconds)
        let mut health_interval = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );
        health_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Stream eviction interval (configurable, default 60 seconds)
        let app_context = self.get_attached_app_context();
        let eviction_interval_secs = app_context.config().stream.eviction_interval_seconds;
        let mut stream_eviction_interval = if eviction_interval_secs > 0 {
            let mut interval = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(eviction_interval_secs),
                Duration::from_secs(eviction_interval_secs),
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            Some(interval)
        } else {
            None
        };
        let stream_eviction_enabled = stream_eviction_interval.is_some();

        // Flush scheduler interval (configurable, default 60 seconds)
        let flush_check_secs = app_context.config().flush.check_interval_seconds;
        let mut flush_check_interval = if flush_check_secs > 0 {
            let mut interval = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(flush_check_secs),
                Duration::from_secs(flush_check_secs),
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            Some(interval)
        } else {
            None
        };
        let flush_check_enabled = flush_check_interval.is_some();

        // WAL cleanup scheduler interval (configurable, default 300 seconds).
        // Flushes all RocksDB memtables so idle column families advance their
        // log numbers and stale WAL files can be reclaimed.
        let wal_cleanup_secs = app_context.config().jobs.wal_cleanup_interval_seconds;
        let mut wal_cleanup_interval = if wal_cleanup_secs > 0 {
            let mut interval = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(wal_cleanup_secs),
                Duration::from_secs(wal_cleanup_secs),
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            Some(interval)
        } else {
            None
        };
        let wal_cleanup_enabled = wal_cleanup_interval.is_some();

        // Leadership check interval (for cluster mode)
        let mut leadership_interval = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        leadership_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut is_leader = self.is_cluster_leader().await;
        let mut was_leader = is_leader;
        let max_concurrent = max_concurrent.max(1);
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let job_manager = self.get_attached_app_context().job_manager();
        let mut join_set = JoinSet::new();

        // Adaptive idle polling (reduces CPU in empty systems)
        let idle_poll_min_ms: u64 = 500;
        let idle_poll_max_ms: u64 = 5_000;
        let mut idle_poll_ms = idle_poll_min_ms;
        let mut poll_interval = tokio::time::interval_at(
            Instant::now() + Duration::from_millis(idle_poll_ms),
            Duration::from_millis(idle_poll_ms),
        );
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Check for shutdown signal (lock-free atomic check)
            if self.shutdown.load(std::sync::atomic::Ordering::Acquire) {
                log::info!("Shutdown signal received, stopping job loop");
                break;
            }

            while let Some(result) = join_set.try_join_next() {
                if let Err(err) = result {
                    log::error!("Job task panicked: {}", err);
                }
            }

            // Event-driven loop: await job wakeups or periodic ticks
            let job_id_opt = tokio::select! {
                biased;
                // Priority 1: awakened jobs from state machine
                Some(job_id) = awake_receiver.recv() => Some(job_id),
                // Priority 2: fallback polling for crash recovery/retries
                _ = poll_interval.tick() => None,
                // Periodic leadership check
                _ = leadership_interval.tick() => {
                    let leader_now = self.is_cluster_leader().await;
                    if leader_now && !was_leader {
                        log::info!("[JobLoop] This node became leader - handling failover");
                        self.handle_leader_failover().await;
                    } else if !leader_now && was_leader {
                        log::info!("[JobLoop] This node lost leadership");
                    }
                    was_leader = leader_now;
                    is_leader = leader_now;
                    continue;
                }
                // Periodic health metrics logging (all nodes)
                _ = health_interval.tick() => {
                    let app_ctx = self.get_attached_app_context();
                    if let Err(e) = HealthMonitor::log_metrics(app_ctx).await {
                        log::warn!("Failed to log health metrics: {}", e);
                    }
                    continue;
                }
                // Periodic WAL cleanup: flush all RocksDB memtables so idle CFs
                // don't pin WAL files forever (prevents WAL file accumulation)
                _ = async {
                    if wal_cleanup_enabled {
                        let interval = wal_cleanup_interval
                            .as_mut()
                            .expect("wal cleanup interval missing");
                        interval.tick().await;
                    }
                }, if wal_cleanup_enabled => {
                    let app_ctx = self.get_attached_app_context();
                    let backend = app_ctx.storage_backend();
                    match tokio::task::spawn_blocking(move || backend.flush_all_memtables()).await {
                        Ok(Ok(())) => {
                            log::debug!("WAL cleanup: flushed all memtables");
                        },
                        Ok(Err(e)) => {
                            log::warn!("WAL cleanup flush_all_memtables failed: {}", e);
                        },
                        Err(e) => {
                            log::warn!("WAL cleanup task join failed: {}", e);
                        },
                    }
                    continue;
                }
                // Periodic stream eviction job creation (leader-only)
                _ = async {
                    if stream_eviction_enabled {
                        let interval = stream_eviction_interval
                            .as_mut()
                            .expect("stream eviction interval missing");
                        interval.tick().await;
                    }
                }, if stream_eviction_enabled => {
                    if is_leader {
                        let app_ctx = self.get_attached_app_context();
                        if let Err(e) = StreamEvictionScheduler::check_and_schedule(&app_ctx, self).await {
                            log::warn!("Failed to check stream eviction: {}", e);
                        }
                    }
                    continue;
                }
                // Periodic flush scheduler (leader-only) — creates flush jobs
                // for tables with pending writes in RocksDB
                _ = async {
                    if flush_check_enabled {
                        let interval = flush_check_interval
                            .as_mut()
                            .expect("flush check interval missing");
                        interval.tick().await;
                    }
                }, if flush_check_enabled => {
                    if is_leader {
                        let app_ctx = self.get_attached_app_context();
                        if let Err(e) = FlushScheduler::check_and_schedule(&app_ctx, self).await {
                            log::warn!("Failed to check periodic flush: {}", e);
                        }
                    }
                    continue;
                }
            };

            if semaphore.available_permits() == 0 {
                if let Some(Err(err)) = join_set.join_next().await {
                    log::error!("Job task panicked: {}", err);
                }
                continue;
            }

            let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    tokio::task::yield_now().await;
                    continue;
                },
            };

            // Fetch job to execute
            let job_result = if let Some(job_id) = job_id_opt {
                // Awakened job - fetch it directly
                self.fetch_awakened_job(&job_id).await
            } else {
                // Fallback polling
                self.poll_next().await
            };

            match job_result {
                Ok(Some((job, job_node))) => {
                    log::debug!(
                        "[{}] Job fetched for execution: type={:?}, status={:?}, is_leader={}",
                        job.job_id,
                        job.job_type,
                        job.status,
                        is_leader
                    );
                    if idle_poll_ms != idle_poll_min_ms {
                        idle_poll_ms = idle_poll_min_ms;
                        poll_interval = tokio::time::interval_at(
                            Instant::now() + Duration::from_millis(idle_poll_ms),
                            Duration::from_millis(idle_poll_ms),
                        );
                        poll_interval
                            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }
                    let jobs_manager = Arc::clone(&job_manager);
                    join_set.spawn(async move {
                        let _permit = permit;
                        if let Err(e) = jobs_manager.execute_job(job, job_node, is_leader).await {
                            log::error!("Job execution failed critically: {}", e);
                        }
                    });
                },
                Ok(None) => {
                    drop(permit);
                    let next_poll_ms = (idle_poll_ms.saturating_mul(2)).min(idle_poll_max_ms);
                    if next_poll_ms != idle_poll_ms {
                        idle_poll_ms = next_poll_ms;
                        poll_interval = tokio::time::interval_at(
                            Instant::now() + Duration::from_millis(idle_poll_ms),
                            Duration::from_millis(idle_poll_ms),
                        );
                        poll_interval
                            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }
                    // No jobs available - continue loop (select! already waited)
                },
                Err(e) => {
                    drop(permit);
                    log::error!("Failed to poll for next job: {}", e);
                    if idle_poll_ms != idle_poll_max_ms {
                        idle_poll_ms = idle_poll_max_ms;
                        poll_interval = tokio::time::interval_at(
                            Instant::now() + Duration::from_millis(idle_poll_ms),
                            Duration::from_millis(idle_poll_ms),
                        );
                        poll_interval
                            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }
                    sleep(Duration::from_secs(1)).await;
                },
            }
        }

        Ok(())
    }

    /// Fetch an awakened job by ID for execution.
    ///
    /// Called when a job_id arrives via the awake channel.
    async fn fetch_awakened_job(
        &self,
        job_id: &JobId,
    ) -> Result<Option<(Job, JobNode)>, KalamDbError> {
        // Fetch the job_node for this node
        let job_node_opt = self
            .job_nodes_provider
            .get_job_node_async(job_id, &self.node_id)
            .await
            .into_kalamdb_error("Failed to get job_node")?;

        let Some(job_node) = job_node_opt else {
            log::warn!("[{}] Awakened but no job_node found for this node", job_id.as_str());
            return Ok(None);
        };

        // Skip if already processed
        if matches!(
            job_node.status,
            JobStatus::Running
                | JobStatus::Completed
                | JobStatus::Failed
                | JobStatus::Cancelled
                | JobStatus::Skipped
        ) {
            log::info!(
                "[{}] Skipping job_node already processed: status={:?}",
                job_id.as_str(),
                job_node.status
            );
            return Ok(None);
        }

        // Fetch the job
        let Some(job) = self.get_job(job_id).await? else {
            self.update_job_node_status(
                job_id,
                JobStatus::Failed,
                Some("Job not found for awakened job_node".to_string()),
            )
            .await?;
            return Ok(None);
        };

        // Skip if job already terminal
        if matches!(
            job.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled | JobStatus::Skipped
        ) {
            log::info!(
                "[{}] Skipping job already terminal: status={:?}",
                job_id.as_str(),
                job.status
            );
            self.update_job_node_status(job_id, job.status, None).await?;
            return Ok(None);
        }

        // Claim the job_node
        self.claim_job_node(job_id).await?;

        Ok(Some((job, job_node)))
    }

    /// Run a single job execution cycle (test helper).
    ///
    /// Returns Ok(true) if a job was executed, Ok(false) if no jobs were available.
    pub async fn run_once_for_tests(&self) -> Result<bool, KalamDbError> {
        let is_leader = self.is_cluster_leader().await;
        if let Some((job, job_node)) = self.poll_next().await? {
            self.execute_job(job, job_node, is_leader).await?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Poll for next per-node job entry to execute
    ///
    /// Finds the next queued job with idempotency check.
    ///
    /// # Returns
    /// Next job to execute, or None if no jobs available
    async fn poll_next(&self) -> Result<Option<(Job, JobNode)>, KalamDbError> {
        let statuses = vec![JobStatus::New, JobStatus::Queued, JobStatus::Retrying];
        let nodes = self
            .job_nodes_provider
            .list_for_node_with_statuses_async(&self.node_id, &statuses, 1)
            .await
            .into_kalamdb_error("Failed to list job_nodes")?;

        let Some(job_node) = nodes.into_iter().next() else {
            return Ok(None);
        };

        let Some(job) = self.get_job(&job_node.job_id).await? else {
            self.update_job_node_status(
                &job_node.job_id,
                JobStatus::Failed,
                Some("Job not found for job_node".to_string()),
            )
            .await?;
            return Ok(None);
        };

        if matches!(
            job.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled | JobStatus::Skipped
        ) {
            self.update_job_node_status(&job_node.job_id, job.status, None).await?;
            return Ok(None);
        }

        self.claim_job_node(&job_node.job_id).await?;

        Ok(Some((job, job_node)))
    }

    /// Execute a single job with two-phase distributed model
    ///
    /// **Phase 1 - Local Work (ALL nodes)**:
    /// - RocksDB flush, local cache eviction, compaction
    /// - Runs on every node in the cluster
    ///
    /// **Phase 2 - Leader Actions (ONLY leader)**:
    /// - Parquet upload to external storage, manifest updates
    /// - Only runs on leader for jobs with `has_leader_actions() == true`
    ///
    /// # Arguments
    /// * `job` - Job to execute
    /// * `is_leader` - Whether this node is currently the cluster leader
    async fn execute_job(
        &self,
        job: Job,
        _job_node: JobNode,
        is_leader: bool,
    ) -> Result<(), KalamDbError> {
        let span = tracing::info_span!(
            "jobs.execution",
            job_id = %job.job_id,
            job_type = ?job.job_type,
            is_leader = is_leader,
            has_local_work = job.job_type.has_local_work(),
            has_leader_actions = job.job_type.has_leader_actions()
        );
        async move {
            let job_id = job.job_id.clone();
            let job_type = job.job_type;

            log::debug!(
                "[{}] execute_job started: type={:?}, has_local_work={}, has_leader_actions={}",
                job_id,
                job_type,
                job_type.has_local_work(),
                job_type.has_leader_actions()
            );

            // Mark job as Running if still Queued/New (leader coordination)
            // Only leader should be executing jobs at this point due to creation constraints
            if is_leader && matches!(job.status, JobStatus::Queued | JobStatus::New) {
                self.mark_job_running(&job_id).await?;
            }

            self.log_job_event(&job_id, &Level::Trace, "Job started (local phase)");

            let app_ctx = self.get_attached_app_context();

            // ============================================
            // Phase 1: Execute LOCAL work (runs on ALL nodes)
            // ============================================
            let local_decision = if job_type.has_local_work() {
                match self.job_registry.execute_local(app_ctx.clone(), &job).await {
                    Ok(d) => d,
                    Err(e) => {
                        let error_msg = format!("Local executor error: {}", e);
                        self.update_job_node_status(
                            &job_id,
                            JobStatus::Failed,
                            Some(error_msg.clone()),
                        )
                        .await?;
                        if is_leader {
                            if let Err(err) = self.mark_job_failed(&job_id, error_msg.clone()).await
                            {
                                log::error!(
                                    "[{}] Failed to mark job as failed via Raft: {}",
                                    job_id,
                                    err
                                );
                            }
                        }
                        self.log_job_event(
                            &job_id,
                            &Level::Error,
                            &format!("Job failed in local phase: {}", e),
                        );
                        return Ok(());
                    },
                }
            } else {
                JobDecision::Completed {
                    message: Some("No local work required".to_string()),
                }
            };

            // Handle local phase result
            match &local_decision {
                JobDecision::Completed { message } => {
                    self.update_job_node_status(&job_id, JobStatus::Completed, None).await?;
                    log_job!(
                        self,
                        &job_id,
                        Level::Trace,
                        "Local phase completed: {}",
                        message.as_deref().unwrap_or("ok")
                    );
                },
                JobDecision::Failed { message, .. } => {
                    self.update_job_node_status(&job_id, JobStatus::Failed, Some(message.clone()))
                        .await?;
                    if is_leader {
                        if let Err(err) = self.mark_job_failed(&job_id, message.clone()).await {
                            log::error!(
                                "[{}] Failed to mark job as failed via Raft: {}",
                                job_id,
                                err
                            );
                        }
                    }
                    self.log_job_event(
                        &job_id,
                        &Level::Error,
                        &format!("Job failed in local phase: {}", message),
                    );
                    return Ok(());
                },
                JobDecision::Skipped { message } => {
                    self.update_job_node_status(&job_id, JobStatus::Completed, None).await?;
                    log_job!(self, &job_id, Level::Trace, "Local phase skipped: {}", message);
                    // Continue to leader phase if applicable
                },
                JobDecision::Retry {
                    message,
                    exception_trace,
                    backoff_ms,
                } => {
                    // Handle retry for local phase
                    return self
                        .handle_job_retry(
                            &job,
                            message,
                            exception_trace.clone(),
                            *backoff_ms,
                            is_leader,
                        )
                        .await;
                },
            }

            // ============================================
            // Phase 2: Execute LEADER actions (ONLY on leader)
            // ============================================
            if is_leader {
                if job_type.has_local_work() {
                    let node_ids = self.active_cluster_node_ids();
                    let quorum_result = self
                        .wait_for_job_nodes_quorum(
                            &job_id,
                            &node_ids,
                            Duration::from_secs(JOB_NODE_QUORUM_TIMEOUT_SECS),
                        )
                        .await?;

                    match quorum_result {
                        JobNodeQuorumResult::Failed { failed_nodes } => {
                            let reason = format!(
                                "Local phase failed on nodes: {}",
                                failed_nodes
                                    .iter()
                                    .map(|id| id.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                            self.mark_job_failed(&job_id, reason).await?;
                            return Ok(());
                        },
                        JobNodeQuorumResult::TimedOut { completed, total } => {
                            self.log_job_event(
                                &job_id,
                                &Level::Warn,
                                &format!(
                                    "Quorum timeout (completed {}/{}); proceeding with leader actions",
                                    completed, total
                                ),
                            );
                        },
                        JobNodeQuorumResult::QuorumReached { completed, total } => {
                            log_job!(
                                self,
                                &job_id,
                                Level::Trace,
                                "Quorum reached (completed {}/{})",
                                completed,
                                total
                            );
                        },
                    }
                }

                if job_type.has_leader_actions() {
                    log::debug!("[{}] Starting leader phase execution", job_id);

                    let leader_decision =
                        match self.job_registry.execute_leader(app_ctx, &job).await {
                            Ok(d) => d,
                            Err(e) => {
                                let error_msg = format!("Leader executor error: {}", e);
                                if let Err(err) =
                                    self.mark_job_failed(&job_id, error_msg.clone()).await
                                {
                                    log::error!(
                                        "[{}] Failed to mark job as failed via Raft: {}",
                                        job_id,
                                        err
                                    );
                                }
                                self.log_job_event(
                                    &job_id,
                                    &Level::Error,
                                    &format!("Job failed in leader phase: {}", e),
                                );
                                return Ok(());
                            },
                        };

                    match leader_decision {
                        JobDecision::Completed { message } => {
                            if let Err(e) = self.mark_job_completed(&job_id, message.clone()).await
                            {
                                log::error!(
                                    "[{}] Failed to mark job as completed via Raft: {}",
                                    job_id,
                                    e
                                );
                                return Err(KalamDbError::Other(format!(
                                    "Failed to update job status to Completed: {}",
                                    e
                                )));
                            }
                            log_job!(
                                self,
                                &job_id,
                                Level::Trace,
                                "Job completed (leader phase): {}",
                                message.unwrap_or_default()
                            );
                        },
                        JobDecision::Skipped { message } => {
                            if let Err(e) = self.mark_job_skipped(&job_id, message.clone()).await {
                                log::error!(
                                    "[{}] Failed to mark job as skipped via Raft: {}",
                                    job_id,
                                    e
                                );
                                return Err(KalamDbError::Other(format!(
                                    "Failed to update job status to Skipped: {}",
                                    e
                                )));
                            }
                            self.log_job_event(
                                &job_id,
                                &Level::Info,
                                &format!("Job skipped (leader phase): {}", message),
                            );
                        },
                        JobDecision::Failed { message, .. } => {
                            if let Err(err) = self.mark_job_failed(&job_id, message.clone()).await {
                                log::error!(
                                    "[{}] Failed to mark job as failed via Raft: {}",
                                    job_id,
                                    err
                                );
                            }
                            self.log_job_event(
                                &job_id,
                                &Level::Error,
                                &format!("Job failed in leader phase: {}", message),
                            );
                        },
                        JobDecision::Retry {
                            message,
                            exception_trace,
                            backoff_ms,
                        } => {
                            return self
                                .handle_job_retry(
                                    &job,
                                    &message,
                                    exception_trace,
                                    backoff_ms,
                                    is_leader,
                                )
                                .await;
                        },
                    }
                } else if let JobDecision::Completed { message } = local_decision {
                    if let Err(e) = self.mark_job_completed(&job_id, message.clone()).await {
                        log::error!("[{}] Failed to mark job as completed via Raft: {}", job_id, e);
                        return Err(KalamDbError::Other(format!(
                            "Failed to update job status to Completed: {}",
                            e
                        )));
                    }
                    self.log_job_event(
                        &job_id,
                        &Level::Debug,
                        &format!("Job completed: {}", message.unwrap_or_default()),
                    );
                }
            }

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Handle job retry logic
    async fn handle_job_retry(
        &self,
        job: &Job,
        message: &str,
        exception_trace: Option<String>,
        backoff_ms: u64,
        is_leader: bool,
    ) -> Result<(), KalamDbError> {
        let mut updated_job = job.clone();
        let job_id = &job.job_id;

        if updated_job.retry_count < updated_job.max_retries {
            updated_job.retry_count += 1;
            updated_job.status = JobStatus::Retrying;
            updated_job.exception_trace = exception_trace;
            updated_job.updated_at = chrono::Utc::now().timestamp_millis();

            if is_leader {
                self.update_job_async(updated_job.clone())
                    .await
                    .into_kalamdb_error("Failed to retry job")?;
            }

            self.update_job_node_status(job_id, JobStatus::Retrying, Some(message.to_string()))
                .await?;

            self.log_job_event(
                job_id,
                &Level::Warn,
                &format!(
                    "Job retry {}/{}, waiting {}ms: {}",
                    updated_job.retry_count, updated_job.max_retries, backoff_ms, message
                ),
            );

            // Sleep before retry
            sleep(Duration::from_millis(backoff_ms)).await;

            self.update_job_node_status(job_id, JobStatus::Queued, None).await?;
        } else {
            if is_leader {
                self.mark_job_failed(job_id, "Max retries exceeded".to_string())
                    .await
                    .into_kalamdb_error("Failed to fail job")?;
            }

            self.update_job_node_status(
                job_id,
                JobStatus::Failed,
                Some("Max retries exceeded".to_string()),
            )
            .await?;

            self.log_job_event(job_id, &Level::Error, "Job failed: max retries exceeded");
        }

        Ok(())
    }

    async fn wait_for_job_nodes_quorum(
        &self,
        job_id: &JobId,
        node_ids: &[NodeId],
        timeout: Duration,
    ) -> Result<JobNodeQuorumResult, KalamDbError> {
        let total = node_ids.len().max(1);
        let quorum = total / 2 + 1;
        let start = Instant::now();

        loop {
            let mut completed = 0;
            let mut failed_nodes = Vec::new();

            for node_id in node_ids {
                if let Some(node) = self
                    .job_nodes_provider
                    .get_job_node_async(job_id, node_id)
                    .await
                    .into_kalamdb_error("Failed to get job_node")?
                {
                    match node.status {
                        JobStatus::Completed => completed += 1,
                        JobStatus::Failed | JobStatus::Cancelled => {
                            failed_nodes.push(*node_id);
                        },
                        _ => {},
                    }
                }
            }

            if !failed_nodes.is_empty() {
                return Ok(JobNodeQuorumResult::Failed { failed_nodes });
            }

            if completed >= quorum {
                return Ok(JobNodeQuorumResult::QuorumReached { completed, total });
            }

            if start.elapsed() >= timeout {
                return Ok(JobNodeQuorumResult::TimedOut { completed, total });
            }

            sleep(Duration::from_millis(JOB_NODE_QUORUM_POLL_MS)).await;
        }
    }

    /// Handle leader failover by recovering orphaned jobs
    ///
    /// Called when this node becomes the leader in cluster mode.
    async fn handle_leader_failover(&self) {
        let mut jobs = Vec::new();
        for status in [JobStatus::Running, JobStatus::Queued] {
            let filter = JobFilter {
                status: Some(status),
                ..Default::default()
            };

            match self.list_jobs(filter).await {
                Ok(mut listed) => jobs.append(&mut listed),
                Err(e) => {
                    log::error!(
                        "[JobLoop] Failed to list {:?} jobs for leader failover: {}",
                        status,
                        e
                    );
                    return;
                },
            }
        }

        for job in jobs {
            if matches!(job.status, JobStatus::Queued) {
                if let Err(e) = self.mark_job_running(&job.job_id).await {
                    log::error!(
                        "[JobLoop] Failed to mark queued job {} as running: {}",
                        job.job_id,
                        e
                    );
                    continue;
                }
            }

            if let Err(e) = self.resume_leader_actions(job).await {
                log::error!("[JobLoop] Failed to resume leader actions: {}", e);
            }
        }
    }

    async fn resume_leader_actions(&self, job: Job) -> Result<(), KalamDbError> {
        let job_id = job.job_id.clone();

        if job.job_type.has_local_work() {
            let node_ids = self.active_cluster_node_ids();
            let quorum_result = self
                .wait_for_job_nodes_quorum(
                    &job_id,
                    &node_ids,
                    Duration::from_secs(JOB_NODE_QUORUM_TIMEOUT_SECS),
                )
                .await?;

            match quorum_result {
                JobNodeQuorumResult::Failed { failed_nodes } => {
                    let reason = format!(
                        "Local phase failed on nodes: {}",
                        failed_nodes.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
                    );
                    self.mark_job_failed(&job_id, reason).await?;
                    return Ok(());
                },
                JobNodeQuorumResult::TimedOut { completed, total } => {
                    // If completed=0, job was never started on workers (failover of queued job)
                    // Use debug level to reduce noise, otherwise warn
                    let level = if completed == 0 {
                        Level::Debug
                    } else {
                        Level::Warn
                    };
                    self.log_job_event(
                        &job_id,
                        &level,
                        &format!(
                            "Quorum timeout (completed {}/{}); proceeding with leader actions",
                            completed, total
                        ),
                    );
                },
                JobNodeQuorumResult::QuorumReached { completed, total } => {
                    self.log_job_event(
                        &job_id,
                        &Level::Debug,
                        &format!("Quorum reached (completed {}/{})", completed, total),
                    );
                },
            }
        }

        if job.job_type.has_leader_actions() {
            let app_ctx = self.get_attached_app_context();
            let leader_decision = self.job_registry.execute_leader(app_ctx, &job).await?;

            match leader_decision {
                JobDecision::Completed { message } => {
                    self.mark_job_completed(&job_id, message).await?;
                },
                JobDecision::Failed { message, .. } => {
                    self.mark_job_failed(&job_id, message).await?;
                },
                JobDecision::Skipped { message } => {
                    self.mark_job_skipped(&job_id, message).await?;
                },
                JobDecision::Retry {
                    message,
                    exception_trace,
                    backoff_ms,
                } => {
                    return self
                        .handle_job_retry(&job, &message, exception_trace, backoff_ms, true)
                        .await;
                },
            }
        } else {
            self.mark_job_completed(&job_id, Some("Local work completed".to_string()))
                .await?;
        }

        Ok(())
    }
}
