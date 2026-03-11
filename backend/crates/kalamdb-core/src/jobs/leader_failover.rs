//! Leader Failover Handler
//!
//! Handles detection and recovery of orphaned jobs from failed leaders.
//! When a node becomes the leader, it checks for jobs that were claimed
//! by the previous leader but never completed.
//!
//! ## Recovery Strategy
//!
//! 1. On becoming leader, scan for jobs with status=Running
//! 2. For each running job:
//!    - If claimed by a node that's no longer in the cluster: re-queue
//!    - If claimed by a node that's offline: wait for timeout, then re-queue
//!    - If claimed by this node: continue execution (crash recovery)
//!
//! ## Job Types and Recovery
//!
//! - **Flush jobs**: Safe to re-run (idempotent)
//! - **Cleanup jobs**: Safe to re-run (idempotent)
//! - **Retention jobs**: Safe to re-run (idempotent)
//! - **Compact jobs**: Safe to re-run (idempotent)
//! - **Backup jobs**: May need special handling (check partial backups)
//! - **Restore jobs**: Should be failed and require manual restart

use chrono::Utc;
use kalamdb_commons::models::{JobId, NodeId};
use kalamdb_system::providers::jobs::models::Job;
use kalamdb_system::{JobFilter, JobSortField, JobStatus, JobType, SortOrder};
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::KalamDbError;
use crate::jobs::leader_guard::LeaderOnlyJobGuard;
use kalamdb_system::JobsTableProvider;

/// How long to wait before considering a job orphaned (in seconds)
const ORPHAN_DETECTION_TIMEOUT_SECS: i64 = 300; // 5 minutes

/// Job recovery action
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRecoveryAction {
    /// Re-queue the job for execution
    Requeue,
    /// Fail the job (requires manual restart)
    Fail { reason: String },
    /// Continue execution (job was claimed by this node)
    Continue,
    /// Skip - job is not orphaned
    Skip,
}

/// Failover handler for orphaned jobs
pub struct LeaderFailoverHandler {
    /// Job guard for leadership checks and job operations
    job_guard: LeaderOnlyJobGuard,
    /// Jobs provider for querying job state
    jobs_provider: Arc<JobsTableProvider>,
    /// This node's ID
    node_id: NodeId,
    /// Set of known active node IDs in the cluster
    active_nodes: HashSet<NodeId>,
}

impl LeaderFailoverHandler {
    /// Create a new failover handler
    pub fn new(
        job_guard: LeaderOnlyJobGuard,
        jobs_provider: Arc<JobsTableProvider>,
        active_nodes: HashSet<NodeId>,
    ) -> Self {
        let node_id = job_guard.node_id();
        Self {
            job_guard,
            jobs_provider,
            node_id,
            active_nodes,
        }
    }

    /// Update the set of active nodes
    pub fn update_active_nodes(&mut self, nodes: HashSet<NodeId>) {
        self.active_nodes = nodes;
    }

    /// Called when this node becomes the leader
    ///
    /// Scans for orphaned jobs and recovers them.
    pub async fn on_become_leader(&self) -> Result<RecoveryReport, KalamDbError> {
        log::debug!(
            "[FailoverHandler] Node {} became leader, scanning for orphaned jobs...",
            self.node_id
        );

        let mut report = RecoveryReport::default();

        // Find all running jobs
        let running_jobs = self.find_running_jobs().await?;
        log::debug!("[FailoverHandler] Found {} running jobs to check", running_jobs.len());

        for job in running_jobs {
            let action = self.determine_recovery_action(&job);
            log::debug!(
                "[FailoverHandler] Job {}: action={:?}, node={}",
                job.job_id,
                action,
                job.node_id
            );

            match action {
                JobRecoveryAction::Requeue => {
                    if let Err(e) = self.requeue_job(&job).await {
                        log::error!(
                            "[FailoverHandler] Failed to requeue job {}: {}",
                            job.job_id,
                            e
                        );
                        report.failed.push((job.job_id.clone(), e.to_string()));
                    } else {
                        log::info!("[FailoverHandler] Requeued orphaned job {}", job.job_id);
                        report.requeued.push(job.job_id.clone());
                    }
                },
                JobRecoveryAction::Fail { reason } => {
                    if let Err(e) = self.fail_job(&job, &reason).await {
                        log::error!("[FailoverHandler] Failed to fail job {}: {}", job.job_id, e);
                        report.failed.push((job.job_id.clone(), e.to_string()));
                    } else {
                        log::warn!(
                            "[FailoverHandler] Failed orphaned job {}: {}",
                            job.job_id,
                            reason
                        );
                        report.marked_failed.push(job.job_id.clone());
                    }
                },
                JobRecoveryAction::Continue => {
                    log::debug!("[FailoverHandler] Job {} was ours, will continue", job.job_id);
                    report.continued.push(job.job_id.clone());
                },
                JobRecoveryAction::Skip => {
                    // Job is not orphaned, skip
                },
            }
        }

        log::debug!(
            "[FailoverHandler] Recovery complete: {} requeued, {} failed, {} continued",
            report.requeued.len(),
            report.marked_failed.len(),
            report.continued.len()
        );

        Ok(report)
    }

    /// Find all jobs with Running status.
    ///
    /// Uses the JobStatusCreatedAtIndex for efficient lookup instead of
    /// scanning the entire jobs table.
    async fn find_running_jobs(&self) -> Result<Vec<Job>, KalamDbError> {
        let filter = JobFilter {
            status: Some(JobStatus::Running),
            sort_by: Some(JobSortField::CreatedAt),
            sort_order: Some(SortOrder::Asc),
            limit: None,
            ..Default::default()
        };
        self.jobs_provider
            .list_jobs_filtered(&filter)
            .map_err(|e| KalamDbError::io_message(format!("Failed to query jobs: {}", e)))
    }

    /// Determine recovery action for a job
    fn determine_recovery_action(&self, job: &Job) -> JobRecoveryAction {
        let job_node_id = &job.node_id;

        // If job was claimed by this node, continue (crash recovery)
        if job_node_id == &self.node_id {
            return JobRecoveryAction::Continue;
        }

        // Check if the claiming node is still in the cluster
        if !self.active_nodes.contains(job_node_id) {
            // Node is not in cluster - decide based on job type
            return self.recovery_action_for_offline_node(job);
        }

        // Node is in cluster but job is still running
        // Check if job has been running too long (potential zombie)
        if let Some(started_at) = job.started_at {
            let now = Utc::now().timestamp_millis();
            let elapsed_secs = (now - started_at) / 1000;

            if elapsed_secs > ORPHAN_DETECTION_TIMEOUT_SECS {
                log::warn!(
                    "[FailoverHandler] Job {} has been running for {} seconds, may be orphaned",
                    job.job_id,
                    elapsed_secs
                );
                return self.recovery_action_for_offline_node(job);
            }
        }

        JobRecoveryAction::Skip
    }

    /// Determine recovery action when the owning node is offline
    fn recovery_action_for_offline_node(&self, job: &Job) -> JobRecoveryAction {
        match job.job_type {
            // Safe to re-run (idempotent operations)
            JobType::Flush
            | JobType::VectorIndex
            | JobType::Cleanup
            | JobType::Retention
            | JobType::Compact
            | JobType::StreamEviction
            | JobType::UserCleanup
            | JobType::JobCleanup
            | JobType::ManifestEviction => JobRecoveryAction::Requeue,

            // May have partial results - fail and require manual restart
            JobType::Backup => JobRecoveryAction::Fail {
                reason: format!(
                    "Backup job was running on offline node {}. Manual verification required.",
                    job.node_id
                ),
            },

            JobType::Restore => JobRecoveryAction::Fail {
                reason: format!(
                    "Restore job was running on offline node {}. Manual restart required.",
                    job.node_id
                ),
            },

            // Unknown job type - fail safely
            _ => JobRecoveryAction::Fail {
                reason: format!("Unknown job type running on offline node {}", job.node_id),
            },
        }
    }

    /// Requeue a job for execution
    async fn requeue_job(&self, job: &Job) -> Result<(), KalamDbError> {
        self.job_guard.release_job(&job.job_id, "Requeued by leader failover").await
    }

    /// Fail a job permanently
    async fn fail_job(&self, job: &Job, reason: &str) -> Result<(), KalamDbError> {
        self.job_guard.fail_job(&job.job_id, reason).await
    }
}

/// Report of recovery actions taken
#[derive(Debug, Default)]
pub struct RecoveryReport {
    /// Jobs that were re-queued for execution
    pub requeued: Vec<JobId>,
    /// Jobs that were marked as failed
    pub marked_failed: Vec<JobId>,
    /// Jobs that will continue (crash recovery)
    pub continued: Vec<JobId>,
    /// Jobs that failed to recover
    pub failed: Vec<(JobId, String)>,
}

impl RecoveryReport {
    /// Check if any recovery action was taken
    pub fn is_empty(&self) -> bool {
        self.requeued.is_empty()
            && self.marked_failed.is_empty()
            && self.continued.is_empty()
            && self.failed.is_empty()
    }

    /// Total number of jobs processed
    pub fn total(&self) -> usize {
        self.requeued.len() + self.marked_failed.len() + self.continued.len() + self.failed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_report_empty() {
        let report = RecoveryReport::default();
        assert!(report.is_empty());
        assert_eq!(report.total(), 0);
    }

    #[test]
    fn test_recovery_report_counts() {
        let mut report = RecoveryReport::default();
        report.requeued.push(JobId::new("job_1"));
        report.requeued.push(JobId::new("job_2"));
        report.marked_failed.push(JobId::new("job_3"));
        report.continued.push(JobId::new("job_4"));
        report.failed.push((JobId::new("job_5"), "error".to_string()));

        assert!(!report.is_empty());
        assert_eq!(report.total(), 5);
    }

    #[test]
    fn test_job_recovery_action_variants() {
        let requeue = JobRecoveryAction::Requeue;
        assert_eq!(requeue, JobRecoveryAction::Requeue);

        let fail = JobRecoveryAction::Fail {
            reason: "test".to_string(),
        };
        if let JobRecoveryAction::Fail { reason } = fail {
            assert_eq!(reason, "test");
        }
    }
}
