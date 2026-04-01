use super::types::JobsManager;
use kalamdb_commons::JobId;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_system::providers::jobs::models::{Job, JobFilter};
use kalamdb_system::JobStatus;

impl JobsManager {
    /// Get job details
    ///
    /// Delegates to provider's async method which handles spawn_blocking internally.
    ///
    /// # Arguments
    /// * `job_id` - ID of job to retrieve
    ///
    /// # Returns
    /// Job struct if found, None otherwise
    pub async fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, KalamDbError> {
        self.jobs_provider
            .get_job_async(job_id)
            .await
            .into_kalamdb_error("Failed to get job")
    }

    /// List jobs matching filter criteria
    ///
    /// Delegates to provider's async method which handles spawn_blocking internally.
    ///
    /// # Arguments
    /// * `filter` - Filter criteria (status, job_type, namespace, etc.)
    ///
    /// # Returns
    /// Vector of matching jobs
    pub async fn list_jobs(&self, filter: JobFilter) -> Result<Vec<Job>, KalamDbError> {
        self.jobs_provider
            .list_jobs_filtered_async(filter)
            .await
            .into_kalamdb_error("Failed to list jobs")
    }

    /// Check if active job with idempotency key exists
    ///
    /// Active = New, Queued, Running, or Retrying status
    pub(crate) async fn has_active_job_with_key(&self, key: &str) -> Result<bool, KalamDbError> {
        let filter = JobFilter {
            idempotency_key: Some(key.to_string()),
            ..Default::default()
        };

        let jobs = self.list_jobs(filter).await?;
        Ok(jobs.into_iter().any(|job| {
            matches!(
                job.status,
                JobStatus::New | JobStatus::Queued | JobStatus::Running | JobStatus::Retrying
            )
        }))
    }
}
