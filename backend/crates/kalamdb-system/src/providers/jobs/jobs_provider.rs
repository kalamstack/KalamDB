//! System.jobs table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.jobs table.
//! Uses `IndexedEntityStore` for automatic secondary index management.
//!
//! ## Indexes
//!
//! The jobs table has two secondary indexes (managed automatically):
//!
//! 1. **JobStatusCreatedAtIndex** - Queries by status + created_at
//!    - Key: `[status_byte][created_at_be][job_id]`
//!    - Enables: "All Running jobs sorted by created_at"
//!
//! 2. **JobIdempotencyKeyIndex** - Lookup by idempotency key
//!    - Key: `{idempotency_key}`
//!    - Enables: Duplicate job prevention
//!
//! Note: namespace_id and table_name are now stored in the parameters JSON field

use super::jobs_indexes::{create_jobs_indexes, status_to_u8};
use crate::error::{SystemError, SystemResultExt};
use crate::providers::base::{system_rows_to_batch, IndexedProviderDefinition};
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};
use crate::JobStatus;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::JobId;
use kalamdb_commons::SystemTable;
use kalamdb_store::entity_store::EntityStore;
use kalamdb_store::{IndexedEntityStore, StorageBackend};
use std::sync::{Arc, OnceLock};

use super::models::{Job, JobFilter, JobSortField, SortOrder};

/// Type alias for the indexed jobs store
pub type JobsStore = IndexedEntityStore<JobId, SystemTableRow>;

/// System.jobs table provider using IndexedEntityStore for automatic index management.
///
/// All insert/update/delete operations automatically maintain secondary indexes
/// using RocksDB's atomic WriteBatch - no manual index management needed.
#[derive(Clone)]
pub struct JobsTableProvider {
    store: JobsStore,
}

impl JobsTableProvider {
    /// Create a new jobs table provider with automatic index management.
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new JobsTableProvider instance with indexes configured
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            SystemTable::Jobs.column_family_name().expect("Jobs is a table, not a view"),
            create_jobs_indexes(),
        );
        Self { store }
    }

    /// Create a new job entry.
    ///
    /// Indexes are automatically maintained via `IndexedEntityStore`.
    /// TODO: Return a string message
    pub fn create_job(&self, job: Job) -> Result<String, SystemError> {
        let row = Self::encode_job_row(&job)?;
        self.store.insert(&job.job_id, &row).into_system_error("insert job error")?;
        Ok(format!("Job {} created", job.job_id))
    }

    /// Async insert path corresponding to `create_job()`.
    ///
    /// Uses `spawn_blocking` internally to avoid blocking the async runtime.
    pub async fn insert_job_async(&self, job: Job) -> Result<(), SystemError> {
        let job_id = job.job_id.clone();
        let row = Self::encode_job_row(&job)?;
        self.store
            .insert_async(job_id, row)
            .await
            .into_system_error("insert_async job error")
    }

    /// Get a job by ID
    pub fn get_job_by_id(&self, job_id: &JobId) -> Result<Option<Job>, SystemError> {
        let row = self.store.get(job_id)?;
        row.map(|value| Self::decode_job_row(&value)).transpose()
    }

    /// Alias for get_job_by_id (for backward compatibility)
    pub fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, SystemError> {
        self.get_job_by_id(job_id)
    }

    /// Async version of `get_job()`.
    pub async fn get_job_async(&self, job_id: &JobId) -> Result<Option<Job>, SystemError> {
        let row = self
            .store
            .get_async(job_id.clone())
            .await
            .into_system_error("get_async error")?;
        row.map(|value| Self::decode_job_row(&value)).transpose()
    }

    /// Update an existing job entry.
    ///
    /// Indexes are automatically maintained via `IndexedEntityStore`.
    /// Stale index entries are removed and new ones added atomically.
    pub fn update_job(&self, job: Job) -> Result<(), SystemError> {
        // Check if job exists
        let old_row = self.store.get(&job.job_id)?;
        if old_row.is_none() {
            return Err(SystemError::NotFound(format!("Job not found: {}", job.job_id)));
        }
        let old_row = old_row.unwrap();

        validate_job_update(&job)?;
        let new_row = Self::encode_job_row(&job)?;

        self.store
            .update_with_old(&job.job_id, Some(&old_row), &new_row)
            .into_system_error("update job error")
    }

    /// Async version of `update_job()`.
    pub async fn update_job_async(&self, job: Job) -> Result<(), SystemError> {
        log::debug!("[{}] update_job_async called: status={:?}", job.job_id, job.status);

        // Check if job exists
        let old_row = self
            .store
            .get_async(job.job_id.clone())
            .await
            .into_system_error("get_async error")?;

        let old_row = match old_row {
            Some(row) => row,
            None => {
                return Err(SystemError::NotFound(format!("Job not found: {}", job.job_id)));
            },
        };

        validate_job_update(&job)?;
        let new_row = Self::encode_job_row(&job)?;

        // We need to do update in blocking context since update_with_old is sync
        let store = self.store.clone();
        let job_id_clone = job.job_id.clone();
        let job_id = job.job_id.clone();
        tokio::task::spawn_blocking(move || {
            store.update_with_old(&job_id_clone, Some(&old_row), &new_row)
        })
        .await
        .into_system_error("spawn_blocking error")?
        .into_system_error("update job error")?;

        log::debug!("[{}] update_job_async completed successfully", job_id);
        Ok(())
    }

    /// Delete a job entry.
    ///
    /// Indexes are automatically cleaned up via `IndexedEntityStore`.
    pub fn delete_job(&self, job_id: &JobId) -> Result<(), SystemError> {
        self.store.delete(job_id).into_system_error("delete job error")
    }

    /// List all jobs
    pub fn list_jobs(&self) -> Result<Vec<Job>, SystemError> {
        self.list_jobs_filtered(&JobFilter::default())
    }

    /// List jobs with filter.
    ///
    /// Optimized: When filtering by status and sorting by CreatedAt ASC, uses
    /// the `JobStatusCreatedAtIndex` for efficient prefix scanning.
    pub fn list_jobs_filtered(&self, filter: &JobFilter) -> Result<Vec<Job>, SystemError> {
        // Optimization: If filtering by status(es) and sorting by CreatedAt ASC, use the status index
        let use_index = filter.sort_by == Some(JobSortField::CreatedAt)
            && filter.sort_order == Some(SortOrder::Asc)
            && (filter.status.is_some() || filter.statuses.is_some());

        if use_index {
            let mut jobs = Vec::new();
            let limit = filter.limit.unwrap_or(usize::MAX);

            // Collect statuses to scan
            let mut statuses = Vec::new();
            if let Some(s) = filter.status {
                statuses.push(s);
            }
            if let Some(ref s_list) = filter.statuses {
                statuses.extend(s_list.iter().cloned());
            }
            // Deduplicate and sort statuses to scan in order (New=0, Queued=1, etc.)
            statuses.sort_by_key(|s| status_to_u8(*s));
            statuses.dedup();

            // Status index is index 0 (JobStatusCreatedAtIndex)
            const STATUS_INDEX: usize = 0;

            for status in statuses {
                if jobs.len() >= limit {
                    break;
                }

                // Prefix for this status: [status_byte]
                let prefix = vec![status_to_u8(status)];

                // Scan by index - returns (JobId, Job) pairs
                let iter = self
                    .store
                    .scan_by_index_iter(STATUS_INDEX, Some(&prefix), Some(limit - jobs.len()))
                    .into_system_error("scan_by_index_iter error")?;

                for entry in iter {
                    let (_job_id, row) = entry.into_system_error("scan_by_index_iter error")?;
                    let job = Self::decode_job_row(&row)?;
                    // Apply other filters that index doesn't cover
                    if self.matches_filter(&job, filter) {
                        jobs.push(job);
                        if jobs.len() >= limit {
                            break;
                        }
                    }
                }
            }

            return Ok(jobs);
        }

        // Fallback to full scan
        let all_rows = self.store.scan_all_typed(None, None, None)?;
        let mut jobs: Vec<Job> = all_rows
            .into_iter()
            .map(|(_, row)| Self::decode_job_row(&row))
            .collect::<Result<Vec<_>, _>>()?;

        // Apply filters in memory
        jobs.retain(|job| self.matches_filter(job, filter));

        // Sort
        if let Some(sort_by) = filter.sort_by {
            match sort_by {
                JobSortField::CreatedAt => jobs.sort_by_key(|j| j.created_at),
                JobSortField::UpdatedAt => jobs.sort_by_key(|j| j.updated_at),
                JobSortField::Priority => jobs.sort_by_key(|j| j.priority.unwrap_or(0)),
            }

            if filter.sort_order == Some(SortOrder::Desc) {
                jobs.reverse();
            }
        }

        // Limit
        if let Some(limit) = filter.limit {
            if jobs.len() > limit {
                jobs.truncate(limit);
            }
        }

        Ok(jobs)
    }

    /// Async version of `list_jobs_filtered()`.
    pub async fn list_jobs_filtered_async(
        &self,
        filter: JobFilter,
    ) -> Result<Vec<Job>, SystemError> {
        // Optimization: If filtering by status(es) and sorting by CreatedAt ASC, use the status index
        let use_index = filter.sort_by == Some(JobSortField::CreatedAt)
            && filter.sort_order == Some(SortOrder::Asc)
            && (filter.status.is_some() || filter.statuses.is_some());

        if use_index {
            let mut jobs = Vec::new();
            let limit = filter.limit.unwrap_or(usize::MAX);

            // Collect statuses to scan
            let mut statuses = Vec::new();
            if let Some(s) = filter.status {
                statuses.push(s);
            }
            if let Some(ref s_list) = filter.statuses {
                statuses.extend(s_list.iter().cloned());
            }
            statuses.sort_by_key(|s| status_to_u8(*s));
            statuses.dedup();

            // Status index is index 0 (JobStatusCreatedAtIndex)
            const STATUS_INDEX: usize = 0;

            for status in statuses {
                if jobs.len() >= limit {
                    break;
                }

                let prefix = vec![status_to_u8(status)];
                let job_entries = self
                    .store
                    .scan_by_index_async(STATUS_INDEX, Some(prefix), Some(limit - jobs.len()))
                    .await
                    .into_system_error("scan_by_index_async error")?;

                for (_job_id, row) in job_entries {
                    let job = Self::decode_job_row(&row)?;
                    if matches_filter_sync(&job, &filter) {
                        jobs.push(job);
                        if jobs.len() >= limit {
                            break;
                        }
                    }
                }
            }

            return Ok(jobs);
        }

        // Fallback to full scan
        let all_jobs: Vec<(Vec<u8>, SystemTableRow)> = self
            .store
            .scan_all_async(None, None, None)
            .await
            .into_system_error("scan_all_async error")?;
        let mut jobs: Vec<Job> = all_jobs
            .into_iter()
            .map(|(_, row)| Self::decode_job_row(&row))
            .collect::<Result<Vec<_>, _>>()?;

        jobs.retain(|job| matches_filter_sync(job, &filter));

        if let Some(sort_by) = filter.sort_by {
            match sort_by {
                JobSortField::CreatedAt => jobs.sort_by_key(|j| j.created_at),
                JobSortField::UpdatedAt => jobs.sort_by_key(|j| j.updated_at),
                JobSortField::Priority => jobs.sort_by_key(|j| j.priority.unwrap_or(0)),
            }
            if filter.sort_order == Some(SortOrder::Desc) {
                jobs.reverse();
            }
        }

        if let Some(limit) = filter.limit {
            if jobs.len() > limit {
                jobs.truncate(limit);
            }
        }

        Ok(jobs)
    }

    fn matches_filter(&self, job: &Job, filter: &JobFilter) -> bool {
        // Filter by status (single)
        if let Some(ref status) = filter.status {
            if status != &job.status {
                return false;
            }
        }

        // Filter by statuses (multiple)
        if let Some(ref statuses) = filter.statuses {
            if !statuses.contains(&job.status) {
                return false;
            }
        }

        // Filter by job type
        if let Some(ref job_type) = filter.job_type {
            if job_type != &job.job_type {
                return false;
            }
        }

        // Filter by idempotency key
        if let Some(ref key) = filter.idempotency_key {
            if job.idempotency_key.as_ref() != Some(key) {
                return false;
            }
        }

        // Filter by created_after
        if let Some(after) = filter.created_after {
            if job.created_at < after {
                return false;
            }
        }

        // Filter by created_before
        if let Some(before) = filter.created_before {
            if job.created_at >= before {
                return false;
            }
        }

        true
    }

    /// Cancel a running job
    pub fn cancel_job(&self, job_id: &JobId) -> Result<(), SystemError> {
        // Get current job
        let job = self
            .get_job(job_id)?
            .ok_or_else(|| SystemError::NotFound(format!("Job not found: {}", job_id)))?;

        // Check if job is still running
        if job.status != JobStatus::Running {
            return Err(SystemError::Other(format!(
                "Cannot cancel job {} with status '{}'",
                job_id, job.status
            )));
        }

        // Update to cancelled status
        let cancelled_job = job.cancel();
        self.update_job(cancelled_job)?;

        Ok(())
    }

    /// Delete jobs older than retention period (in days).
    ///
    /// Optimized to use the status index to avoid full table scan.
    /// Only cleans up terminal statuses: Completed, Failed, Cancelled.
    pub fn cleanup_old_jobs(&self, retention_days: i64) -> Result<usize, SystemError> {
        let now = chrono::Utc::now().timestamp_millis();
        let retention_ms = retention_days * 24 * 60 * 60 * 1000;
        let cutoff_time = now - retention_ms;

        let mut deleted = 0;

        // Only clean up terminal statuses
        let target_statuses = [
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ];

        // Status index is index 0 (JobStatusCreatedAtIndex)
        const STATUS_INDEX: usize = 0;

        for status in target_statuses {
            let status_byte = status_to_u8(status);
            let prefix = vec![status_byte];

            // Scan index for this status using scan_index_raw
            // Keys are [status_byte][created_at_be][job_id_bytes]
            // Sorted by created_at ASC
            let iter = self
                .store
                .scan_index_raw_typed_iter(STATUS_INDEX, Some(&prefix), None, None)
                .into_system_error("scan_index_raw_typed_iter error")?;

            for entry in iter {
                let (key_bytes, job_id) =
                    entry.into_system_error("scan_index_raw_typed_iter error")?;
                // Extract created_at (bytes 1..9)
                if key_bytes.len() < 9 {
                    continue;
                }

                let mut created_at_bytes = [0u8; 8];
                created_at_bytes.copy_from_slice(&key_bytes[1..9]);
                let created_at = i64::from_be_bytes(created_at_bytes);

                // Optimization: Since index is sorted by created_at, if we encounter
                // a job created AFTER the cutoff, we can stop scanning this status.
                if created_at > cutoff_time {
                    break;
                }

                // Load job to check actual finished_at
                if let Some(row) = self.store.get(&job_id)? {
                    let job = Self::decode_job_row(&row)?;
                    let reference_time =
                        job.finished_at.or(job.started_at).unwrap_or(job.created_at);

                    if reference_time < cutoff_time {
                        self.delete_job(&job.job_id)?;
                        deleted += 1;
                    }
                }
            }
        }

        Ok(deleted)
    }

    /// Helper to create RecordBatch from jobs
    fn create_batch(&self, jobs: Vec<(JobId, SystemTableRow)>) -> Result<RecordBatch, SystemError> {
        let rows = jobs.into_iter().map(|(_, row)| row).collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    /// Scan all jobs and return as RecordBatch
    pub fn scan_all_jobs(&self) -> Result<RecordBatch, SystemError> {
        let jobs = self.store.scan_all_typed(None, None, None)?;
        self.create_batch(jobs)
    }

    fn encode_job_row(job: &Job) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(job, &Job::definition())
    }

    fn decode_job_row(row: &SystemTableRow) -> Result<Job, SystemError> {
        system_row_to_model(row, &Job::definition())
    }
}

fn validate_job_update(job: &Job) -> Result<(), SystemError> {
    let status = job.status;

    if matches!(
        status,
        JobStatus::Running
            | JobStatus::Retrying
            | JobStatus::Completed
            | JobStatus::Failed
            | JobStatus::Cancelled
            | JobStatus::Skipped
    ) && job.started_at.is_none()
    {
        return Err(SystemError::Other(format!(
            "Job {}: started_at must be set before marking status {}",
            job.job_id, status
        )));
    }

    if matches!(
        status,
        JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled | JobStatus::Skipped
    ) && job.finished_at.is_none()
    {
        return Err(SystemError::Other(format!(
            "Job {}: finished_at must be set before marking status {}",
            job.job_id, status
        )));
    }

    if status == JobStatus::Completed && job.message.is_none() {
        return Err(SystemError::Other(format!(
            "Job {}: result/message must be set before marking status {}",
            job.job_id, status
        )));
    }

    if status == JobStatus::Skipped && job.message.is_none() {
        return Err(SystemError::Other(format!(
            "Job {}: message must be set before marking status {}",
            job.job_id, status
        )));
    }

    Ok(())
}

/// Synchronous filter matching helper
fn matches_filter_sync(job: &Job, filter: &JobFilter) -> bool {
    if let Some(ref status) = filter.status {
        if status != &job.status {
            return false;
        }
    }
    if let Some(ref statuses) = filter.statuses {
        if !statuses.contains(&job.status) {
            return false;
        }
    }
    if let Some(ref job_type) = filter.job_type {
        if job_type != &job.job_type {
            return false;
        }
    }
    if let Some(ref key) = filter.idempotency_key {
        if job.idempotency_key.as_ref() != Some(key) {
            return false;
        }
    }
    if let Some(after) = filter.created_after {
        if job.created_at < after {
            return false;
        }
    }
    if let Some(before) = filter.created_before {
        if job.created_at >= before {
            return false;
        }
    }
    true
}

crate::impl_system_table_provider_metadata!(
    indexed,
    provider = JobsTableProvider,
    key = JobId,
    table_name = SystemTable::Jobs.table_name(),
    primary_key_column = "job_id",
    parse_key = |value| Some(JobId::new(value)),
    schema = Job::definition().to_arrow_schema().expect("failed to build jobs schema")
);

crate::impl_indexed_system_table_provider!(
    provider = JobsTableProvider,
    key = JobId,
    value = SystemTableRow,
    store = store,
    definition = provider_definition,
    build_batch = create_batch
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JobStatus, JobType};
    use datafusion::datasource::TableProvider;
    use kalamdb_commons::NodeId;
    use kalamdb_store::test_utils::InMemoryBackend;

    fn create_test_provider() -> JobsTableProvider {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        JobsTableProvider::new(backend)
    }

    fn create_test_job(job_id: &str) -> Job {
        let now = chrono::Utc::now().timestamp_millis();
        Job {
            job_id: JobId::new(job_id),
            job_type: JobType::Flush,
            status: JobStatus::Running,
            leader_status: None,
            parameters: Some(serde_json::json!({"namespace_id":"default","table_name":"events"})),
            message: None,
            exception_trace: None,
            idempotency_key: None,
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            finished_at: None,
            node_id: NodeId::from(1u64),
            leader_node_id: None,
            queue: None,
            priority: None,
        }
    }

    #[test]
    fn test_create_and_get_job() {
        let provider = create_test_provider();
        let job = create_test_job("job1");

        provider.create_job(job.clone()).unwrap();

        let job_id = JobId::new("job1");
        let retrieved = provider.get_job_by_id(&job_id).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.job_id, job_id);
        assert_eq!(retrieved.status, JobStatus::Running);
    }

    #[test]
    fn test_update_job() {
        let provider = create_test_provider();
        let mut job = create_test_job("job1");
        provider.create_job(job.clone()).unwrap();

        // Update
        job.status = JobStatus::Completed;
        job.finished_at = Some(job.created_at + 1);
        job.message = Some("flush complete".to_string());
        provider.update_job(job.clone()).unwrap();

        // Verify
        let job_id = JobId::new("job1");
        let retrieved = provider.get_job_by_id(&job_id).unwrap().unwrap();
        assert_eq!(retrieved.status, JobStatus::Completed);
    }

    #[test]
    fn test_update_job_requires_started_at_for_completed() {
        let provider = create_test_provider();
        let mut job = create_test_job("job1");
        provider.create_job(job.clone()).unwrap();

        job.status = JobStatus::Completed;
        job.started_at = None;
        job.finished_at = Some(job.created_at + 1);
        job.message = Some("flush complete".to_string());

        let err = provider.update_job(job).unwrap_err();
        match err {
            SystemError::Other(msg) => assert!(msg.contains("started_at")),
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_update_job_requires_result_for_completed() {
        let provider = create_test_provider();
        let mut job = create_test_job("job1");
        provider.create_job(job.clone()).unwrap();

        job.status = JobStatus::Completed;
        job.finished_at = Some(job.created_at + 1);
        job.message = None;

        let err = provider.update_job(job).unwrap_err();
        match err {
            SystemError::Other(msg) => assert!(msg.contains("result/message")),
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_delete_job() {
        let provider = create_test_provider();
        let job = create_test_job("job1");

        provider.create_job(job).unwrap();

        let job_id = JobId::new("job1");
        provider.delete_job(&job_id).unwrap();

        let retrieved = provider.get_job_by_id(&job_id).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_scan_all_jobs() {
        let provider = create_test_provider();

        // Insert multiple jobs
        for i in 1..=3 {
            let job = create_test_job(&format!("job{}", i));
            provider.create_job(job).unwrap();
        }

        // Scan
        let batch = provider.scan_all_jobs().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 20);
    }

    #[tokio::test]
    async fn test_datafusion_scan() {
        let provider = create_test_provider();

        // Insert test data
        let job = create_test_job("job1");
        provider.create_job(job).unwrap();

        // Create DataFusion session
        let ctx = datafusion::execution::context::SessionContext::new();
        let state = ctx.state();

        // Scan via DataFusion
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        assert!(!plan.schema().fields().is_empty());
    }
}
