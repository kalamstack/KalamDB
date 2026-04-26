//! Jobs table index definitions
//!
//! This module defines secondary indexes for the system.jobs table.

use std::sync::Arc;

use kalamdb_commons::{models::rows::SystemTableRow, storage::Partition, JobId};
use kalamdb_store::IndexDefinition;

use crate::{
    providers::jobs::models::Job, system_row_mapper::system_row_to_model, JobStatus,
    StoragePartition,
};

/// Index for querying jobs by status + created_at (sorted).
///
/// Key format: `[status_byte][created_at_be][job_id_bytes]`
///
/// This index allows efficient queries like:
/// - "All Running jobs sorted by created_at"
/// - "All Completed jobs from the last hour"
///
/// The index key is designed to:
/// 1. Group jobs by status (first byte)
/// 2. Sort by created_at within each status (big-endian timestamp)
/// 3. Be unique by appending job_id
pub struct JobStatusCreatedAtIndex;

impl IndexDefinition<JobId, SystemTableRow> for JobStatusCreatedAtIndex {
    fn partition(&self) -> Partition {
        Partition::new(StoragePartition::SystemJobsStatusIdx.name())
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec!["status", "created_at"]
    }

    fn extract_key(&self, _primary_key: &JobId, row: &SystemTableRow) -> Option<Vec<u8>> {
        let job: Job = system_row_to_model(row, &Job::definition()).ok()?;
        let status_byte = status_to_u8(job.status);
        let mut key = Vec::with_capacity(1 + 8 + job.job_id.as_bytes().len());
        key.push(status_byte);
        key.extend_from_slice(&job.created_at.to_be_bytes());
        key.extend_from_slice(job.job_id.as_bytes());
        Some(key)
    }

    fn filter_to_prefix(&self, filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        use kalamdb_store::extract_string_equality;

        if let Some((col, val)) = extract_string_equality(filter) {
            if col == "status" {
                if let Some(status) = parse_job_status(val) {
                    return Some(vec![status_to_u8(status)]);
                }
            }
        }
        None
    }
}

/// Index for querying jobs by idempotency key.
///
/// Key format: `{idempotency_key}` → `{job_id}`
///
/// This index allows efficient lookup of jobs by their idempotency key
/// to prevent duplicate job creation.
pub struct JobIdempotencyKeyIndex;

impl IndexDefinition<JobId, SystemTableRow> for JobIdempotencyKeyIndex {
    fn partition(&self) -> Partition {
        Partition::new(StoragePartition::SystemJobsIdempotencyIdx.name())
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec!["idempotency_key"]
    }

    fn extract_key(&self, _primary_key: &JobId, row: &SystemTableRow) -> Option<Vec<u8>> {
        let job: Job = system_row_to_model(row, &Job::definition()).ok()?;
        // Only index jobs that have an idempotency key
        job.idempotency_key.as_ref().map(|k| k.as_bytes().to_vec())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert JobStatus to a u8 for index key ordering.
///
/// Order: New(0) < Queued(1) < Running(2) < Retrying(3) < Completed(4) < Failed(5) < Cancelled(6) <
/// Skipped(7)
pub fn status_to_u8(status: JobStatus) -> u8 {
    match status {
        JobStatus::New => 0,
        JobStatus::Queued => 1,
        JobStatus::Running => 2,
        JobStatus::Retrying => 3,
        JobStatus::Completed => 4,
        JobStatus::Failed => 5,
        JobStatus::Cancelled => 6,
        JobStatus::Skipped => 7,
    }
}

/// Parse a string to JobStatus.
pub fn parse_job_status(s: &str) -> Option<JobStatus> {
    match s.to_lowercase().as_str() {
        "new" => Some(JobStatus::New),
        "queued" => Some(JobStatus::Queued),
        "running" => Some(JobStatus::Running),
        "retrying" => Some(JobStatus::Retrying),
        "completed" => Some(JobStatus::Completed),
        "failed" => Some(JobStatus::Failed),
        "cancelled" => Some(JobStatus::Cancelled),
        "skipped" => Some(JobStatus::Skipped),
        _ => None,
    }
}

/// Create the default set of indexes for the jobs table.
pub fn create_jobs_indexes() -> Vec<Arc<dyn IndexDefinition<JobId, SystemTableRow>>> {
    vec![
        Arc::new(JobStatusCreatedAtIndex),
        Arc::new(JobIdempotencyKeyIndex),
    ]
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::NodeId;

    use super::*;
    use crate::{system_row_mapper::model_to_system_row, JobType};

    fn create_test_job(id: &str, status: JobStatus) -> Job {
        let now = chrono::Utc::now().timestamp_millis();
        Job {
            job_id: JobId::new(id),
            job_type: JobType::Flush,
            status,
            leader_status: None,
            parameters: Some(serde_json::json!({"namespace_id":"default","table_name":"events"})),
            message: None,
            exception_trace: None,
            idempotency_key: Some(format!("FL:default:events:{}", id)),
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now,
            updated_at: now,
            started_at: if status != JobStatus::New && status != JobStatus::Queued {
                Some(now)
            } else {
                None
            },
            finished_at: if status == JobStatus::Completed
                || status == JobStatus::Failed
                || status == JobStatus::Cancelled
                || status == JobStatus::Skipped
            {
                Some(now)
            } else {
                None
            },
            node_id: NodeId::from(1u64),
            leader_node_id: None,
            queue: None,
            priority: None,
        }
    }

    #[test]
    fn test_status_index_key_format() {
        let job = create_test_job("job1", JobStatus::Running);
        let job_id = job.job_id.clone();

        let index = JobStatusCreatedAtIndex;
        let row = model_to_system_row(&job, &Job::definition()).unwrap();
        let key = index.extract_key(&job_id, &row).unwrap();

        // First byte is status (Running = 2)
        assert_eq!(key[0], 2);

        // Next 8 bytes are created_at in big-endian
        let mut created_at_bytes = [0u8; 8];
        created_at_bytes.copy_from_slice(&key[1..9]);
        let created_at = i64::from_be_bytes(created_at_bytes);
        assert_eq!(created_at, job.created_at);

        // Rest is job_id
        let job_id_bytes = &key[9..];
        assert_eq!(job_id_bytes, job.job_id.as_bytes());
    }

    #[test]
    fn test_idempotency_index_only_indexes_jobs_with_key() {
        let mut job = create_test_job("job1", JobStatus::Running);
        let job_id = job.job_id.clone();

        let index = JobIdempotencyKeyIndex;
        let row = model_to_system_row(&job, &Job::definition()).unwrap();

        // With idempotency key
        let key = index.extract_key(&job_id, &row);
        assert!(key.is_some());

        // Without idempotency key
        job.idempotency_key = None;
        let row = model_to_system_row(&job, &Job::definition()).unwrap();
        let key = index.extract_key(&job_id, &row);
        assert!(key.is_none());
    }

    #[test]
    fn test_status_to_u8_ordering() {
        assert!(status_to_u8(JobStatus::New) < status_to_u8(JobStatus::Queued));
        assert!(status_to_u8(JobStatus::Queued) < status_to_u8(JobStatus::Running));
        assert!(status_to_u8(JobStatus::Running) < status_to_u8(JobStatus::Retrying));
        assert!(status_to_u8(JobStatus::Retrying) < status_to_u8(JobStatus::Completed));
        assert!(status_to_u8(JobStatus::Completed) < status_to_u8(JobStatus::Failed));
        assert!(status_to_u8(JobStatus::Failed) < status_to_u8(JobStatus::Cancelled));
    }
}
