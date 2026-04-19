//! Job entity for system.jobs table.
//!
//! Represents a background job (flush, retention, cleanup, etc.).

use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::ids::{JobId, NamespaceId, NodeId};
use kalamdb_commons::models::TableName;
use kalamdb_commons::KSerializable;
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{JobStatus, JobType};

/// Job entity for system.jobs table.
///
/// Represents a background job (flush, retention, cleanup, etc.).
///
/// ## Fields
/// - `job_id`: Unique job identifier (e.g., "job_123456")
/// - `job_type`: Type of job (Flush, Compact, Cleanup, Backup, Restore)
/// - `status`: Job status (Running, Completed, Failed, Cancelled)
/// - `parameters`: Optional JSON object containing job parameters (includes namespace_id, table_name, etc.)
/// - `result`: Optional result message (for completed jobs)
/// - `trace`: Optional stack trace (for failed jobs)
/// - `memory_used`: Optional memory usage in bytes
/// - `cpu_used`: Optional CPU time in microseconds
/// - `created_at`: Unix timestamp in milliseconds when job was created
/// - `started_at`: Optional Unix timestamp in milliseconds when job started
/// - `completed_at`: Optional Unix timestamp in milliseconds when job completed
/// - `node_id`: Node/server that owns this job
/// - `error_message`: Optional error message (for failed jobs)
///
/// ## Serialization
/// - **RocksDB**: FlatBuffers envelope + FlexBuffers payload
/// - **API**: JSON via Serde
///
/// ## Example
///
/// ```rust,ignore
/// use kalamdb_system::providers::jobs::models::Job;
/// use kalamdb_system::{JobStatus, JobType};
/// use kalamdb_commons::{JobId, NodeId};
///
/// let job = Job {
///     job_id: JobId::new("job_123456"),
///     job_type: JobType::Flush,
///     status: JobStatus::Running,
///     leader_status: None,
///     parameters: Some(serde_json::json!({"namespace_id":"default","table_name":"events"})),
///     message: None,
///     exception_trace: None,
///     idempotency_key: None,
///     retry_count: 0,
///     max_retries: 3,
///     memory_used: None,
///     cpu_used: None,
///     created_at: 1730000000000,
///     updated_at: 1730000000000,
///     started_at: Some(1730000000000),
///     finished_at: None,
///     node_id: NodeId::from(1u64),
///     leader_node_id: None,
///     queue: None,
///     priority: None,
/// };
/// ```
/// Job struct with fields ordered for optimal memory alignment.
/// 8-byte aligned fields first (i64, pointers/String), then smaller types.
/// This minimizes struct padding and improves cache efficiency.
///
/// ## Distributed Job Execution Model
///
/// Jobs in a cluster have two execution phases:
/// - **Local work**: Runs on ALL nodes (e.g., RocksDB flush, local cache eviction)
/// - **Leader actions**: Runs ONLY on leader (e.g., Parquet upload to S3, shared metadata updates)
///
/// The `status` field tracks overall job status (local work on this node).
/// The `leader_status` field tracks leader-only actions when this node is the leader.
/// The `leader_node_id` field indicates which node performed leader actions.
#[table(
    name = "jobs",
    comment = "Background jobs for maintenance and data management"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Job {
    // 8-byte aligned fields first (i64, Option<i64>, String/pointer types)
    #[column(
        id = 15,
        ordinal = 15,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unix timestamp in milliseconds when job was created"
    )]
    pub created_at: i64, // Unix timestamp in milliseconds
    #[column(
        id = 16,
        ordinal = 16,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unix timestamp in milliseconds when job was last updated"
    )]
    pub updated_at: i64, // Unix timestamp in milliseconds
    #[column(
        id = 17,
        ordinal = 17,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Unix timestamp in milliseconds when job started"
    )]
    pub started_at: Option<i64>, // Unix timestamp in milliseconds
    #[column(
        id = 18,
        ordinal = 18,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Unix timestamp in milliseconds when job completed"
    )]
    pub finished_at: Option<i64>, // Unix timestamp in milliseconds
    #[column(
        id = 13,
        ordinal = 13,
        data_type(KalamDataType::BigInt),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Memory usage in bytes"
    )]
    pub memory_used: Option<i64>, // bytes
    #[column(
        id = 14,
        ordinal = 14,
        data_type(KalamDataType::BigInt),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "CPU time in microseconds"
    )]
    pub cpu_used: Option<i64>, // microseconds
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Unique job identifier"
    )]
    pub job_id: JobId,
    #[column(
        id = 19,
        ordinal = 19,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Node/server that owns this job"
    )]
    pub node_id: NodeId,
    /// Node that performed leader actions (if any). Only set when leader_status is Some.
    #[column(
        id = 20,
        ordinal = 20,
        data_type(KalamDataType::BigInt),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Node that performed leader actions"
    )]
    pub leader_node_id: Option<NodeId>,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "JSON object containing job parameters"
    )]
    #[serde(default)]
    pub parameters: Option<Value>,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Result or error message"
    )]
    pub message: Option<String>, // Unified field replacing result/error_message
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Full stack trace on failures"
    )]
    pub exception_trace: Option<String>, // Full stack trace on failures
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Key for preventing duplicate jobs"
    )]
    pub idempotency_key: Option<String>, // For preventing duplicate jobs
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Queue name for job routing"
    )]
    pub queue: Option<String>, // Queue name (future use)
    // 4-byte aligned fields (enums, i32)
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Int),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Priority value (higher = more priority)"
    )]
    pub priority: Option<i32>, // Priority value (future use)
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Type of job (Flush, Compact, Cleanup, Backup, Restore)"
    )]
    pub job_type: JobType,
    /// Status of local work (runs on all nodes)
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Job status (New, Queued, Running, Completed, Failed, Cancelled, Retrying)"
    )]
    pub status: JobStatus,
    /// Status of leader-only actions (only set on leader node for jobs with leader actions)
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Status of leader-only actions"
    )]
    pub leader_status: Option<JobStatus>,
    // 1-byte fields last
    #[column(
        id = 11,
        ordinal = 11,
        data_type(KalamDataType::SmallInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Number of retries attempted"
    )]
    pub retry_count: u8, // Number of retries attempted (default 0)
    #[column(
        id = 12,
        ordinal = 12,
        data_type(KalamDataType::SmallInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Maximum retries allowed"
    )]
    pub max_retries: u8, // Maximum retries allowed (default 3)
}

impl KSerializable for Job {}

impl Job {
    /// Mark job as cancelled
    #[inline]
    pub fn cancel(mut self) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        self.status = JobStatus::Cancelled;
        self.updated_at = now;
        self.finished_at = Some(now);
        self
    }

    /// Queue the job (transition from New to Queued)
    #[inline]
    pub fn queue(mut self) -> Self {
        self.status = JobStatus::Queued;
        self.updated_at = chrono::Utc::now().timestamp_millis();
        self
    }

    /// Start the job (transition to Running)
    #[inline]
    pub fn start(mut self) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        self.status = JobStatus::Running;
        self.updated_at = now;
        self.started_at = Some(now);
        self
    }

    /// Check if job can be retried
    #[inline]
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Extract namespace_id from parameters JSON
    pub fn namespace_id(&self) -> Option<NamespaceId> {
        self.parameters.as_ref().and_then(|p| {
            p.get("namespace_id")?.as_str().map(NamespaceId::new)
        })
    }

    /// Extract table_name from parameters JSON
    pub fn table_name(&self) -> Option<TableName> {
        self.parameters.as_ref().and_then(|p| {
            p.get("table_name")?.as_str().map(TableName::new)
        })
    }

    /// Set parameters (JSON value)
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Set idempotency key for duplicate prevention
    pub fn with_idempotency_key(mut self, key: String) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Set max retries
    pub fn with_max_retries(mut self, max_retries: u8) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set queue name
    pub fn with_queue(mut self, queue: String) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Set resource metrics (memory and CPU usage)
    pub fn with_metrics(mut self, memory_used: Option<i64>, cpu_used: Option<i64>) -> Self {
        self.memory_used = memory_used;
        self.cpu_used = cpu_used;
        self
    }

    /// get the parameters as T if possible
    pub fn get_parameters_as<T: for<'de> Deserialize<'de>>(&self) -> Option<T> {
        match &self.parameters {
            Some(params) => serde_json::from_value(params.clone()).ok(),
            None => None,
        }
    }
}

/// Options for job creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobOptions {
    /// Maximum number of retries (default: 3)
    pub max_retries: Option<u8>,
    /// Queue name for job routing (future use)
    pub queue: Option<String>,
    /// Priority value (higher = more priority, future use)
    pub priority: Option<i32>,
    /// Idempotency key to prevent duplicate job creation
    pub idempotency_key: Option<String>,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            max_retries: Some(3),
            queue: None,
            priority: None,
            idempotency_key: None,
        }
    }
}

/// Sort order for job queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Fields to sort jobs by
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobSortField {
    CreatedAt,
    UpdatedAt,
    Priority,
}

/// Filter criteria for job queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFilter {
    /// Filter by job type
    pub job_type: Option<JobType>,
    /// Filter by job status (single) - Deprecated, use statuses
    pub status: Option<JobStatus>,
    /// Filter by multiple job statuses
    pub statuses: Option<Vec<JobStatus>>,
    /// Filter by idempotency key
    pub idempotency_key: Option<String>,
    /// Limit number of results
    pub limit: Option<usize>,
    /// Start from created_at timestamp (inclusive)
    pub created_after: Option<i64>,
    /// End at created_at timestamp (exclusive)
    pub created_before: Option<i64>,
    /// Sort field
    pub sort_by: Option<JobSortField>,
    /// Sort order
    pub sort_order: Option<SortOrder>,
}

impl Default for JobFilter {
    fn default() -> Self {
        Self {
            job_type: None,
            status: None,
            statuses: None,
            idempotency_key: None,
            limit: Some(100),
            created_after: None,
            created_before: None,
            sort_by: None,
            sort_order: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn test_job_serialization() {
        let job = Job {
            job_id: "job_123".into(),
            job_type: JobType::Flush,
            status: JobStatus::Completed,
            leader_status: Some(JobStatus::Completed),
            parameters: Some(serde_json::json!({"namespace_id":"default","table_name":"events"})),
            message: Some("Job completed successfully".to_string()),
            exception_trace: None,
            idempotency_key: None,
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: 1730000000000,
            updated_at: 1730000300000,
            started_at: Some(1730000000000),
            finished_at: Some(1730000300000),
            node_id: NodeId::from(1u64),
            leader_node_id: Some(NodeId::from(1u64)),
            queue: None,
            priority: None,
        };

        let bytes = serde_json::to_vec(&job).unwrap();
        let deserialized: Job = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(job, deserialized);
    }

    #[test]
    fn test_job_cancel() {
        let job = Job {
            job_id: JobId::new("job_123"),
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
            created_at: 1730000000000,
            updated_at: 1730000000000,
            started_at: Some(1730000000000),
            finished_at: None,
            node_id: NodeId::from(1u64),
            leader_node_id: None,
            queue: None,
            priority: None,
        };

        let cancelled = job.cancel();
        assert_eq!(cancelled.status, JobStatus::Cancelled);
        assert!(cancelled.finished_at.is_some());
    }
}
