use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Enum representing job types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Flush,
    VectorIndex,
    Compact,
    Cleanup,
    JobCleanup,
    Backup,
    Restore,
    Retention,
    StreamEviction,
    UserCleanup,
    ManifestEviction,
    TopicCleanup,
    TopicRetention,
    UserExport,
    Unknown,
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobType::Flush => "flush",
            JobType::VectorIndex => "vector_index",
            JobType::Compact => "compact",
            JobType::Cleanup => "cleanup",
            JobType::JobCleanup => "job_cleanup",
            JobType::Backup => "backup",
            JobType::Restore => "restore",
            JobType::Retention => "retention",
            JobType::StreamEviction => "stream_eviction",
            JobType::UserCleanup => "user_cleanup",
            JobType::ManifestEviction => "manifest_eviction",
            JobType::TopicCleanup => "topic_cleanup",
            JobType::TopicRetention => "topic_retention",
            JobType::UserExport => "user_export",
            JobType::Unknown => "unknown",
        }
    }

    /// Returns the 2-letter uppercase prefix for JobId generation
    ///
    /// Prefix mapping:
    /// - FL: Flush
    /// - VI: VectorIndex
    /// - CO: Compact
    /// - CL: Cleanup
    /// - BK: Backup
    /// - RS: Restore
    /// - RT: Retention
    /// - SE: StreamEviction
    /// - UC: UserCleanup
    /// - ME: ManifestEviction
    /// - UN: Unknown
    pub fn short_prefix(&self) -> &'static str {
        match self {
            JobType::Flush => "FL",
            JobType::VectorIndex => "VI",
            JobType::Compact => "CO",
            JobType::Cleanup => "CL",
            JobType::JobCleanup => "JC",
            JobType::Backup => "BK",
            JobType::Restore => "RS",
            JobType::Retention => "RT",
            JobType::StreamEviction => "SE",
            JobType::UserCleanup => "UC",
            JobType::TopicCleanup => "TC",
            JobType::ManifestEviction => "ME",
            JobType::TopicRetention => "TR",
            JobType::UserExport => "UE",
            JobType::Unknown => "UN",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "flush" => Some(JobType::Flush),
            "vector_index" => Some(JobType::VectorIndex),
            "compact" => Some(JobType::Compact),
            "cleanup" => Some(JobType::Cleanup),
            "job_cleanup" => Some(JobType::JobCleanup),
            "backup" => Some(JobType::Backup),
            "restore" => Some(JobType::Restore),
            "retention" => Some(JobType::Retention),
            "stream_eviction" => Some(JobType::StreamEviction),
            "user_cleanup" => Some(JobType::UserCleanup),
            "topic_cleanup" => Some(JobType::TopicCleanup),
            "manifest_eviction" => Some(JobType::ManifestEviction),
            "topic_retention" => Some(JobType::TopicRetention),
            "user_export" => Some(JobType::UserExport),
            "unknown" => Some(JobType::Unknown),
            _ => None,
        }
    }

    /// Whether this job has leader-only actions (external storage, shared metadata).
    ///
    /// Leader actions include:
    /// - Writing Parquet files to external storage
    /// - Updating shared manifest metadata
    /// - Deleting files from external storage
    /// - Global backup/restore operations
    ///
    /// These actions must only be performed by the leader to maintain consistency.
    pub fn has_leader_actions(&self) -> bool {
        matches!(
            self,
            JobType::Flush |        // Parquet upload + manifest update
            JobType::VectorIndex |  // Vector snapshot persistence to cold storage
            JobType::Cleanup |      // Delete external Parquet + metadata
            JobType::Backup |       // External storage upload
            JobType::Restore |      // External storage download
            JobType::JobCleanup |   // Raft-replicated job table cleanup
            JobType::UserCleanup |  // Cascade via Raft
            JobType::TopicCleanup | // Delete topic messages + offsets
            JobType::UserExport // Export user data to zip
        )
    }

    /// Whether this job has local work that all nodes should perform.
    ///
    /// Local work includes:
    /// - RocksDB compaction (each node owns its local files)
    /// - Local cache eviction (node-local only)
    /// - Deleting flushed rows from local RocksDB
    ///
    /// This work is safe to run on all nodes independently.
    pub fn has_local_work(&self) -> bool {
        matches!(
            self,
            JobType::Flush |            // Delete flushed rows from RocksDB + compact
            JobType::Compact |          // RocksDB compaction (local files)
            JobType::ManifestEviction | // Local cache eviction
            JobType::StreamEviction |   // Local stream log cleanup
            JobType::Retention |        // Local soft-delete cleanup
            JobType::TopicRetention // Local topic message cleanup
        )
    }

    /// Whether this job should ONLY run on the leader.
    ///
    /// Leader-only jobs:
    /// - Only the leader creates and executes the job
    /// - Followers ignore job creation (don't create job_nodes)
    /// - Used for jobs that don't have local work on followers
    ///
    /// Returns true for jobs that ONLY have leader actions and NO local work.
    pub fn is_leader_only(&self) -> bool {
        self.has_leader_actions() && !self.has_local_work()
    }
}

impl FromStr for JobType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobType::from_str_opt(s).ok_or_else(|| format!("Invalid JobType: {}", s))
    }
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for JobType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "flush" => JobType::Flush,
            "vector_index" => JobType::VectorIndex,
            "compact" => JobType::Compact,
            "cleanup" => JobType::Cleanup,
            "job_cleanup" => JobType::JobCleanup,
            "backup" => JobType::Backup,
            "restore" => JobType::Restore,
            "retention" => JobType::Retention,
            "stream_eviction" => JobType::StreamEviction,
            "user_cleanup" => JobType::UserCleanup,
            "manifest_eviction" => JobType::ManifestEviction,
            "topic_cleanup" => JobType::TopicCleanup,
            "topic_retention" => JobType::TopicRetention,
            "user_export" => JobType::UserExport,
            "unknown" => JobType::Unknown,
            _ => JobType::Unknown,
        }
    }
}

impl From<String> for JobType {
    fn from(s: String) -> Self {
        JobType::from(s.as_str())
    }
}
