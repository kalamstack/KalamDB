//! Unified Meta group commands
//!
//! Combines all metadata operations into a single totally-ordered Raft group:
//! - System operations: namespaces, tables, storages (was MetaSystem)
//! - User operations: user CRUD, login, locking (was MetaUsers)
//! - Job operations: jobs, claims (was MetaJobs)
//!
//! This unified group ensures all metadata dependencies are captured in a single
//! monotonically increasing log index, enabling simple watermark-based ordering
//! for data groups.

use chrono::{DateTime, Utc};
use kalamdb_commons::{
    models::{
        schemas::{TableDefinition, TableType},
        JobId, NamespaceId, NodeId, StorageId, UserId,
    },
    TableId,
};
use kalamdb_system::{providers::jobs::models::Job, JobStatus, Storage, User};
use serde::{Deserialize, Serialize};

/// Commands for the unified metadata Raft group
///
/// This replaces the three separate groups (MetaSystem, MetaUsers, MetaJobs)
/// with a single totally-ordered log. Benefits:
/// - Single watermark (`meta_index`) for data group ordering
/// - No cross-metadata race conditions
/// - Simpler catch-up coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    // =========================================================================
    // Namespace Operations (was in SystemCommand)
    // =========================================================================
    /// Create a new namespace
    CreateNamespace {
        namespace_id: NamespaceId,
        created_by: Option<UserId>,
    },

    /// Delete a namespace
    DeleteNamespace { namespace_id: NamespaceId },

    // =========================================================================
    // Table Operations (was in SystemCommand)
    // =========================================================================
    /// Create a new table
    CreateTable {
        table_id: TableId,
        table_type: TableType,
        /// Table schema definition
        table_def: TableDefinition,
    },

    /// Alter an existing table
    AlterTable {
        table_id: TableId,
        /// Updated table schema definition
        table_def: TableDefinition,
    },

    /// Drop a table
    DropTable { table_id: TableId },

    // =========================================================================
    // Storage Operations (was in SystemCommand)
    // =========================================================================
    /// Register a storage backend
    RegisterStorage {
        storage_id: StorageId,
        /// Storage configuration
        storage: Storage,
    },

    /// Unregister a storage backend
    UnregisterStorage { storage_id: StorageId },

    // =========================================================================
    // User Operations (was in UsersCommand)
    // =========================================================================
    /// Create a new user
    CreateUser { user: User },

    /// Update user information
    UpdateUser { user: User },

    /// Soft-delete a user
    DeleteUser {
        user_id: UserId,
        deleted_at: DateTime<Utc>,
    },

    /// Update last login timestamp
    RecordLogin {
        user_id: UserId,
        logged_in_at: DateTime<Utc>,
    },

    /// Lock/unlock a user account
    SetUserLocked {
        user_id: UserId,
        locked_until: Option<i64>,
        updated_at: DateTime<Utc>,
    },

    // =========================================================================
    // Job Operations (was in JobsCommand)
    // =========================================================================
    /// Create a new job
    ///
    /// Uses the `Job` struct from kalamdb-commons for cleaner API.
    CreateJob { job: Job },

    /// Create a per-node job entry
    CreateJobNode {
        job_id: JobId,
        node_id: NodeId,
        status: JobStatus,
        created_at: DateTime<Utc>,
    },

    /// Claim a per-node job entry
    ClaimJobNode {
        job_id: JobId,
        node_id: NodeId,
        claimed_at: DateTime<Utc>,
    },

    /// Update per-node job status
    UpdateJobNodeStatus {
        job_id: JobId,
        node_id: NodeId,
        status: JobStatus,
        error_message: Option<String>,
        updated_at: DateTime<Utc>,
    },

    /// Claim a job for execution (leader-only)
    ClaimJob {
        job_id: JobId,
        node_id: NodeId,
        claimed_at: DateTime<Utc>,
    },

    /// Update job status
    UpdateJobStatus {
        job_id: JobId,
        status: JobStatus,
        updated_at: DateTime<Utc>,
    },

    /// Complete a job successfully
    CompleteJob {
        job_id: JobId,
        /// Job result as JSON string
        result: Option<String>,
        completed_at: DateTime<Utc>,
    },

    /// Fail a job
    FailJob {
        job_id: JobId,
        error_message: String,
        failed_at: DateTime<Utc>,
    },

    /// Release a claimed job (on failure or leader change)
    ReleaseJob {
        job_id: JobId,
        reason: String,
        released_at: DateTime<Utc>,
    },

    /// Cancel a job
    CancelJob {
        job_id: JobId,
        reason: String,
        cancelled_at: DateTime<Utc>,
    },
}

impl MetaCommand {
    /// Returns the category of this command for logging/metrics
    pub fn category(&self) -> &'static str {
        match self {
            Self::CreateNamespace { .. } | Self::DeleteNamespace { .. } => "namespace",
            Self::CreateTable { .. } | Self::AlterTable { .. } | Self::DropTable { .. } => "table",
            Self::RegisterStorage { .. } | Self::UnregisterStorage { .. } => "storage",
            Self::CreateUser { .. }
            | Self::UpdateUser { .. }
            | Self::DeleteUser { .. }
            | Self::RecordLogin { .. }
            | Self::SetUserLocked { .. } => "user",
            Self::CreateJob { .. }
            | Self::CreateJobNode { .. }
            | Self::ClaimJob { .. }
            | Self::ClaimJobNode { .. }
            | Self::UpdateJobStatus { .. }
            | Self::UpdateJobNodeStatus { .. }
            | Self::CompleteJob { .. }
            | Self::FailJob { .. }
            | Self::ReleaseJob { .. }
            | Self::CancelJob { .. } => "job",
        }
    }
}

/// Response from meta commands
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum MetaResponse {
    #[default]
    Ok,

    // === Generic response (preserves original success message) ===
    Message {
        message: String,
    },

    // === Namespace responses ===
    NamespaceCreated {
        namespace_id: NamespaceId,
        message: String,
    },

    // === Table responses ===
    TableCreated {
        table_id: TableId,
        message: String,
    },

    // === User responses ===
    UserCreated {
        user_id: UserId,
        message: String,
    },

    // === Job responses ===
    JobCreated {
        job_id: JobId,
        message: String,
    },
    JobClaimed {
        job_id: JobId,
        node_id: NodeId,
        message: String,
    },

    // === Error ===
    Error {
        message: String,
    },
}

impl MetaResponse {
    /// Create an error response with the given message
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error {
            message: msg.into(),
        }
    }

    /// Returns true if this is not an error response
    pub fn is_ok(&self) -> bool {
        !matches!(self, Self::Error { .. })
    }

    /// Get the error message if this is an error response
    pub fn error_message(&self) -> Option<&str> {
        match self {
            Self::Error { message } => Some(message),
            _ => None,
        }
    }

    /// Get the message from any response variant
    pub fn get_message(&self) -> String {
        match self {
            Self::Ok => "Operation completed successfully".to_string(),
            Self::Message { message } => message.clone(),
            Self::NamespaceCreated { message, .. } => message.clone(),
            Self::TableCreated { message, .. } => message.clone(),
            Self::UserCreated { message, .. } => message.clone(),
            Self::JobCreated { message, .. } => message.clone(),
            Self::JobClaimed { message, .. } => message.clone(),
            Self::Error { message } => message.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{AuthType, Role};

    use super::*;

    fn test_user() -> User {
        User {
            user_id: UserId::from("test_user"),
            password_hash: "hash".to_string(),
            email: None,
            auth_type: AuthType::Password,
            auth_data: None,
            role: Role::User,
            storage_id: None,
            storage_mode: kalamdb_system::providers::storages::models::StorageMode::Table,
            locked_until: None,
            failed_login_attempts: 0,
            last_login_at: None,
            created_at: 0,
            updated_at: 0,
            last_seen: None,
            deleted_at: None,
        }
    }

    #[test]
    fn test_meta_command_category() {
        let cmd = MetaCommand::CreateNamespace {
            namespace_id: NamespaceId::new("test".to_string()),
            created_by: None,
        };
        assert_eq!(cmd.category(), "namespace");

        let cmd = MetaCommand::CreateUser { user: test_user() };
        assert_eq!(cmd.category(), "user");
    }

    #[test]
    fn test_meta_response_is_ok() {
        assert!(MetaResponse::Ok.is_ok());
        assert!(!MetaResponse::error("test").is_ok());
    }
}
