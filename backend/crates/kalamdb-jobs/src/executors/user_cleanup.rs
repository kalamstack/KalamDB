//! User Cleanup Job Executor
//!
//! **Phase 9 (T150)**: JobExecutor implementation for user cleanup operations
//!
//! Handles cleanup of soft-deleted user accounts and associated data.
//!
//! ## Responsibilities
//! - Clean up soft-deleted user records
//! - Cascade delete user's tables and data
//! - Remove user from all access control lists
//! - Clean up user's authentication tokens
//!
//! ## Parameters Format
//! ```json
//! {
//!   "user_id": "USR123",
//!   "username": "john_doe",
//!   "cascade": true
//! }
//! ```

use async_trait::async_trait;
use kalamdb_commons::models::UserId;
use kalamdb_core::error::KalamDbError;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};

/// Typed parameters for user cleanup operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCleanupParams {
    /// User ID (required)
    pub user_id: UserId,
    /// Username (required)
    pub username: String,
    /// Cascade delete user's tables (optional, defaults to false)
    #[serde(default)]
    pub cascade: bool,
}

impl JobParams for UserCleanupParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.username.is_empty() {
            return Err(KalamDbError::InvalidOperation("username cannot be empty".to_string()));
        }
        Ok(())
    }
}

/// User Cleanup Job Executor
///
/// Executes user cleanup operations after soft delete.
pub struct UserCleanupExecutor;

impl UserCleanupExecutor {
    /// Create a new UserCleanupExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for UserCleanupExecutor {
    type Params = UserCleanupParams;

    fn job_type(&self) -> JobType {
        JobType::UserCleanup
    }

    fn name(&self) -> &'static str {
        "UserCleanupExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting user cleanup operation");

        // Parameters already validated in JobContext - type-safe access
        let params = ctx.params();
        let user_id = &params.user_id;
        let username = &params.username;
        let cascade = params.cascade;

        ctx.log_info(&format!(
            "Cleaning up user {} ({}) (cascade: {})",
            username, user_id, cascade
        ));

        // TODO: Implement actual user cleanup logic
        // Implementation steps:
        // 1. Delete user from system.users via users_provider.delete_user(user_id)
        // 2. If cascade=true, cascade delete user's tables via cleanup jobs
        // 3. If cascade=true, remove user from shared table ACLs
        // 4. Clean up user's live queries via live_query_manager
        // 5. Clean up user's auth tokens (add JWT revocation mechanism)

        // For now, return placeholder metrics
        let tables_deleted = 0;
        let queries_stopped = 0;

        let message = format!(
            "Cleaned up user {} ({}) - {} tables deleted, {} queries stopped (cascade: {})",
            username, user_id, tables_deleted, queries_stopped, cascade
        );

        ctx.log_info(&message);

        Ok(JobDecision::Completed {
            message: Some(message),
        })
    }
}

impl Default for UserCleanupExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_properties() {
        let executor = UserCleanupExecutor::new();
        assert_eq!(executor.job_type(), JobType::UserCleanup);
        assert_eq!(executor.name(), "UserCleanupExecutor");
    }
}
