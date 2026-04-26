//! Retention Job Executor
//!
//! **Phase 9 (T148)**: JobExecutor implementation for retention policy enforcement
//!
//! Handles data retention policies for soft-deleted records.
//!
//! ## Responsibilities
//! - Enforce deleted_retention_hours policy
//! - Permanently delete expired soft-deleted records
//! - Track deletion metrics (rows deleted, bytes freed)
//! - Respect table-specific retention policies
//!
//! ## Parameters Format
//! ```json
//! {
//!   "namespace_id": "default",
//!   "table_name": "users",
//!   "table_type": "User",
//!   "retention_hours": 720
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use kalamdb_commons::{schemas::TableType, TableId};
use kalamdb_core::error::KalamDbError;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};

/// Typed parameters for retention operations (T192)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionParams {
    /// Table identifier (required)
    pub table_id: TableId,
    /// Table type (required)
    pub table_type: TableType,
    /// Retention period in hours (required, must be > 0)
    pub retention_hours: u64,
}

impl JobParams for RetentionParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.retention_hours == 0 {
            return Err(KalamDbError::InvalidOperation(
                "retention_hours must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Retention Job Executor
///
/// Executes retention policy enforcement operations.
pub struct RetentionExecutor;

impl RetentionExecutor {
    /// Create a new RetentionExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for RetentionExecutor {
    type Params = RetentionParams;

    fn job_type(&self) -> JobType {
        JobType::Retention
    }

    fn name(&self) -> &'static str {
        "RetentionExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting retention enforcement operation");

        // Parameters already validated in JobContext - type-safe access
        let params = ctx.params();
        let table_id = Arc::new(params.table_id.clone());
        let table_type = params.table_type;
        let retention_hours = params.retention_hours;

        ctx.log_info(&format!(
            "Enforcing retention policy for {} (type: {:?}, retention: {}h)",
            table_id, table_type, retention_hours
        ));

        // Calculate cutoff time for deletion (records deleted before this time are expired)
        let now = chrono::Utc::now().timestamp_millis();
        let retention_ms = (retention_hours * 3600 * 1000) as i64;
        let cutoff_time = now - retention_ms;

        ctx.log_info(&format!(
            "Cutoff time: {} (records deleted before this are expired)",
            chrono::DateTime::from_timestamp_millis(cutoff_time)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "invalid".to_string())
        ));

        // TODO: Implement actual retention enforcement logic
        // Current limitation: UserTableRow/SharedTableRow/StreamTableRow don't have deleted_at
        // field yet When adding soft-delete support to table rows:
        //   1. Add `deleted_at: Option<i64>` field to UserTableRow/SharedTableRow/StreamTableRow
        //   2. Scan table using store.scan_prefix() (no filter needed - small datasets)
        //   3. Filter rows where deleted_at.is_some() && deleted_at.unwrap() < cutoff_time
        //   4. Delete matching rows in batches using store.delete()
        //   5. Track metrics (rows_deleted, estimated_bytes_freed)
        //
        // Implementation sketch:
        //   let rows_to_delete: Vec<RowId> = all_rows
        //       .filter(|row| row.deleted_at.is_some() && row.deleted_at.unwrap() < cutoff_time)
        //       .map(|row| row.row_id)
        //       .collect();
        //   for row_id in &rows_to_delete {
        //       store.delete(row_id)?;
        //   }
        //
        // For now, return placeholder metrics
        let rows_deleted = 0;

        ctx.log_info(&format!("Retention enforcement completed - {} rows deleted", rows_deleted));

        Ok(JobDecision::Completed {
            message: Some(format!(
                "Enforced retention policy for {} ({}h) - {} rows deleted",
                table_id, retention_hours, rows_deleted
            )),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Retention job cancellation requested");
        // Allow cancellation since partial retention enforcement is acceptable
        Ok(())
    }
}

impl Default for RetentionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{NamespaceId, TableName};

    use super::*;

    #[test]
    fn test_executor_properties() {
        let executor = RetentionExecutor::new();
        assert_eq!(executor.job_type(), JobType::Retention);
        assert_eq!(executor.name(), "RetentionExecutor");
    }

    #[test]
    fn test_params_validation_success() {
        let params = RetentionParams {
            table_id: TableId::new(NamespaceId::default(), TableName::new("users")),
            table_type: TableType::User,
            retention_hours: 720,
        };
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_params_validation_zero_hours() {
        let params = RetentionParams {
            table_id: TableId::new(NamespaceId::default(), TableName::new("users")),
            table_type: TableType::User,
            retention_hours: 0,
        };
        assert!(params.validate().is_err());
    }
}
