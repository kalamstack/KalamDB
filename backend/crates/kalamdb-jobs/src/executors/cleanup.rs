//! Cleanup Job Executor
//!
//! **Phase 9 (T147)**: JobExecutor implementation for cleanup operations
//!
//! Handles cleanup of deleted table data and metadata.
//!
//! ## Responsibilities
//! - Clean up soft-deleted table data
//! - Remove orphaned Parquet files
//! - Clean up metadata from system tables
//! - Track cleanup metrics (files deleted, bytes freed)
//!
//! ## Parameters Format
//! ```json
//! {
//!   "table_id": "default:users",
//!   "table_type": "User",
//!   "operation": "drop_table"
//! }
//! ```

use kalamdb_core::error::KalamDbError;
use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use kalamdb_core::operations::table_cleanup::{
    cleanup_metadata_internal, cleanup_parquet_files_internal, cleanup_table_data_internal,
};
use async_trait::async_trait;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::TableId;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Re-export so consumers can keep importing from this module.
pub use kalamdb_core::operations::table_cleanup::{CleanupOperation, StorageCleanupDetails};

/// Typed parameters for cleanup operations (T191)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupParams {
    /// Table identifier (required)
    pub table_id: TableId,
    /// Table type (required)
    pub table_type: TableType,
    /// Cleanup operation (required)
    pub operation: CleanupOperation,
    /// Storage cleanup details captured at DROP time
    pub storage: StorageCleanupDetails,
}

impl JobParams for CleanupParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        // TableId and TableType validation is handled by their constructors
        Ok(())
    }
}

/// Cleanup Job Executor
///
/// Executes cleanup operations for deleted tables and orphaned data.
pub struct CleanupExecutor;

impl CleanupExecutor {
    /// Create a new CleanupExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for CleanupExecutor {
    type Params = CleanupParams;

    fn job_type(&self) -> JobType {
        JobType::Cleanup
    }

    fn name(&self) -> &'static str {
        "CleanupExecutor"
    }

    async fn execute(&self, _ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        // Cleanup is leader-only work, so execute_local (called via execute) does nothing.
        // All actual cleanup work happens in execute_leader.
        Ok(JobDecision::Completed {
            message: Some("No local work required for cleanup".to_string()),
        })
    }

    async fn execute_leader(
        &self,
        ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        ctx.log_debug("Starting cleanup operation (leader phase)");

        // Parameters already validated in JobContext - type-safe access
        let params = ctx.params();
        let table_id = Arc::new(params.table_id.clone());
        let table_type = params.table_type;
        let operation = params.operation;

        ctx.log_debug(&format!(
            "Cleaning up table {} (operation: {:?}, type: {:?})",
            table_id, operation, table_type
        ));

        // Execute cleanup in 3 phases:
        // 1. Clean up table data (rows) from RocksDB stores
        let rows_deleted = cleanup_table_data_internal(&ctx.app_ctx, &table_id, table_type).await?;

        ctx.log_debug(&format!("Cleaned up {} rows from table data", rows_deleted));

        // 2. Clean up Parquet files from storage backend
        let bytes_freed =
            cleanup_parquet_files_internal(&ctx.app_ctx, &table_id, table_type, &params.storage)
                .await?;

        ctx.log_debug(&format!("Freed {} bytes from Parquet files", bytes_freed));

        // 3. Invalidate manifest cache (L1 hot cache + L2 RocksDB)
        let manifest_service = ctx.app_ctx.manifest_service();
        let cache_entries_invalidated =
            manifest_service.invalidate_table(&table_id).map_err(|e| {
                KalamDbError::Other(format!("Failed to invalidate manifest cache: {}", e))
            })?;

        ctx.log_debug(&format!("Invalidated {} manifest cache entries", cache_entries_invalidated));

        // 4. Clean up metadata from SchemaRegistry
        let schema_registry = ctx.app_ctx.schema_registry();
        cleanup_metadata_internal(ctx.app_ctx.as_ref(), &schema_registry, &table_id).await?;

        ctx.log_debug("Removed table metadata from SchemaRegistry");

        // Build success message with metrics
        let message = format!(
            "Cleaned up table {} successfully - {} rows deleted, {} bytes freed, {} cache entries invalidated",
            table_id, rows_deleted, bytes_freed, cache_entries_invalidated
        );

        ctx.log_debug(&message);

        Ok(JobDecision::Completed {
            message: Some(message),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Cleanup job cancellation requested");
        // Cleanup jobs should complete to avoid orphaned data
        Err(KalamDbError::InvalidOperation(
            "Cleanup jobs cannot be safely cancelled".to_string(),
        ))
    }
}

impl Default for CleanupExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::{NamespaceId, TableName};
    use kalamdb_commons::StorageId;

    #[test]
    fn test_executor_properties() {
        let executor = CleanupExecutor::new();
        assert_eq!(executor.job_type(), JobType::Cleanup);
        assert_eq!(executor.name(), "CleanupExecutor");
    }

    #[test]
    fn test_default_constructor() {
        let executor = CleanupExecutor;
        assert_eq!(executor.job_type(), JobType::Cleanup);
    }

    #[test]
    fn test_params_validation() {
        let params = CleanupParams {
            table_id: TableId::new(NamespaceId::default(), TableName::new("users")),
            table_type: TableType::User,
            operation: CleanupOperation::DropTable,
            storage: StorageCleanupDetails {
                storage_id: StorageId::local(),
                base_directory: "/tmp".to_string(),
                relative_path_template: "users/{userId}".to_string(),
            },
        };
        assert!(params.validate().is_ok());
    }
}
