//! CLUSTER SNAPSHOT handler
//!
//! Forces all Raft logs to be written to snapshots across the cluster

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::executor::handlers::{ExecutionContext, ExecutionResult, ScalarValue, StatementHandler},
};
use kalamdb_raft::RaftExecutor;
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind};

pub struct ClusterSnapshotHandler {
    app_context: Arc<AppContext>,
}

impl ClusterSnapshotHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl StatementHandler for ClusterSnapshotHandler {
    async fn execute(
        &self,
        statement: SqlStatement,
        _params: Vec<ScalarValue>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        if !matches!(statement.kind(), SqlStatementKind::ClusterSnapshot) {
            return Err(KalamDbError::InvalidOperation(format!(
                "CLUSTER SNAPSHOT handler received wrong statement type: {}",
                statement.name()
            )));
        }

        log::info!("CLUSTER SNAPSHOT initiated by user: {}", ctx.user_id());

        // Get the RaftExecutor to trigger snapshots
        let executor = self.app_context.executor();
        let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() else {
            return Err(KalamDbError::InvalidOperation(
                "CLUSTER SNAPSHOT requires cluster mode (Raft executor not available)".to_string(),
            ));
        };

        let manager = raft_executor.manager();

        // Trigger snapshots for all groups
        let results = manager.trigger_all_snapshots().await.map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to trigger snapshots: {}", e))
        })?;

        // Build response message
        let success_count = results.iter().filter(|r| r.success).count();
        let total_count = results.len();
        let failed_count = total_count - success_count;

        // Get snapshot directory from config
        let config = self.app_context.config();
        let snapshots_dir = config.storage.resolved_snapshots_dir();

        // Build detailed message
        let mut message = format!(
            "Cluster snapshot completed: {}/{} snapshots triggered successfully\nSnapshots \
             directory: {}",
            success_count,
            total_count,
            snapshots_dir.display()
        );

        if failed_count > 0 {
            message.push_str("\n\nFailed groups:");
            for result in results.iter().filter(|r| !r.success) {
                if let Some(ref err) = result.error {
                    message.push_str(&format!("\n  - {:?}: {}", result.group_id, err));
                }
            }
        }

        // Add snapshot index info for successfully triggered groups
        let snapshots_with_idx: Vec<_> =
            results.iter().filter(|r| r.success && r.snapshot_index.is_some()).collect();

        if !snapshots_with_idx.is_empty() {
            message.push_str("\n\nSnapshot indices:");
            for result in snapshots_with_idx.iter().take(5) {
                if let Some(idx) = result.snapshot_index {
                    message.push_str(&format!("\n  - {:?}: index {}", result.group_id, idx));
                }
            }
            if snapshots_with_idx.len() > 5 {
                message
                    .push_str(&format!("\n  ... and {} more groups", snapshots_with_idx.len() - 5));
            }
        }

        log::info!("CLUSTER SNAPSHOT completed: {}/{} snapshots", success_count, total_count);

        Ok(ExecutionResult::Success { message })
    }
}
