//! CLUSTER TRANSFER-LEADER handler
//!
//! Attempts to transfer leadership for all Raft groups to the specified node.

use kalamdb_commons::models::NodeId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::executor::handlers::{
    ExecutionContext, ExecutionResult, ScalarValue, StatementHandler,
};
use kalamdb_raft::RaftExecutor;
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind};
use std::sync::Arc;

pub struct ClusterTransferLeaderHandler {
    app_context: Arc<AppContext>,
}

impl ClusterTransferLeaderHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl StatementHandler for ClusterTransferLeaderHandler {
    async fn execute(
        &self,
        statement: SqlStatement,
        _params: Vec<ScalarValue>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let SqlStatementKind::ClusterTransferLeader(node_id) = statement.kind() else {
            return Err(KalamDbError::InvalidOperation(format!(
                "CLUSTER TRANSFER-LEADER handler received wrong statement type: {}",
                statement.name()
            )));
        };

        log::info!(
            "CLUSTER TRANSFER-LEADER initiated by user: {} (target={})",
            ctx.user_id(),
            node_id
        );

        let executor = self.app_context.executor();
        let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() else {
            return Err(KalamDbError::InvalidOperation(
                "CLUSTER TRANSFER-LEADER requires cluster mode (Raft executor not available)"
                    .to_string(),
            ));
        };

        let manager = raft_executor.manager();
        let results =
            manager.transfer_leadership_all(NodeId::from(*node_id)).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to transfer leadership: {}", e))
            })?;

        let success_count = results.iter().filter(|r| r.success).count();
        let total_count = results.len();
        let failed_count = total_count - success_count;

        let mut message = if success_count == 0 && failed_count == total_count {
            format!(
                "CLUSTER TRANSFER-LEADER is unsupported in the current OpenRaft version (target={}).",
                node_id
            )
        } else {
            format!(
                "Cluster transfer-leader completed: {}/{} groups requested (target={})",
                success_count, total_count, node_id
            )
        };

        if failed_count > 0 {
            message.push_str("\n\nFailed groups:");
            for result in results.iter().filter(|r| !r.success) {
                if let Some(ref err) = result.error {
                    message.push_str(&format!("\n  - {:?}: {}", result.group_id, err));
                }
            }
        }

        Ok(ExecutionResult::Success { message })
    }
}
