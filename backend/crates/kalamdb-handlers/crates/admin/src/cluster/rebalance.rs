//! CLUSTER REBALANCE handler
//!
//! Requests best-effort data-group leader redistribution.

use std::sync::Arc;

use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::executor::handlers::{ExecutionContext, ExecutionResult, ScalarValue, StatementHandler},
};
use kalamdb_raft::RaftExecutor;
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind};

pub struct ClusterRebalanceHandler {
    app_context: Arc<AppContext>,
}

impl ClusterRebalanceHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl StatementHandler for ClusterRebalanceHandler {
    async fn execute(
        &self,
        statement: SqlStatement,
        _params: Vec<ScalarValue>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        if !matches!(statement.kind(), SqlStatementKind::ClusterRebalance) {
            return Err(KalamDbError::InvalidOperation(format!(
                "CLUSTER REBALANCE handler received wrong statement type: {}",
                statement.name()
            )));
        }

        log::info!("CLUSTER REBALANCE initiated by user: {}", ctx.user_id());

        let executor = self.app_context.executor();
        let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() else {
            return Err(KalamDbError::InvalidOperation(
                "CLUSTER REBALANCE requires cluster mode (Raft executor not available)".to_string(),
            ));
        };

        let results =
            raft_executor.manager().rebalance_data_leaders().await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to rebalance: {}", e))
            })?;

        let success_count = results.iter().filter(|result| result.success).count();
        let total_count = results.len();
        let failed_count = total_count - success_count;

        let mut message = format!(
            "Cluster rebalance requested for {}/{} data groups",
            success_count, total_count
        );

        if failed_count > 0 {
            message.push_str("\n\nFailed groups:");
            for result in results.iter().filter(|result| !result.success) {
                if let Some(ref err) = result.error {
                    message.push_str(&format!("\n  - {:?}: {}", result.group_id, err));
                }
            }
        }

        Ok(ExecutionResult::Success { message })
    }
}
