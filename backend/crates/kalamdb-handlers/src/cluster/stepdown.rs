//! CLUSTER STEPDOWN handler
//!
//! Attempts to step down leaders for all Raft groups.

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::executor::handlers::{
    ExecutionContext, ExecutionResult, ScalarValue, StatementHandler,
};
use kalamdb_raft::RaftExecutor;
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind};
use std::sync::Arc;

pub struct ClusterStepdownHandler {
    app_context: Arc<AppContext>,
}

impl ClusterStepdownHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl StatementHandler for ClusterStepdownHandler {
    async fn execute(
        &self,
        statement: SqlStatement,
        _params: Vec<ScalarValue>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        if !matches!(statement.kind(), SqlStatementKind::ClusterStepdown) {
            return Err(KalamDbError::InvalidOperation(format!(
                "CLUSTER STEPDOWN handler received wrong statement type: {}",
                statement.name()
            )));
        }

        log::info!("CLUSTER STEPDOWN initiated by user: {}", ctx.user_id());

        let executor = self.app_context.executor();
        let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() else {
            return Err(KalamDbError::InvalidOperation(
                "CLUSTER STEPDOWN requires cluster mode (Raft executor not available)".to_string(),
            ));
        };

        let manager = raft_executor.manager();
        let results = manager.step_down_all().await.map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to step down leaders: {}", e))
        })?;

        let success_count = results.iter().filter(|r| r.success).count();
        let total_count = results.len();
        let failed_count = total_count - success_count;

        let mut message = format!(
            "Cluster stepdown completed: {}/{} groups requested",
            success_count, total_count
        );

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
