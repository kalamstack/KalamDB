//! SQL execution helpers

use kalamdb_commons::models::UserId;
use kalamdb_commons::Role;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::executor::{PreparedExecutionStatement, ScalarValue, SqlExecutor};
use kalamdb_core::sql::ExecutionResult;
use std::sync::Arc;

use super::super::models::QueryResult;
use super::converter::record_batch_to_query_result;

pub async fn execute_single_statement_raw(
    metadata: &PreparedExecutionStatement,
    sql_executor: &Arc<SqlExecutor>,
    exec_ctx: &ExecutionContext,
    execute_as_user: Option<UserId>,
    params: Vec<ScalarValue>,
) -> Result<ExecutionResult, Box<dyn std::error::Error>> {
    let exec_result = if let Some(user_id) = execute_as_user {
        let effective_ctx = exec_ctx.with_effective_identity(user_id, Role::User);
        sql_executor.execute_with_metadata(metadata, &effective_ctx, params).await
    } else {
        sql_executor.execute_with_metadata(metadata, exec_ctx, params).await
    };

    exec_result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

pub fn execution_result_to_query_result(
    exec_result: ExecutionResult,
    user_role: Option<Role>,
) -> Result<QueryResult, Box<dyn std::error::Error>> {
    match exec_result {
        ExecutionResult::Success { message } => Ok(QueryResult::with_message(message)),
        ExecutionResult::Rows {
            batches, schema, ..
        } => record_batch_to_query_result(batches, schema, user_role),
        ExecutionResult::Inserted { rows_affected } => Ok(QueryResult::with_affected_rows(
            rows_affected,
            Some(format!("Inserted {} row(s)", rows_affected)),
        )),
        ExecutionResult::Updated { rows_affected } => Ok(QueryResult::with_affected_rows(
            rows_affected,
            Some(format!("Updated {} row(s)", rows_affected)),
        )),
        ExecutionResult::Deleted { rows_affected } => Ok(QueryResult::with_affected_rows(
            rows_affected,
            Some(format!("Deleted {} row(s)", rows_affected)),
        )),
        ExecutionResult::Flushed {
            tables,
            bytes_written,
        } => Ok(QueryResult::with_affected_rows(
            tables.len(),
            Some(format!("Flushed {} table(s), {} bytes written", tables.len(), bytes_written)),
        )),
        ExecutionResult::Subscription {
            subscription_id,
            channel,
            select_query,
        } => {
            let sub_data = serde_json::json!({
                "status": "active",
                "ws_url": channel,
                "subscription": {
                    "id": subscription_id,
                    "sql": select_query
                },
                "message": "WebSocket subscription created. Connect to ws_url to receive updates."
            });
            Ok(QueryResult::subscription(sub_data))
        },
        ExecutionResult::JobKilled { job_id, status } => {
            Ok(QueryResult::with_message(format!("Job {} killed: {}", job_id, status)))
        },
    }
}

/// Execute a single SQL statement
pub async fn execute_single_statement(
    metadata: &PreparedExecutionStatement,
    _app_context: &Arc<kalamdb_core::app_context::AppContext>,
    sql_executor: &Arc<SqlExecutor>,
    exec_ctx: &ExecutionContext,
    execute_as_user: Option<UserId>,
    params: Vec<ScalarValue>,
) -> Result<QueryResult, Box<dyn std::error::Error>> {
    let user_role = if execute_as_user.is_some() {
        Some(Role::User)
    } else {
        Some(exec_ctx.user_role())
    };
    let exec_result =
        execute_single_statement_raw(metadata, sql_executor, exec_ctx, execute_as_user, params)
            .await?;
    execution_result_to_query_result(exec_result, user_role)
}
