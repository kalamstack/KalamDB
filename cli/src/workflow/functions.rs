//! `kalam functions` build/status/rollback/logs.

use crate::{
    error::{CLIError, Result},
    workflow::{
        generate_schema,
        sql::{build_workflow_client, execute_single_statement},
        WorkflowContext,
    },
};

pub async fn build_functions(ctx: &WorkflowContext) -> Result<()> {
    generate_schema(ctx, None)
}

pub async fn show_function_status(ctx: &WorkflowContext) -> Result<()> {
    let output = ctx.output();
    let env = ctx.resolved_environment()?;
    let client = build_workflow_client(ctx, &env)?;
    output.status("listing system.routines");
    execute_single_statement(
        &client,
        "SELECT routine_id, language, security FROM system.routines ORDER BY routine_id",
        Some(env.namespace.as_str()),
        "functions status",
    )
    .await
}

pub fn rollback_function(_ctx: &WorkflowContext, revision: &str) -> Result<()> {
    Err(CLIError::ConfigurationError(format!(
        "function rollback of revision {revision} is done with CREATE OR REPLACE PROCEDURE using \
         the previous body; catalog history is in system.function_revisions"
    )))
}

pub async fn show_function_logs(ctx: &WorkflowContext, procedure: Option<&str>) -> Result<()> {
    let output = ctx.output();
    let env = ctx.resolved_environment()?;
    let client = build_workflow_client(ctx, &env)?;
    let sql = match procedure {
        Some(name) => format!(
            "SELECT trigger_id, status, attempt, error FROM system.trigger_attempts WHERE \
             trigger_id LIKE '%{name}%' ORDER BY updated_at DESC LIMIT 50"
        ),
        None => "SELECT trigger_id, status, attempt, error FROM system.trigger_attempts ORDER BY \
                 updated_at DESC LIMIT 50"
            .to_string(),
    };
    output.status("listing system.trigger_attempts");
    execute_single_statement(&client, &sql, Some(env.namespace.as_str()), "functions logs").await
}
