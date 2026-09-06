//! Typed handler for CREATE TRIGGER.

use std::sync::Arc;

use kalamdb_commons::models::{ConsumerGroupId, UserId};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::CreateTriggerStatement;
use kalamdb_system::CatalogTrigger;

use crate::helpers::{async_blocking::run_blocking, guards::require_admin};

pub struct CreateTriggerHandler {
    app_context: Arc<AppContext>,
}

impl CreateTriggerHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<CreateTriggerStatement> for CreateTriggerHandler {
    async fn execute(
        &self,
        statement: CreateTriggerStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        require_admin(context, "create trigger")?;
        let app = Arc::clone(&self.app_context);
        let session_user = context.user_id().clone();
        run_blocking(move || persist_create_trigger(&app, statement, session_user)).await
    }
}

fn persist_create_trigger(
    app: &AppContext,
    statement: CreateTriggerStatement,
    session_user: UserId,
) -> Result<ExecutionResult, KalamDbError> {
    let stores = app.system_tables().catalog_stores();
    if stores
        .get_trigger(&statement.trigger_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .is_some()
    {
        return Err(KalamDbError::AlreadyExists(format!(
            "trigger {} already exists",
            statement.trigger_id
        )));
    }
    if stores
        .get_routine(&statement.routine_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .is_none()
    {
        return Err(KalamDbError::NotFound(format!(
            "procedure {} not found",
            statement.routine_id
        )));
    }
    if !app.topic_publisher().topic_exists(&statement.topic_id) {
        return Err(KalamDbError::NotFound(format!("topic {} not found", statement.topic_id)));
    }

    let principal_user_id = resolve_principal(app, &statement.principal, session_user)?;
    seed_trigger_offsets(
        app,
        statement.trigger_id.as_str(),
        &statement.topic_id,
        &statement.start_from,
    )?;
    stores
        .upsert_trigger(CatalogTrigger {
            trigger_id: statement.trigger_id.clone(),
            namespace_id: statement.namespace_id,
            name: statement.name,
            topic_id: statement.topic_id,
            routine_id: statement.routine_id,
            principal_user_id,
            start_from: statement.start_from,
            retries: statement.retries,
            retry_backoff_ms: statement.retry_backoff_ms,
            concurrency: statement.concurrency,
            enabled: true,
        })
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    Ok(ExecutionResult::Success {
        message: format!("Trigger {} created", statement.trigger_id),
    })
}

fn seed_trigger_offsets(
    app: &AppContext,
    trigger_id: &str,
    topic_id: &kalamdb_commons::models::TopicId,
    start_from: &str,
) -> Result<(), KalamDbError> {
    let publisher = app.topic_publisher();
    let group_id = ConsumerGroupId::new(format!("trigger:{trigger_id}"));
    let partitions = app
        .system_tables()
        .topics()
        .get_topic_by_id(topic_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .map(|topic| topic.partitions)
        .unwrap_or(1);
    for partition_id in 0..partitions {
        let next = if start_from == "earliest" {
            publisher
                .earliest_available_offset(topic_id, partition_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        } else {
            publisher
                .latest_offset(topic_id, partition_id)
                .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
                .map(|last| last.saturating_add(1))
                .unwrap_or(0)
        };
        publisher
            .reset_group_offset(topic_id, &group_id, partition_id, next)
            .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?;
    }
    Ok(())
}

fn resolve_principal(
    app: &AppContext,
    principal: &str,
    session_user: UserId,
) -> Result<UserId, KalamDbError> {
    if principal.is_empty() {
        return Ok(session_user);
    }
    let by_id = UserId::new(principal);
    if by_id == UserId::system() {
        return Ok(by_id);
    }
    let users = app.system_tables().users();
    if users
        .get_user_by_id(&by_id)
        .map_err(|error| KalamDbError::ExecutionError(error.to_string()))?
        .is_some()
    {
        return Ok(by_id);
    }
    Err(KalamDbError::NotFound(format!("trigger principal '{principal}' not found")))
}
