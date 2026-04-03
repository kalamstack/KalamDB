use kalamdb_commons::models::TopicId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::AddTopicSourceStatement;
use kalamdb_system::providers::topics::models::TopicRoute;
use std::sync::Arc;

pub struct AddTopicSourceHandler {
    app_context: Arc<AppContext>,
}

impl AddTopicSourceHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<AddTopicSourceStatement> for AddTopicSourceHandler {
    async fn execute(
        &self,
        statement: AddTopicSourceStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = TopicId::new(&statement.topic_name);
        let topics_provider = self.app_context.system_tables().topics();

        let mut topic = topics_provider.get_topic_by_id_async(&topic_id).await?.ok_or_else(|| {
            KalamDbError::NotFound(format!("Topic '{}' does not exist", statement.topic_name))
        })?;

        let route = TopicRoute {
            table_id: statement.table_id.clone(),
            op: statement.operation,
            payload_mode: statement.payload_mode,
            filter_expr: statement.filter_expr.clone(),
            partition_key_expr: None,
        };

        let duplicate = topic
            .routes
            .iter()
            .any(|existing| existing.table_id == route.table_id && existing.op == route.op);
        if duplicate {
            return Err(KalamDbError::AlreadyExists(format!(
                "Route for {}.{} ON {:?} already exists in topic '{}'",
                route.table_id.namespace_id(),
                route.table_id.table_name(),
                route.op,
                statement.topic_name
            )));
        }

        topic.routes.push(route);
        topic.updated_at = chrono::Utc::now().timestamp_millis();
        topics_provider.update_topic_async(topic.clone()).await?;
        self.app_context.topic_publisher().update_topic(topic);

        Ok(ExecutionResult::Success {
            message: format!(
                "Added source {}.{} ON {:?} to topic '{}'",
                statement.table_id.namespace_id(),
                statement.table_id.table_name(),
                statement.operation,
                statement.topic_name
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &AddTopicSourceStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        match context.user_role() {
            Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "ALTER TOPIC requires DBA or System role".to_string(),
            )),
        }
    }
}