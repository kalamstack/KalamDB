use kalamdb_commons::models::{NamespaceId, TopicId};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::CreateTopicStatement;
use kalamdb_system::providers::topics::models::Topic;
use std::sync::Arc;

pub struct CreateTopicHandler {
    app_context: Arc<AppContext>,
}

impl CreateTopicHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn extract_namespace_id(topic_name: &str) -> Result<NamespaceId, KalamDbError> {
        let (namespace, topic_local_name) = topic_name.split_once('.').ok_or_else(|| {
            KalamDbError::InvalidOperation(
                "Topic name must be namespace-qualified: <namespace>.<topic>".to_string(),
            )
        })?;

        if namespace.is_empty() || topic_local_name.is_empty() {
            return Err(KalamDbError::InvalidOperation(
                "Topic name must be namespace-qualified: <namespace>.<topic>".to_string(),
            ));
        }

        Ok(NamespaceId::new(namespace))
    }
}

impl TypedStatementHandler<CreateTopicStatement> for CreateTopicHandler {
    async fn execute(
        &self,
        statement: CreateTopicStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let namespace_id = Self::extract_namespace_id(&statement.topic_name)?;
        let namespaces_provider = self.app_context.system_tables().namespaces();
        if namespaces_provider.get_namespace_async(&namespace_id).await?.is_none() {
            return Err(KalamDbError::NotFound(format!(
                "Namespace '{}' does not exist",
                namespace_id
            )));
        }

        let topic_id = TopicId::new(&statement.topic_name);
        let topics_provider = self.app_context.system_tables().topics();
        if topics_provider.get_topic_by_id_async(&topic_id).await?.is_some() {
            return Err(KalamDbError::AlreadyExists(format!(
                "Topic '{}' already exists",
                statement.topic_name
            )));
        }

        let mut topic = Topic::new(topic_id.clone(), statement.topic_name.clone());
        topic.partitions = statement.partitions.unwrap_or(1);
        topic.retention_seconds = Some(7 * 24 * 60 * 60);
        topic.retention_max_bytes = Some(1024 * 1024 * 1024);

        topics_provider.create_topic_async(topic.clone()).await?;
        self.app_context.topic_publisher().add_topic(topic.clone());

        Ok(ExecutionResult::Success {
            message: format!(
                "Created topic '{}' with {} partition(s)",
                statement.topic_name, topic.partitions
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CreateTopicStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        match context.user_role() {
            Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "CREATE TOPIC requires DBA or System role".to_string(),
            )),
        }
    }
}