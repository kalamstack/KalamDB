use kalamdb_commons::models::TopicId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::DropTopicStatement;
use std::sync::Arc;

pub struct DropTopicHandler {
    app_context: Arc<AppContext>,
}

impl DropTopicHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<DropTopicStatement> for DropTopicHandler {
    async fn execute(
        &self,
        statement: DropTopicStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = TopicId::new(&statement.topic_name);
        let topics_provider = self.app_context.system_tables().topics();
        let topic = topics_provider.get_topic_by_id_async(&topic_id).await?;

        if topic.is_none() {
            return Err(KalamDbError::NotFound(format!(
                "Topic '{}' does not exist",
                statement.topic_name
            )));
        }

        let topic_name = topic.expect("checked is_some").name;
        topics_provider.delete_topic_async(&topic_id).await?;
        self.app_context.topic_publisher().remove_topic(&topic_id);

        use kalamdb_jobs::executors::topic_cleanup::TopicCleanupParams;
        use kalamdb_system::JobType;

        let cleanup_params = TopicCleanupParams {
            topic_id: topic_id.clone(),
            topic_name: topic_name.clone(),
        };
        let params_json = serde_json::to_value(&cleanup_params).map_err(|e| {
            KalamDbError::SerializationError(format!("Failed to serialize job params: {}", e))
        })?;

        let job_id = self
            .app_context
            .job_manager()
            .create_job(
                JobType::TopicCleanup,
                params_json,
                Some(format!("drop_topic:{}", topic_id.as_str())),
                None,
            )
            .await?;

        log::info!("Dropped topic '{}' and scheduled cleanup job [{}]", topic_name, job_id);

        Ok(ExecutionResult::Success {
            message: format!(
                "Dropped topic '{}' and scheduled cleanup job [{}]",
                topic_name, job_id
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &DropTopicStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        match context.user_role() {
            Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "DROP TOPIC requires DBA or System role".to_string(),
            )),
        }
    }
}
