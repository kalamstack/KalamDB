use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::ClearTopicStatement;
use std::sync::Arc;

pub struct ClearTopicHandler {
    app_context: Arc<AppContext>,
}

impl ClearTopicHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<ClearTopicStatement> for ClearTopicHandler {
    async fn execute(
        &self,
        statement: ClearTopicStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = &statement.topic_id;
        let topics_provider = self.app_context.system_tables().topics();
        let topic = topics_provider.get_topic_by_id_async(topic_id).await?;

        if topic.is_none() {
            return Err(KalamDbError::NotFound(format!(
                "Topic '{}' does not exist",
                topic_id.as_str()
            )));
        }

        let topic_name = topic.expect("checked is_some").name;

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
                Some(format!("clear_topic:{}", topic_id.as_str())),
                None,
            )
            .await?;

        log::info!(
            "Scheduled topic cleanup job [{}] for topic '{}' ({})",
            job_id,
            topic_name,
            topic_id.as_str()
        );

        Ok(ExecutionResult::Success {
            message: format!("Scheduled cleanup job [{}] for topic '{}'", job_id, topic_name),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &ClearTopicStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        match context.user_role() {
            Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "CLEAR TOPIC requires DBA or System role".to_string(),
            )),
        }
    }
}
