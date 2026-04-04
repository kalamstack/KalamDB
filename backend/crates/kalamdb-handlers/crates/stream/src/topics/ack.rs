use crate::result_rows;
use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::AckStatement;
use std::sync::Arc;

pub struct AckHandler {
    app_context: Arc<AppContext>,
}

impl AckHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<AckStatement> for AckHandler {
    async fn execute(
        &self,
        statement: AckStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = TopicId::new(&statement.topic_name);
        let group_id = ConsumerGroupId::new(&statement.group_id);

        let topics_provider = self.app_context.system_tables().topics();
        let _topic = topics_provider.get_topic_by_id_async(&topic_id).await?.ok_or_else(|| {
            KalamDbError::NotFound(format!("Topic '{}' does not exist", statement.topic_name))
        })?;

        self.app_context
            .topic_publisher()
            .ack_offset(&topic_id, &group_id, statement.partition_id, statement.upto_offset)
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to commit offset: {}", e))
            })?;

        result_rows::ack_result(
            &statement.topic_name,
            &statement.group_id,
            statement.partition_id,
            statement.upto_offset,
        )
    }

    async fn check_authorization(
        &self,
        _statement: &AckStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        match context.user_role() {
            Role::Service | Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "Only service, dba, or system roles can acknowledge topic offsets".to_string(),
            )),
        }
    }
}
