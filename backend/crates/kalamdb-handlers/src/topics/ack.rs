//! ACK statement handler for committing consumer group offsets

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use datafusion::arrow::{
    array::{ArrayRef, Int32Array, Int64Array, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use kalamdb_sql::ddl::AckStatement;
use std::sync::Arc;

/// Handler for ACK statements
///
/// Commits consumer group offset for a topic partition.
/// Returns acknowledgment status.
pub struct AckHandler {
    app_context: Arc<AppContext>,
}

impl AckHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<AckStatement> for AckHandler {
    async fn execute(
        &self,
        statement: AckStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = TopicId::new(&statement.topic_name);
        let group_id = ConsumerGroupId::new(&statement.group_id);

        // Verify topic exists
        let topics_provider = self.app_context.system_tables().topics();
        let _topic = topics_provider.get_topic_by_id_async(&topic_id).await?.ok_or_else(|| {
            KalamDbError::NotFound(format!("Topic '{}' does not exist", statement.topic_name))
        })?;

        // Commit the offset
        let topic_publisher = self.app_context.topic_publisher();
        topic_publisher
            .ack_offset(&topic_id, &group_id, statement.partition_id, statement.upto_offset)
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to commit offset: {}", e))
            })?;

        // Build result showing what was acknowledged
        let schema = Arc::new(Schema::new(vec![
            Field::new("topic", DataType::Utf8, false),
            Field::new("group_id", DataType::Utf8, false),
            Field::new("partition", DataType::Int32, false),
            Field::new("committed_offset", DataType::Int64, false),
        ]));

        let mut topic_builder = StringBuilder::new();
        let mut group_builder = StringBuilder::new();

        topic_builder.append_value(&statement.topic_name);
        group_builder.append_value(&statement.group_id);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(topic_builder.finish()) as ArrayRef,
                Arc::new(group_builder.finish()) as ArrayRef,
                Arc::new(Int32Array::from(vec![statement.partition_id as i32])) as ArrayRef,
                Arc::new(Int64Array::from(vec![statement.upto_offset as i64])) as ArrayRef,
            ],
        )
        .map_err(|e| {
            KalamDbError::SerializationError(format!("Failed to create RecordBatch: {}", e))
        })?;

        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count: 1,
            schema: Some(schema),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &AckStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        // Only Service, DBA, and System roles can acknowledge topic offsets
        // User role is restricted to prevent unauthorized offset manipulation
        match context.user_role() {
            Role::Service | Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "Only service, dba, or system roles can acknowledge topic offsets".to_string(),
            )),
        }
    }
}
