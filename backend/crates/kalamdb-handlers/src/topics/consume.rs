//! CONSUME FROM handler

use datafusion::arrow::{
    array::{ArrayRef, BinaryBuilder, Int32Array, Int64Array, StringBuilder},
    record_batch::RecordBatch,
};
use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::{ConsumePosition, ConsumeStatement};
use kalamdb_tables::topics::topic_message_schema::topic_message_schema;
use std::sync::Arc;

/// Handler for CONSUME FROM statements
pub struct ConsumeHandler {
    app_context: Arc<AppContext>,
}

impl ConsumeHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<ConsumeStatement> for ConsumeHandler {
    async fn execute(
        &self,
        statement: ConsumeStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = TopicId::new(&statement.topic_name);

        // Verify topic exists
        let topics_provider = self.app_context.system_tables().topics();
        let topic = topics_provider.get_topic_by_id_async(&topic_id).await?.ok_or_else(|| {
            KalamDbError::NotFound(format!("Topic '{}' does not exist", statement.topic_name))
        })?;

        let topic_publisher = self.app_context.topic_publisher();
        let limit = statement.limit.unwrap_or(100) as usize;

        // Default to partition 0 for now (partition selection can be enhanced later)
        let partition_id = 0u32;

        // Determine start offset based on position and group
        let start_offset = match (&statement.position, &statement.group_id) {
            // Explicit offset always wins
            (ConsumePosition::Offset(o), _) => *o,
            // Earliest means from beginning
            (ConsumePosition::Earliest, None) => 0,
            // Latest means only new messages (use high-water mark)
            (ConsumePosition::Latest, _) => topic_publisher
                .latest_offset(&topic_id, partition_id)
                .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?
                .map(|offset| offset + 1)
                .unwrap_or(0),
            // Earliest with group defaults to the beginning when no committed offset exists.
            // Group-aware claiming is enforced by `fetch_messages_for_group`.
            (ConsumePosition::Earliest, Some(_)) => 0,
        };

        // Fetch messages from topic publisher
        let messages = if let Some(group_name) = &statement.group_id {
            let group_id = ConsumerGroupId::new(group_name);
            topic_publisher
                .fetch_messages_for_group(&topic_id, &group_id, partition_id, start_offset, limit)
                .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?
        } else {
            topic_publisher
                .fetch_messages(&topic_id, partition_id, start_offset, limit)
                .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?
        };

        // Convert messages to RecordBatch using cached schema
        let schema = topic_message_schema();

        let num_messages = messages.len();
        let mut topics_builder = StringBuilder::new();
        let mut partitions = Vec::with_capacity(num_messages);
        let mut offsets = Vec::with_capacity(num_messages);
        let mut keys_builder = StringBuilder::new();
        let mut payloads_builder = BinaryBuilder::new();
        let mut timestamps = Vec::with_capacity(num_messages);
        let mut ops_builder = StringBuilder::new();

        for msg in &messages {
            topics_builder.append_value(topic.topic_id.as_str());
            partitions.push(msg.partition_id as i32);
            offsets.push(msg.offset as i64);

            match &msg.key {
                Some(k) => keys_builder.append_value(k),
                None => keys_builder.append_null(),
            }

            payloads_builder.append_value(&msg.payload);
            timestamps.push(msg.timestamp_ms);
            ops_builder.append_value(msg.op.as_str());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(topics_builder.finish()) as ArrayRef,
                Arc::new(Int32Array::from(partitions)) as ArrayRef,
                Arc::new(Int64Array::from(offsets)) as ArrayRef,
                Arc::new(keys_builder.finish()) as ArrayRef,
                Arc::new(payloads_builder.finish()) as ArrayRef,
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(ops_builder.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            KalamDbError::SerializationError(format!("Failed to create RecordBatch: {}", e))
        })?;

        // Auto-commit offset if using consumer group
        if let Some(group_name) = &statement.group_id {
            if let Some(last_msg) = messages.last() {
                let group_id = ConsumerGroupId::new(group_name);
                topic_publisher
                    .ack_offset(&topic_id, &group_id, partition_id, last_msg.offset)
                    .map_err(|e| {
                        KalamDbError::InvalidOperation(format!("Failed to commit offset: {}", e))
                    })?;
            }
        }

        let row_count = batch.num_rows();
        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count,
            schema: Some(schema),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &ConsumeStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        // Only Service, DBA, and System roles can consume from topics
        // User role is restricted to prevent unauthorized data access
        match context.user_role() {
            Role::Service | Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "Only service, dba, or system roles can consume from topics".to_string(),
            )),
        }
    }
}
