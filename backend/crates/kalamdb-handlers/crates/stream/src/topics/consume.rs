use std::sync::Arc;

use datafusion::arrow::{
    array::{ArrayRef, BinaryBuilder, Int32Array, Int64Array, StringBuilder},
    record_batch::RecordBatch,
};
use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::{ConsumePosition, ConsumeStatement};
use kalamdb_tables::topics::topic_message_schema::topic_message_schema;

pub struct ConsumeHandler {
    app_context: Arc<AppContext>,
}

impl ConsumeHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<ConsumeStatement> for ConsumeHandler {
    async fn execute(
        &self,
        statement: ConsumeStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let topic_id = TopicId::new(&statement.topic_name);
        let topics_provider = self.app_context.system_tables().topics();
        let _topic = topics_provider.get_topic_by_id_async(&topic_id).await?.ok_or_else(|| {
            KalamDbError::NotFound(format!("Topic '{}' does not exist", statement.topic_name))
        })?;

        let topic_publisher = self.app_context.topic_publisher();
        let limit = statement.limit.unwrap_or(100) as usize;
        let partition_id = 0u32;
        let group_id =
            statement.group_id.as_ref().map(|group_name| ConsumerGroupId::new(group_name));

        let committed_offset = group_id.as_ref().and_then(|group_id| {
            topic_publisher.get_group_offsets(&topic_id, group_id).ok().and_then(|offsets| {
                offsets
                    .iter()
                    .find(|offset| offset.partition_id == partition_id)
                    .map(|offset| offset.last_acked_offset + 1)
            })
        });

        let start_offset = match committed_offset {
            Some(committed) => committed,
            None => match statement.position {
                ConsumePosition::Offset(offset) => offset,
                ConsumePosition::Earliest => 0,
                ConsumePosition::Latest => topic_publisher
                    .latest_offset(&topic_id, partition_id)
                    .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?
                    .map(|offset| offset + 1)
                    .unwrap_or(0),
            },
        };

        let messages = if let Some(group_id) = group_id.as_ref() {
            topic_publisher
                .fetch_messages_for_group(&topic_id, group_id, partition_id, start_offset, limit)
                .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?
        } else {
            topic_publisher
                .fetch_messages(&topic_id, partition_id, start_offset, limit)
                .map_err(|e| KalamDbError::InvalidOperation(e.to_string()))?
        };

        let schema = topic_message_schema();
        let num_messages = messages.len();
        let mut topic_ids_builder = StringBuilder::new();
        let mut partition_ids = Vec::with_capacity(num_messages);
        let mut offsets = Vec::with_capacity(num_messages);
        let mut keys_builder = StringBuilder::new();
        let mut payloads_builder = BinaryBuilder::new();
        let mut timestamps = Vec::with_capacity(num_messages);
        let mut user_ids_builder = StringBuilder::new();
        let mut ops_builder = StringBuilder::new();

        for msg in &messages {
            topic_ids_builder.append_value(msg.topic_id.as_str());
            partition_ids.push(msg.partition_id as i32);
            offsets.push(msg.offset as i64);

            match &msg.key {
                Some(key) => keys_builder.append_value(key),
                None => keys_builder.append_null(),
            }

            payloads_builder.append_value(&msg.payload);
            timestamps.push(msg.timestamp_ms);

            match &msg.user_id {
                Some(user_id) => user_ids_builder.append_value(user_id.as_str()),
                None => user_ids_builder.append_null(),
            }

            ops_builder.append_value(msg.op.as_str());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(topic_ids_builder.finish()) as ArrayRef,
                Arc::new(Int32Array::from(partition_ids)) as ArrayRef,
                Arc::new(Int64Array::from(offsets)) as ArrayRef,
                Arc::new(keys_builder.finish()) as ArrayRef,
                Arc::new(payloads_builder.finish()) as ArrayRef,
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(user_ids_builder.finish()) as ArrayRef,
                Arc::new(ops_builder.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            KalamDbError::SerializationError(format!("Failed to create RecordBatch: {}", e))
        })?;

        if let Some(group_id) = group_id.as_ref() {
            if let Some(last_msg) = messages.last() {
                topic_publisher
                    .ack_offset(&topic_id, group_id, partition_id, last_msg.offset)
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

        match context.user_role() {
            Role::Service | Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "Only service, dba, or system roles can consume from topics".to_string(),
            )),
        }
    }
}
