//! Topic Cleanup Job Executor
//!
//! **Phase 24**: Background job for cleaning up all messages and metadata after topic deletion
//!
//! Handles complete cleanup of dropped topics:
//! - Delete all topic messages from message store
//! - Delete all consumer group offsets
//! - Clean up topic metadata
//!
//! ## Responsibilities
//! - Scan topic message store and delete all messages for the topic
//! - Delete all consumer group offsets for the topic
//! - Track cleanup metrics (messages deleted, bytes freed)
//! - Safe to re-run (idempotent)
//!
//! ## Parameters Format
//! ```json
//! {
//!   "topic_id": "topic_abc123",
//!   "topic_name": "my_topic"
//! }
//! ```

use kalamdb_core::error::KalamDbError;
use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_commons::models::TopicId;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

/// Typed parameters for topic cleanup operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicCleanupParams {
    /// Topic identifier (required)
    pub topic_id: TopicId,
    /// Topic name for logging (required)
    pub topic_name: String,
}

impl JobParams for TopicCleanupParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.topic_name.is_empty() {
            return Err(KalamDbError::InvalidOperation("topic_name cannot be empty".to_string()));
        }
        Ok(())
    }
}

/// Topic Cleanup Job Executor
///
/// Executes complete cleanup of dropped topics.
pub struct TopicCleanupExecutor;

impl TopicCleanupExecutor {
    /// Create a new TopicCleanupExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for TopicCleanupExecutor {
    type Params = TopicCleanupParams;

    fn job_type(&self) -> JobType {
        JobType::TopicCleanup
    }

    fn name(&self) -> &'static str {
        "TopicCleanupExecutor"
    }

    async fn execute(&self, _ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        // No local work needed for topic cleanup
        Ok(JobDecision::Completed {
            message: Some("Topic cleanup has no local work".to_string()),
        })
    }

    async fn execute_leader(
        &self,
        ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting topic cleanup operation (leader)");

        // Parameters already validated in JobContext
        let params = ctx.params();
        let topic_id = &params.topic_id;
        let topic_name = &params.topic_name;

        ctx.log_info(&format!("Cleaning up topic '{}' ({})", topic_name, topic_id));

        // Get topic publisher service from AppContext
        let topic_publisher = ctx.app_ctx.topic_publisher();
        let offset_store = topic_publisher.offset_store();

        // Step 1: Clean up consumer group offsets
        let offsets_deleted = match offset_store.delete_topic_offsets(topic_id) {
            Ok(count) => {
                ctx.log_info(&format!(
                    "Deleted {} consumer group offsets for topic '{}'",
                    count, topic_name
                ));
                count
            },
            Err(e) => {
                ctx.log_warn(&format!(
                    "Failed to delete consumer offsets for topic '{}': {}",
                    topic_name, e
                ));
                0
            },
        };

        // Step 2: Clean up topic messages from message store
        let message_store = topic_publisher.message_store();
        let (messages_deleted, bytes_freed) = match message_store.delete_topic_messages(topic_id) {
            Ok(count) => {
                ctx.log_info(&format!("Deleted {} messages from topic '{}'", count, topic_name));
                // Estimate bytes freed (rough approximation)
                let estimated_bytes = count * 512; // Average message size estimate
                (count, estimated_bytes)
            },
            Err(e) => {
                ctx.log_warn(&format!(
                    "Failed to delete messages for topic '{}': {}",
                    topic_name, e
                ));
                (0, 0)
            },
        };

        // NOTE: The topic should already be removed from system.topics
        // and from the TopicPublisherService cache before this job runs.
        // This job is responsible for cleaning up the actual data (messages + offsets).

        let message = format!(
            "Cleaned up topic '{}' - {} consumer group offsets deleted, {} messages deleted, {} bytes freed",
            topic_name, offsets_deleted, messages_deleted, bytes_freed
        );

        ctx.log_info(&message);

        Ok(JobDecision::Completed {
            message: Some(message),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Topic cleanup job cancellation requested");
        // Allow cancellation since partial cleanup is acceptable
        // (offsets and messages are independent)
        Ok(())
    }
}

impl Default for TopicCleanupExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_cleanup_params_validation() {
        // Valid params
        let valid_params = TopicCleanupParams {
            topic_id: TopicId::new("topic_123"),
            topic_name: "my_topic".to_string(),
        };
        assert!(valid_params.validate().is_ok());

        // Invalid: empty topic name
        let invalid_params = TopicCleanupParams {
            topic_id: TopicId::new("topic_123"),
            topic_name: String::new(),
        };
        assert!(invalid_params.validate().is_err());
    }

    #[test]
    fn test_topic_cleanup_params_serialization() {
        let params = TopicCleanupParams {
            topic_id: TopicId::new("topic_abc"),
            topic_name: "test_topic".to_string(),
        };

        // Test JSON round-trip
        let json = serde_json::to_string(&params).unwrap();
        let deserialized: TopicCleanupParams = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.topic_id.as_str(), "topic_abc");
        assert_eq!(deserialized.topic_name, "test_topic");
    }

    #[test]
    fn test_executor_properties() {
        let executor = TopicCleanupExecutor::new();
        assert_eq!(executor.job_type(), JobType::TopicCleanup);
        assert_eq!(executor.name(), "TopicCleanupExecutor");
    }
}
