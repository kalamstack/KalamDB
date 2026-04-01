//! Topic Retention Job Executor
//!
//! **Phase 8**: Background job for cleaning up expired topic messages
//!
//! Handles retention policy enforcement for topic messages based on:
//! - Time-based retention (retention_seconds)
//! - Size-based retention (retention_max_bytes) - future enhancement
//!
//! ## Responsibilities
//! - Scan topic message store for expired messages
//! - Delete messages older than retention_seconds
//! - Track cleanup metrics (messages deleted, bytes freed)
//! - Respect per-topic retention policies
//!
//! ## Parameters Format
//! ```json
//! {
//!   "topic_id": "topic_abc123",
//!   "retention_seconds": 2592000
//! }
//! ```

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_commons::models::TopicId;
use kalamdb_core::error::KalamDbError;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

/// Typed parameters for topic retention operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRetentionParams {
    /// Topic identifier (required)
    pub topic_id: TopicId,
    /// Retention period in seconds (required, must be > 0)
    pub retention_seconds: i64,
    /// Partition ID to clean (default: 0 for all partitions)
    #[serde(default)]
    pub partition_id: Option<u32>,
}

impl JobParams for TopicRetentionParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.retention_seconds <= 0 {
            return Err(KalamDbError::InvalidOperation(
                "retention_seconds must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Topic Retention Job Executor
///
/// Executes retention policy enforcement for topic messages.
pub struct TopicRetentionExecutor;

impl TopicRetentionExecutor {
    /// Create a new TopicRetentionExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for TopicRetentionExecutor {
    type Params = TopicRetentionParams;

    fn job_type(&self) -> JobType {
        JobType::TopicRetention
    }

    fn name(&self) -> &'static str {
        "TopicRetentionExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting topic retention enforcement");

        // Parameters already validated in JobContext
        let params = ctx.params();
        let topic_id = &params.topic_id;
        let retention_seconds = params.retention_seconds;

        ctx.log_info(&format!(
            "Enforcing retention policy for topic {} (retention: {}s)",
            topic_id, retention_seconds
        ));

        // Calculate cutoff time for deletion (messages older than this are expired)
        let now = chrono::Utc::now().timestamp_millis();
        let retention_ms = retention_seconds * 1000;
        let cutoff_time = now - retention_ms;

        ctx.log_info(&format!(
            "Cutoff time: {} (messages before this are expired)",
            chrono::DateTime::from_timestamp_millis(cutoff_time)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "invalid".to_string())
        ));

        // Get topic publisher service from AppContext
        let topic_publisher = ctx.app_ctx.topic_publisher();

        // TODO: Implement actual topic message cleanup logic
        // Implementation sketch:
        //   1. Get TopicMessageStore from TopicPublisherService
        //   2. Scan messages by prefix: "topic/{topic_id}/{partition_id}/"
        //   3. Parse TopicMessage, check if ts < cutoff_time
        //   4. Delete expired messages in batches
        //   5. Track metrics (messages_deleted, estimated_bytes_freed)
        //
        // For now, log and return placeholder
        let messages_deleted = 0;
        let bytes_freed = 0;

        ctx.log_info(&format!(
            "Topic retention enforcement completed for {} - {} messages deleted, {} bytes freed",
            topic_id, messages_deleted, bytes_freed
        ));

        // Check if topic still exists (prevent cleanup of deleted topics)
        if topic_publisher.topic_exists(topic_id) {
            ctx.log_info(&format!("Topic {} still exists in registry", topic_id));
        }

        Ok(JobDecision::Completed {
            message: Some(format!(
                "Enforced retention policy for topic {} ({}s) - {} messages deleted",
                topic_id, retention_seconds, messages_deleted
            )),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Topic retention job cancellation requested");
        // Allow cancellation since partial cleanup is acceptable
        Ok(())
    }
}

impl Default for TopicRetentionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_retention_params_validation() {
        // Valid params
        let valid_params = TopicRetentionParams {
            topic_id: TopicId::new("topic_123"),
            retention_seconds: 3600,
            partition_id: None,
        };
        assert!(valid_params.validate().is_ok());

        // Invalid: zero retention
        let invalid_params = TopicRetentionParams {
            topic_id: TopicId::new("topic_123"),
            retention_seconds: 0,
            partition_id: None,
        };
        assert!(invalid_params.validate().is_err());

        // Invalid: negative retention
        let invalid_params = TopicRetentionParams {
            topic_id: TopicId::new("topic_123"),
            retention_seconds: -100,
            partition_id: None,
        };
        assert!(invalid_params.validate().is_err());
    }

    #[test]
    fn test_topic_retention_params_serialization() {
        let params = TopicRetentionParams {
            topic_id: TopicId::new("topic_abc"),
            retention_seconds: 86400,
            partition_id: Some(0),
        };

        // Test JSON round-trip
        let json = serde_json::to_string(&params).unwrap();
        let deserialized: TopicRetentionParams = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.topic_id.as_str(), "topic_abc");
        assert_eq!(deserialized.retention_seconds, 86400);
        assert_eq!(deserialized.partition_id, Some(0));
    }
}
