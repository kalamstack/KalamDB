//! Consume request model

use super::StartPosition;
use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use serde::Deserialize;

fn default_start_position() -> StartPosition {
    StartPosition::Latest
}

fn default_limit() -> u64 {
    100
}

/// Request body for POST /api/topics/consume
#[derive(Debug, Deserialize)]
pub struct ConsumeRequest {
    /// Topic identifier (type-safe)
    #[serde(deserialize_with = "deserialize_topic_id")]
    pub topic_id: TopicId,
    /// Consumer group identifier (type-safe)
    #[serde(deserialize_with = "deserialize_consumer_group_id")]
    pub group_id: ConsumerGroupId,
    /// Starting position: "Latest", "Earliest", or {"Offset": 12345}
    #[serde(default = "default_start_position")]
    pub start: StartPosition,
    /// Maximum messages to return (default 100)
    #[serde(default = "default_limit")]
    pub limit: u64,
    /// Partition to consume from (default 0)
    #[serde(default)]
    pub partition_id: u32,
    /// Long polling timeout in seconds (default from server config)
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

fn deserialize_topic_id<'de, D>(deserializer: D) -> Result<TopicId, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(TopicId::new(&s))
}

fn deserialize_consumer_group_id<'de, D>(deserializer: D) -> Result<ConsumerGroupId, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(ConsumerGroupId::new(&s))
}
