//! Ack request model

use kalamdb_commons::models::{ConsumerGroupId, TopicId};
use serde::Deserialize;

/// Request body for POST /api/topics/ack
#[derive(Debug, Deserialize)]
pub struct AckRequest {
    /// Topic identifier (type-safe)
    #[serde(deserialize_with = "deserialize_topic_id")]
    pub topic_id: TopicId,
    /// Consumer group identifier (type-safe)
    #[serde(deserialize_with = "deserialize_consumer_group_id")]
    pub group_id: ConsumerGroupId,
    /// Partition ID (default 0)
    #[serde(default)]
    pub partition_id: u32,
    /// Offset to acknowledge up to (inclusive)
    pub upto_offset: u64,
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
