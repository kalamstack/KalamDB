//! Consume response model

use super::TopicMessage;
use serde::Serialize;

/// Response for POST /api/topics/consume
#[derive(Debug, Serialize)]
pub struct ConsumeResponse {
    /// Messages retrieved
    pub messages: Vec<TopicMessage>,
    /// Next offset to consume from
    pub next_offset: u64,
    /// Whether there are more messages available
    pub has_more: bool,
}
