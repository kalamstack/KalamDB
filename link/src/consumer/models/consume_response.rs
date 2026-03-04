use serde::{Deserialize, Serialize};

use super::consume_message::ConsumeMessage;

/// Result of consuming from a topic.
///
/// Contains the batch of messages and metadata for pagination.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ConsumeResponse {
    /// Consumed messages in this batch
    pub messages: Vec<ConsumeMessage>,

    /// Next offset to consume from (for subsequent polls)
    pub next_offset: u64,

    /// Whether more messages are available beyond this batch
    pub has_more: bool,
}
