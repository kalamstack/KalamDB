use serde::{Deserialize, Serialize};

/// Options for consuming messages from a topic.
///
/// Controls polling behavior including batch size, partition targeting,
/// start offset, and long-poll timeout.
///
/// # Example
///
/// ```json
/// {
///   "topic": "orders",
///   "group_id": "billing",
///   "start": "latest",
///   "batch_size": 10,
///   "partition_id": 0
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ConsumeRequest {
    /// Topic to consume from (e.g., "orders" or "chat.messages")
    pub topic: String,

    /// Consumer group ID for coordinated consumption
    pub group_id: String,

    /// Where to start consuming: "earliest", "latest", or a numeric offset
    #[serde(default = "default_start")]
    pub start: String,

    /// Max messages to return per poll (default: 10)
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,

    /// Partition to consume from (default: 0)
    #[serde(default)]
    pub partition_id: u32,

    /// Long-poll timeout in seconds (server holds connection until messages arrive)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    /// Whether to automatically acknowledge messages after the handler returns
    #[serde(default)]
    pub auto_ack: bool,

    /// Max concurrent message handlers per partition (default: 1)
    #[serde(default = "default_concurrency")]
    pub concurrency_per_partition: u32,
}

fn default_start() -> String {
    "latest".to_string()
}

fn default_batch_size() -> u32 {
    10
}

fn default_concurrency() -> u32 {
    1
}

impl Default for ConsumeRequest {
    fn default() -> Self {
        Self {
            topic: String::new(),
            group_id: String::new(),
            start: default_start(),
            batch_size: default_batch_size(),
            partition_id: 0,
            timeout_seconds: None,
            auto_ack: false,
            concurrency_per_partition: default_concurrency(),
        }
    }
}
