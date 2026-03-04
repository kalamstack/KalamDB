//! Kafka-style topic consumer for kalam-link.

/// Data models (always available — serializable structs, no tokio dependency).
pub mod models;

#[cfg(feature = "tokio-runtime")]
pub mod core;
#[cfg(feature = "tokio-runtime")]
pub mod utils;

#[cfg(feature = "tokio-runtime")]
pub use core::{ConsumerBuilder, TopicConsumer};
pub use models::{
    AckResponse, AutoOffsetReset, CommitMode, CommitResult, ConsumeMessage, ConsumeRequest,
    ConsumeResponse, ConsumerConfig, ConsumerOffsets, ConsumerRecord, PayloadMode, TopicOp,
};
