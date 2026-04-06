//! Kafka-style topic consumer support shared by the KalamDB SDK.

/// Data models (always available — serializable structs, no tokio dependency).
pub mod models;

#[cfg(all(feature = "tokio-runtime", feature = "consumer"))]
pub mod core;
#[cfg(all(feature = "tokio-runtime", feature = "consumer"))]
pub mod utils;

#[cfg(all(feature = "tokio-runtime", feature = "consumer"))]
pub use core::{ConsumerBuilder, TopicConsumer};
pub use models::{
    AckResponse, AutoOffsetReset, CommitMode, CommitResult, ConsumeMessage, ConsumeRequest,
    ConsumeResponse, ConsumerConfig, ConsumerOffsets, ConsumerRecord, PayloadMode, TopicOp,
};
