//! Consumer data models.

pub mod ack_response;
pub mod commit_result;
pub mod consume_message;
pub mod consume_request;
pub mod consume_response;
pub mod consumer_config;
pub mod consumer_record;
pub mod enums;

pub use ack_response::AckResponse;
pub use commit_result::CommitResult;
pub use consume_message::ConsumeMessage;
pub use consume_request::ConsumeRequest;
pub use consume_response::ConsumeResponse;
pub use consumer_config::ConsumerConfig;
pub use consumer_record::ConsumerRecord;
pub use enums::{AutoOffsetReset, CommitMode, PayloadMode, TopicOp};

#[derive(Debug, Clone, Default)]
pub struct ConsumerOffsets {
    pub position: u64,
    pub last_committed: Option<u64>,
    pub highest_processed: Option<u64>,
}
