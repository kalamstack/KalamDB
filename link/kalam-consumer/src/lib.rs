//! Consumer-focused crate layered on top of `kalam-client`.

pub mod models {
    pub use kalam_client::models::{
        AckResponse, ConsumeMessage, ConsumeRequest, ConsumeResponse, RowData, UserId,
    };
}

#[cfg(feature = "native-sdk")]
pub use kalam_client::consumer::ConsumerBuilder;
#[cfg(feature = "native-sdk")]
pub use kalam_client::TopicConsumer;
pub use kalam_client::{
    models::UserId, AckResponse, AutoOffsetReset, CommitMode, CommitResult, ConsumeMessage,
    ConsumeRequest, ConsumeResponse, ConsumerConfig, ConsumerOffsets, ConsumerRecord, PayloadMode,
    RowData, TopicOp,
};
