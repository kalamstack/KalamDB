//! Core consumer implementation.

pub mod offset_manager;
pub mod poller;
pub mod topic_consumer;

pub use topic_consumer::{ConsumerBuilder, TopicConsumer};
