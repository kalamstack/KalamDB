//! Topic Publisher Service for KalamDB Pub/Sub Infrastructure
//!
//! This crate provides the core topic publishing functionality:
//! - Route matching: mapping table changes to topic subscriptions
//! - Payload extraction: serializing row data per route's PayloadMode
//! - Offset allocation: atomic monotonic offset counters per topic-partition
//! - Message persistence: writing TopicMessages to RocksDB via TopicMessageStore
//! - Consumer offset tracking: managing consumer group committed offsets
//!
//! # Architecture
//!
//! The publisher is called **synchronously** from table providers (SharedTableProvider,
//! UserTableProvider, StreamTableProvider) during insert/update/delete operations.
//! This ensures topic messages are durable before the write is acknowledged.
//!
//! ```text
//! table_write() → TopicPublisherService::publish_message() → TopicMessageStore::put()
//! ```
//!
//! Live query notifications remain separate (async, best-effort via NotificationService).

mod models;
mod offset;
mod payload;
mod routing;
mod service;

pub use models::TopicCacheStats;
pub use service::{TopicPrimaryKeyLookup, TopicPublisherService};
