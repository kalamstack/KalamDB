//! WebSocket subscription management for real-time updates.
//!
//! `models` (subscription models) is always available — no `tokio-runtime` needed.
//! The WebSocket manager requires the `tokio-runtime` feature.

pub mod models;

pub use models::{SubscriptionConfig, SubscriptionInfo, SubscriptionOptions, SubscriptionRequest};

#[cfg(feature = "tokio-runtime")]
mod manager;
#[cfg(feature = "tokio-runtime")]
mod reader;
#[cfg(feature = "tokio-runtime")]
pub use manager::SubscriptionManager;
#[cfg(feature = "tokio-runtime")]
pub(crate) use reader::ws_reader_loop;
