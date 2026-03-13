//! WebSocket subscription management for real-time updates.
//!
//! `models` (subscription models) is always available — no `tokio-runtime` needed.
//! The WebSocket manager requires the `tokio-runtime` feature.

mod checkpoint;
mod live_rows_config;
mod live_rows_event;
mod live_rows_materializer;
pub mod models;

pub use live_rows_config::LiveRowsConfig;
pub use live_rows_event::LiveRowsEvent;
pub use live_rows_materializer::LiveRowsMaterializer;
pub use models::{SubscriptionConfig, SubscriptionInfo, SubscriptionOptions, SubscriptionRequest};

#[cfg(feature = "tokio-runtime")]
mod live_rows_subscription;
#[cfg(feature = "tokio-runtime")]
mod manager;
#[cfg(feature = "tokio-runtime")]
mod reader;
pub(crate) use checkpoint::filter_replayed_event;
#[cfg(feature = "tokio-runtime")]
pub(crate) use checkpoint::{
    batch_envelope, buffer_event, event_progress, final_resume_seq, subscription_start_ready,
};
#[cfg(feature = "tokio-runtime")]
pub use live_rows_subscription::LiveRowsSubscription;
#[cfg(feature = "tokio-runtime")]
pub use manager::SubscriptionManager;
#[cfg(feature = "tokio-runtime")]
pub(crate) use reader::ws_reader_loop;
