//! KalamDB Live Query Subsystem
//!
//! This crate handles:
//! - WebSocket-based live query subscriptions
//! - Topic consumer sessions (long polling)
//! - Real-time change notifications
//! - Post-commit notification fanout
//!
//! Live query notifications are handled through Raft-replicated data appliers.
//! When data is applied on any node (leader or follower), the provider's insert/update/delete
//! methods fire local notifications to connected clients.

pub mod error;
pub mod fanout;
pub mod helpers;
pub mod manager;
pub mod models;
pub mod notification;
pub mod subscription;
pub mod traits;

// Re-export types from kalamdb-commons (canonical source)
// Re-export fanout types
pub use fanout::{
    CommitSideEffectPlan, FanoutDispatchPlan, FanoutOwnerScope, TransactionSideEffects,
};
// Re-export from helpers
pub use helpers::{
    filter_eval::{matches as filter_matches, parse_where_clause},
    initial_data::{InitialDataFetcher, InitialDataOptions, InitialDataResult},
};
pub use kalamdb_commons::{
    models::{ConnectionId, LiveQueryId, TableId, UserId},
    NodeId,
};
// Re-export from kalamdb-publisher crate
pub use kalamdb_publisher::{TopicCacheStats, TopicPrimaryKeyLookup, TopicPublisherService};
// Re-export from manager modules
pub use manager::{ConnectionsManager, LiveQueryManager};
// Re-export from models (consolidated model definitions)
pub use models::{
    BufferedNotification, ChangeNotification, ChangeType, ConnectionEvent, ConnectionRegistration,
    ConnectionState, EventReceiver, EventSender, InitialLoadState, NotificationReceiver,
    NotificationSender, SharedConnectionState, SubscriptionFlowControl, SubscriptionHandle,
    SubscriptionResult, SubscriptionState, EVENT_CHANNEL_CAPACITY, NOTIFICATION_CHANNEL_CAPACITY,
};
// Re-export from other modules
pub use notification::NotificationService;
pub use subscription::SubscriptionService;
