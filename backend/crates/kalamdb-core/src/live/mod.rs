//! Live query and consumer module for subscription management
//!
//! This module handles:
//! - WebSocket-based live query subscriptions
//! - Topic consumer sessions (long polling)
//! - Real-time change notifications
//! - Topic pub/sub message publishing
//!
//! Live query notifications are now handled through Raft-replicated data appliers.
//! When data is applied on any node (leader or follower), the provider's insert/update/delete
//! methods fire local notifications to connected clients.

pub mod cluster_handler;
pub mod helpers;
pub mod manager;
pub mod models; // Consolidated model definitions
pub mod notification;
pub mod subscription;

// Re-export types from kalamdb-commons (canonical source)
pub use kalamdb_commons::models::{ConnectionId, LiveQueryId, TableId, UserId};
pub use kalamdb_commons::NodeId;

// Re-export from models (consolidated model definitions)
pub use models::{
    BufferedNotification, ChangeNotification, ChangeType, ConnectionEvent, ConnectionRegistration,
    ConnectionState, EventReceiver, EventSender, InitialLoadState, NotificationReceiver,
    NotificationSender, RegistryStats, SharedConnectionState,
    SubscriptionFlowControl, SubscriptionHandle, SubscriptionResult, SubscriptionState,
    EVENT_CHANNEL_CAPACITY, NOTIFICATION_CHANNEL_CAPACITY,
};

// Re-export from manager modules
pub use manager::{ConnectionsManager, LiveQueryManager};

// Re-export from helpers
pub use helpers::{
    filter_eval::{matches as filter_matches, parse_where_clause},
    initial_data::{InitialDataFetcher, InitialDataOptions, InitialDataResult},
    query_parser::QueryParser,
};

// Re-export from other modules
pub use cluster_handler::CoreClusterHandler;
pub use notification::NotificationService;
pub use subscription::SubscriptionService;

// Re-export from kalamdb-publisher crate (extracted from former topic_publisher.rs)
pub use kalamdb_publisher::{TopicCacheStats, TopicPublisherService};
