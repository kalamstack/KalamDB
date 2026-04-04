//! Models module for live query and consumer connections
//!
//! Contains shared data structures used by both:
//! - WebSocket live query subscriptions
//! - Topic consumer connections (long polling)

pub mod connection;
pub mod subscription;

// Re-export commonly used types
pub use connection::{
    BufferedNotification, ConnectionEvent, ConnectionRegistration, ConnectionState, EventReceiver,
    EventSender, InitialLoadState, NotificationReceiver, NotificationSender, SharedConnectionState,
    SubscriptionFlowControl, SubscriptionHandle, SubscriptionRuntimeMetadata, SubscriptionState,
    EVENT_CHANNEL_CAPACITY, NOTIFICATION_CHANNEL_CAPACITY,
};

pub use subscription::{
    ChangeNotification, ChangeType, RegistryStats, SubscriptionResult,
};
