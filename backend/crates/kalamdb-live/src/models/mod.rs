//! Models module for live query and consumer connections
//!
//! Contains shared data structures used by both:
//! - WebSocket live query subscriptions
//! - Topic consumer connections (long polling)

pub mod connection;
pub mod subscription;

pub(crate) use connection::epoch_millis;

// Re-export commonly used types
pub use connection::{
    BufferedNotification, ConnectionEvent, ConnectionRegistration, ConnectionState, EventReceiver,
    EventSender, InitialLoadState, NotificationReceiver, NotificationSender, SharedConnectionState,
    SubscriptionFlowControl, SubscriptionHandle, SubscriptionRuntimeMetadata, SubscriptionState,
    EVENT_CHANNEL_CAPACITY, MAX_SUBSCRIPTIONS_PER_CONNECTION, NOTIFICATION_CHANNEL_CAPACITY,
};
pub use subscription::{ChangeNotification, ChangeType, SubscriptionResult};
