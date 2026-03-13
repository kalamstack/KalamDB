//! Connection-related models for WebSocket and consumer connections
//!
//! These models support both live query WebSocket subscriptions and topic consumer connections.

use dashmap::DashMap;
use datafusion::sql::sqlparser::ast::Expr;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{ConnectionId, ConnectionInfo, LiveQueryId, TableId, UserId};
use kalamdb_commons::Notification;
use kalamdb_commons::Role;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// Get current epoch time in milliseconds (for lock-free heartbeat tracking)
#[inline]
fn epoch_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Maximum pending notifications per connection before dropping new ones
pub const NOTIFICATION_CHANNEL_CAPACITY: usize = 1000;

/// Maximum pending control events per connection.
/// At high connection counts, the heartbeat checker may queue multiple events
/// (ping, timeout, shutdown) before the handler processes them.
pub const EVENT_CHANNEL_CAPACITY: usize = 64;

/// Maximum buffered notifications per subscription while initial snapshot loading is in progress.
///
/// Without this bound, a slow initial snapshot can accumulate unbounded change events and
/// exhaust memory under high write volume or many concurrent subscribers.
pub const MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION: usize = 1_024;

/// Type alias for sending notifications to WebSocket or consumer clients
pub type NotificationSender = mpsc::Sender<Arc<Notification>>;

/// Type alias for receiving notifications
pub type NotificationReceiver = mpsc::Receiver<Arc<Notification>>;

/// Type alias for sending control events to connections
pub type EventSender = mpsc::Sender<ConnectionEvent>;

/// Type alias for receiving control events
pub type EventReceiver = mpsc::Receiver<ConnectionEvent>;

/// Shared connection state - can be held by handlers for direct access
pub type SharedConnectionState = Arc<RwLock<ConnectionState>>;

/// Events sent to connection tasks from the heartbeat checker
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Authentication timeout - close connection
    AuthTimeout,
    /// Heartbeat timeout - close connection
    HeartbeatTimeout,
    /// Server is shutting down - close connection gracefully
    Shutdown,
}

/// Lightweight handle for subscription indices
///
/// Contains only the data needed for notification routing.
/// ~48 bytes per handle (vs ~800+ bytes for full SubscriptionState)
#[derive(Debug, Clone)]
pub struct SubscriptionHandle {
    /// Shared filter expression (Arc for zero-copy across indices)
    pub filter_expr: Option<Arc<Expr>>,
    /// Column projections for filtering notification payload (None = all columns)
    pub projections: Option<Arc<Vec<String>>>,
    /// Shared notification channel
    pub notification_tx: Arc<NotificationSender>,
    /// Flow control for initial load buffering and snapshot gating
    pub flow_control: Arc<SubscriptionFlowControl>,
}

/// Buffered notification with optional SeqId ordering key
#[derive(Debug, Clone)]
pub struct BufferedNotification {
    pub seq: Option<SeqId>,
    pub notification: Arc<Notification>,
}

/// Flow control for subscription initial load gating
#[derive(Debug)]
pub struct SubscriptionFlowControl {
    snapshot_end_seq: AtomicI64,
    has_snapshot: AtomicBool,
    initial_complete: AtomicBool,
    buffer: Mutex<VecDeque<BufferedNotification>>,
}

impl SubscriptionFlowControl {
    pub fn new() -> Self {
        Self {
            snapshot_end_seq: AtomicI64::new(0),
            has_snapshot: AtomicBool::new(false),
            initial_complete: AtomicBool::new(false),
            buffer: Mutex::new(VecDeque::with_capacity(
                MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION,
            )),
        }
    }

    pub fn set_snapshot_end_seq(&self, snapshot_end_seq: Option<SeqId>) {
        if let Some(seq) = snapshot_end_seq {
            self.snapshot_end_seq.store(seq.as_i64(), Ordering::Release);
            self.has_snapshot.store(true, Ordering::Release);

            let max_seq = seq.as_i64();
            let mut buffer = self.buffer.lock();
            buffer.retain(|item| match item.seq {
                Some(item_seq) => item_seq.as_i64() > max_seq,
                None => true,
            });
        } else {
            self.has_snapshot.store(false, Ordering::Release);
        }
    }

    pub fn snapshot_end_seq(&self) -> Option<i64> {
        if self.has_snapshot.load(Ordering::Acquire) {
            Some(self.snapshot_end_seq.load(Ordering::Acquire))
        } else {
            None
        }
    }

    pub fn is_initial_complete(&self) -> bool {
        self.initial_complete.load(Ordering::Acquire)
    }

    pub fn mark_initial_complete(&self) {
        self.initial_complete.store(true, Ordering::Release);
    }

    pub fn buffer_notification(&self, notification: Arc<Notification>, seq: Option<SeqId>) {
        let mut buffer = self.buffer.lock();
        if buffer.len() >= MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION {
            buffer.pop_front();
        }
        buffer.push_back(BufferedNotification { seq, notification });
    }

    pub fn drain_buffered_notifications(&self) -> Vec<BufferedNotification> {
        let mut buffer = self.buffer.lock();
        let mut drained: Vec<_> = buffer.drain(..).collect();
        drained.sort_by(|a, b| match (a.seq, b.seq) {
            (Some(a_seq), Some(b_seq)) => a_seq.as_i64().cmp(&b_seq.as_i64()),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        drained
    }
}

/// Subscription state - stored only in ConnectionState.subscriptions
///
/// Contains all metadata needed for:
/// - Notification filtering (filter_expr)
/// - Column projections (projections)
/// - Initial data batch fetching (sql, batch_size, snapshot_end_seq)
/// - Batch pagination tracking (current_batch_num)
///
/// Memory optimization: Uses Arc<str> for SQL and Arc<Expr> for filter
/// to share data with SubscriptionHandle indices.
#[derive(Debug, Clone)]
pub struct SubscriptionState {
    pub live_id: LiveQueryId,
    pub table_id: TableId,
    /// Original SQL query for batch fetching (Arc for zero-copy)
    pub sql: Arc<str>,
    /// Compiled filter expression from WHERE clause (parsed once at subscription time)
    /// None means no filter (SELECT * without WHERE)
    /// Arc-wrapped for sharing with SubscriptionHandle
    pub filter_expr: Option<Arc<Expr>>,
    /// Column projections from SELECT clause (None = SELECT *, i.e., all columns)
    /// Arc-wrapped for sharing with SubscriptionHandle
    pub projections: Option<Arc<Vec<String>>>,
    /// Batch size for initial data loading
    pub batch_size: usize,
    /// Snapshot boundary SeqId for consistent batch loading
    pub snapshot_end_seq: Option<SeqId>,
    /// Current batch number for pagination tracking (0-indexed)
    /// Incremented after each batch is sent
    pub current_batch_num: u32,
    /// Flow control for initial load buffering and snapshot gating
    pub flow_control: Arc<SubscriptionFlowControl>,
    /// Whether this subscription is for a shared table (affects index cleanup)
    pub is_shared: bool,
}

/// Connection state - everything about a connection in one struct
///
/// Handlers hold `Arc<RwLock<ConnectionState>>` for direct access.
/// Used for both WebSocket live query connections and topic consumer connections.
pub struct ConnectionState {
    // === Identity ===
    /// Connection ID
    pub connection_id: ConnectionId,
    /// User ID (None until authenticated)
    pub user_id: Option<UserId>,
    /// User role (None until authenticated)
    pub user_role: Option<Role>,
    /// Client IP address (for localhost bypass check)
    pub client_ip: ConnectionInfo,

    // === Authentication State ===
    /// Whether connection has been authenticated
    pub is_authenticated: bool,
    /// Whether auth attempt has started (for timeout logic)
    pub auth_started: bool,

    // === Heartbeat/Timing ===
    /// When connection was established
    pub connected_at: Instant,
    /// Last heartbeat timestamp in epoch millis (atomic for lock-free updates)
    /// Use `update_heartbeat()` and `millis_since_heartbeat()` to access.
    pub last_heartbeat_ms: AtomicU64,

    // === Subscriptions ===
    /// Active subscriptions for this connection (subscription_id -> state)
    /// For WebSocket: live query subscriptions
    /// For topic consumers: topic consumption sessions
    pub subscriptions: DashMap<String, SubscriptionState>,

    // === Channels ===
    /// Channel to send notifications to this connection's handler task (bounded for backpressure)
    pub notification_tx: Arc<NotificationSender>,
    /// Channel to send control events (ping, timeout, shutdown) (bounded)
    pub event_tx: EventSender,
}

impl ConnectionState {
    /// Update heartbeat timestamp — lock-free, callable with `&self` (no write lock needed)
    #[inline]
    pub fn update_heartbeat(&self) {
        self.last_heartbeat_ms.store(epoch_millis(), Ordering::Release);
    }

    /// Milliseconds elapsed since last heartbeat activity
    #[inline]
    pub fn millis_since_heartbeat(&self) -> u64 {
        epoch_millis().saturating_sub(self.last_heartbeat_ms.load(Ordering::Acquire))
    }

    /// Mark that authentication has started (for timeout logic)
    #[inline]
    pub fn mark_auth_started(&mut self) {
        self.auth_started = true;
    }

    /// Mark connection as authenticated with user identity
    #[inline]
    pub fn mark_authenticated(&mut self, user_id: UserId, user_role: Role) {
        self.is_authenticated = true;
        self.user_id = Some(user_id);
        self.user_role = Some(user_role);
    }

    /// Check if connection is authenticated
    #[inline]
    pub fn is_authenticated(&self) -> bool {
        self.is_authenticated
    }

    /// Get user ID (None if not authenticated)
    #[inline]
    pub fn user_id(&self) -> Option<&UserId> {
        self.user_id.as_ref()
    }

    /// Get user role (None if not authenticated)
    #[inline]
    pub fn user_role(&self) -> Option<Role> {
        self.user_role
    }

    /// Get connection ID
    #[inline]
    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    /// Get client IP
    #[inline]
    pub fn client_ip(&self) -> Option<&ConnectionInfo> {
        Some(&self.client_ip)
    }

    /// Get a subscription by ID
    pub fn get_subscription(&self, subscription_id: &str) -> Option<SubscriptionState> {
        self.subscriptions.get(subscription_id).map(|s| s.clone())
    }

    /// Update snapshot_end_seq for a subscription
    pub fn update_snapshot_end_seq(&self, subscription_id: &str, snapshot_end_seq: Option<SeqId>) {
        if let Some(mut sub) = self.subscriptions.get_mut(subscription_id) {
            sub.snapshot_end_seq = snapshot_end_seq;
            sub.flow_control.set_snapshot_end_seq(snapshot_end_seq);
        }
    }

    /// Mark initial load complete and flush buffered notifications
    pub fn complete_initial_load(&self, subscription_id: &str) -> usize {
        if let Some(sub) = self.subscriptions.get(subscription_id) {
            let flow_control = Arc::clone(&sub.flow_control);
            flow_control.mark_initial_complete();
            let buffered = flow_control.drain_buffered_notifications();

            let mut sent = 0usize;
            for item in buffered {
                // TODO: Backpressure limitation: buffered notifications are flushed with try_send.
                // If the channel is full, remaining buffered notifications are dropped.
                // Consider a blocking flush, retry queue, or persistent spool for lossless delivery.
                if let Err(e) = self.notification_tx.try_send(item.notification) {
                    if matches!(e, mpsc::error::TrySendError::Full(_)) {
                        log::warn!(
                            "Notification channel full while flushing buffered notifications for {}",
                            subscription_id
                        );
                        break;
                    }
                } else {
                    sent += 1;
                }
            }
            sent
        } else {
            0
        }
    }

    /// Increment current batch number for a subscription and return the new value
    pub fn increment_batch_num(&self, subscription_id: &str) -> Option<u32> {
        if let Some(mut sub) = self.subscriptions.get_mut(subscription_id) {
            sub.current_batch_num += 1;
            Some(sub.current_batch_num)
        } else {
            None
        }
    }
}

/// Registration info returned when a connection is registered
pub struct ConnectionRegistration {
    pub connection_id: ConnectionId,
    pub state: SharedConnectionState,
    pub notification_rx: NotificationReceiver,
    pub event_rx: EventReceiver,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_notification(subscription_id: &str) -> Arc<Notification> {
        Arc::new(Notification::insert(subscription_id.to_string(), vec![HashMap::new()]))
    }

    #[test]
    fn test_subscription_flow_control_limits_buffer_growth() {
        let flow_control = SubscriptionFlowControl::new();

        for seq in 1..=2_048 {
            flow_control.buffer_notification(make_notification("sub-1"), Some(SeqId::from(seq)));
        }

        let buffered = flow_control.drain_buffered_notifications();

        assert_eq!(
            buffered.len(),
            MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION,
            "buffered notifications should be capped to avoid unbounded per-subscription growth"
        );
        assert_eq!(
            buffered.first().and_then(|item| item.seq),
            Some(SeqId::from((2_048 - MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION + 1) as i64))
        );
        assert_eq!(buffered.last().and_then(|item| item.seq), Some(SeqId::from(2_048)));
    }

    #[test]
    fn test_subscription_flow_control_drains_in_seq_order() {
        let flow_control = SubscriptionFlowControl::new();

        flow_control.buffer_notification(make_notification("sub-ordered"), Some(SeqId::from(9)));
        flow_control.buffer_notification(make_notification("sub-ordered"), Some(SeqId::from(3)));
        flow_control.buffer_notification(make_notification("sub-ordered"), Some(SeqId::from(6)));

        let buffered = flow_control.drain_buffered_notifications();
        let seqs: Vec<_> = buffered
            .into_iter()
            .map(|item| item.seq.expect("seq should be present").as_i64())
            .collect();

        assert_eq!(seqs, vec![3, 6, 9]);
    }
}
