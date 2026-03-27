//! Connection-related models for WebSocket and consumer connections
//!
//! These models support both live query WebSocket subscriptions and topic consumer connections.

use dashmap::DashMap;
use datafusion::sql::sqlparser::ast::Expr;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{ConnectionId, ConnectionInfo, LiveQueryId, TableId, UserId};
use kalamdb_commons::Notification;
use kalamdb_commons::Role;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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

/// Shared connection state — lock-free, held by handlers for direct access.
///
/// All fields are either immutable after construction, set-once (OnceLock/AtomicBool),
/// or interior-mutable (DashMap/AtomicU64), so no outer RwLock is needed.
pub type SharedConnectionState = Arc<ConnectionState>;

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

/// Connection state — lock-free interior mutability for 100k+ concurrent connections.
///
/// Identity and channel fields are immutable after construction.
/// Auth fields use set-once primitives (OnceLock + AtomicBool).
/// Subscriptions use DashMap for concurrent shard-level locking.
/// Heartbeat uses AtomicU64 for zero-contention updates.
///
/// Used for both WebSocket live query connections and topic consumer connections.
pub struct ConnectionState {
    // === Identity (immutable) ===
    connection_id: ConnectionId,
    client_ip: ConnectionInfo,
    connected_at: Instant,

    // === Authentication (set-once) ===
    is_authenticated: AtomicBool,
    auth_started: AtomicBool,
    user_id: OnceLock<UserId>,
    user_role: OnceLock<Role>,

    // === Heartbeat (atomic) ===
    last_heartbeat_ms: AtomicU64,

    // === Subscriptions (concurrent interior-mutable) ===
    subscriptions: DashMap<String, SubscriptionState>,

    // === Channels (immutable after construction) ===
    pub notification_tx: Arc<NotificationSender>,
    pub event_tx: EventSender,
}

impl ConnectionState {
    /// Create a new connection state with the given identity and channels.
    pub fn new(
        connection_id: ConnectionId,
        client_ip: ConnectionInfo,
        notification_tx: Arc<NotificationSender>,
        event_tx: EventSender,
    ) -> Self {
        Self {
            connection_id,
            client_ip,
            connected_at: Instant::now(),
            is_authenticated: AtomicBool::new(false),
            auth_started: AtomicBool::new(false),
            user_id: OnceLock::new(),
            user_role: OnceLock::new(),
            last_heartbeat_ms: AtomicU64::new(epoch_millis()),
            subscriptions: DashMap::new(),
            notification_tx,
            event_tx,
        }
    }

    // === Identity accessors ===

    #[inline]
    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    #[inline]
    pub fn client_ip(&self) -> Option<&ConnectionInfo> {
        Some(&self.client_ip)
    }

    #[inline]
    pub fn connected_at(&self) -> Instant {
        self.connected_at
    }

    // === Authentication ===

    /// Mark that authentication has started (for timeout logic). Lock-free.
    #[inline]
    pub fn mark_auth_started(&self) {
        self.auth_started.store(true, Ordering::Release);
    }

    /// Whether auth attempt has started.
    #[inline]
    pub fn auth_started(&self) -> bool {
        self.auth_started.load(Ordering::Acquire)
    }

    /// Mark connection as authenticated with user identity. Lock-free (set-once).
    #[inline]
    pub fn mark_authenticated(&self, user_id: UserId, user_role: Role) {
        let _ = self.user_id.set(user_id);
        let _ = self.user_role.set(user_role);
        self.is_authenticated.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_authenticated(&self) -> bool {
        self.is_authenticated.load(Ordering::Acquire)
    }

    #[inline]
    pub fn user_id(&self) -> Option<&UserId> {
        self.user_id.get()
    }

    #[inline]
    pub fn user_role(&self) -> Option<Role> {
        self.user_role.get().copied()
    }

    // === Heartbeat ===

    /// Update heartbeat timestamp — lock-free atomic store.
    #[inline]
    pub fn update_heartbeat(&self) {
        self.last_heartbeat_ms.store(epoch_millis(), Ordering::Release);
    }

    /// Milliseconds elapsed since last heartbeat activity.
    #[inline]
    pub fn millis_since_heartbeat(&self) -> u64 {
        epoch_millis().saturating_sub(self.last_heartbeat_ms.load(Ordering::Acquire))
    }

    // === Subscription management ===

    /// Number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Insert a subscription into the connection's map.
    pub fn insert_subscription(&self, key: String, state: SubscriptionState) {
        self.subscriptions.insert(key, state);
    }

    /// Remove a subscription by key, returning the removed value.
    pub fn remove_subscription(&self, key: &str) -> Option<(String, SubscriptionState)> {
        self.subscriptions.remove(key)
    }

    /// Remove a subscription by primary key, falling back to a secondary key
    /// produced by `fallback_fn` if the primary is not found.
    pub fn remove_subscription_with_fallback<F>(
        &self,
        primary_key: &str,
        fallback_fn: F,
    ) -> Option<(String, SubscriptionState)>
    where
        F: FnOnce() -> Option<String>,
    {
        self.subscriptions
            .remove(primary_key)
            .or_else(|| fallback_fn().and_then(|key| self.subscriptions.remove(&key)))
    }

    /// Get a subscription by ID (cloned out of the DashMap).
    pub fn get_subscription(&self, subscription_id: &str) -> Option<SubscriptionState> {
        self.subscriptions.get(subscription_id).map(|s| s.clone())
    }

    /// Iterate all subscriptions, calling `f` for each.
    pub fn for_each_subscription<F>(&self, mut f: F)
    where
        F: FnMut(&str, &SubscriptionState),
    {
        for entry in self.subscriptions.iter() {
            f(entry.key(), entry.value());
        }
    }

    /// Collect info from all subscriptions, removing them in the process.
    /// Returns the collected values.
    pub fn collect_subscription_info<F, T>(&self, mut f: F) -> Vec<T>
    where
        F: FnMut(&SubscriptionState) -> T,
    {
        let mut result = Vec::with_capacity(self.subscriptions.len());
        // Drain all entries
        self.subscriptions.retain(|_k, v| {
            result.push(f(v));
            false // remove every entry
        });
        result
    }

    /// Update snapshot_end_seq for a subscription.
    pub fn update_snapshot_end_seq(&self, subscription_id: &str, snapshot_end_seq: Option<SeqId>) {
        if let Some(mut sub) = self.subscriptions.get_mut(subscription_id) {
            sub.snapshot_end_seq = snapshot_end_seq;
            sub.flow_control.set_snapshot_end_seq(snapshot_end_seq);
        }
    }

    /// Mark initial load complete and flush buffered notifications.
    ///
    /// Clones the flow_control Arc and drops the DashMap ref before flushing
    /// to avoid holding the shard lock during channel sends.
    pub fn complete_initial_load(&self, subscription_id: &str) -> usize {
        let flow_control = match self.subscriptions.get(subscription_id) {
            Some(sub) => Arc::clone(&sub.flow_control),
            None => return 0,
        };
        // DashMap ref dropped — shard lock released before sending.

        flow_control.mark_initial_complete();
        let buffered = flow_control.drain_buffered_notifications();

        let mut sent = 0usize;
        for item in buffered {
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
    }

    /// Increment current batch number for a subscription and return the new value.
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
