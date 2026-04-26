//! Connection-related models for WebSocket and consumer connections
//!
//! These models support both live query WebSocket subscriptions and topic consumer connections.

use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, OnceLock, Weak,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use datafusion::sql::sqlparser::ast::Expr;
use kalamdb_commons::{
    ids::SeqId,
    models::{ConnectionId, ConnectionInfo, LiveQueryId, TableId, UserId},
    websocket::{CompressionType, ProtocolOptions, SerializationType, WireNotification},
    Role,
};
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

/// Get current epoch time in milliseconds (for lock-free heartbeat and metadata tracking)
#[inline]
pub(crate) fn epoch_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[derive(Default)]
struct SubscriptionStringPool {
    buckets: HashMap<u64, Vec<Weak<str>>>,
}

impl SubscriptionStringPool {
    fn intern(&mut self, value: &str) -> Arc<str> {
        let hash = hash_subscription_str(value);
        let bucket = self.buckets.entry(hash).or_default();
        let mut dead_entries = 0usize;

        for weak in bucket.iter() {
            match weak.upgrade() {
                Some(existing) if existing.as_ref() == value => return existing,
                Some(_) => {},
                None => dead_entries += 1,
            }
        }

        if dead_entries > 0 {
            bucket.retain(|weak| weak.strong_count() > 0);
        }

        let interned: Arc<str> = Arc::from(value);
        bucket.push(Arc::downgrade(&interned));
        interned
    }
}

fn hash_subscription_str(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn subscription_string_pool() -> &'static Mutex<SubscriptionStringPool> {
    static STRING_POOL: OnceLock<Mutex<SubscriptionStringPool>> = OnceLock::new();
    STRING_POOL.get_or_init(|| Mutex::new(SubscriptionStringPool::default()))
}

fn intern_subscription_str(value: &str) -> Arc<str> {
    if value.is_empty() {
        return Arc::from("");
    }

    subscription_string_pool().lock().intern(value)
}

/// Maximum live-query subscriptions allowed on a single WebSocket connection.
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 100;

/// Maximum pending notifications per connection before dropping new ones.
/// This must cover one full table fanout for a saturated shared WebSocket.
/// The benchmark and backend contract allow 100 subscriptions per connection;
/// 128 keeps a small cushion while staying tight for idle-connection memory.
pub const NOTIFICATION_CHANNEL_CAPACITY: usize = 128;

/// Maximum pending control events per connection.
/// Only a few event kinds exist (auth timeout, heartbeat timeout, shutdown),
/// so a small queue is sufficient and reduces fixed per-connection footprint.
pub const EVENT_CHANNEL_CAPACITY: usize = 1;

/// Maximum buffered notifications per subscription while initial snapshot loading is in progress.
///
/// Without this bound, a slow initial snapshot can accumulate unbounded change events and
/// exhaust memory under high write volume or many concurrent subscribers.
pub const MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION: usize = 1_024;

/// Type alias for sending notifications to WebSocket or consumer clients
pub type NotificationSender = mpsc::Sender<Arc<WireNotification>>;

/// Type alias for receiving notifications
pub type NotificationReceiver = mpsc::Receiver<Arc<WireNotification>>;

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
    /// Stable subscription identifier shared across all notifications for this subscriber.
    pub subscription_id: Arc<str>,
    /// Shared filter expression (Arc for zero-copy across indices)
    pub filter_expr: Option<Arc<Expr>>,
    /// Column projections for filtering notification payload (None = all columns)
    pub projections: Option<Arc<Vec<String>>>,
    /// Shared notification channel
    pub notification_tx: NotificationSender,
    /// Flow control for initial load buffering and snapshot gating.
    /// None means the subscription was created without initial data.
    pub flow_control: Option<Arc<SubscriptionFlowControl>>,
    /// Runtime metadata shared with the full subscription state.
    pub runtime_metadata: Arc<SubscriptionRuntimeMetadata>,
}

/// In-memory metadata tracked for active subscriptions only.
#[derive(Debug)]
pub struct SubscriptionRuntimeMetadata {
    query: Arc<str>,
    options_json: Option<Arc<str>>,
    created_at_ms: i64,
    last_update_ms: AtomicI64,
    changes: AtomicI64,
}

impl SubscriptionRuntimeMetadata {
    pub fn new(query: &str, options_json: Option<&str>, created_at_ms: i64) -> Self {
        Self {
            query: intern_subscription_str(query),
            options_json: options_json.map(intern_subscription_str),
            created_at_ms,
            last_update_ms: AtomicI64::new(created_at_ms),
            changes: AtomicI64::new(0),
        }
    }

    #[inline]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[inline]
    pub fn options_json(&self) -> Option<&str> {
        self.options_json.as_deref()
    }

    #[inline]
    pub fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[inline]
    pub fn last_update_ms(&self) -> i64 {
        self.last_update_ms.load(Ordering::Acquire)
    }

    #[inline]
    pub fn changes(&self) -> i64 {
        self.changes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn record_delivery(&self) {
        self.record_delivery_at(epoch_millis());
    }

    #[inline]
    pub fn record_delivery_at(&self, epoch_millis: u64) {
        self.last_update_ms.store(epoch_millis as i64, Ordering::Release);
        self.changes.fetch_add(1, Ordering::AcqRel);
    }
}

/// Buffered notification with optional SeqId ordering key
#[derive(Debug, Clone)]
pub struct BufferedNotification {
    pub seq: Option<SeqId>,
    pub commit_seq: Option<u64>,
    pub notification: Arc<WireNotification>,
}

/// Flow control for subscription initial load gating
#[derive(Debug)]
pub struct SubscriptionFlowControl {
    snapshot_end_seq: AtomicI64,
    snapshot_end_commit_seq: AtomicU64,
    has_snapshot: AtomicBool,
    has_commit_snapshot: AtomicBool,
    initial_complete: AtomicBool,
    buffer: Mutex<VecDeque<BufferedNotification>>,
}

impl SubscriptionFlowControl {
    pub fn new() -> Self {
        Self {
            snapshot_end_seq: AtomicI64::new(0),
            snapshot_end_commit_seq: AtomicU64::new(0),
            has_snapshot: AtomicBool::new(false),
            has_commit_snapshot: AtomicBool::new(false),
            initial_complete: AtomicBool::new(false),
            buffer: Mutex::new(VecDeque::new()),
        }
    }

    pub fn set_snapshot_end_seq(&self, snapshot_end_seq: Option<SeqId>) {
        self.set_snapshot_boundaries(snapshot_end_seq, None);
    }

    pub fn set_snapshot_boundaries(
        &self,
        snapshot_end_seq: Option<SeqId>,
        snapshot_end_commit_seq: Option<u64>,
    ) {
        if let Some(seq) = snapshot_end_seq {
            self.snapshot_end_seq.store(seq.as_i64(), Ordering::Release);
            self.has_snapshot.store(true, Ordering::Release);
        } else {
            self.has_snapshot.store(false, Ordering::Release);
        }

        if let Some(commit_seq) = snapshot_end_commit_seq {
            self.snapshot_end_commit_seq.store(commit_seq, Ordering::Release);
            self.has_commit_snapshot.store(true, Ordering::Release);
        } else {
            self.has_commit_snapshot.store(false, Ordering::Release);
        }

        if snapshot_end_seq.is_some() || snapshot_end_commit_seq.is_some() {
            let max_seq = snapshot_end_seq.map(|seq| seq.as_i64());
            let mut buffer = self.buffer.lock();
            buffer.retain(|item| match item.seq {
                Some(item_seq) if snapshot_end_commit_seq.is_none() => {
                    max_seq.map(|seq| item_seq.as_i64() > seq).unwrap_or(true)
                },
                _ => match (snapshot_end_commit_seq, item.commit_seq) {
                    (Some(max_commit), Some(item_commit)) => item_commit > max_commit,
                    _ => true,
                },
            });
        }
    }

    pub fn snapshot_end_seq(&self) -> Option<i64> {
        if self.has_snapshot.load(Ordering::Acquire) {
            Some(self.snapshot_end_seq.load(Ordering::Acquire))
        } else {
            None
        }
    }

    pub fn snapshot_end_commit_seq(&self) -> Option<u64> {
        if self.has_commit_snapshot.load(Ordering::Acquire) {
            Some(self.snapshot_end_commit_seq.load(Ordering::Acquire))
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

    pub fn buffer_notification(
        &self,
        notification: Arc<WireNotification>,
        seq: Option<SeqId>,
        commit_seq: Option<u64>,
    ) {
        let mut buffer = self.buffer.lock();
        if buffer.len() >= MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION {
            buffer.pop_front();
        }
        buffer.push_back(BufferedNotification {
            seq,
            commit_seq,
            notification,
        });
    }

    pub fn drain_buffered_notifications(&self) -> Vec<BufferedNotification> {
        let mut buffer = self.buffer.lock();
        // Sort in-place via contiguous slice, then drain — avoids a second Vec allocation
        let slice = buffer.make_contiguous();
        slice.sort_by(|a, b| {
            let commit_order = match (a.commit_seq, b.commit_seq) {
                (Some(a_commit), Some(b_commit)) => a_commit.cmp(&b_commit),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            if commit_order != std::cmp::Ordering::Equal {
                return commit_order;
            }

            match (a.seq, b.seq) {
                (Some(a_seq), Some(b_seq)) => a_seq.as_i64().cmp(&b_seq.as_i64()),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        buffer.drain(..).collect()
    }
}

/// Optional initial-load state for subscriptions that fetch a snapshot.
#[derive(Debug, Clone)]
pub struct InitialLoadState {
    /// Batch size for initial data loading
    pub batch_size: usize,
    /// Snapshot boundary SeqId for consistent batch loading
    pub snapshot_end_seq: Option<SeqId>,
    /// Deterministic snapshot boundary for reconnects across followers
    pub snapshot_end_commit_seq: Option<u64>,
    /// Current batch number for pagination tracking (0-indexed)
    /// Incremented after each batch is sent
    pub current_batch_num: u32,
    /// Flow control for initial load buffering and snapshot gating
    pub flow_control: Arc<SubscriptionFlowControl>,
}

/// Subscription state - stored only in ConnectionState.subscriptions
///
/// Contains all metadata needed for:
/// - Notification filtering (filter_expr)
/// - Column projections (projections)
/// - Optional initial data batch fetching and pagination
///
/// Memory optimization: Uses Arc<Expr> for filter sharing and allocates
/// snapshot/batch state only when the subscription actually requests it.
#[derive(Debug, Clone)]
pub struct SubscriptionState {
    pub live_id: LiveQueryId,
    pub table_id: TableId,
    /// Compiled filter expression from WHERE clause (parsed once at subscription time)
    /// None means no filter (SELECT * without WHERE)
    /// Arc-wrapped for sharing with SubscriptionHandle
    pub filter_expr: Option<Arc<Expr>>,
    /// Column projections from SELECT clause (None = SELECT *, i.e., all columns)
    /// Arc-wrapped for sharing with SubscriptionHandle
    pub projections: Option<Arc<Vec<String>>>,
    /// Optional initial-load state. None means the subscription streams live changes only.
    pub initial_load: Option<InitialLoadState>,
    /// Whether this subscription is for a shared table (affects index cleanup)
    pub is_shared: bool,
    /// Runtime metadata exposed by the in-memory live views.
    pub runtime_metadata: Arc<SubscriptionRuntimeMetadata>,
}

/// Connection state — lock-free interior mutability for 100k+ concurrent connections.
///
/// Identity and channel fields are immutable after construction.
/// Auth fields use set-once primitives (OnceLock + AtomicBool).
/// Subscriptions use a compact per-connection map.
/// Notification fanout reads from manager-level indices, so connection-local
/// subscription access is low-contention and does not need a sharded map.
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

    // === Protocol negotiation (set-once at auth time) ===
    protocol: OnceLock<ProtocolOptions>,

    // === Heartbeat (atomic) ===
    last_heartbeat_ms: AtomicU64,

    // === Subscriptions (concurrent interior-mutable) ===
    subscriptions: RwLock<HashMap<Arc<str>, SubscriptionState>>,

    // === Channels (immutable after construction) ===
    pub notification_tx: NotificationSender,
    pub event_tx: EventSender,
}

impl ConnectionState {
    /// Create a new connection state with the given identity and channels.
    pub fn new(
        connection_id: ConnectionId,
        client_ip: ConnectionInfo,
        notification_tx: NotificationSender,
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
            protocol: OnceLock::new(),
            last_heartbeat_ms: AtomicU64::new(epoch_millis()),
            subscriptions: RwLock::new(HashMap::new()),
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

    // === Protocol ===

    /// Store negotiated protocol options (set-once at auth time).
    #[inline]
    pub fn set_protocol(&self, opts: ProtocolOptions) {
        let _ = self.protocol.set(opts);
    }

    /// Negotiated serialization type (defaults to Json if not yet set).
    #[inline]
    pub fn serialization_type(&self) -> SerializationType {
        self.protocol.get().map_or(SerializationType::Json, |p| p.serialization)
    }

    /// Negotiated compression type (defaults to Gzip if not yet set).
    #[inline]
    pub fn compression_type(&self) -> CompressionType {
        self.protocol.get().map_or(CompressionType::Gzip, |p| p.compression)
    }

    /// Negotiated protocol options (defaults if not yet set).
    #[inline]
    pub fn protocol(&self) -> ProtocolOptions {
        self.protocol.get().copied().unwrap_or_default()
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

    /// Last observed heartbeat epoch timestamp in milliseconds.
    #[inline]
    pub fn last_heartbeat_ms(&self) -> u64 {
        self.last_heartbeat_ms.load(Ordering::Acquire)
    }

    // === Subscription management ===

    /// Number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.read().len()
    }

    /// Insert a subscription into the connection's map.
    pub fn insert_subscription(&self, key: Arc<str>, state: SubscriptionState) {
        self.subscriptions.write().insert(key, state);
    }

    /// Remove a subscription by key, returning the removed value.
    pub fn remove_subscription(&self, key: &str) -> Option<(Arc<str>, SubscriptionState)> {
        self.subscriptions.write().remove_entry(key)
    }

    /// Remove a subscription by primary key, falling back to a secondary key
    /// produced by `fallback_fn` if the primary is not found.
    pub fn remove_subscription_with_fallback<F>(
        &self,
        primary_key: &str,
        fallback_fn: F,
    ) -> Option<(Arc<str>, SubscriptionState)>
    where
        F: FnOnce() -> Option<String>,
    {
        let mut subscriptions = self.subscriptions.write();
        subscriptions
            .remove_entry(primary_key)
            .or_else(|| fallback_fn().and_then(|key| subscriptions.remove_entry(key.as_str())))
    }

    /// Get a subscription by ID (cloned out of the connection map).
    pub fn get_subscription(&self, subscription_id: &str) -> Option<SubscriptionState> {
        self.subscriptions.read().get(subscription_id).cloned()
    }

    /// Iterate all subscriptions, calling `f` for each.
    pub fn for_each_subscription<F>(&self, mut f: F)
    where
        F: FnMut(&str, &SubscriptionState),
    {
        let subscriptions = self.subscriptions.read();
        for (key, value) in subscriptions.iter() {
            f(key.as_ref(), value);
        }
    }

    /// Collect info from all subscriptions, removing them in the process.
    /// Returns the collected values.
    pub fn collect_subscription_info<F, T>(&self, mut f: F) -> Vec<T>
    where
        F: FnMut(&SubscriptionState) -> T,
    {
        let mut subscriptions = self.subscriptions.write();
        let mut result = Vec::with_capacity(subscriptions.len());
        for (_, value) in subscriptions.drain() {
            result.push(f(&value));
        }
        result
    }

    /// Update snapshot_end_seq for a subscription.
    pub fn update_snapshot_end_seq(&self, subscription_id: &str, snapshot_end_seq: Option<SeqId>) {
        self.update_snapshot_boundaries(subscription_id, snapshot_end_seq, None);
    }

    /// Update snapshot boundaries for a subscription.
    pub fn update_snapshot_boundaries(
        &self,
        subscription_id: &str,
        snapshot_end_seq: Option<SeqId>,
        snapshot_end_commit_seq: Option<u64>,
    ) {
        if let Some(sub) = self.subscriptions.write().get_mut(subscription_id) {
            if let Some(initial_load) = sub.initial_load.as_mut() {
                initial_load.snapshot_end_seq = snapshot_end_seq;
                initial_load.snapshot_end_commit_seq = snapshot_end_commit_seq;
                initial_load
                    .flow_control
                    .set_snapshot_boundaries(snapshot_end_seq, snapshot_end_commit_seq);
            }
        }
    }

    /// Mark initial load complete and flush buffered notifications.
    ///
    /// Clones the flow_control Arc and drops the map read lock before flushing
    /// to avoid holding the lock during channel sends.
    pub fn complete_initial_load(&self, subscription_id: &str) -> usize {
        let (flow_control, runtime_metadata) = {
            let subscriptions = self.subscriptions.read();
            match subscriptions.get(subscription_id) {
                Some(sub) => (
                    sub.initial_load
                        .as_ref()
                        .map(|initial_load| Arc::clone(&initial_load.flow_control)),
                    Arc::clone(&sub.runtime_metadata),
                ),
                None => return 0,
            }
        };
        let Some(flow_control) = flow_control else {
            return 0;
        };
        // Map guard dropped before sending.

        flow_control.mark_initial_complete();
        let buffered = flow_control.drain_buffered_notifications();

        let mut sent = 0usize;
        let delivery_timestamp_ms = epoch_millis();
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
                runtime_metadata.record_delivery_at(delivery_timestamp_ms);
                sent += 1;
            }
        }
        sent
    }

    /// Increment current batch number for a subscription and return the new value.
    pub fn increment_batch_num(&self, subscription_id: &str) -> Option<u32> {
        if let Some(sub) = self.subscriptions.write().get_mut(subscription_id) {
            if let Some(initial_load) = sub.initial_load.as_mut() {
                initial_load.current_batch_num += 1;
                Some(initial_load.current_batch_num)
            } else {
                None
            }
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
    use kalamdb_commons::websocket::{ChangeType, SharedChangePayload};

    use super::*;

    fn make_notification(subscription_id: &str) -> Arc<WireNotification> {
        Arc::new(WireNotification {
            subscription_id: Arc::from(subscription_id),
            payload: Arc::new(SharedChangePayload::new(ChangeType::Insert, Some(Vec::new()), None)),
        })
    }

    #[test]
    fn test_subscription_flow_control_limits_buffer_growth() {
        let flow_control = SubscriptionFlowControl::new();

        for seq in 1..=2_048 {
            flow_control.buffer_notification(
                make_notification("sub-1"),
                Some(SeqId::from(seq)),
                None,
            );
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

        flow_control.buffer_notification(
            make_notification("sub-ordered"),
            Some(SeqId::from(9)),
            None,
        );
        flow_control.buffer_notification(
            make_notification("sub-ordered"),
            Some(SeqId::from(3)),
            None,
        );
        flow_control.buffer_notification(
            make_notification("sub-ordered"),
            Some(SeqId::from(6)),
            None,
        );

        let buffered = flow_control.drain_buffered_notifications();
        let seqs: Vec<_> = buffered
            .into_iter()
            .map(|item| item.seq.expect("seq should be present").as_i64())
            .collect();

        assert_eq!(seqs, vec![3, 6, 9]);
    }

    #[test]
    fn test_subscription_runtime_metadata_interns_query_and_options() {
        let first = SubscriptionRuntimeMetadata::new(
            "SELECT * FROM shared.events",
            Some(r#"{"batch_size":100}"#),
            1,
        );
        let second = SubscriptionRuntimeMetadata::new(
            "SELECT * FROM shared.events",
            Some(r#"{"batch_size":100}"#),
            2,
        );

        assert!(Arc::ptr_eq(&first.query, &second.query));
        assert!(Arc::ptr_eq(
            first.options_json.as_ref().expect("options should exist"),
            second.options_json.as_ref().expect("options should exist"),
        ));
    }
}
