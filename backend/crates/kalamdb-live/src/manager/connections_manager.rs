//! Connections Manager
//!
//! Unified manager for both WebSocket and consumer connections:
//! - Connection lifecycle (connected, authenticated, subscriptions)
//! - Heartbeat tracking (last activity, auth timeout)
//! - Subscription management
//! - Notification delivery
//! - Graceful shutdown coordination
//!
//! ## Heartbeat Design
//!
//! Liveness is driven by the **client SDK**, not the server:
//! - the KalamDB SDK sends a WebSocket `Ping` frame every `keepalive_interval` seconds
//! - The server handler calls `update_heartbeat()` on every incoming frame (Ping, Pong, Text)
//! - The background checker only scans for *timed-out* connections — it never initiates pings
//!
//! Benefits over server-driven pings:
//! - No thundering-herd: each client pings on its own schedule, naturally staggered
//! - Dead TCP detected at the client (write failure) rather than waiting for timeout
//! - O(N) atomic reads per tick — no mpsc events, no synchronised wakeups

use super::super::models::{
    ConnectionEvent, ConnectionRegistration, ConnectionState, SharedConnectionState,
    SubscriptionHandle, EVENT_CHANNEL_CAPACITY, NOTIFICATION_CHANNEL_CAPACITY,
};
use dashmap::DashMap;
use kalamdb_commons::models::{ConnectionId, ConnectionInfo, LiveQueryId, TableId, UserId};
use kalamdb_commons::NodeId;
#[cfg(any(test, feature = "test-helpers"))]
use kalamdb_commons::WireNotification;
use kalamdb_system::{LiveQuery, LiveQueryStatus};
use log::{debug, info, warn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Connections Manager
///
/// Responsibilities:
/// - Connection lifecycle (register, unregister, heartbeat check)
/// - Authentication state management
/// - Maintaining indices for efficient lookups
/// - Background heartbeat checker
///
/// Used by both:
/// - WebSocket handlers for live queries
/// - HTTP handlers for topic consumer long polling
///
/// NOTE: Subscription management is delegated to LiveQueryManager and TopicConsumerManager.
/// The manager only maintains indices for notification routing.
pub struct ConnectionsManager {
    // === Primary Storage ===
    /// All active connections: ConnectionId → SharedConnectionState
    connections: DashMap<ConnectionId, SharedConnectionState>,

    // === Secondary Indices (for efficient lookups) ===
    /// (UserId, TableId) → DashMap<LiveQueryId, SubscriptionHandle> for O(1) notification delivery
    /// Uses lightweight handles (~48 bytes) instead of full state (~800+ bytes)
    user_table_subscriptions:
        DashMap<(UserId, TableId), Arc<DashMap<LiveQueryId, SubscriptionHandle>>>,
    /// TableId → DashMap<LiveQueryId, SubscriptionHandle> for shared table subscriptions
    /// Shared tables are not scoped per-user, so notifications go to all subscribers
    shared_table_subscriptions: DashMap<TableId, Arc<DashMap<LiveQueryId, SubscriptionHandle>>>,
    /// Shared empty map to avoid allocations on lookup misses
    empty_subscriptions: Arc<DashMap<LiveQueryId, SubscriptionHandle>>,

    // === Configuration ===
    /// Node identifier for this server
    pub node_id: NodeId,
    /// Client heartbeat timeout
    client_timeout: Duration,
    /// Authentication timeout
    auth_timeout: Duration,
    /// Heartbeat check interval
    heartbeat_interval: Duration,
    /// Maximum concurrent connections allowed (DoS protection)
    max_connections: usize,

    // === Shutdown Coordination ===
    shutdown_token: CancellationToken,
    is_shutting_down: AtomicBool,

    // === Metrics ===
    total_connections: AtomicUsize,
    peak_connections: AtomicUsize,
    total_subscriptions: AtomicUsize,
    peak_subscriptions: AtomicUsize,
}

impl ConnectionsManager {
    /// Release retained DashMap bucket capacity once the live registry is fully idle.
    pub fn trim_idle_capacity(&self) {
        if self.connection_count() > 0 || self.subscription_count() > 0 {
            return;
        }

        self.connections.shrink_to_fit();
        self.user_table_subscriptions.shrink_to_fit();
        self.shared_table_subscriptions.shrink_to_fit();
        self.empty_subscriptions.shrink_to_fit();
    }

    /// Default maximum connections (100,000 concurrent connections)
    pub const DEFAULT_MAX_CONNECTIONS: usize = 100_000;

    /// Create a new connections manager and start the background heartbeat checker
    pub fn new(
        node_id: NodeId,
        client_timeout: Duration,
        auth_timeout: Duration,
        heartbeat_interval: Duration,
    ) -> Arc<Self> {
        Self::with_max_connections(
            node_id,
            client_timeout,
            auth_timeout,
            heartbeat_interval,
            Self::DEFAULT_MAX_CONNECTIONS,
        )
    }

    /// Create a new connections manager with a custom max connections limit
    pub fn with_max_connections(
        node_id: NodeId,
        client_timeout: Duration,
        auth_timeout: Duration,
        heartbeat_interval: Duration,
        max_connections: usize,
    ) -> Arc<Self> {
        let shutdown_token = CancellationToken::new();

        let registry = Arc::new(Self {
            connections: DashMap::new(),
            user_table_subscriptions: DashMap::new(),
            shared_table_subscriptions: DashMap::new(),
            empty_subscriptions: Arc::new(DashMap::new()),
            node_id,
            client_timeout,
            auth_timeout,
            heartbeat_interval,
            max_connections,
            shutdown_token,
            is_shutting_down: AtomicBool::new(false),
            total_connections: AtomicUsize::new(0),
            peak_connections: AtomicUsize::new(0),
            total_subscriptions: AtomicUsize::new(0),
            peak_subscriptions: AtomicUsize::new(0),
        });

        // Start background heartbeat checker
        let registry_clone = registry.clone();
        tokio::spawn(async move {
            registry_clone.run_heartbeat_checker().await;
        });

        debug!(
            "ConnectionsManager initialized (node={}, client_timeout={}s, auth_timeout={}s)",
            registry.node_id,
            client_timeout.as_secs(),
            auth_timeout.as_secs()
        );

        registry
    }

    // ==================== Connection Lifecycle ====================

    /// Register a new connection (called immediately when WebSocket opens)
    ///
    /// Returns `ConnectionRegistration` containing:
    /// - `SharedConnectionState` - hold this for direct access to connection state
    /// - `event_rx` - control events (ping, timeout, shutdown)
    /// - `notification_rx` - live query notifications
    ///
    /// Returns None if server is shutting down or max connections reached.
    pub fn register_connection(
        &self,
        connection_id: ConnectionId,
        client_ip: ConnectionInfo,
    ) -> Option<ConnectionRegistration> {
        if self.is_shutting_down.load(Ordering::Acquire) {
            warn!("Rejecting new connection during shutdown: {}", connection_id);
            return None;
        }

        // DoS protection: reject if at max connections
        let current = self.total_connections.load(Ordering::Acquire);
        if current >= self.max_connections {
            warn!(
                "Rejecting connection {}: max connections ({}) reached",
                connection_id, self.max_connections
            );
            return None;
        }

        // Use bounded channels for backpressure and memory control
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (notification_tx, notification_rx) = mpsc::channel(NOTIFICATION_CHANNEL_CAPACITY);

        let state =
            ConnectionState::new(connection_id.clone(), client_ip, notification_tx, event_tx);

        let shared_state = Arc::new(state);
        self.connections.insert(connection_id.clone(), Arc::clone(&shared_state));
        let count = self.total_connections.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_connections.fetch_max(count, Ordering::AcqRel);

        Some(ConnectionRegistration {
            connection_id,
            state: shared_state,
            notification_rx,
            event_rx,
        })
    }

    /// Unregister a connection and all its subscriptions
    ///
    /// Returns the list of removed LiveQueryIds for cleanup.
    pub fn unregister_connection(&self, connection_id: &ConnectionId) -> Vec<LiveQueryId> {
        debug!("unregister_connection: starting cleanup for connection {}", connection_id);
        let removed_live_ids = if let Some((_, shared_state)) =
            self.connections.remove(connection_id)
        {
            self.total_connections.fetch_sub(1, Ordering::AcqRel);

            // Remove from user_table_subscriptions and shared_table_subscriptions indices
            if let Some(user_id) = shared_state.user_id() {
                shared_state.for_each_subscription(|_id, sub| {
                    // Try user_table_subscriptions first
                    let key = (user_id.clone(), sub.table_id.clone());
                    if let Some(entries) = self.user_table_subscriptions.get(&key) {
                        entries.remove(&sub.live_id);
                    }
                    self.user_table_subscriptions.remove_if(&key, |_, handles| handles.is_empty());

                    // Also try shared_table_subscriptions
                    if let Some(entries) = self.shared_table_subscriptions.get(&sub.table_id) {
                        entries.remove(&sub.live_id);
                    }
                    self.shared_table_subscriptions
                        .remove_if(&sub.table_id, |_, handles| handles.is_empty());
                });
            }

            // Collect and remove all subscriptions
            let removed = shared_state.collect_subscription_info(|sub| sub.live_id.clone());

            let sub_count = removed.len();
            if sub_count > 0 {
                self.total_subscriptions.fetch_sub(sub_count, Ordering::AcqRel);
            }

            removed
        } else {
            Vec::new()
        };

        if !removed_live_ids.is_empty() {
            debug!(
                "Connection unregistered: {} (removed {} subscriptions)",
                connection_id,
                removed_live_ids.len()
            );
        }

        if self.connection_count() == 0 && self.subscription_count() == 0 {
            self.trim_idle_capacity();
        }

        removed_live_ids
    }

    // ==================== Subscription Index Management ====================
    // NOTE: Actual subscription storage is in ConnectionState.subscriptions
    // These methods maintain secondary indices for efficient notification routing

    /// Add subscription to user_table_subscriptions index (called by LiveQueryManager after adding to ConnectionState)
    ///
    /// Uses lightweight SubscriptionHandle for the index instead of cloning full state.
    pub fn index_subscription(
        &self,
        user_id: &UserId,
        connection_id: &ConnectionId,
        live_id: LiveQueryId,
        table_id: TableId,
        handle: SubscriptionHandle,
    ) {
        log::debug!(
            "ConnectionsManager::index_subscription: user={}, table={}, live_id={}",
            user_id,
            table_id,
            live_id
        );
        // Add to (UserId, TableId) → Arc<DashMap<LiveQueryId, SubscriptionHandle>> index
        self.user_table_subscriptions
            .entry((user_id.clone(), table_id))
            .and_modify(|handles| {
                handles.insert(live_id.clone(), handle.clone());
            })
            .or_insert_with(|| {
                let handles = DashMap::new();
                handles.insert(live_id.clone(), handle.clone());
                Arc::new(handles)
            });
        let _ = connection_id;

        let count = self.total_subscriptions.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_subscriptions.fetch_max(count, Ordering::AcqRel);
    }

    /// Add subscription to shared_table_subscriptions index (for shared tables)
    ///
    /// Shared tables are not scoped per-user, so all subscribers receive notifications
    /// regardless of which user made the change.
    pub fn index_shared_subscription(
        &self,
        connection_id: &ConnectionId,
        live_id: LiveQueryId,
        table_id: TableId,
        handle: SubscriptionHandle,
    ) {
        log::debug!(
            "ConnectionsManager::index_shared_subscription: table={}, live_id={}",
            table_id,
            live_id
        );
        // Add to TableId → Arc<DashMap<LiveQueryId, SubscriptionHandle>> index
        self.shared_table_subscriptions
            .entry(table_id)
            .and_modify(|handles| {
                handles.insert(live_id.clone(), handle.clone());
            })
            .or_insert_with(|| {
                let handles = DashMap::new();
                handles.insert(live_id.clone(), handle.clone());
                Arc::new(handles)
            });
        let _ = connection_id;

        let count = self.total_subscriptions.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_subscriptions.fetch_max(count, Ordering::AcqRel);
    }

    /// Remove subscription from indices (called by LiveQueryManager after removing from ConnectionState)
    pub fn unindex_subscription(
        &self,
        user_id: &UserId,
        live_id: &LiveQueryId,
        table_id: &TableId,
    ) {
        // Remove from user_table_subscriptions index
        let key = (user_id.clone(), table_id.clone());
        if let Some(handles) = self.user_table_subscriptions.get(&key) {
            handles.remove(live_id);
            if handles.is_empty() {
                drop(handles);
                self.user_table_subscriptions.remove(&key);
            }
        }

        self.total_subscriptions.fetch_sub(1, Ordering::AcqRel);
    }

    /// Remove shared table subscription from indices
    pub fn unindex_shared_subscription(&self, live_id: &LiveQueryId, table_id: &TableId) {
        // Remove from shared_table_subscriptions index
        if let Some(handles) = self.shared_table_subscriptions.get(table_id) {
            handles.remove(live_id);
            if handles.is_empty() {
                drop(handles);
                self.shared_table_subscriptions.remove(table_id);
            }
        }

        self.total_subscriptions.fetch_sub(1, Ordering::AcqRel);
    }

    // ==================== Query Methods ====================

    /// Get shared connection state by ID (rarely needed - handlers should hold their own reference)
    pub fn get_connection(&self, connection_id: &ConnectionId) -> Option<SharedConnectionState> {
        self.connections.get(connection_id).map(|c| c.clone())
    }

    /// Check if any subscriptions exist for a (user, table) pair
    pub fn has_subscriptions(&self, user_id: &UserId, table_id: &TableId) -> bool {
        self.user_table_subscriptions.contains_key(&(user_id.clone(), table_id.clone()))
    }

    /// Check if any shared table subscriptions exist for a table
    pub fn has_shared_subscriptions(&self, table_id: &TableId) -> bool {
        self.shared_table_subscriptions.contains_key(table_id)
    }

    /// Get subscription handles for a specific (user, table) pair for notification routing
    ///
    /// Returns lightweight handles containing only the data needed for filtering and routing.
    #[inline]
    pub fn get_subscriptions_for_table(
        &self,
        user_id: &UserId,
        table_id: &TableId,
    ) -> Arc<DashMap<LiveQueryId, SubscriptionHandle>> {
        self.user_table_subscriptions
            .get(&(user_id.clone(), table_id.clone()))
            .map(|handles| Arc::clone(handles.value()))
            .unwrap_or_else(|| Arc::clone(&self.empty_subscriptions))
    }

    /// Get subscription handles for a shared table for notification routing
    ///
    /// Returns all subscribers to a shared table regardless of which user made the change.
    #[inline]
    pub fn get_shared_subscriptions_for_table(
        &self,
        table_id: &TableId,
    ) -> Arc<DashMap<LiveQueryId, SubscriptionHandle>> {
        self.shared_table_subscriptions
            .get(table_id)
            .map(|handles| Arc::clone(handles.value()))
            .unwrap_or_else(|| Arc::clone(&self.empty_subscriptions))
    }

    /// Send a notification directly to all authenticated connections for a user.
    ///
    /// This bypasses subscription indexing and is intended only for transport-layer
    /// tests that need to exercise the connection notification channel itself.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn notify_connections_for_user(
        &self,
        user_id: &UserId,
        notification: WireNotification,
    ) -> bool {
        let notification = Arc::new(notification);
        let mut sent = false;

        for state in self.connections.iter() {
            if state.user_id() == Some(user_id) {
                if state.notification_tx.try_send(Arc::clone(&notification)).is_ok() {
                    sent = true;
                }
            }
        }

        sent
    }

    // ==================== Shutdown ====================

    /// Initiate graceful shutdown of all connections
    pub async fn shutdown(&self, timeout: Duration) {
        let count = self.total_connections.load(Ordering::Acquire);
        info!("Initiating WebSocket shutdown with {} active connections", count);

        self.is_shutting_down.store(true, Ordering::Release);

        // Send shutdown event to all connections
        let mut force_unregister = Vec::new();
        for entry in self.connections.iter() {
            let conn_id = entry.key().clone();
            let state = entry.value();
            match state.event_tx.try_send(ConnectionEvent::Shutdown) {
                Ok(_) => {},
                Err(mpsc::error::TrySendError::Full(_))
                | Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Handler is likely stalled or gone; unregister immediately so shutdown doesn't wait forever.
                    force_unregister.push(conn_id);
                },
            }
        }

        for conn_id in force_unregister {
            self.unregister_connection(&conn_id);
        }

        // Wait for connections to close with timeout
        let deadline = Instant::now() + timeout;
        while self.total_connections.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let remaining = self.total_connections.load(Ordering::Acquire);
        if remaining > 0 {
            warn!("Force closing {} connections after timeout", remaining);
            self.connections.clear();
            self.user_table_subscriptions.clear();
            self.shared_table_subscriptions.clear();
            self.total_connections.store(0, Ordering::Release);
            self.total_subscriptions.store(0, Ordering::Release);
        }

        self.shutdown_token.cancel();
        info!("WebSocket shutdown complete");
    }

    /// Check if shutdown is in progress
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Acquire)
    }

    // ==================== Metrics ====================

    pub fn connection_count(&self) -> usize {
        self.total_connections.load(Ordering::Acquire)
    }

    pub fn peak_connection_count(&self) -> usize {
        self.peak_connections.load(Ordering::Acquire)
    }

    pub fn subscription_count(&self) -> usize {
        self.total_subscriptions.load(Ordering::Acquire)
    }

    pub fn peak_subscription_count(&self) -> usize {
        self.peak_subscriptions.load(Ordering::Acquire)
    }

    pub fn max_connection_limit(&self) -> usize {
        self.max_connections
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Snapshot active subscriptions into live-query rows backed by in-memory state.
    pub fn snapshot_live_queries(&self, node_id: NodeId) -> Vec<LiveQuery> {
        let mut live_queries = Vec::with_capacity(self.subscription_count());

        for entry in self.connections.iter() {
            let state = entry.value();
            let Some(user_id) = state.user_id().cloned() else {
                continue;
            };

            let connection_id = state.connection_id().as_str().to_string();
            let last_ping_at = state.last_heartbeat_ms() as i64;

            state.for_each_subscription(|subscription_id, subscription| {
                let runtime_metadata = &subscription.runtime_metadata;
                live_queries.push(LiveQuery {
                    live_id: subscription.live_id.clone(),
                    connection_id: connection_id.clone(),
                    subscription_id: subscription_id.to_string(),
                    namespace_id: subscription.table_id.namespace_id().clone(),
                    table_name: subscription.table_id.table_name().clone(),
                    user_id: user_id.clone(),
                    query: runtime_metadata.query().to_string(),
                    options: runtime_metadata.options_json()
                        .and_then(|s| serde_json::from_str(s).ok()),
                    status: LiveQueryStatus::Active,
                    created_at: runtime_metadata.created_at_ms(),
                    last_update: runtime_metadata.last_update_ms(),
                    last_ping_at,
                    changes: runtime_metadata.changes(),
                    node_id,
                });
            });
        }

        live_queries
            .sort_by(|left, right| left.live_id.to_string().cmp(&right.live_id.to_string()));
        live_queries
    }

    // ==================== Background Tasks ====================

    async fn run_heartbeat_checker(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_token.cancelled() => {
                    debug!("Heartbeat checker shutting down");
                    break;
                }
                _ = interval.tick() => {
                    self.check_all_connections();
                }
            }
        }
    }

    fn check_all_connections(&self) {
        let now = Instant::now();
        let client_timeout_ms = self.client_timeout.as_millis() as u64;
        let mut force_unregister = Vec::new();

        for entry in self.connections.iter() {
            let conn_id = entry.key();
            let state = entry.value();

            // Check auth timeout (only if auth hasn't started)
            if !state.is_authenticated()
                && !state.auth_started()
                && now.duration_since(state.connected_at()) > self.auth_timeout
            {
                debug!("Auth timeout for connection: {}", conn_id);
                match state.event_tx.try_send(ConnectionEvent::AuthTimeout) {
                    Ok(_) => {},
                    Err(mpsc::error::TrySendError::Full(_))
                    | Err(mpsc::error::TrySendError::Closed(_)) => {
                        force_unregister.push(conn_id.clone());
                    },
                }
                continue;
            }

            // Check heartbeat timeout using lock-free atomic read.
            // Clients send their own Ping frames periodically which
            // reset this timestamp, so the server never needs to initiate pings.
            // This check is the last line of defence for crashed / misbehaving clients.
            let ms_since = state.millis_since_heartbeat();
            if ms_since > client_timeout_ms {
                debug!(
                    "Heartbeat timeout for connection: {} ({}ms since last activity)",
                    conn_id, ms_since
                );
                match state.event_tx.try_send(ConnectionEvent::HeartbeatTimeout) {
                    Ok(_) => {},
                    Err(mpsc::error::TrySendError::Full(_))
                    | Err(mpsc::error::TrySendError::Closed(_)) => {
                        force_unregister.push(conn_id.clone());
                    },
                }
            }
        }

        for conn_id in force_unregister {
            self.unregister_connection(&conn_id);
        }
    }
}

impl Drop for ConnectionsManager {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
        debug!("ConnectionsManager dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notify_shared_table_for_test(
        registry: &ConnectionsManager,
        table_id: &TableId,
        notification: WireNotification,
    ) {
        let handles = registry.get_shared_subscriptions_for_table(table_id);
        let notification = Arc::new(notification);
        for handle in handles.iter() {
            let live_id = handle.key().clone();
            if let Err(e) = handle.notification_tx.try_send(Arc::clone(&notification)) {
                if matches!(e, mpsc::error::TrySendError::Full(_)) {
                    warn!("Notification channel full for {}, dropping", live_id);
                }
            }
        }
    }

    fn notify_table_for_user_for_test(
        registry: &ConnectionsManager,
        user_id: &UserId,
        table_id: &TableId,
        notification: WireNotification,
    ) {
        let handles = registry.get_subscriptions_for_table(user_id, table_id);
        let notification = Arc::new(notification);
        for handle in handles.iter() {
            let live_id = handle.key().clone();
            if let Err(e) = handle.notification_tx.try_send(Arc::clone(&notification)) {
                if matches!(e, mpsc::error::TrySendError::Full(_)) {
                    warn!("Notification channel full for {}, dropping", live_id);
                }
            }
        }
    }

    fn create_test_registry() -> Arc<ConnectionsManager> {
        ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_secs(10),
            Duration::from_secs(3),
            Duration::from_secs(5),
        )
    }

    #[tokio::test]
    async fn test_register_unregister_connection() {
        let registry = create_test_registry();
        let conn_id = ConnectionId::new("conn1");

        let reg = registry.register_connection(
            conn_id.clone(),
            ConnectionInfo::new(Some("127.0.0.1".to_string())),
        );
        assert!(reg.is_some());
        assert_eq!(registry.connection_count(), 1);
        assert_eq!(registry.peak_connection_count(), 1);

        registry.unregister_connection(&conn_id);
        assert_eq!(registry.connection_count(), 0);
        assert_eq!(registry.peak_connection_count(), 1);
    }

    #[tokio::test]
    async fn test_trim_idle_capacity_after_last_connection_removed() {
        let registry = create_test_registry();
        let conn_id = ConnectionId::new("trim_idle_conn");

        registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .unwrap();
        registry.unregister_connection(&conn_id);

        registry.trim_idle_capacity();

        assert_eq!(registry.connection_count(), 0);
        assert_eq!(registry.subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_authentication_flow() {
        let registry = create_test_registry();
        let conn_id = ConnectionId::new("conn1");
        let user_id = UserId::new("user1");

        let reg = registry.register_connection(conn_id.clone(), ConnectionInfo::new(None));
        assert!(reg.is_some());
        let reg = reg.unwrap();

        {
            let state = &reg.state;
            assert!(!state.is_authenticated());
            state.mark_auth_started();
            state.mark_authenticated(user_id.clone(), kalamdb_commons::Role::User);
        }

        let state = &reg.state;
        assert!(state.is_authenticated());
        assert_eq!(state.user_id(), Some(&user_id));
    }

    #[tokio::test]
    async fn test_reject_during_shutdown() {
        let registry = create_test_registry();

        registry.is_shutting_down.store(true, Ordering::Release);

        let reg =
            registry.register_connection(ConnectionId::new("conn1"), ConnectionInfo::new(None));
        assert!(reg.is_none());
    }

    #[tokio::test]
    async fn test_heartbeat_timeout_sends_event() {
        // Use a very short client timeout so we don't have to sleep long
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_millis(50), // client_timeout
            Duration::from_secs(60),   // auth_timeout (long, not under test)
            Duration::from_secs(5),    // heartbeat_interval (unused – we call check manually)
        );

        let conn_id = ConnectionId::new("timeout_conn");
        let mut reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .expect("should register");

        // Mark authenticated so auth-timeout path is skipped
        {
            reg.state.mark_auth_started();
            reg.state.mark_authenticated(UserId::new("u1"), kalamdb_commons::Role::User);
        }

        // Wait longer than client_timeout
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Run the checker – should detect the stale connection
        registry.check_all_connections();

        // The event channel should contain a HeartbeatTimeout
        let event = reg.event_rx.try_recv();
        assert!(
            matches!(event, Ok(ConnectionEvent::HeartbeatTimeout)),
            "expected HeartbeatTimeout, got {:?}",
            event
        );
    }

    #[tokio::test]
    async fn test_heartbeat_refresh_prevents_timeout() {
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_millis(100), // client_timeout
            Duration::from_secs(60),
            Duration::from_secs(5),
        );

        let conn_id = ConnectionId::new("alive_conn");
        let mut reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .expect("should register");

        // Mark authenticated
        {
            reg.state.mark_auth_started();
            reg.state.mark_authenticated(UserId::new("u2"), kalamdb_commons::Role::User);
        }

        // Simulate client activity before the timeout fires
        tokio::time::sleep(Duration::from_millis(60)).await;
        reg.state.update_heartbeat();

        // Wait a bit more (total elapsed > 100ms from registration but < 100ms from refresh)
        tokio::time::sleep(Duration::from_millis(60)).await;

        registry.check_all_connections();

        // Channel should be empty — heartbeat was refreshed
        let event = reg.event_rx.try_recv();
        assert!(
            event.is_err(),
            "expected no timeout event after heartbeat refresh, got {:?}",
            event
        );
    }

    #[tokio::test]
    async fn test_auth_timeout_for_unauthenticated() {
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_secs(60),   // client_timeout (long)
            Duration::from_millis(50), // auth_timeout (short)
            Duration::from_secs(5),
        );

        let conn_id = ConnectionId::new("noauth");
        let mut reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .expect("should register");
        // Don't mark auth_started or authenticated

        tokio::time::sleep(Duration::from_millis(80)).await;

        registry.check_all_connections();

        let event = reg.event_rx.try_recv();
        assert!(
            matches!(event, Ok(ConnectionEvent::AuthTimeout)),
            "expected AuthTimeout, got {:?}",
            event
        );
    }

    #[tokio::test]
    async fn test_force_unregister_on_full_channel() {
        // Create manager with very short timeout
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_millis(50),
            Duration::from_secs(60),
            Duration::from_secs(5),
        );

        let conn_id = ConnectionId::new("full_chan");
        let reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .expect("should register");

        // Mark authenticated
        {
            reg.state.mark_auth_started();
            reg.state.mark_authenticated(UserId::new("u3"), kalamdb_commons::Role::User);
        }

        // Fill the event channel to capacity
        for _ in 0..EVENT_CHANNEL_CAPACITY {
            let _ = reg.state.event_tx.try_send(ConnectionEvent::Shutdown);
        }

        // Wait for heartbeat timeout
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(registry.connection_count(), 1);
        registry.check_all_connections();

        // Connection should be force-unregistered since the channel was full
        assert_eq!(
            registry.connection_count(),
            0,
            "connection should be force-unregistered when event channel is full"
        );
    }

    // ==================== Shared Table Subscription Tests ====================

    use super::super::super::models::{SubscriptionFlowControl, SubscriptionRuntimeMetadata};
    use kalamdb_commons::models::{NamespaceId, TableName};

    /// Helper: create a SubscriptionHandle with pre-completed flow control
    fn create_test_handle(
        notification_tx: tokio::sync::mpsc::Sender<Arc<WireNotification>>,
    ) -> SubscriptionHandle {
        let flow_control = Arc::new(SubscriptionFlowControl::new());
        flow_control.mark_initial_complete();
        let runtime_metadata =
            Arc::new(SubscriptionRuntimeMetadata::new("SELECT * FROM shared.test", None, 1));
        SubscriptionHandle {
            subscription_id: Arc::from("test-subscription"),
            filter_expr: None,
            projections: None,
            notification_tx,
            flow_control: Some(flow_control),
            runtime_metadata,
        }
    }

    fn make_table_id(ns: &str, table: &str) -> TableId {
        TableId::new(NamespaceId::from(ns), TableName::from(table))
    }

    fn make_wire_notification(sub_id: &str) -> WireNotification {
        use kalamdb_commons::websocket::SharedChangePayload;
        WireNotification {
            subscription_id: Arc::from(sub_id),
            payload: Arc::new(SharedChangePayload::new(
                kalamdb_commons::websocket::ChangeType::Insert,
                Some(vec![]),
                None,
            )),
        }
    }

    #[tokio::test]
    async fn test_shared_subscription_index_and_unindex() {
        let registry = create_test_registry();
        let conn_id = ConnectionId::new("sc1");
        let table_id = make_table_id("shared", "products");
        let live_id = LiveQueryId::new(UserId::new("u1"), conn_id.clone(), "sub1".to_string());

        let reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .unwrap();
        let (tx, _rx) = mpsc::channel(64);
        let handle = create_test_handle(tx);

        // Index a shared subscription
        registry.index_shared_subscription(&conn_id, live_id.clone(), table_id.clone(), handle);

        assert_eq!(registry.subscription_count(), 1);
        assert_eq!(registry.peak_subscription_count(), 1);
        assert!(registry.has_shared_subscriptions(&table_id));
        assert!(!registry.has_subscriptions(&UserId::new("u1"), &table_id));

        // Verify get_shared_subscriptions_for_table returns the subscription
        let subs = registry.get_shared_subscriptions_for_table(&table_id);
        assert_eq!(subs.len(), 1);
        assert!(subs.contains_key(&live_id));

        // Unindex
        registry.unindex_shared_subscription(&live_id, &table_id);
        assert_eq!(registry.subscription_count(), 0);
        assert_eq!(registry.peak_subscription_count(), 1);
        assert!(!registry.has_shared_subscriptions(&table_id));

        let subs_after = registry.get_shared_subscriptions_for_table(&table_id);
        assert!(subs_after.is_empty());

        drop(reg);
    }

    #[tokio::test]
    async fn test_shared_subscription_cleanup_on_disconnect() {
        let registry = create_test_registry();
        let conn_id = ConnectionId::new("sc2");
        let user_id = UserId::new("u2");
        let table_id = make_table_id("shared", "orders");
        let live_id = LiveQueryId::new(user_id.clone(), conn_id.clone(), "sub1".to_string());

        let reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .unwrap();

        // Authenticate connection
        {
            reg.state.mark_auth_started();
            reg.state.mark_authenticated(user_id.clone(), kalamdb_commons::Role::User);
        }

        // Add a shared subscription to ConnectionState.subscriptions
        {
            reg.state.insert_subscription(
                Arc::from("sub1"),
                super::super::super::models::SubscriptionState {
                    live_id: live_id.clone(),
                    table_id: table_id.clone(),
                    filter_expr: None,
                    projections: None,
                    initial_load: Some(super::super::super::models::InitialLoadState {
                        batch_size: 100,
                        snapshot_end_seq: None,
                        current_batch_num: 0,
                        flow_control: Arc::new(SubscriptionFlowControl::new()),
                    }),
                    is_shared: true,
                    runtime_metadata: Arc::new(SubscriptionRuntimeMetadata::new(
                        "SELECT * FROM shared.orders",
                        None,
                        1,
                    )),
                },
            );
        }

        let (tx, _rx) = mpsc::channel(64);
        registry.index_shared_subscription(
            &conn_id,
            live_id.clone(),
            table_id.clone(),
            create_test_handle(tx),
        );

        assert_eq!(registry.subscription_count(), 1);
        assert!(registry.has_shared_subscriptions(&table_id));

        // Unregister connection — should clean up shared subscriptions
        let removed = registry.unregister_connection(&conn_id);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], live_id);
        assert!(!registry.has_shared_subscriptions(&table_id));
        assert_eq!(registry.subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_shared_subscription_notification_delivery() {
        let registry = create_test_registry();
        let table_id = make_table_id("shared", "chat");
        let subscriber_count = 5;

        let mut receivers = Vec::new();

        for i in 0..subscriber_count {
            let conn_id = ConnectionId::new(&format!("conn_{}", i));
            let user_id = UserId::new(&format!("user_{}", i));
            let live_id = LiveQueryId::new(user_id, conn_id.clone(), format!("sub_{}", i));

            let _reg = registry
                .register_connection(conn_id.clone(), ConnectionInfo::new(None))
                .unwrap();

            let (tx, rx) = mpsc::channel(64);
            let handle = create_test_handle(tx);

            registry.index_shared_subscription(&conn_id, live_id, table_id.clone(), handle);

            receivers.push(rx);
        }

        assert_eq!(registry.subscription_count(), subscriber_count);

        // Send a notification to all shared table subscribers
        let notification = make_wire_notification("test");
        notify_shared_table_for_test(&registry, &table_id, notification);

        // All subscribers should receive it
        for rx in &mut receivers {
            let msg = rx.try_recv();
            assert!(msg.is_ok(), "subscriber should have received notification");
        }
    }

    #[tokio::test]
    async fn test_shared_subscription_many_subscribers_performance() {
        // Performance test: verify streaming handles 1000+ subscribers efficiently
        let registry = create_test_registry();
        let table_id = make_table_id("shared", "perf_table");
        let subscriber_count = 1_000;

        let mut receivers = Vec::with_capacity(subscriber_count);

        for i in 0..subscriber_count {
            let conn_id = ConnectionId::new(&format!("perf_conn_{}", i));
            let user_id = UserId::new(&format!("perf_user_{}", i));
            let live_id = LiveQueryId::new(user_id, conn_id.clone(), format!("perf_sub_{}", i));

            let _reg = registry
                .register_connection(conn_id.clone(), ConnectionInfo::new(None))
                .unwrap();

            let (tx, rx) = mpsc::channel(64);
            let handle = create_test_handle(tx);

            registry.index_shared_subscription(&conn_id, live_id, table_id.clone(), handle);

            receivers.push(rx);
        }

        assert_eq!(registry.subscription_count(), subscriber_count);

        // Time the notification fan-out
        let handles = registry.get_shared_subscriptions_for_table(&table_id);
        assert_eq!(handles.len(), subscriber_count);

        let notification = make_wire_notification("perf");

        let start = std::time::Instant::now();
        notify_shared_table_for_test(&registry, &table_id, notification);
        let elapsed = start.elapsed();

        // Should complete in well under 100ms for 1000 subscribers
        assert!(
            elapsed < Duration::from_millis(100),
            "Notification fan-out to {} subscribers took {:?}, expected <100ms",
            subscriber_count,
            elapsed
        );

        // Verify all subscribers received the notification
        let mut received = 0;
        for rx in &mut receivers {
            if rx.try_recv().is_ok() {
                received += 1;
            }
        }
        assert_eq!(
            received, subscriber_count,
            "all {} subscribers should receive notification, got {}",
            subscriber_count, received
        );
    }

    #[tokio::test]
    async fn test_shared_subscription_parallel_registration_and_notification() {
        let registry = create_test_registry();
        let table_id = make_table_id("shared", "parallel_changes");
        let subscriber_count = 16;

        let handles: Vec<_> = (0..subscriber_count)
            .map(|i| {
                let registry = Arc::clone(&registry);
                let table_id = table_id.clone();
                tokio::spawn(async move {
                    let conn_id = ConnectionId::new(&format!("parallel_conn_{}", i));
                    let user_id = UserId::new(&format!("parallel_user_{}", i));
                    registry
                        .register_connection(conn_id.clone(), ConnectionInfo::new(None))
                        .unwrap();
                    let live_id =
                        LiveQueryId::new(user_id, conn_id.clone(), format!("parallel_sub_{}", i));
                    let (tx, rx) = mpsc::channel(64);
                    registry.index_shared_subscription(
                        &conn_id,
                        live_id,
                        table_id,
                        create_test_handle(tx),
                    );
                    rx
                })
            })
            .collect();

        let mut receivers = Vec::with_capacity(subscriber_count);
        for handle in handles {
            receivers.push(handle.await.expect("registration task panicked"));
        }

        assert_eq!(
            registry.subscription_count(),
            subscriber_count,
            "expected all parallel subscriptions to register"
        );

        let notification = make_wire_notification("parallel");
        notify_shared_table_for_test(&registry, &table_id, notification);

        for mut rx in receivers {
            let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
            assert!(msg.is_ok(), "subscriber should receive notification within timeout");
            assert!(msg.unwrap().is_some(), "subscriber channel closed");
        }
    }

    #[tokio::test]
    async fn test_shared_subscription_mixed_with_user_subscriptions() {
        let registry = create_test_registry();
        let user_id = UserId::new("mixed_user");
        let shared_table = make_table_id("shared", "config");
        let user_table = make_table_id("app", "notes");

        let conn_id = ConnectionId::new("mixed_conn");
        let _reg = registry
            .register_connection(conn_id.clone(), ConnectionInfo::new(None))
            .unwrap();

        // Add a shared subscription
        let shared_live_id =
            LiveQueryId::new(user_id.clone(), conn_id.clone(), "shared_sub".to_string());
        let (tx1, mut rx1) = mpsc::channel(64);
        registry.index_shared_subscription(
            &conn_id,
            shared_live_id,
            shared_table.clone(),
            create_test_handle(tx1),
        );

        // Add a user subscription
        let user_live_id =
            LiveQueryId::new(user_id.clone(), conn_id.clone(), "user_sub".to_string());
        let (tx2, mut rx2) = mpsc::channel(64);
        registry.index_subscription(
            &user_id,
            &conn_id,
            user_live_id,
            user_table.clone(),
            create_test_handle(tx2),
        );

        assert_eq!(registry.subscription_count(), 2);

        // Shared table notification should only go to shared subscriber
        let shared_notif = make_wire_notification("shared");
        notify_shared_table_for_test(&registry, &shared_table, shared_notif);
        assert!(rx1.try_recv().is_ok(), "shared subscriber should receive");
        assert!(
            rx2.try_recv().is_err(),
            "user subscriber should NOT receive shared notification"
        );

        // User table notification should only go to user subscriber
        let user_notif = make_wire_notification("user");
        notify_table_for_user_for_test(&registry, &user_id, &user_table, user_notif);
        assert!(
            rx1.try_recv().is_err(),
            "shared subscriber should NOT receive user notification"
        );
        assert!(rx2.try_recv().is_ok(), "user subscriber should receive");
    }

    #[tokio::test]
    async fn test_shared_subscription_empty_table_returns_empty() {
        let registry = create_test_registry();
        let table_id = make_table_id("shared", "nonexistent");

        let subs = registry.get_shared_subscriptions_for_table(&table_id);
        assert!(subs.is_empty());
        assert!(!registry.has_shared_subscriptions(&table_id));
    }
}
