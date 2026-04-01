//! Notification service for live queries
//!
//! Handles dispatching change notifications to subscribed live query clients,
//! including filtering based on WHERE clauses stored in SubscriptionState.
//!
//! Topic pub/sub publishing is now handled synchronously in table providers
//! via the TopicPublisher trait — see kalamdb-publisher crate.
//!
//! In cluster mode, the leader broadcasts notifications to follower nodes
//! via the gRPC ClusterService (see `kalamdb_raft::ClusterClient`).
//!
//! Used by:
//! - WebSocket live query subscribers

use super::helpers::filter_eval::matches as filter_matches;
use super::manager::ConnectionsManager;
use super::models::{ChangeNotification, ChangeType, SubscriptionHandle};
use crate::error::KalamDbError;
use crate::providers::arrow_json_conversion::{row_to_json_map, scalar_value_to_json};
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{KalamCellValue, LiveQueryId, TableId, UserId};
use kalamdb_raft::ClusterClient;
use kalamdb_raft::NotifyFollowersRequest;
use kalamdb_system::NotificationService as NotificationServiceTrait;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{mpsc, OnceCell};

/// Number of sharded notification workers.
/// Deterministic routing by table_id hash preserves per-table ordering
/// while achieving parallelism across different tables.
const NUM_NOTIFY_WORKERS: usize = 4;

/// Per-worker queue capacity. Total capacity = NUM_NOTIFY_WORKERS × this value.
const NOTIFY_QUEUE_PER_WORKER: usize = 4_096;

/// Number of subscribers per parallel chunk for shared table streaming notification.
/// Tuned to amortize tokio::spawn overhead while achieving parallelism at scale.
const SHARED_NOTIFY_CHUNK_SIZE: usize = 256;

struct NotificationTask {
    user_id: Option<UserId>,
    table_id: TableId,
    notification: ChangeNotification,
}

#[inline]
fn extract_seq(change_notification: &ChangeNotification) -> Option<SeqId> {
    use datafusion::scalar::ScalarValue;
    change_notification
        .row_data
        .values
        .get(SystemColumnNames::SEQ)
        .and_then(|value| match value {
            ScalarValue::Int64(Some(seq)) => Some(SeqId::from(*seq)),
            ScalarValue::UInt64(Some(seq)) => Some(SeqId::from(*seq as i64)),
            _ => None,
        })
}

/// Project a Row to only include the requested columns + `_seq`, then convert
/// each ScalarValue to KalamCellValue. When projections is None (SELECT *),
/// converts all columns. Avoids creating intermediate KalamCellValues for
/// columns that would be discarded by projection.
#[inline]
fn project_row_to_json(
    row: &Row,
    projections: &Option<Arc<Vec<String>>>,
) -> Result<HashMap<String, KalamCellValue>, KalamDbError> {
    match projections {
        None => row_to_json_map(row),
        Some(cols) => {
            let mut projected = HashMap::with_capacity(cols.len() + 1);
            for col in cols.iter() {
                if let Some(sv) = row.values.get(col.as_str()) {
                    projected.insert(col.clone(), scalar_value_to_json(sv)?);
                }
            }
            if let Some(sv) = row.values.get(SystemColumnNames::SEQ) {
                if !projected.contains_key(SystemColumnNames::SEQ) {
                    projected.insert(SystemColumnNames::SEQ.to_string(), scalar_value_to_json(sv)?);
                }
            }
            Ok(projected)
        },
    }
}

/// Build a `Notification` directly from Row data, converting only the columns
/// needed for the wire format. For INSERT/DELETE, converts only projected columns.
/// For UPDATE, computes delta on ScalarValues and converts only changed + PK + `_seq`.
#[inline]
fn build_notification_from_rows(
    sub_id: String,
    change_type: &ChangeType,
    new_row: &Row,
    old_row: Option<&Row>,
    pk_columns: &[String],
    projections: &Option<Arc<Vec<String>>>,
) -> Result<kalamdb_commons::Notification, KalamDbError> {
    match change_type {
        ChangeType::Insert => {
            let row_json = project_row_to_json(new_row, projections)?;
            Ok(kalamdb_commons::Notification::insert(sub_id, vec![row_json]))
        },
        ChangeType::Update => {
            let old = old_row.unwrap_or(new_row);
            let (delta_new, delta_old) =
                compute_update_delta(old, new_row, pk_columns, projections)?;
            Ok(kalamdb_commons::Notification::update(sub_id, vec![delta_new], vec![delta_old]))
        },
        ChangeType::Delete => {
            let row_json = project_row_to_json(new_row, projections)?;
            Ok(kalamdb_commons::Notification::delete(sub_id, vec![row_json]))
        },
    }
}

/// Try to deliver a notification to a subscriber, handling flow control.
/// Returns `true` if the notification was sent to the channel.
#[inline]
fn try_deliver(
    live_id: &LiveQueryId,
    handle: &SubscriptionHandle,
    notification: Arc<kalamdb_commons::Notification>,
    seq_value: Option<SeqId>,
) -> bool {
    let flow_control = &handle.flow_control;

    if !flow_control.is_initial_complete() {
        if let Some(snapshot_seq) = flow_control.snapshot_end_seq() {
            if let Some(seq) = seq_value {
                if seq.as_i64() <= snapshot_seq {
                    return false;
                }
            }
        }
        flow_control.buffer_notification(Arc::clone(&notification), seq_value);
        return false;
    }

    match handle.notification_tx.try_send(notification) {
        Ok(()) => true,
        Err(e) => {
            use tokio::sync::mpsc::error::TrySendError;
            match e {
                TrySendError::Full(_) => {
                    log::warn!(
                        "Notification channel full for live_id={}, dropping notification",
                        live_id
                    );
                },
                TrySendError::Closed(_) => {
                    log::debug!(
                        "Notification channel closed for live_id={}, connection likely disconnected",
                        live_id
                    );
                },
            }
            false
        },
    }
}

/// Compute UPDATE delta directly from Row ScalarValues, converting only the
/// columns that appear in the output (changed + PK + `_seq`).
/// Returns `(delta_new, delta_old)` where delta_new has non-null columns + PK + `_seq`,
/// and delta_old has only changed columns + PK + `_seq`.
#[inline]
fn compute_update_delta(
    old_row: &Row,
    new_row: &Row,
    pk_columns: &[String],
    projections: &Option<Arc<Vec<String>>>,
) -> Result<(HashMap<String, KalamCellValue>, HashMap<String, KalamCellValue>), KalamDbError> {
    let mut delta_new = HashMap::new();
    let mut delta_old = HashMap::new();

    // Always include _seq and PK columns for row identification
    for key in std::iter::once(SystemColumnNames::SEQ).chain(pk_columns.iter().map(|s| s.as_str()))
    {
        if let Some(v) = new_row.values.get(key) {
            delta_new.insert(key.to_string(), scalar_value_to_json(v)?);
        }
        if let Some(v) = old_row.values.get(key) {
            delta_old.insert(key.to_string(), scalar_value_to_json(v)?);
        }
    }

    // Single loop body — column source differs by projection mode
    let owned_keys;
    let col_iter: Box<dyn Iterator<Item = &str>> = match projections {
        None => {
            owned_keys = new_row.values.keys().collect::<Vec<_>>();
            Box::new(owned_keys.iter().map(|s| s.as_str()))
        },
        Some(cols) => Box::new(cols.iter().map(|s| s.as_str())),
    };

    for col_name in col_iter {
        if col_name.starts_with('_') || pk_columns.iter().any(|pk| pk == col_name) {
            continue;
        }
        let new_sv = new_row.values.get(col_name);
        let old_sv = old_row.values.get(col_name);
        if let Some(nv) = new_sv {
            if !nv.is_null() {
                delta_new.insert(col_name.to_string(), scalar_value_to_json(nv)?);
            }
        }
        let changed = match (old_sv, new_sv) {
            (Some(ov), Some(nv)) => ov != nv,
            (None, Some(_)) => true,
            _ => false,
        };
        if changed {
            if let Some(ov) = old_sv {
                delta_old.insert(col_name.to_string(), scalar_value_to_json(ov)?);
            }
        }
    }

    Ok((delta_new, delta_old))
}

/// Service for notifying subscribers of changes
///
/// Uses Arc<ConnectionsManager> directly since ConnectionsManager internally
/// uses DashMap for lock-free concurrent access - no RwLock wrapper needed.
///
/// Also handles topic pub/sub routing via TopicPublisherService.
///
/// In cluster mode, the leader node broadcasts notifications to follower nodes
/// via cluster gRPC so followers can dispatch to their locally-connected
/// WebSocket subscribers.
pub struct NotificationService {
    /// Manager uses DashMap internally for lock-free access
    registry: Arc<ConnectionsManager>,
    /// Sharded notification channels — deterministic routing by table_id hash
    /// preserves per-table ordering while achieving parallelism across tables.
    worker_txs: Vec<mpsc::Sender<NotificationTask>>,
    /// AppContext for leadership checks (set after initialization to avoid circular dependency)
    /// Required for Raft cluster mode to ensure only leader fires notifications
    app_context: OnceCell<std::sync::Weak<crate::app_context::AppContext>>,
    /// gRPC cluster client for broadcasting notifications to follower nodes.
    /// Set after initialization when Raft cluster mode is active.
    cluster_client: OnceCell<Arc<ClusterClient>>,
}

impl NotificationService {
    /// Check if there are any subscribers for a given user and table
    pub fn has_subscribers(&self, user_id: &UserId, table_id: &TableId) -> bool {
        self.registry.has_subscriptions(user_id, table_id)
    }

    /// Set the app context for leadership checks
    ///
    /// Called after AppContext creation to enable Raft-aware notification filtering.
    /// Uses Weak reference to avoid circular Arc dependency.
    pub fn set_app_context(&self, app_context: std::sync::Weak<crate::app_context::AppContext>) {
        if self.app_context.set(app_context).is_err() {
            log::warn!("AppContext already set in NotificationService");
        }
    }

    /// Set the gRPC cluster client for cross-node notification broadcasting.
    ///
    /// Called after Raft initialization when cluster mode is active.
    pub fn set_cluster_client(&self, client: Arc<ClusterClient>) {
        if self.cluster_client.set(client).is_err() {
            log::warn!("ClusterClient already set in NotificationService");
        }
    }

    pub fn new(registry: Arc<ConnectionsManager>) -> Arc<Self> {
        let mut worker_txs = Vec::with_capacity(NUM_NOTIFY_WORKERS);
        let mut worker_rxs = Vec::with_capacity(NUM_NOTIFY_WORKERS);

        for _ in 0..NUM_NOTIFY_WORKERS {
            let (tx, rx) = mpsc::channel(NOTIFY_QUEUE_PER_WORKER);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        let service = Arc::new(Self {
            registry,
            worker_txs,
            app_context: OnceCell::new(),
            cluster_client: OnceCell::new(),
        });

        // Spawn sharded notification workers
        for (idx, rx) in worker_rxs.into_iter().enumerate() {
            let notify_service = Arc::clone(&service);
            tokio::spawn(Self::run_worker(notify_service, rx, idx));
        }

        service
    }

    /// Route a table_id to a deterministic worker index.
    #[inline]
    fn worker_index(&self, table_id: &TableId) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        table_id.namespace_id().as_str().hash(&mut hasher);
        table_id.table_name().as_str().hash(&mut hasher);
        hasher.finish() as usize % self.worker_txs.len()
    }

    /// Single notification worker loop.
    async fn run_worker(
        service: Arc<Self>,
        mut rx: mpsc::Receiver<NotificationTask>,
        worker_idx: usize,
    ) {
        while let Some(task) = rx.recv().await {
            service.process_notification(task, worker_idx).await;
        }
        log::debug!("Notification worker {} shutting down", worker_idx);
    }

    /// Process a single notification task (leadership check + fan-out).
    async fn process_notification(&self, task: NotificationTask, worker_idx: usize) {
        // Leadership check (Raft cluster mode)
        if let Some(weak_ctx) = self.app_context.get() {
            if let Some(ctx) = weak_ctx.upgrade() {
                let is_leader = match task.user_id.as_ref() {
                    Some(uid) => ctx.is_leader_for_user(uid).await,
                    None => ctx.is_leader_for_shared().await,
                };

                if !is_leader {
                    return;
                }

                if ctx.is_cluster_mode() {
                    if let Some(cluster_client) = self.cluster_client.get() {
                        Self::broadcast_to_followers(
                            cluster_client,
                            task.user_id.as_ref(),
                            &task.table_id,
                            &task.notification,
                        )
                        .await;
                    }
                }
            }
        }

        // Route to subscriptions
        let handles = if let Some(ref user_id) = task.user_id {
            self.registry.get_subscriptions_for_table(user_id, &task.table_id)
        } else {
            self.registry.get_shared_subscriptions_for_table(&task.table_id)
        };

        if handles.is_empty() {
            return;
        }

        if let Err(e) =
            Self::dispatch_to_subscribers(&task.table_id, task.notification, handles).await
        {
            log::warn!(
                "[worker-{}] Failed to notify subscribers for table {}: {}",
                worker_idx,
                task.table_id,
                e
            );
        }
    }

    /// Handle a notification forwarded from the leader node.
    pub async fn notify_forwarded(
        &self,
        user_id: UserId,
        table_id: TableId,
        notification: ChangeNotification,
    ) {
        let handles = self.registry.get_subscriptions_for_table(&user_id, &table_id);
        if handles.is_empty() {
            return;
        }
        if let Err(e) = Self::dispatch_to_subscribers(&table_id, notification, handles).await {
            log::warn!("Failed to dispatch forwarded notification for table {}: {}", table_id, e);
        }
    }

    /// Handle a shared table notification forwarded from the leader node.
    pub async fn notify_forwarded_shared(
        &self,
        table_id: TableId,
        notification: ChangeNotification,
    ) {
        let handles = self.registry.get_shared_subscriptions_for_table(&table_id);
        if handles.is_empty() {
            return;
        }
        if let Err(e) = Self::dispatch_to_subscribers(&table_id, notification, handles).await {
            log::warn!(
                "Failed to dispatch forwarded shared notification for table {}: {}",
                table_id,
                e
            );
        }
    }

    /// Broadcast a notification to all other cluster nodes via gRPC.
    ///
    /// Called on the leader after local notification dispatch.  Uses
    /// fire-and-forget semantics — errors are logged but don't fail the
    /// local notification path.
    async fn broadcast_to_followers(
        cluster_client: &ClusterClient,
        user_id: Option<&UserId>,
        table_id: &TableId,
        notification: &ChangeNotification,
    ) {
        let payload = match kalamdb_raft::network::cluster_serde::serialize(notification) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::warn!("Failed to serialize notification for forwarding: {}", e);
                return;
            },
        };

        let request = NotifyFollowersRequest {
            user_id: user_id.map(|u| u.to_string()),
            table_namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            payload,
        };

        cluster_client.broadcast_notify(request).await;
    }

    /// Notify subscribers about a table change (fire-and-forget async)
    ///
    /// In Raft cluster mode, only the leader node fires notifications to prevent
    /// duplicate messages. Followers silently drop notifications since they
    /// already persist data via the Raft applier.
    pub fn notify_async(
        &self,
        user_id: Option<UserId>,
        table_id: TableId,
        notification: ChangeNotification,
    ) {
        let worker_idx = self.worker_index(&table_id);
        let task = NotificationTask {
            user_id,
            table_id,
            notification,
        };
        if let Err(e) = self.worker_txs[worker_idx].try_send(task) {
            if matches!(e, mpsc::error::TrySendError::Full(_)) {
                log::warn!("Notification worker {} queue full, dropping notification", worker_idx);
            }
        }
    }

    /// Unified dispatch: filters, builds notifications, and delivers to subscribers.
    ///
    /// Groups subscribers by projection pointer identity so identical projections
    /// share a single `Arc<Notification>` (avoids N duplicate JSON conversions for
    /// SELECT * subscribers). For large subscriber counts (>SHARED_NOTIFY_CHUNK_SIZE),
    /// uses parallel chunked fan-out via `tokio::spawn`.
    async fn dispatch_to_subscribers(
        table_id: &TableId,
        change_notification: ChangeNotification,
        all_handles: Arc<dashmap::DashMap<LiveQueryId, SubscriptionHandle>>,
    ) -> Result<usize, KalamDbError> {
        let seq_value = extract_seq(&change_notification);
        let change_type = change_notification.change_type.clone();
        let pk_columns = Arc::new(change_notification.pk_columns);
        let new_row = Arc::new(change_notification.row_data);
        let old_row = change_notification.old_data.map(Arc::new);

        // Collect handles (clone is cheap: all fields are Arc-wrapped)
        let handles: Vec<(LiveQueryId, SubscriptionHandle)> = all_handles
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        if handles.is_empty() {
            return Ok(0);
        }

        // Small fan-out: inline dispatch (no spawn overhead)
        if handles.len() <= SHARED_NOTIFY_CHUNK_SIZE {
            return dispatch_chunk(
                &handles,
                &new_row,
                old_row.as_deref(),
                &change_type,
                &pk_columns,
                seq_value,
            );
        }

        // Large fan-out: parallel chunked dispatch
        let mut join_handles = Vec::with_capacity(
            (handles.len() + SHARED_NOTIFY_CHUNK_SIZE - 1) / SHARED_NOTIFY_CHUNK_SIZE,
        );

        for chunk in handles.chunks(SHARED_NOTIFY_CHUNK_SIZE) {
            let chunk = chunk.to_vec();
            let nr = Arc::clone(&new_row);
            let or = old_row.as_ref().map(Arc::clone);
            let ct = change_type.clone();
            let pk = Arc::clone(&pk_columns);

            join_handles.push(tokio::spawn(async move {
                dispatch_chunk(&chunk, &nr, or.as_deref(), &ct, &pk, seq_value)
            }));
        }

        let mut total = 0usize;
        for jh in join_handles {
            match jh.await {
                Ok(Ok(count)) => total += count,
                Ok(Err(e)) => {
                    log::error!("Notification dispatch error for table {}: {}", table_id, e);
                },
                Err(e) => log::error!("Notification chunk task panicked: {}", e),
            }
        }

        Ok(total)
    }
}

/// Dispatch notifications to a slice of subscribers.
///
/// Groups subscribers by projection pointer identity so that subscribers with
/// identical projections (common case: all SELECT *) share a single
/// `Arc<Notification>`, avoiding redundant Row→JSON conversion.
fn dispatch_chunk(
    handles: &[(LiveQueryId, SubscriptionHandle)],
    new_row: &Row,
    old_row: Option<&Row>,
    change_type: &ChangeType,
    pk_columns: &[String],
    seq_value: Option<SeqId>,
) -> Result<usize, KalamDbError> {
    // Cache: projection pointer → built notification
    // For N subscribers with identical projections, we build once.
    let mut cache: HashMap<usize, Arc<kalamdb_commons::Notification>> = HashMap::new();
    let mut count = 0usize;

    for (live_id, handle) in handles {
        // Filter check (operates on ScalarValue — no JSON conversion)
        if let Some(ref filter_expr) = handle.filter_expr {
            match filter_matches(filter_expr, new_row) {
                Ok(true) => {},
                Ok(false) => continue,
                Err(e) => {
                    log::error!("Filter error for live_id={}: {}", live_id, e);
                    continue;
                },
            }
        }

        // Projection cache key: Arc pointer address for Some, 0 for None (SELECT *)
        let proj_key = match &handle.projections {
            Some(arc) => Arc::as_ptr(arc) as usize,
            None => 0,
        };

        let notification = if let Some(cached) = cache.get(&proj_key) {
            // Same projection set — reuse built notification, just patch subscription_id
            let mut cloned = cached.as_ref().clone();
            cloned.set_subscription_id(live_id.subscription_id().to_string());
            Arc::new(cloned)
        } else {
            let sub_id = live_id.subscription_id().to_string();
            let n = Arc::new(build_notification_from_rows(
                sub_id,
                change_type,
                new_row,
                old_row,
                pk_columns,
                &handle.projections,
            )?);
            cache.insert(proj_key, Arc::clone(&n));
            n
        };

        if try_deliver(live_id, handle, notification, seq_value) {
            count += 1;
        }
    }

    Ok(count)
}

impl NotificationServiceTrait for NotificationService {
    type Notification = ChangeNotification;

    fn has_subscribers(&self, user_id: Option<&UserId>, table_id: &TableId) -> bool {
        if let Some(uid) = user_id {
            if self.registry.has_subscriptions(uid, table_id) {
                return true;
            }
        }
        self.registry.has_shared_subscriptions(table_id)
    }

    fn notify_table_change(
        &self,
        user_id: Option<UserId>,
        table_id: TableId,
        notification: ChangeNotification,
    ) {
        self.notify_async(user_id, table_id, notification);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::helpers::filter_eval::parse_where_clause;
    use crate::live::models::{SubscriptionFlowControl, SubscriptionHandle};
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::{ConnectionId, NamespaceId, TableName};
    use kalamdb_commons::NodeId;
    use kalamdb_commons::Notification;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn make_table_id(ns: &str, table: &str) -> TableId {
        TableId::new(NamespaceId::from(ns), TableName::from(table))
    }

    fn make_row(id: i64, body: &str, seq: i64) -> Row {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(id)));
        values.insert("body".to_string(), ScalarValue::Utf8(Some(body.to_string())));
        values.insert(SystemColumnNames::SEQ.to_string(), ScalarValue::Int64(Some(seq)));
        Row::new(values)
    }

    fn make_shared_handle(
        tx: Arc<crate::live::models::NotificationSender>,
        flow_control: Arc<SubscriptionFlowControl>,
        filter_expr: Option<&str>,
        projections: Option<Vec<&str>>,
    ) -> SubscriptionHandle {
        SubscriptionHandle {
            filter_expr: filter_expr
                .map(|sql| parse_where_clause(sql).expect("filter should parse"))
                .map(Arc::new),
            projections: projections.map(|cols| {
                Arc::new(cols.into_iter().map(std::string::ToString::to_string).collect())
            }),
            notification_tx: tx,
            flow_control,
        }
    }

    #[tokio::test]
    async fn test_notify_forwarded_shared_applies_filter_and_projection() {
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(5),
        );
        let service = NotificationService::new(Arc::clone(&registry));

        let table_id = make_table_id("shared", "events");
        let conn_id = ConnectionId::new("c1");

        let (tx_ok, mut rx_ok) = mpsc::channel(8);
        let flow_ok = Arc::new(SubscriptionFlowControl::new());
        flow_ok.mark_initial_complete();
        registry.index_shared_subscription(
            &conn_id,
            LiveQueryId::new(UserId::new("u1"), conn_id.clone(), "sub_ok".to_string()),
            table_id.clone(),
            make_shared_handle(
                Arc::new(tx_ok),
                Arc::clone(&flow_ok),
                Some("id >= 10"),
                Some(vec!["id"]),
            ),
        );

        let (tx_skip, mut rx_skip) = mpsc::channel(8);
        let flow_skip = Arc::new(SubscriptionFlowControl::new());
        flow_skip.mark_initial_complete();
        registry.index_shared_subscription(
            &conn_id,
            LiveQueryId::new(UserId::new("u2"), conn_id.clone(), "sub_skip".to_string()),
            table_id.clone(),
            make_shared_handle(Arc::new(tx_skip), Arc::clone(&flow_skip), Some("id >= 100"), None),
        );

        let change = ChangeNotification::insert(table_id.clone(), make_row(42, "hello", 42));
        service.notify_forwarded_shared(table_id, change).await;

        let delivered = rx_ok.recv().await.expect("matching subscriber gets message");
        match delivered.as_ref() {
            Notification::Change {
                subscription_id,
                rows,
                old_values,
                ..
            } => {
                assert_eq!(subscription_id, "sub_ok");
                assert!(old_values.is_none());
                let payload = rows.as_ref().expect("rows present for insert");
                assert_eq!(payload.len(), 1);
                assert!(payload[0].contains_key("id"));
                assert!(!payload[0].contains_key("body"));
                assert!(payload[0].contains_key(SystemColumnNames::SEQ));
            },
            _ => panic!("expected change notification"),
        }

        assert!(rx_skip.try_recv().is_err(), "filtered subscriber should not receive");
    }

    #[tokio::test]
    async fn test_notify_forwarded_user_table_projection_keeps_seq() {
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(5),
        );
        let service = NotificationService::new(Arc::clone(&registry));

        let user_id = UserId::new("user-proj");
        let table_id = make_table_id("default", "events");
        let conn_id = ConnectionId::new("c-user-1");
        let live_id = LiveQueryId::new(user_id.clone(), conn_id.clone(), "sub_user".to_string());

        let (tx, mut rx) = mpsc::channel(8);
        let flow = Arc::new(SubscriptionFlowControl::new());
        flow.mark_initial_complete();

        registry.index_subscription(
            &user_id,
            &conn_id,
            live_id,
            table_id.clone(),
            make_shared_handle(Arc::new(tx), flow, None, Some(vec!["id"])),
        );

        let change = ChangeNotification::insert(table_id.clone(), make_row(42, "hello", 42));
        service.notify_forwarded(user_id, table_id, change).await;

        let delivered = rx.recv().await.expect("projected subscriber gets message");
        match delivered.as_ref() {
            Notification::Change {
                subscription_id,
                rows,
                old_values,
                ..
            } => {
                assert_eq!(subscription_id, "sub_user");
                assert!(old_values.is_none());
                let payload = rows.as_ref().expect("rows present for insert");
                assert_eq!(payload.len(), 1);
                assert!(payload[0].contains_key("id"));
                assert!(!payload[0].contains_key("body"));
                assert!(payload[0].contains_key(SystemColumnNames::SEQ));
            },
            _ => panic!("expected change notification"),
        }
    }

    #[tokio::test]
    async fn test_notify_forwarded_shared_buffers_when_initial_load_incomplete() {
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(5),
        );
        let service = NotificationService::new(Arc::clone(&registry));

        let table_id = make_table_id("shared", "logs");
        let conn_id = ConnectionId::new("c2");

        let (tx, mut rx) = mpsc::channel(8);
        let flow = Arc::new(SubscriptionFlowControl::new());
        flow.set_snapshot_end_seq(Some(SeqId::from(10)));

        registry.index_shared_subscription(
            &conn_id,
            LiveQueryId::new(UserId::new("u3"), conn_id.clone(), "sub_buffer".to_string()),
            table_id.clone(),
            make_shared_handle(Arc::new(tx), Arc::clone(&flow), None, None),
        );

        // seq=11 is newer than snapshot end seq=10, should be buffered (not sent yet)
        let change = ChangeNotification::insert(table_id, make_row(7, "buffer-me", 11));
        service.notify_forwarded_shared(make_table_id("shared", "logs"), change).await;

        assert!(rx.try_recv().is_err(), "should not send while initial load incomplete");
        let buffered = flow.drain_buffered_notifications();
        assert_eq!(buffered.len(), 1, "notification should be buffered");
    }

    #[tokio::test]
    async fn test_notify_async_shared_worker_dispatches() {
        let registry = ConnectionsManager::new(
            NodeId::new(1),
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::from_secs(5),
        );
        let service = NotificationService::new(Arc::clone(&registry));

        let table_id = make_table_id("shared", "alerts");
        let conn_id = ConnectionId::new("c3");

        let (tx, mut rx) = mpsc::channel(8);
        let flow = Arc::new(SubscriptionFlowControl::new());
        flow.mark_initial_complete();

        registry.index_shared_subscription(
            &conn_id,
            LiveQueryId::new(UserId::new("u4"), conn_id.clone(), "sub_async".to_string()),
            table_id.clone(),
            make_shared_handle(Arc::new(tx), flow, None, None),
        );

        let change = ChangeNotification::insert(table_id.clone(), make_row(1, "async", 1));
        service.notify_async(None, table_id.clone(), change);

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("worker should dispatch within timeout")
            .expect("message should be present");

        match delivered.as_ref() {
            Notification::Change {
                subscription_id, ..
            } => {
                assert_eq!(subscription_id, "sub_async");
            },
            _ => panic!("expected change notification"),
        }

        assert!(NotificationServiceTrait::has_subscribers(&*service, None, &table_id));
    }
}
