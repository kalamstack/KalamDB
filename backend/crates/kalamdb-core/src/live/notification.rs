//! Notification service for live queries
//!
//! Handles dispatching change notifications to subscribed live query clients,
//! including filtering based on WHERE clauses stored in SubscriptionState.
//!
//! Topic pub/sub publishing is now handled synchronously in table providers
//! via the TopicPublisher trait — see kalamdb-publisher crate.
//!
//! Used by:
//! - WebSocket live query subscribers

use super::helpers::filter_eval::matches as filter_matches;
use super::manager::ConnectionsManager;
use super::models::{ChangeNotification, ChangeType, SubscriptionHandle};
use crate::error::KalamDbError;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::arrow_json_conversion::scalar_value_to_json;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{LiveQueryId, TableId, UserId};
use kalamdb_commons::websocket::{RowData, SharedChangePayload, WireNotification};
use kalamdb_system::NotificationService as NotificationServiceTrait;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc;

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

/// Convert a Row to a projected RowData map (`HashMap<String, KalamCellValue>`).
/// Includes `_seq` always. When `projections` is `None`, includes all columns.
fn project_row(row: &Row, projections: &Option<Arc<Vec<String>>>) -> Result<RowData, KalamDbError> {
    let mut map = HashMap::new();
    for (col, sv) in &row.values {
        let include = match projections {
            None => true,
            Some(proj) => col == SystemColumnNames::SEQ || proj.iter().any(|p| p == col),
        };
        if include {
            let cell = scalar_value_to_json(sv)
                .map_err(|e| KalamDbError::SerializationError(e.to_string()))?;
            map.insert(col.clone(), cell);
        }
    }
    Ok(map)
}

/// Compute UPDATE delta rows from old/new Row data.
/// `new_map` contains all non-null projected columns + PK + `_seq`.
/// `old_map` contains only changed columns + PK + `_seq`.
fn project_update_delta(
    old_row: &Row,
    new_row: &Row,
    pk_columns: &[String],
    projections: &Option<Arc<Vec<String>>>,
) -> Result<(RowData, RowData), KalamDbError> {
    let mut new_map = HashMap::new();
    let mut old_map = HashMap::new();

    // Always include _seq and PK columns
    for key in std::iter::once(SystemColumnNames::SEQ).chain(pk_columns.iter().map(String::as_str))
    {
        if let Some(sv) = new_row.values.get(key) {
            new_map.insert(
                key.to_string(),
                scalar_value_to_json(sv)
                    .map_err(|e| KalamDbError::SerializationError(e.to_string()))?,
            );
        }
        if let Some(sv) = old_row.values.get(key) {
            old_map.insert(
                key.to_string(),
                scalar_value_to_json(sv)
                    .map_err(|e| KalamDbError::SerializationError(e.to_string()))?,
            );
        }
    }

    let owned_keys;
    let col_names: Box<dyn Iterator<Item = &str>> = match projections {
        None => {
            owned_keys = new_row.values.keys().collect::<Vec<_>>();
            Box::new(owned_keys.iter().map(|s| s.as_str()))
        },
        Some(cols) => Box::new(cols.iter().map(String::as_str)),
    };

    for col in col_names {
        if col.starts_with('_') || pk_columns.iter().any(|pk| pk == col) {
            continue;
        }
        let new_sv = new_row.values.get(col);
        let old_sv = old_row.values.get(col);
        if let Some(nv) = new_sv {
            if !nv.is_null() {
                new_map.insert(
                    col.to_string(),
                    scalar_value_to_json(nv)
                        .map_err(|e| KalamDbError::SerializationError(e.to_string()))?,
                );
            }
        }
        let changed = match (old_sv, new_sv) {
            (Some(ov), Some(nv)) => ov != nv,
            (None, Some(_)) => true,
            _ => false,
        };
        if changed {
            if let Some(ov) = old_sv {
                old_map.insert(
                    col.to_string(),
                    scalar_value_to_json(ov)
                        .map_err(|e| KalamDbError::SerializationError(e.to_string()))?,
                );
            }
        }
    }

    Ok((new_map, old_map))
}

/// Build the `SharedChangePayload` for a notification.
/// Converts Row data to `RowData` (`HashMap<String, KalamCellValue>`) once per
/// projection group; the result is shared via `Arc` across all subscribers.
fn build_shared_payload(
    change_type: &ChangeType,
    new_row: &Row,
    old_row: Option<&Row>,
    pk_columns: &[String],
    projections: &Option<Arc<Vec<String>>>,
) -> Result<SharedChangePayload, KalamDbError> {
    match change_type {
        ChangeType::Insert => Ok(SharedChangePayload::new(
            kalamdb_commons::websocket::ChangeType::Insert,
            Some(vec![project_row(new_row, projections)?]),
            None,
        )),
        ChangeType::Update => {
            let (new_rd, old_rd) =
                project_update_delta(old_row.unwrap_or(new_row), new_row, pk_columns, projections)?;
            Ok(SharedChangePayload::new(
                kalamdb_commons::websocket::ChangeType::Update,
                Some(vec![new_rd]),
                Some(vec![old_rd]),
            ))
        },
        ChangeType::Delete => Ok(SharedChangePayload::new(
            kalamdb_commons::websocket::ChangeType::Delete,
            None,
            Some(vec![project_row(new_row, projections)?]),
        )),
    }
}

/// Try to deliver a notification to a subscriber, handling flow control.
/// Returns `true` if the notification was sent to the channel.
#[inline]
fn try_deliver(
    handle: &SubscriptionHandle,
    notification: Arc<WireNotification>,
    seq_value: Option<SeqId>,
) -> bool {
    if let Some(flow_control) = handle.flow_control.as_ref() {
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
    }

    match handle.notification_tx.try_send(notification) {
        Ok(()) => {
            handle.runtime_metadata.record_delivery();
            true
        },
        Err(e) => {
            use tokio::sync::mpsc::error::TrySendError;
            match e {
                TrySendError::Full(_) => {
                    log::warn!(
                        "Notification channel full for subscription_id={}, dropping notification",
                        handle.subscription_id
                    );
                },
                TrySendError::Closed(_) => {
                    log::debug!(
                        "Notification channel closed for subscription_id={}, connection likely disconnected",
                        handle.subscription_id
                    );
                },
            }
            false
        },
    }
}

/// Service for notifying subscribers of changes
///
/// Uses Arc<ConnectionsManager> directly since ConnectionsManager internally
/// uses DashMap for lock-free concurrent access - no RwLock wrapper needed.
///
/// Also handles topic pub/sub routing via TopicPublisherService.
///
/// Notifications are delivered from the local table-apply path only.
/// In cluster mode, each node dispatches to its own locally-connected
/// subscribers when the committed Raft entry is applied on that node.
pub struct NotificationService {
    /// Manager uses DashMap internally for lock-free access
    registry: Arc<ConnectionsManager>,
    /// Sharded notification channels — deterministic routing by table_id hash
    /// preserves per-table ordering while achieving parallelism across tables.
    worker_txs: Vec<mpsc::Sender<NotificationTask>>,
}

impl NotificationService {
    /// Check if there are any subscribers for a given user and table
    pub fn has_subscribers(&self, user_id: &UserId, table_id: &TableId) -> bool {
        self.registry.has_subscriptions(user_id, table_id)
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

    /// Process a single notification task by dispatching it to local subscribers.
    ///
    /// In cluster mode, this method is called only after the local Raft state
    /// machine has applied the committed mutation on this node.
    async fn process_notification(&self, task: NotificationTask, worker_idx: usize) {
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

    /// Notify subscribers about a table change (fire-and-forget async)
    ///
    /// In cluster mode, this is called from the local apply path on every node,
    /// so each replica notifies only its own local subscribers.
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

        let handle_count = all_handles.len();
        if handle_count == 0 {
            return Ok(0);
        }

        // Small fan-out: inline dispatch directly from DashMap refs (no clone/spawn overhead)
        if handle_count <= SHARED_NOTIFY_CHUNK_SIZE {
            return dispatch_chunk(
                all_handles.iter().map(|entry| entry.value().clone()),
                &new_row,
                old_row.as_deref(),
                &change_type,
                &pk_columns,
                seq_value,
            );
        }

        // Large fan-out: parallel chunked dispatch
        // Large fan-out: collect handles once, then parallel chunked dispatch
        let handles: Vec<SubscriptionHandle> =
            all_handles.iter().map(|entry| entry.value().clone()).collect();

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
                dispatch_chunk(chunk.into_iter(), &nr, or.as_deref(), &ct, &pk, seq_value)
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
/// `Arc<SharedChangePayload>`, avoiding redundant Row→RowData conversion.
fn dispatch_chunk<I>(
    handles: I,
    new_row: &Row,
    old_row: Option<&Row>,
    change_type: &ChangeType,
    pk_columns: &[String],
    seq_value: Option<SeqId>,
) -> Result<usize, KalamDbError>
where
    I: IntoIterator<Item = SubscriptionHandle>,
{
    // Cache: projection pointer → shared payload (built once per projection group).
    let mut cache: HashMap<usize, Arc<SharedChangePayload>> = HashMap::new();
    let mut count = 0usize;

    for handle in handles {
        // Filter check (operates on ScalarValue — no serialization)
        if let Some(ref filter_expr) = handle.filter_expr {
            match filter_matches(filter_expr, new_row) {
                Ok(true) => {},
                Ok(false) => continue,
                Err(e) => {
                    log::error!(
                        "Filter error for subscription_id={}: {}",
                        handle.subscription_id,
                        e
                    );
                    continue;
                },
            }
        }

        // Projection cache key: Arc pointer address for Some, 0 for None (SELECT *)
        let proj_key = match &handle.projections {
            Some(arc) => Arc::as_ptr(arc) as usize,
            None => 0,
        };

        let payload = if let Some(cached) = cache.get(&proj_key) {
            Arc::clone(cached)
        } else {
            let p = Arc::new(build_shared_payload(
                change_type,
                new_row,
                old_row,
                pk_columns,
                &handle.projections,
            )?);
            cache.insert(proj_key, Arc::clone(&p));
            p
        };

        // Per-subscriber: only allocate the subscription_id string + Arc clone of payload
        let notification = Arc::new(WireNotification {
            subscription_id: Arc::clone(&handle.subscription_id),
            payload,
        });

        if try_deliver(&handle, notification, seq_value) {
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
    use crate::live::models::{
        SubscriptionFlowControl, SubscriptionHandle, SubscriptionRuntimeMetadata,
    };
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::{ConnectionId, NamespaceId, TableName};
    use kalamdb_commons::NodeId;
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
        subscription_id: &str,
        tx: Arc<crate::live::models::NotificationSender>,
        flow_control: Arc<SubscriptionFlowControl>,
        filter_expr: Option<&str>,
        projections: Option<Vec<&str>>,
    ) -> SubscriptionHandle {
        SubscriptionHandle {
            subscription_id: Arc::from(subscription_id),
            filter_expr: filter_expr
                .map(|sql| parse_where_clause(sql).expect("filter should parse"))
                .map(Arc::new),
            projections: projections.map(|cols| {
                Arc::new(cols.into_iter().map(std::string::ToString::to_string).collect())
            }),
            notification_tx: tx,
            flow_control: Some(flow_control),
            runtime_metadata: Arc::new(SubscriptionRuntimeMetadata::new(
                Arc::from("SELECT * FROM shared.events"),
                None,
                1,
            )),
        }
    }

    #[tokio::test]
    async fn test_notify_async_shared_applies_filter_and_projection() {
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
                "sub_ok",
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
            make_shared_handle(
                "sub_skip",
                Arc::new(tx_skip),
                Arc::clone(&flow_skip),
                Some("id >= 100"),
                None,
            ),
        );

        let change = ChangeNotification::insert(table_id.clone(), make_row(42, "hello", 42));
        service.notify_async(None, table_id, change);

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx_ok.recv())
            .await
            .expect("matching subscriber gets message within timeout")
            .expect("matching subscriber gets message");
        let wire = delivered.as_ref();
        assert_eq!(wire.subscription_id.as_ref(), "sub_ok");
        assert_eq!(wire.payload.change_type, kalamdb_commons::websocket::ChangeType::Insert);
        assert!(wire.payload.old_values.is_none());
        assert!(wire.payload.rows.is_some());
        // Verify projected columns via JSON serialization
        let json: serde_json::Value = serde_json::from_slice(&wire.to_json()).unwrap();
        let rows = json["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get("id").is_some());
        assert!(rows[0].get("body").is_none());
        assert!(rows[0].get(SystemColumnNames::SEQ).is_some());

        assert!(rx_skip.try_recv().is_err(), "filtered subscriber should not receive");
    }

    #[tokio::test]
    async fn test_notify_async_user_table_projection_keeps_seq() {
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
            make_shared_handle("sub_user", Arc::new(tx), flow, None, Some(vec!["id"])),
        );

        let change = ChangeNotification::insert(table_id.clone(), make_row(42, "hello", 42));
        service.notify_async(Some(user_id), table_id, change);

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("projected subscriber gets message within timeout")
            .expect("projected subscriber gets message");
        let wire = delivered.as_ref();
        assert_eq!(wire.subscription_id.as_ref(), "sub_user");
        assert_eq!(wire.payload.change_type, kalamdb_commons::websocket::ChangeType::Insert);
        assert!(wire.payload.old_values.is_none());
        assert!(wire.payload.rows.is_some());
        let json: serde_json::Value = serde_json::from_slice(&wire.to_json()).unwrap();
        let rows = json["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get("id").is_some());
        assert!(rows[0].get("body").is_none());
        assert!(rows[0].get(SystemColumnNames::SEQ).is_some());
    }

    #[tokio::test]
    async fn test_notify_async_shared_buffers_when_initial_load_incomplete() {
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
            make_shared_handle("sub_buffer", Arc::new(tx), Arc::clone(&flow), None, None),
        );

        // seq=11 is newer than snapshot end seq=10, should be buffered (not sent yet)
        let change = ChangeNotification::insert(table_id, make_row(7, "buffer-me", 11));
        service.notify_async(None, make_table_id("shared", "logs"), change);

        tokio::time::sleep(Duration::from_millis(50)).await;

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
            make_shared_handle("sub_async", Arc::new(tx), flow, None, None),
        );

        let change = ChangeNotification::insert(table_id.clone(), make_row(1, "async", 1));
        service.notify_async(None, table_id.clone(), change);

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("worker should dispatch within timeout")
            .expect("message should be present");

        assert_eq!(delivered.subscription_id.as_ref(), "sub_async");

        assert!(NotificationServiceTrait::has_subscribers(&*service, None, &table_id));
    }
}
