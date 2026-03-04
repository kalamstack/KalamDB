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
use crate::providers::arrow_json_conversion::row_to_json_map;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{LiveQueryId, TableId, UserId};
use kalamdb_raft::ClusterClient;
use kalamdb_raft::NotifyFollowersRequest;
use kalamdb_system::NotificationService as NotificationServiceTrait;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{mpsc, OnceCell};

const NOTIFY_QUEUE_CAPACITY: usize = 10_000;

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

/// Apply column projections to a Row, returning only the requested columns.
/// Uses Cow to avoid cloning when no projections are needed (SELECT *).
#[inline]
fn apply_projections<'a>(row: &'a Row, projections: &Option<Arc<Vec<String>>>) -> Cow<'a, Row> {
    match projections {
        None => Cow::Borrowed(row),
        Some(cols) => {
            let filtered_values: BTreeMap<String, ScalarValue> = cols
                .iter()
                .filter_map(|col| row.values.get(col).map(|v| (col.clone(), v.clone())))
                .collect();
            Cow::Owned(Row::new(filtered_values))
        },
    }
}

/// Compute the delta between old and new JSON row maps for UPDATE notifications.
///
/// Returns `(delta_new, delta_old)` where:
/// - `delta_new` contains only the changed columns + `_seq` + any user-defined PK
///   columns (new values).
/// - `delta_old` contains only the changed columns + `_seq` + PK columns (old values).
///
/// `pk_columns` lists the user-defined primary key column name(s) that must always be
/// included so clients can identify the row. `_seq` is always included automatically.
/// `_deleted` is **not** included — DELETE events are sent as separate notifications.
#[inline]
fn compute_json_update_delta(
    old_json: &std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>,
    new_json: &std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>,
    pk_columns: &[String],
) -> (
    std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>,
    std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>,
) {
    let mut delta_new = std::collections::HashMap::new();
    let mut delta_old = std::collections::HashMap::new();

    // Always include _seq for row identification
    let seq_col = SystemColumnNames::SEQ.to_string();
    if let Some(v) = new_json.get(&seq_col) {
        delta_new.insert(seq_col.clone(), v.clone());
    }
    if let Some(v) = old_json.get(&seq_col) {
        delta_old.insert(seq_col.clone(), v.clone());
    }

    // Always include user-defined PK column(s) for row identification
    for pk_col in pk_columns {
        if let Some(v) = new_json.get(pk_col) {
            delta_new.insert(pk_col.clone(), v.clone());
        }
        if let Some(v) = old_json.get(pk_col) {
            delta_old.insert(pk_col.clone(), v.clone());
        }
    }

    // Compare all non-system columns; include only those that actually changed
    for (col_name, new_val) in new_json {
        // Skip _seq (already handled) and other system columns (_deleted, etc.)
        if col_name.starts_with('_') {
            continue;
        }
        // Skip PK columns (already handled above)
        if pk_columns.contains(col_name) {
            continue;
        }

        let old_val = old_json.get(col_name);
        let changed = match old_val {
            Some(old_v) => old_v != new_val,
            None => true,
        };

        if changed {
            delta_new.insert(col_name.clone(), new_val.clone());
            if let Some(old_v) = old_val {
                delta_old.insert(col_name.clone(), old_v.clone());
            }
        }
    }

    (delta_new, delta_old)
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
    notify_tx: mpsc::Sender<NotificationTask>,
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
        let (notify_tx, mut notify_rx) = mpsc::channel(NOTIFY_QUEUE_CAPACITY);
        let service = Arc::new(Self {
            registry,
            notify_tx,
            app_context: OnceCell::new(),
            cluster_client: OnceCell::new(),
        });

        // Notification worker (single task, no per-notification spawn)
        let notify_service = Arc::clone(&service);
        tokio::spawn(async move {
            while let Some(task) = notify_rx.recv().await {
                // Step 0: Leadership check (Raft cluster mode)
                //
                // Only the leader processes and broadcasts notifications.
                // Followers silently drop locally-generated notifications because
                // writes only land on the leader (user/stream tables).
                if let Some(weak_ctx) = notify_service.app_context.get() {
                    if let Some(ctx) = weak_ctx.upgrade() {
                        let is_leader = match task.user_id.as_ref() {
                            Some(uid) => ctx.is_leader_for_user(uid).await,
                            None => ctx.is_leader_for_shared().await,
                        };

                        if !is_leader {
                            log::trace!(
                                "Skipping notification on follower node for table {}",
                                task.table_id
                            );
                            continue;
                        }

                        // Leader: broadcast to all other cluster nodes via gRPC
                        // so followers can dispatch to their locally-connected subscribers.
                        if ctx.is_cluster_mode() {
                            if let Some(cluster_client) = notify_service.cluster_client.get() {
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

                // Step 1: Route to live query subscriptions
                // Topic publishing is now handled synchronously in table providers,
                // so the notification worker only handles live query fan-out.
                if let Some(ref user_id) = task.user_id {
                    // User/stream table: specific user lookup
                    let handles = notify_service
                        .registry
                        .get_subscriptions_for_table(user_id, &task.table_id);
                    if handles.is_empty() {
                        log::debug!(
                            "NotificationWorker: No subscriptions for user={}, table={} (skipping notification)",
                            user_id, task.table_id
                        );
                        continue;
                    }
                    log::debug!(
                        "NotificationWorker: Found {} subscriptions for user={}, table={}",
                        handles.len(),
                        user_id,
                        task.table_id
                    );

                    if let Err(e) = notify_service
                        .notify_table_change_with_handles(
                            &task.table_id,
                            task.notification,
                            handles,
                        )
                        .await
                    {
                        log::warn!(
                            "Failed to notify subscribers for table {}: {}",
                            task.table_id,
                            e
                        );
                    }
                } else {
                    // Shared table: streaming parallel fan-out
                    let handles = notify_service
                        .registry
                        .get_shared_subscriptions_for_table(&task.table_id);
                    if handles.is_empty() {
                        log::debug!(
                            "NotificationWorker: No shared subscriptions for table={} (skipping notification)",
                            task.table_id
                        );
                        continue;
                    }
                    log::debug!(
                        "NotificationWorker: Found {} shared subscriptions for table={}",
                        handles.len(),
                        task.table_id
                    );

                    if let Err(e) = notify_service
                        .notify_shared_table_streaming(
                            &task.table_id,
                            task.notification,
                            handles,
                        )
                        .await
                    {
                        log::warn!(
                            "Failed to notify shared table subscribers for table {}: {}",
                            task.table_id,
                            e
                        );
                    }
                }
            }
        });

        service
    }

    /// Handle a notification forwarded from the leader node.
    ///
    /// This bypasses the leadership check because the leader already validated
    /// ownership and is broadcasting to followers.  The follower dispatches
    /// to any locally-connected WebSocket subscribers.
    pub async fn notify_forwarded(
        &self,
        user_id: UserId,
        table_id: TableId,
        notification: ChangeNotification,
    ) {
        let handles = self.registry.get_subscriptions_for_table(&user_id, &table_id);
        if handles.is_empty() {
            log::trace!(
                "notify_forwarded: no local subscriptions for user={}, table={}",
                user_id,
                table_id
            );
            return;
        }
        log::debug!(
            "notify_forwarded: dispatching to {} local subscriptions for user={}, table={}",
            handles.len(),
            user_id,
            table_id
        );
        if let Err(e) = self
            .notify_table_change_with_handles(&table_id, notification, handles)
            .await
        {
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
            log::trace!(
                "notify_forwarded_shared: no local shared subscriptions for table={}",
                table_id
            );
            return;
        }
        log::debug!(
            "notify_forwarded_shared: dispatching to {} local shared subscriptions for table={}",
            handles.len(),
            table_id
        );
        if let Err(e) = self
            .notify_shared_table_streaming(&table_id, notification, handles)
            .await
        {
            log::warn!("Failed to dispatch forwarded shared notification for table {}: {}", table_id, e);
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
        let task = NotificationTask {
            user_id,
            table_id,
            notification,
        };
        if let Err(e) = self.notify_tx.try_send(task) {
            if matches!(e, mpsc::error::TrySendError::Full(_)) {
                log::warn!("Notification queue full, dropping notification");
            }
        }
    }

    // /// Notify live query subscribers of a table change
    // pub async fn notify_table_change(
    //     &self,
    //     user_id: &UserId,
    //     table_id: &TableId,
    //     change_notification: ChangeNotification,
    // ) -> Result<usize, KalamDbError> {
    //     // Fetch handles and delegate to common implementation
    //     let handles = self.registry.get_subscriptions_for_table(user_id, table_id);
    //     self.notify_table_change_with_handles(user_id, table_id, change_notification, handles)
    //         .await
    // }

    /// Notify live query subscribers with pre-fetched handles
    /// Avoids double DashMap lookup when handles are already available
    async fn notify_table_change_with_handles(
        &self,
        table_id: &TableId,
        change_notification: ChangeNotification,
        all_handles: Arc<dashmap::DashMap<LiveQueryId, SubscriptionHandle>>,
    ) -> Result<usize, KalamDbError> {
        log::debug!(
            "notify_table_change called for table: '{}', change_type: {:?}",
            table_id,
            change_notification.change_type
        );

        // Send notifications and increment changes
        let mut notification_count = 0usize;
        let filtering_row = &change_notification.row_data;
        let mut full_row_json: Option<std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>> = None;
        let mut full_old_json: Option<std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>> = None;
        let seq_value = extract_seq(&change_notification);
        for entry in all_handles.iter() {
            let live_id = entry.key();
            let handle = entry.value();
            let should_notify = if let Some(ref filter_expr) = handle.filter_expr {
                match filter_matches(filter_expr, filtering_row) {
                    Ok(true) => true,
                    Ok(false) => {
                        log::trace!(
                            "Filter didn't match for live_id={}, skipping notification",
                            live_id
                        );
                        false
                    },
                    Err(e) => {
                        log::error!("Filter evaluation error for live_id={}: {}", live_id, e);
                        false
                    },
                }
            } else {
                true
            };

            if !should_notify {
                continue;
            }

            let projections = &handle.projections;
            let tx = &handle.notification_tx;

            let use_full_row = projections.is_none();
            let row_json = if use_full_row {
                match full_row_json {
                    Some(ref json) => json.clone(),
                    None => match row_to_json_map(&change_notification.row_data) {
                        Ok(json) => {
                            full_row_json = Some(json.clone());
                            json
                        },
                        Err(e) => {
                            log::error!(
                                "Failed to convert row to JSON for live_id={}: {}",
                                live_id,
                                e
                            );
                            continue;
                        },
                    },
                }
            } else {
                // Apply projections to row data (filters columns based on subscription)
                // Uses Cow to avoid cloning when no projections (SELECT *)
                let projected_row_data =
                    apply_projections(&change_notification.row_data, projections);
                match row_to_json_map(&projected_row_data) {
                    Ok(json) => json,
                    Err(e) => {
                        log::error!("Failed to convert row to JSON for live_id={}: {}", live_id, e);
                        continue;
                    },
                }
            };

            let old_json = if let Some(old) = change_notification.old_data.as_ref() {
                if use_full_row {
                    match full_old_json {
                        Some(ref json) => Some(json.clone()),
                        None => match row_to_json_map(old) {
                            Ok(json) => {
                                full_old_json = Some(json.clone());
                                Some(json)
                            },
                            Err(e) => {
                                log::error!(
                                    "Failed to convert old row to JSON for live_id={}: {}",
                                    live_id,
                                    e
                                );
                                continue;
                            },
                        },
                    }
                } else {
                    let projected_old_data = apply_projections(old, projections);
                    match row_to_json_map(&projected_old_data) {
                        Ok(json) => Some(json),
                        Err(e) => {
                            log::error!(
                                "Failed to convert old row to JSON for live_id={}: {}",
                                live_id,
                                e
                            );
                            continue;
                        },
                    }
                }
            } else {
                None
            };

            // Build the notification with JSON data.
            // Use the original subscription_id (client-assigned name, e.g. "latency_1")
            // NOT the full LiveQueryId ("user-conn-sub").  This keeps the
            // subscription_id consistent between Ack and change events so the
            // client can send the correct ID back in Unsubscribe messages.
            let sub_id = live_id.subscription_id().to_string();
            let notification = match change_notification.change_type {
                ChangeType::Insert => kalamdb_commons::Notification::insert(sub_id, vec![row_json]),
                ChangeType::Update => {
                    // Compute delta: send only changed columns + PK + _seq
                    let full_old = old_json.unwrap_or_default();
                    let (delta_new, delta_old) =
                        compute_json_update_delta(&full_old, &row_json, &change_notification.pk_columns);
                    kalamdb_commons::Notification::update(
                        sub_id,
                        vec![delta_new],
                        vec![delta_old],
                    )
                },
                ChangeType::Delete => kalamdb_commons::Notification::delete(sub_id, vec![row_json]),
            };

            // Send notification through channel (non-blocking, bounded)
            let notification = Arc::new(notification);
            let flow_control = &handle.flow_control;

            if !flow_control.is_initial_complete() {
                if let Some(snapshot_seq) = flow_control.snapshot_end_seq() {
                    if let Some(seq) = seq_value {
                        if seq.as_i64() <= snapshot_seq {
                            continue;
                        }
                    }
                }

                flow_control.buffer_notification(Arc::clone(&notification), seq_value);
                continue;
            }

            if let Err(e) = tx.try_send(notification) {
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
            }

            notification_count += 1;
        }

        Ok(notification_count)
    }

    /// Streaming notification dispatch for shared tables with many subscribers.
    ///
    /// Optimized for high subscriber counts (hundreds to thousands):
    /// 1. Pre-computes row JSON ONCE (avoids N × ScalarValue→JSON conversions)
    /// 2. Fans out to subscribers in parallel chunks via `tokio::spawn`
    /// 3. Projection filtering operates on pre-computed JSON (no re-conversion)
    /// 4. Each chunk runs concurrently, preventing single-task bottleneck
    ///
    /// Complexity: O(N/chunk_size) parallelism, O(1) JSON pre-computation.
    async fn notify_shared_table_streaming(
        &self,
        table_id: &TableId,
        change_notification: ChangeNotification,
        all_handles: Arc<dashmap::DashMap<LiveQueryId, SubscriptionHandle>>,
    ) -> Result<usize, KalamDbError> {
        let handle_count = all_handles.len();
        log::debug!(
            "notify_shared_table_streaming: table='{}', change_type={:?}, subscribers={}",
            table_id,
            change_notification.change_type,
            handle_count
        );

        // === Phase 1: Pre-compute JSON once ===
        // This is the most expensive operation — do it exactly once instead of
        // lazily per-subscriber (which still triggers N HashMap clones).
        let full_row_json = row_to_json_map(&change_notification.row_data).map_err(|e| {
            log::error!("Failed to convert row to JSON for shared notification: {}", e);
            e
        })?;
        let full_row_json = Arc::new(full_row_json);

        let full_old_json = match change_notification.old_data.as_ref() {
            Some(old) => {
                let json = row_to_json_map(old).map_err(|e| {
                    log::error!("Failed to convert old row to JSON for shared notification: {}", e);
                    e
                })?;
                Some(Arc::new(json))
            },
            None => None,
        };

        let seq_value = extract_seq(&change_notification);
        let change_type = change_notification.change_type.clone();
        let pk_columns = Arc::new(change_notification.pk_columns.clone());
        // Wrap row in Arc for sharing with filter evaluation across chunks
        let filtering_row = Arc::new(change_notification.row_data);

        // === Phase 2: Collect handles for parallel processing ===
        // DashMap iteration yields temporary references; collect into owned Vec.
        // Clone is cheap: all SubscriptionHandle fields are Arc-wrapped.
        let handles: Vec<(LiveQueryId, SubscriptionHandle)> = all_handles
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        if handles.is_empty() {
            return Ok(0);
        }

        // === Phase 3: Parallel chunked fan-out ===
        let mut join_handles = Vec::with_capacity(
            (handles.len() + SHARED_NOTIFY_CHUNK_SIZE - 1) / SHARED_NOTIFY_CHUNK_SIZE,
        );

        for chunk in handles.chunks(SHARED_NOTIFY_CHUNK_SIZE) {
            let chunk: Vec<(LiveQueryId, SubscriptionHandle)> = chunk.to_vec();
            let row_json = Arc::clone(&full_row_json);
            let old_json = full_old_json.as_ref().map(Arc::clone);
            let ct = change_type.clone();
            let filter_row = Arc::clone(&filtering_row);
            let seq = seq_value;
            let pk_cols_ref = Arc::clone(&pk_columns);

            join_handles.push(tokio::spawn(async move {
                let mut count = 0usize;
                for (live_id, handle) in &chunk {
                    // Filter check
                    if let Some(ref filter_expr) = handle.filter_expr {
                        match filter_matches(filter_expr, &filter_row) {
                            Ok(true) => {},
                            Ok(false) => continue,
                            Err(e) => {
                                log::error!(
                                    "Filter error for live_id={}: {}",
                                    live_id,
                                    e
                                );
                                continue;
                            },
                        }
                    }

                    // Build per-subscriber row JSON:
                    // - No projections: clone pre-computed full JSON
                    // - With projections: filter columns from pre-computed JSON
                    //   (avoids ScalarValue→JSON re-conversion)
                    let subscriber_row_json = match &handle.projections {
                        None => (*row_json).clone(),
                        Some(cols) => cols
                            .iter()
                            .filter_map(|col| {
                                row_json.get(col).map(|v| (col.clone(), v.clone()))
                            })
                            .collect(),
                    };

                    let subscriber_old_json = old_json.as_ref().map(|oj| match &handle.projections {
                        None => (**oj).clone(),
                        Some(cols) => cols
                            .iter()
                            .filter_map(|col| oj.get(col).map(|v| (col.clone(), v.clone())))
                            .collect(),
                    });

                    // Build notification with per-subscriber subscription_id
                    let sub_id = live_id.subscription_id().to_string();
                    let notification = match ct {
                        ChangeType::Insert => {
                            kalamdb_commons::Notification::insert(sub_id, vec![subscriber_row_json])
                        },
                        ChangeType::Update => {
                            // Compute delta: send only changed columns + PK + _seq
                            let full_old = subscriber_old_json.unwrap_or_default();
                            let (delta_new, delta_old) =
                                compute_json_update_delta(&full_old, &subscriber_row_json, &pk_cols_ref);
                            kalamdb_commons::Notification::update(
                                sub_id,
                                vec![delta_new],
                                vec![delta_old],
                            )
                        },
                        ChangeType::Delete => {
                            kalamdb_commons::Notification::delete(sub_id, vec![subscriber_row_json])
                        },
                    };

                    let notification = Arc::new(notification);
                    let flow_control = &handle.flow_control;

                    // Flow control: buffer during initial data load
                    if !flow_control.is_initial_complete() {
                        if let Some(snapshot_seq) = flow_control.snapshot_end_seq() {
                            if let Some(seq) = seq {
                                if seq.as_i64() <= snapshot_seq {
                                    continue;
                                }
                            }
                        }
                        flow_control.buffer_notification(Arc::clone(&notification), seq);
                        continue;
                    }

                    // Send through channel (non-blocking)
                    if let Err(e) = handle.notification_tx.try_send(notification) {
                        use tokio::sync::mpsc::error::TrySendError;
                        match e {
                            TrySendError::Full(_) => {
                                log::warn!(
                                    "Notification channel full for live_id={}, dropping",
                                    live_id
                                );
                            },
                            TrySendError::Closed(_) => {
                                log::debug!(
                                    "Notification channel closed for live_id={}, disconnected",
                                    live_id
                                );
                            },
                        }
                    }

                    count += 1;
                }
                count
            }));
        }

        // Await all chunks
        let mut total_count = 0usize;
        for jh in join_handles {
            match jh.await {
                Ok(count) => total_count += count,
                Err(e) => {
                    log::error!("Shared notification chunk task panicked: {}", e);
                },
            }
        }

        log::debug!(
            "notify_shared_table_streaming: delivered {} notifications to {} subscribers for table '{}'",
            total_count,
            handle_count,
            table_id
        );

        Ok(total_count)
    }
}

impl NotificationServiceTrait for NotificationService {
    type Notification = ChangeNotification;

    fn has_subscribers(&self, user_id: Option<&UserId>, table_id: &TableId) -> bool {
        // Only check live query subscriptions.
        // Topic publishing is now handled synchronously in table providers via TopicPublisher trait,
        // so we no longer need to check for topic subscribers here.
        if let Some(uid) = user_id {
            if self.registry.has_subscriptions(uid, table_id) {
                return true;
            }
        }

        // Also check shared table subscriptions (user_id is None for shared tables)
        if self.registry.has_shared_subscriptions(table_id) {
            return true;
        }

        false
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
    use kalamdb_commons::Notification;
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
        values.insert(
            SystemColumnNames::SEQ.to_string(),
            ScalarValue::Int64(Some(seq)),
        );
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
            make_shared_handle(
                Arc::new(tx_skip),
                Arc::clone(&flow_skip),
                Some("id >= 100"),
                None,
            ),
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
                assert!(!payload[0].contains_key(SystemColumnNames::SEQ));
            },
            _ => panic!("expected change notification"),
        }

        assert!(rx_skip.try_recv().is_err(), "filtered subscriber should not receive");
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
            Notification::Change { subscription_id, .. } => {
                assert_eq!(subscription_id, "sub_async");
            },
            _ => panic!("expected change notification"),
        }

        assert!(NotificationServiceTrait::has_subscribers(&*service, None, &table_id));
    }
}
