//! Subscription service for live queries and consumer sessions
//!
//! Handles registration and unregistration of:
//! - Live query subscriptions (WebSocket)
//! - Topic consumer sessions (HTTP long polling)
//!
//! All SQL parsing is done inside register_subscription - no intermediate ParsedSubscription.
//!
//! Active subscriptions are tracked in node-local memory and surfaced through
//! `system.live`. Table mutations still replicate through Raft separately.

use super::manager::ConnectionsManager;
use super::models::{
    InitialLoadState, SharedConnectionState, SubscriptionFlowControl, SubscriptionHandle,
    SubscriptionRuntimeMetadata, SubscriptionState,
};
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use chrono::Utc;
use datafusion::sql::sqlparser::ast::Expr;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{ConnectionId, LiveQueryId, TableId, UserId};
use kalamdb_commons::websocket::SubscriptionRequest;
use kalamdb_commons::TableType;
use log::debug;
use std::sync::Arc;

/// Service for managing subscriptions
///
/// Uses Arc<ConnectionsManager> directly since ConnectionsManager internally
/// uses DashMap for lock-free concurrent access - no RwLock wrapper needed.
pub struct SubscriptionService {
    /// Manager uses DashMap internally for lock-free access
    registry: Arc<ConnectionsManager>,
}

impl SubscriptionService {
    pub fn new(registry: Arc<ConnectionsManager>) -> Self {
        Self { registry }
    }

    /// Register a live query subscription
    ///
    /// This method performs all SQL parsing internally and creates the SubscriptionState.
    /// The ws_handler passes the SharedConnectionState directly.
    ///
    /// Parameters:
    /// - connection_state: Shared reference to the connection state
    /// - request: Client subscription request containing SQL and options
    /// - table_id: Pre-validated table identifier (validated in ws_handler)
    /// - filter_expr: Optional parsed WHERE clause expression
    /// - projections: Optional column projections (None = SELECT *, i.e., all columns)
    /// - batch_size: Batch size for initial data fetching
    /// - enable_initial_load: Whether the subscription needs snapshot and batch state
    /// - table_type: Type of the table (User, Shared, System, Stream)
    pub async fn register_subscription(
        &self,
        connection_state: &SharedConnectionState,
        request: &SubscriptionRequest,
        table_id: TableId,
        filter_expr: Option<Expr>,
        projections: Option<Vec<String>>,
        batch_size: usize,
        enable_initial_load: bool,
        table_type: TableType,
    ) -> Result<LiveQueryId, KalamDbError> {
        // Read connection info from state and check subscription limit
        let (connection_id, user_id, notification_tx) = {
            let user_id = connection_state.user_id().cloned().ok_or_else(|| {
                KalamDbError::InvalidOperation("Connection not authenticated".to_string())
            })?;

            // Prevent DoS via excessive subscriptions per connection
            const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 100;
            if connection_state.subscription_count() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Maximum subscriptions ({}) per connection exceeded",
                    MAX_SUBSCRIPTIONS_PER_CONNECTION
                )));
            }

            (
                connection_state.connection_id().clone(),
                user_id,
                connection_state.notification_tx.clone(),
            )
        };

        // Generate LiveQueryId
        let live_id = LiveQueryId::new(user_id.clone(), connection_id.clone(), request.id.clone());

        let options_json = request
            .options
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .into_serialization_error("Failed to serialize options")?
            .map(Arc::<str>::from);
        let created_at_ms = Utc::now().timestamp_millis();
        let runtime_metadata = Arc::new(SubscriptionRuntimeMetadata::new(
            Arc::from(request.sql.as_str()),
            options_json,
            created_at_ms,
        ));

        // Wrap filter_expr and projections in Arc for zero-copy sharing between state and handle
        let filter_expr_arc = filter_expr.map(Arc::new);
        let projections_arc = projections.map(Arc::new);
        let subscription_id = Arc::<str>::from(request.id.as_str());

        // Create SubscriptionState with all necessary data (stored in ConnectionState)
        let flow_control = enable_initial_load.then(|| Arc::new(SubscriptionFlowControl::new()));

        let is_shared = table_type == TableType::Shared;

        let subscription_state = SubscriptionState {
            live_id: live_id.clone(),
            table_id: table_id.clone(),
            filter_expr: filter_expr_arc.clone(),
            projections: projections_arc.clone(),
            initial_load: flow_control.as_ref().map(|flow_control| InitialLoadState {
                batch_size,
                snapshot_end_seq: None,
                current_batch_num: 0,
                flow_control: Arc::clone(flow_control),
            }),
            is_shared,
            runtime_metadata: Arc::clone(&runtime_metadata),
        };

        // Create lightweight handle for the index (~48 bytes vs ~800+ bytes)
        let subscription_handle = SubscriptionHandle {
            subscription_id,
            filter_expr: filter_expr_arc,
            projections: projections_arc,
            notification_tx,
            flow_control,
            runtime_metadata,
        };

        // CRITICAL: Index subscription BEFORE sending Raft command to avoid race condition.
        // If we index after Raft, INSERT commands might be applied before the subscription
        // is indexed, causing notifications to be missed.
        // Add subscription to connection state
        connection_state.insert_subscription(request.id.clone(), subscription_state);

        // Add lightweight handle to registry's table index for efficient lookups
        if table_type == TableType::Shared {
            self.registry.index_shared_subscription(
                &connection_id,
                live_id.clone(),
                table_id.clone(),
                subscription_handle,
            );
        } else {
            self.registry.index_subscription(
                &user_id,
                &connection_id,
                live_id.clone(),
                table_id.clone(),
                subscription_handle,
            );
        }

        debug!(
            "Registered subscription {} for connection {} on {}",
            request.id, connection_id, table_id
        );

        Ok(live_id)
    }

    /// Update the snapshot_end_seq for a subscription after initial data fetch
    pub fn update_snapshot_end_seq(
        &self,
        connection_state: &SharedConnectionState,
        subscription_id: &str,
        snapshot_end_seq: SeqId,
    ) {
        connection_state.update_snapshot_end_seq(subscription_id, Some(snapshot_end_seq));
    }

    /// Unregister a single live query subscription
    pub async fn unregister_subscription(
        &self,
        connection_state: &SharedConnectionState,
        subscription_id: &str,
        live_id: &LiveQueryId,
    ) -> Result<(), KalamDbError> {
        // Get user_id and subscription details, then remove from connection state.
        // Try the raw subscription_id first; if it looks like a full LiveQueryId
        // ("user-conn-sub"), also try extracting just the trailing subscription part.
        // This handles the case where notifications were previously sending the full
        // LiveQueryId as the subscription_id and the client echoes it back.
        let (connection_id, user_id, table_id, is_shared) = {
            let user_id = connection_state.user_id().cloned().ok_or_else(|| {
                KalamDbError::InvalidOperation("Connection not authenticated".to_string())
            })?;
            let subscription =
                connection_state.remove_subscription_with_fallback(subscription_id, || {
                    LiveQueryId::from_string(subscription_id)
                        .ok()
                        .map(|parsed| parsed.subscription_id().to_string())
                });
            match subscription {
                Some((_key, sub)) => {
                    (connection_state.connection_id().clone(), user_id, sub.table_id, sub.is_shared)
                },
                None => {
                    // This is benign — the subscription may already have been
                    // removed by cleanup_connection (WS close race).
                    debug!("Subscription already removed (benign): {}", subscription_id);
                    return Ok(());
                },
            }
        };

        // Remove from registry's table index
        if is_shared {
            self.registry.unindex_shared_subscription(live_id, &table_id);
        } else {
            self.registry.unindex_subscription(&user_id, live_id, &table_id);
        }

        debug!("Unregistered subscription {} for connection {}", subscription_id, connection_id);

        Ok(())
    }

    /// Unregister a WebSocket connection and all its subscriptions
    ///
    /// Cleans up both the system.live view rows and the
    /// ConnectionsManager registry.
    pub async fn unregister_connection(
        &self,
        _user_id: &UserId,
        connection_id: &ConnectionId,
    ) -> Result<Vec<LiveQueryId>, KalamDbError> {
        // Unregister from connections manager (removes connection and returns live_ids)
        let live_ids = self.registry.unregister_connection(connection_id);

        Ok(live_ids)
    }

    /// Get subscription state from a connection
    pub fn get_subscription(
        &self,
        connection_state: &SharedConnectionState,
        subscription_id: &str,
    ) -> Option<SubscriptionState> {
        connection_state.get_subscription(subscription_id)
    }
}
