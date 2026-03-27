//! Subscription service for live queries and consumer sessions
//!
//! Handles registration and unregistration of:
//! - Live query subscriptions (WebSocket)
//! - Topic consumer sessions (HTTP long polling)
//!
//! All SQL parsing is done inside register_subscription - no intermediate ParsedSubscription.
//!
//! Live query records are replicated through Raft UserDataCommand for cluster-wide visibility.
//! They are sharded by user_id for efficient per-user subscription management.

use super::manager::ConnectionsManager;
use super::models::{
    SharedConnectionState, SubscriptionFlowControl, SubscriptionHandle, SubscriptionState,
};
use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use chrono::Utc;
use datafusion::sql::sqlparser::ast::Expr;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{ConnectionId, LiveQueryId, TableId, UserId};
use kalamdb_commons::websocket::SubscriptionRequest;
use kalamdb_commons::{NodeId, TableType};
use kalamdb_raft::UserDataCommand;
use kalamdb_system::providers::live_queries::models::{LiveQuery, LiveQueryStatus};
use log::debug;
use std::sync::Arc;

/// Service for managing subscriptions
///
/// Uses Arc<ConnectionsManager> directly since ConnectionsManager internally
/// uses DashMap for lock-free concurrent access - no RwLock wrapper needed.
///
/// Live query operations are replicated through Raft for cluster-wide visibility.
pub struct SubscriptionService {
    /// Manager uses DashMap internally for lock-free access
    registry: Arc<ConnectionsManager>,
    node_id: NodeId,
    /// AppContext for executing Raft commands
    app_context: Arc<AppContext>,
}

impl SubscriptionService {
    pub fn new(
        registry: Arc<ConnectionsManager>,
        node_id: NodeId,
        app_context: Arc<AppContext>,
    ) -> Self {
        Self {
            registry,
            node_id,
            app_context,
        }
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
    /// - table_type: Type of the table (User, Shared, System, Stream)
    pub async fn register_subscription(
        &self,
        connection_state: &SharedConnectionState,
        request: &SubscriptionRequest,
        table_id: TableId,
        filter_expr: Option<Expr>,
        projections: Option<Vec<String>>,
        batch_size: usize,
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

            (connection_state.connection_id().clone(), user_id, connection_state.notification_tx.clone())
        };

        // Generate LiveQueryId
        let live_id = LiveQueryId::new(user_id.clone(), connection_id.clone(), request.id.clone());

        // Clone subscription options directly
        let options = request.options.clone();

        // Serialize options to JSON
        let options_json = serde_json::to_string(&options)
            .into_serialization_error("Failed to serialize options")?;

        // Wrap filter_expr and projections in Arc for zero-copy sharing between state and handle
        let filter_expr_arc = filter_expr.map(Arc::new);
        let projections_arc = projections.map(Arc::new);

        // Create SubscriptionState with all necessary data (stored in ConnectionState)
        let flow_control = Arc::new(SubscriptionFlowControl::new());

        let is_shared = table_type == TableType::Shared;

        let subscription_state = SubscriptionState {
            live_id: live_id.clone(),
            table_id: table_id.clone(),
            sql: request.sql.as_str().into(), // Arc<str> for zero-copy
            filter_expr: filter_expr_arc.clone(),
            projections: projections_arc.clone(),
            batch_size,
            snapshot_end_seq: None,
            current_batch_num: 0, // Start at batch 0
            flow_control: Arc::clone(&flow_control),
            is_shared,
        };

        // Create lightweight handle for the index (~48 bytes vs ~800+ bytes)
        let subscription_handle = SubscriptionHandle {
            filter_expr: filter_expr_arc,
            projections: projections_arc,
            notification_tx,
            flow_control,
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

        // Create live query through Raft for cluster-wide replication
        // This ensures all nodes see the same live queries in system.live_queries
        // Note: We index the subscription first to ensure notifications aren't missed
        // If Raft fails, we clean up the subscription state below
        let now = Utc::now().timestamp_millis();
        let live_query = LiveQuery {
            live_id: live_id.clone(),
            connection_id: connection_id.as_str().to_string(),
            subscription_id: request.id.clone(),
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            user_id: user_id.clone(),
            query: request.sql.clone(),
            options: Some(options_json),
            status: LiveQueryStatus::Active,
            created_at: now,
            last_update: now,
            last_ping_at: now,
            changes: 0,
            node_id: self.node_id,
        };

        let cmd = UserDataCommand::CreateLiveQuery {
            required_meta_index: 0, // Schema was already validated during parse
            live_query,
        };

        if let Err(e) = self.app_context.executor().execute_user_data(&user_id, cmd).await {
            // Raft failed - clean up the subscription we just indexed
            if table_type == TableType::Shared {
                self.registry.unindex_shared_subscription(&live_id, &table_id);
            } else {
                self.registry.unindex_subscription(&user_id, &live_id, &table_id);
            }
            connection_state.remove_subscription(&request.id);
            return Err(KalamDbError::Other(format!("Failed to create live query: {}", e)));
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
            let subscription = connection_state.remove_subscription_with_fallback(
                subscription_id,
                || {
                    LiveQueryId::from_string(subscription_id)
                        .ok()
                        .map(|parsed| parsed.subscription_id().to_string())
                },
            );
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

        // Delete from system.live_queries through Raft for cluster-wide replication
        let cmd = UserDataCommand::DeleteLiveQuery {
            user_id: user_id.clone(),
            live_id: live_id.clone(),
            deleted_at: Utc::now(),
            required_meta_index: 0,
        };

        self.app_context
            .executor()
            .execute_user_data(&user_id, cmd)
            .await
            .map_err(|e| KalamDbError::Other(format!("Failed to delete live query: {}", e)))?;

        debug!("Unregistered subscription {} for connection {}", subscription_id, connection_id);

        Ok(())
    }

    /// Unregister a WebSocket connection and all its subscriptions
    ///
    /// Cleans up both the system.live_queries table entries and the
    /// ConnectionsManager registry.
    pub async fn unregister_connection(
        &self,
        user_id: &UserId,
        connection_id: &ConnectionId,
    ) -> Result<Vec<LiveQueryId>, KalamDbError> {
        // Delete from system.live_queries through Raft for cluster-wide replication
        let cmd = UserDataCommand::DeleteLiveQueriesByConnection {
            user_id: user_id.clone(),
            connection_id: connection_id.clone(),
            deleted_at: Utc::now(),
            required_meta_index: 0,
        };

        self.app_context.executor().execute_user_data(user_id, cmd).await.map_err(|e| {
            KalamDbError::Other(format!("Failed to delete live queries by connection: {}", e))
        })?;

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
