//! WebSocket connection cleanup handler
//!
//! Handles cleanup when a WebSocket connection is closed.

use kalamdb_core::live::{ConnectionsManager, LiveQueryManager, SharedConnectionState};
use log::debug;
use std::sync::Arc;

use crate::limiter::RateLimiter;

/// Cleanup connection on close
///
/// ConnectionsManager.unregister_connection handles all subscription cleanup.
///
/// Uses connection_id from SharedConnectionState, no separate parameter needed.
pub async fn cleanup_connection(
    connection_state: &SharedConnectionState,
    registry: &Arc<ConnectionsManager>,
    rate_limiter: &Arc<RateLimiter>,
    live_query_manager: &Arc<LiveQueryManager>,
) {
    // Get connection_id, user ID and subscription count before unregistering
    let (connection_id, user_id, subscription_count) = (
        connection_state.connection_id().clone(),
        connection_state.user_id().cloned(),
        connection_state.subscription_count(),
    );

    let removed_live_ids = if let Some(ref uid) = user_id {
        match live_query_manager.unregister_connection(uid, &connection_id).await {
            Ok(live_ids) => live_ids,
            Err(e) => {
                log::warn!(
                    "Failed to cleanup live queries for connection {}: {}",
                    connection_id,
                    e
                );
                registry.unregister_connection(&connection_id)
            },
        }
    } else {
        registry.unregister_connection(&connection_id)
    };

    if let Some(ref uid) = user_id {
        // Update rate limiter
        rate_limiter.cleanup_connection(&connection_id);
        for _ in 0..subscription_count {
            rate_limiter.decrement_subscription(uid);
        }
    }

    debug!(
        "Connection cleanup complete: {} (removed {} subscriptions)",
        connection_id,
        removed_live_ids.len()
    );
}
