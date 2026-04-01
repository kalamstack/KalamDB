//! WebSocket connection context

use kalamdb_auth::UserRepository;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::live::{ConnectionsManager, LiveQueryManager};
use std::sync::Arc;

use crate::limiter::RateLimiter;

/// Shared context for WebSocket handlers
///
/// This struct bundles all the shared dependencies needed by WebSocket
/// event handlers, reducing the number of parameters passed around.
pub struct WsContext {
    pub app_context: Arc<AppContext>,
    pub rate_limiter: Arc<RateLimiter>,
    pub live_query_manager: Arc<LiveQueryManager>,
    pub user_repo: Arc<dyn UserRepository>,
    pub connections_manager: Arc<ConnectionsManager>,
    pub max_message_size: usize,
}

impl WsContext {
    /// Create a new WebSocket context
    pub fn new(
        app_context: Arc<AppContext>,
        rate_limiter: Arc<RateLimiter>,
        live_query_manager: Arc<LiveQueryManager>,
        user_repo: Arc<dyn UserRepository>,
        connections_manager: Arc<ConnectionsManager>,
        max_message_size: usize,
    ) -> Self {
        Self {
            app_context,
            rate_limiter,
            live_query_manager,
            user_repo,
            connections_manager,
            max_message_size,
        }
    }
}
