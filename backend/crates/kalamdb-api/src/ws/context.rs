use std::sync::Arc;

use kalamdb_auth::{AuthRequest, UserRepository};
use kalamdb_commons::websocket::ProtocolOptions;
use kalamdb_core::app_context::AppContext;
use kalamdb_live::{ConnectionsManager, LiveQueryManager};

use crate::limiter::RateLimiter;

pub(super) struct UpgradeAuth {
    pub(super) auth_request: AuthRequest,
    pub(super) protocol: ProtocolOptions,
}

#[derive(Clone)]
pub(super) struct WsHandlerContext {
    pub(super) app_context: Arc<AppContext>,
    pub(super) rate_limiter: Arc<RateLimiter>,
    pub(super) live_query_manager: Arc<LiveQueryManager>,
    pub(super) user_repo: Arc<dyn UserRepository>,
    pub(super) connection_registry: Arc<ConnectionsManager>,
    pub(super) max_message_size: usize,
    pub(super) compression_enabled: bool,
}

impl WsHandlerContext {
    pub(super) fn new(
        app_context: Arc<AppContext>,
        rate_limiter: Arc<RateLimiter>,
        live_query_manager: Arc<LiveQueryManager>,
        user_repo: Arc<dyn UserRepository>,
        connection_registry: Arc<ConnectionsManager>,
        max_message_size: usize,
        compression_enabled: bool,
    ) -> Self {
        Self {
            app_context,
            rate_limiter,
            live_query_manager,
            user_repo,
            connection_registry,
            max_message_size,
            compression_enabled,
        }
    }
}
