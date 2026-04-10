//! WebSocket handler for live query subscriptions
//!
//! This module provides the HTTP endpoint for establishing WebSocket connections
//! and managing live query subscriptions using actix-ws (non-actor based).
//!
//! Connection lifecycle and heartbeat management is handled by the shared
//! ConnectionsManager from kalamdb-core.
//!
//! Architecture:
//! - Connection created in ConnectionsManager on WebSocket open
//! - Subscriptions stored in ConnectionState.subscriptions
//! - No local tracking needed - everything is in ConnectionState

use actix_web::{get, web, Error, HttpRequest, HttpResponse};
use kalamdb_auth::UserRepository;
use kalamdb_core::app_context::AppContext;
use kalamdb_live::{ConnectionId, ConnectionsManager, LiveQueryManager};
use log::{debug, warn};
use std::sync::Arc;

use super::context::WsHandlerContext;
use super::protocol::{authenticate_upgrade, compression_enabled_from_query, validate_origin};
use super::runtime::run_websocket;
use crate::limiter::RateLimiter;

/// GET /v1/ws - Establish WebSocket connection
///
/// Accepts unauthenticated WebSocket connections.
/// Authentication happens via post-connection Authenticate message (3-second timeout enforced).
/// Uses ConnectionsManager for consolidated connection state management.
///
/// Security:
/// - Origin header validation (if configured)
/// - Message size limits enforced
/// - Rate limiting per connection
#[get("/ws")]
pub async fn websocket_handler(
    req: HttpRequest,
    stream: web::Payload,
    app_context: web::Data<Arc<AppContext>>,
    rate_limiter: web::Data<Arc<RateLimiter>>,
    live_query_manager: web::Data<Arc<LiveQueryManager>>,
    user_repo: web::Data<Arc<dyn UserRepository>>,
    connection_registry: web::Data<Arc<ConnectionsManager>>,
) -> Result<HttpResponse, Error> {
    if connection_registry.is_shutting_down() {
        return Ok(HttpResponse::ServiceUnavailable().body("Server is shutting down"));
    }

    if let Err(response) = validate_origin(&req, app_context.get_ref()) {
        return Ok(response);
    }

    let compression_enabled = compression_enabled_from_query(&req);
    let pre_auth = match authenticate_upgrade(&req, user_repo.get_ref()).await {
        Ok(pre_auth) => pre_auth,
        Err(response) => return Ok(response),
    };

    let connection_id = ConnectionId::new(uuid::Uuid::new_v4().simple().to_string());
    let client_ip = kalamdb_auth::extract_client_ip_secure(&req);
    let _connect_span = tracing::debug_span!(
        "ws.connect",
        connection_id = %connection_id,
        client_ip = ?client_ip,
        compression_enabled
    )
    .entered();

    debug!(
        "New WebSocket connection: {} (pre_auth={})",
        connection_id,
        if pre_auth.is_some() {
            "header"
        } else {
            "pending"
        }
    );

    let registration =
        match connection_registry.register_connection(connection_id.clone(), client_ip.clone()) {
            Some(reg) => reg,
            None => {
                warn!("Rejecting WebSocket during shutdown: {}", connection_id);
                return Ok(HttpResponse::ServiceUnavailable().body("Server shutting down"));
            },
        };

    let (response, session, msg_stream) = actix_ws::handle(&req, stream)?;

    let handler_context = WsHandlerContext::new(
        Arc::clone(app_context.get_ref()),
        Arc::clone(rate_limiter.get_ref()),
        Arc::clone(live_query_manager.get_ref()),
        Arc::clone(user_repo.get_ref()),
        Arc::clone(connection_registry.get_ref()),
        app_context.config().security.max_ws_message_size,
        compression_enabled,
    );

    actix_web::rt::spawn(async move {
        run_websocket(client_ip, session, msg_stream, registration, handler_context, pre_auth)
            .await;
    });

    Ok(response)
}
