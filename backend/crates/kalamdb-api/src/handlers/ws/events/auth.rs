//! WebSocket authentication handler
//!
//! Handles the Authenticate message for WebSocket connections.
//! Uses the unified authentication module from kalamdb-auth.
//!
//! Only JWT token authentication is accepted for WebSocket connections.
//! This keeps username/password auth limited to the login endpoint.

use actix_ws::Session;
use kalamdb_auth::{authenticate, extract_username_for_audit, AuthRequest, UserRepository};
use kalamdb_commons::models::ConnectionInfo;
use kalamdb_commons::websocket::WsAuthCredentials;
use kalamdb_commons::WebSocketMessage;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::live::{ConnectionsManager, SharedConnectionState};
use log::debug;
use std::sync::Arc;

use super::{send_auth_error, send_json};
use crate::limiter::RateLimiter;

/// Handle authentication message with any supported credentials type
///
/// Uses connection_id from SharedConnectionState, no separate parameter needed.
/// Delegates to the unified authentication module in kalamdb-auth.
///
/// Supports:
/// - JWT token
/// - Future token-based auth methods (API keys, OAuth, etc.)
pub async fn handle_authenticate(
    connection_state: &SharedConnectionState,
    client_ip: &ConnectionInfo,
    credentials: WsAuthCredentials,
    session: &mut Session,
    registry: &Arc<ConnectionsManager>,
    _app_context: &Arc<AppContext>,
    rate_limiter: &Arc<RateLimiter>,
    user_repo: &Arc<dyn UserRepository>,
) -> Result<(), String> {
    // SECURITY: Rate limit auth attempts per IP to prevent brute-force via WebSocket.
    // This mirrors the rate limiting applied to the HTTP login endpoint.
    if !rate_limiter.check_auth_rate(client_ip) {
        let _ = send_auth_error(
            session.clone(),
            "Too many authentication attempts. Please retry shortly.",
        )
        .await;
        return Err("Auth rate limit exceeded".to_string());
    }

    // Only accept JWT tokens for WebSocket authentication
    let auth_request = match credentials {
        WsAuthCredentials::Jwt { token } => AuthRequest::Jwt { token },
    };

    authenticate_with_request(
        connection_state,
        client_ip,
        auth_request,
        session,
        registry,
        user_repo,
    )
    .await
}

/// Internal function that handles authentication for any AuthRequest type
async fn authenticate_with_request(
    connection_state: &SharedConnectionState,
    connection_info: &ConnectionInfo,
    auth_request: AuthRequest,
    session: &mut Session,
    registry: &Arc<ConnectionsManager>,
    user_repo: &Arc<dyn UserRepository>,
) -> Result<(), String> {
    let connection_id = connection_state.read().connection_id().clone();

    // Get username for logging (before authentication attempt)
    let username_for_log = extract_username_for_audit(&auth_request);

    debug!(
        "Authenticating WebSocket: connection_id={}, username={}",
        connection_id,
        username_for_log.as_str()
    );

    // Authenticate using unified auth module
    let auth_result = match authenticate(auth_request, connection_info, user_repo).await {
        Ok(result) => {
            // // Log successful authentication
            // let event_type = if connection_info.is_localhost() {
            //     "LOGIN_WS_LOCALHOST"
            // } else {
            //     "LOGIN_WS"
            // };
            // let entry = audit::log_auth_event(&result.user.user_id, event_type, true, None);
            // if let Err(_e) = audit::persist_audit_entry(app_context, &entry).await {
            //     error!("Failed to persist audit log: {}", _e);
            // }
            result.user
        },
        Err(_e) => {
            // Log failed authentication
            // let entry = audit::log_auth_event_with_username(
            //     &username_for_log,
            //     "LOGIN_WS",
            //     false,
            //     Some(format!("{}", e)),
            // );
            // if let Err(e) = audit::persist_audit_entry(app_context, &entry).await {
            //     error!("Failed to persist audit log: {}", e);
            // }

            let _ = send_auth_error(session.clone(), "Invalid username or password").await;
            return Err("Authentication failed".to_string());
        },
    };

    // Mark authenticated in connection state
    connection_state
        .write()
        .mark_authenticated(auth_result.user_id.clone(), auth_result.role);
    // Update registry's user index
    registry.on_authenticated(&connection_id, auth_result.user_id.clone());

    // Send success
    let msg = WebSocketMessage::AuthSuccess {
        user_id: auth_result.user_id.clone(),
        role: format!("{:?}", auth_result.role),
    };
    let _ = send_json(session, &msg, true).await;

    debug!(
        "WebSocket authenticated: {} as {} ({:?})",
        connection_id,
        auth_result.user_id.as_str(),
        auth_result.role
    );

    Ok(())
}
