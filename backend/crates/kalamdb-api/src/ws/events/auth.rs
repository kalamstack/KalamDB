//! WebSocket authentication service
//!
//! Provides a single code path for completing WebSocket authentication
//! regardless of how the user was validated (header-auth fast path or
//! explicit Authenticate message).
//!
//! Only JWT token authentication is accepted for WebSocket connections.
//! This keeps user/password auth limited to the login endpoint.

use std::sync::Arc;

use actix_ws::Session;
use kalamdb_auth::{authenticate, extract_user_id_for_audit, AuthRequest, UserRepository};
use kalamdb_commons::{
    models::{ConnectionInfo, UserId},
    websocket::{ProtocolOptions, WsAuthCredentials},
    Role, WebSocketMessage,
};
use kalamdb_core::app_context::AppContext;
use kalamdb_live::SharedConnectionState;
use log::debug;
use tracing::Instrument;

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
    protocol: ProtocolOptions,
    session: &mut Session,
    _app_context: &Arc<AppContext>,
    rate_limiter: &Arc<RateLimiter>,
    user_repo: &Arc<dyn UserRepository>,
    compression: bool,
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

    authenticate_ws_request(
        connection_state,
        client_ip,
        auth_request,
        protocol,
        session,
        user_repo,
        compression,
    )
    .await
}

/// Authenticate a request that was supplied during the HTTP upgrade.
///
/// This runs after the WebSocket upgrade has completed so the TCP/WebSocket
/// handshake is not blocked on JWT validation or user lookup.
pub async fn handle_upgrade_auth(
    connection_state: &SharedConnectionState,
    client_ip: &ConnectionInfo,
    auth_request: AuthRequest,
    protocol: ProtocolOptions,
    session: &mut Session,
    rate_limiter: &Arc<RateLimiter>,
    user_repo: &Arc<dyn UserRepository>,
    compression: bool,
) -> Result<(), String> {
    if !rate_limiter.check_auth_rate(client_ip) {
        let _ = send_auth_error(
            session.clone(),
            "Too many authentication attempts. Please retry shortly.",
        )
        .await;
        return Err("Auth rate limit exceeded".to_string());
    }

    authenticate_ws_request(
        connection_state,
        client_ip,
        auth_request,
        protocol,
        session,
        user_repo,
        compression,
    )
    .await
}

/// Complete WebSocket authentication after a user has been validated.
///
/// This is the single source of truth for post-validation auth steps.
/// Called from both the upgrade-header auth path and the explicit
/// Authenticate message path. Consolidates:
/// - Marking the connection as authenticated
/// - Setting the negotiated protocol
/// - Sending the AuthSuccess response
pub async fn complete_ws_auth(
    connection_state: &SharedConnectionState,
    user_id: UserId,
    role: Role,
    protocol: ProtocolOptions,
    session: &mut Session,
    compression: bool,
) -> Result<(), String> {
    let connection_id = connection_state.connection_id().clone();

    connection_state.mark_authenticated(user_id.clone(), role);
    connection_state.set_protocol(protocol);

    let msg = WebSocketMessage::AuthSuccess {
        user: user_id.clone(),
        role,
        protocol,
    };
    let _ = send_json(session, &msg, compression).await;

    debug!(
        "WebSocket authenticated: {} as {} ({:?})",
        connection_id,
        user_id.as_str(),
        role
    );

    Ok(())
}

/// Send an AuthSuccess response for an already-authenticated connection.
///
/// Used when a client sends an Authenticate message on a connection that
/// was already authenticated (e.g. header-auth). Idempotent — simply echoes
/// the current auth state without re-validating.
pub async fn send_current_auth_success(
    connection_state: &SharedConnectionState,
    session: &mut Session,
    compression: bool,
) -> Result<(), String> {
    if let (Some(user_id), Some(role)) = (connection_state.user_id(), connection_state.user_role())
    {
        let msg = WebSocketMessage::AuthSuccess {
            user: user_id.clone(),
            role,
            protocol: connection_state.protocol(),
        };
        let _ = send_json(session, &msg, compression).await;
    }
    Ok(())
}

/// Internal function that handles authentication for any AuthRequest type
async fn authenticate_ws_request(
    connection_state: &SharedConnectionState,
    connection_info: &ConnectionInfo,
    auth_request: AuthRequest,
    protocol: ProtocolOptions,
    session: &mut Session,
    user_repo: &Arc<dyn UserRepository>,
    compression: bool,
) -> Result<(), String> {
    let connection_id = connection_state.connection_id().clone();

    // Get the requested user for logging before authentication attempt.
    let user_id_for_log = extract_user_id_for_audit(&auth_request);
    let auth_span = tracing::debug_span!(
        "ws.authenticate",
        connection_id = %connection_id,
        audit_user_id = %user_id_for_log.as_str(),
        user_id = tracing::field::Empty,
        role = tracing::field::Empty
    );

    async move {
        debug!(
            "Authenticating WebSocket: connection_id={}, user_id={}",
            connection_id,
            user_id_for_log.as_str()
        );

        let auth_result = match authenticate(auth_request, connection_info, user_repo).await {
            Ok(result) => result.user,
            Err(_e) => {
                let _ = send_auth_error(session.clone(), "Invalid credentials").await;
                return Err("Authentication failed".to_string());
            },
        };

        tracing::Span::current().record("user_id", auth_result.user_id.as_str());
        tracing::Span::current().record("role", format!("{:?}", auth_result.role).as_str());

        complete_ws_auth(
            connection_state,
            auth_result.user_id,
            auth_result.role,
            protocol,
            session,
            compression,
        )
        .await
    }
    .instrument(auth_span)
    .await
}
