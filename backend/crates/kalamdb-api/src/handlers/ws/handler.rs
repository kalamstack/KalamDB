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
use actix_ws::{CloseCode, CloseReason, Message, ProtocolError, Session};
use futures_util::StreamExt;
use kalamdb_auth::UserRepository;
use kalamdb_commons::models::ConnectionInfo;
use kalamdb_commons::websocket::ClientMessage;
use kalamdb_commons::WebSocketMessage;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::jobs::health_monitor::{
    decrement_websocket_sessions, increment_websocket_sessions,
};
use kalamdb_core::live::{
    ConnectionEvent, ConnectionId, ConnectionsManager, SharedConnectionState,
};
use log::{debug, error, info, warn};
use std::sync::Arc;

/// Returns `true` for WebSocket errors that represent a normal client
/// disconnect rather than a server-side fault.
///
/// Clients can disappear at any time (network drop, process killed, mobile
/// radio sleep, 3G timeout, etc.). In those cases actix-ws surfaces a
/// `ProtocolError::Io` carrying one of several standard OS error kinds or
/// the actix-codec sentinel message "payload reached EOF before completing".
/// Logging these at ERROR level produces misleading noise; they are expected
/// in production and should be DEBUG.
fn is_expected_ws_disconnect(e: &ProtocolError) -> bool {
    match e {
        ProtocolError::Io(io_err) => {
            use std::io::ErrorKind::*;
            // Standard OS-level disconnect signals
            if matches!(
                io_err.kind(),
                BrokenPipe | ConnectionReset | ConnectionAborted | UnexpectedEof
            ) {
                return true;
            }
            // actix-codec raises ErrorKind::Other for an abrupt stream end;
            // detect by message substring (stable across actix-http versions).
            let msg = io_err.to_string().to_ascii_lowercase();
            msg.contains("eof")
                || msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection aborted")
                || msg.contains("payload reached eof")
                || msg.contains("connection closed")
        },
        // A clean close-frame received just before we polled is also harmless.
        _ => false,
    }
}

use super::events::{
    auth::handle_authenticate, batch::handle_next_batch, cleanup::cleanup_connection, send_error,
    send_json, subscription::handle_subscribe, unsubscribe::handle_unsubscribe,
};
use crate::handlers::ws::models::WsErrorCode;
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
    live_query_manager: web::Data<Arc<kalamdb_core::live::LiveQueryManager>>,
    user_repo: web::Data<Arc<dyn UserRepository>>,
    connection_registry: web::Data<Arc<ConnectionsManager>>,
) -> Result<HttpResponse, Error> {
    // Check if server is shutting down
    if connection_registry.is_shutting_down() {
        return Ok(HttpResponse::ServiceUnavailable().body("Server is shutting down"));
    }

    // Security: Validate Origin header if configured
    let config = app_context.config();
    let allowed_ws_origins = if config.security.allowed_ws_origins.is_empty() {
        &config.security.cors.allowed_origins
    } else {
        &config.security.allowed_ws_origins
    };

    if !allowed_ws_origins.is_empty() && !allowed_ws_origins.contains(&"*".to_string()) {
        if let Some(origin) = req.headers().get("Origin") {
            if let Ok(origin_str) = origin.to_str() {
                if !allowed_ws_origins.iter().any(|allowed| allowed == origin_str) {
                    warn!("WebSocket connection rejected: invalid origin '{}'", origin_str);
                    return Ok(HttpResponse::Forbidden().body("Origin not allowed"));
                }
            }
        } else if config.security.strict_ws_origin_check {
            warn!("WebSocket connection rejected: missing Origin header");
            return Ok(HttpResponse::Forbidden().body("Origin header required"));
        }
    }

    // Parse ?compress=false query parameter for development/debug mode
    let compression_enabled = !req
        .query_string()
        .split('&')
        .any(|kv| kv.eq_ignore_ascii_case("compress=false"));

    // Generate unique connection ID
    let connection_id = ConnectionId::new(uuid::Uuid::new_v4().simple().to_string());

    // Extract client IP with security checks against spoofing
    let client_ip = kalamdb_auth::extract_client_ip_secure(&req);
    let _connect_span = tracing::debug_span!(
        "ws.connect",
        connection_id = %connection_id,
        client_ip = ?client_ip,
        compression_enabled
    )
    .entered();

    debug!("New WebSocket connection: {} (auth required within 3s)", connection_id);

    // Register connection with unified registry (handles heartbeat tracking)
    let registration =
        match connection_registry.register_connection(connection_id.clone(), client_ip.clone()) {
            Some(reg) => reg,
            None => {
                warn!("Rejecting WebSocket during shutdown: {}", connection_id);
                return Ok(HttpResponse::ServiceUnavailable().body("Server shutting down"));
            },
        };

    // Upgrade to WebSocket using actix-ws
    let (response, session, msg_stream) = actix_ws::handle(&req, stream)?;

    // Clone references for the async task
    let registry = connection_registry.get_ref().clone();
    let app_ctx = app_context.get_ref().clone();
    let limiter = rate_limiter.get_ref().clone();
    let lq_manager = live_query_manager.get_ref().clone();
    let user_repository = user_repo.get_ref().clone();
    let max_message_size = config.security.max_ws_message_size;

    // Spawn WebSocket handling task
    actix_web::rt::spawn(async move {
        increment_websocket_sessions();

        handle_websocket(
            client_ip,
            session,
            msg_stream,
            registration,
            registry,
            app_ctx,
            limiter,
            lq_manager,
            user_repository,
            max_message_size,
            compression_enabled,
        )
        .await;

        decrement_websocket_sessions();
    });

    Ok(response)
}

/// Main WebSocket handling loop
///
/// All subscription state is stored in ConnectionState.subscriptions.
/// No local tracking needed - cleanup is handled by ConnectionsManager.
#[allow(clippy::too_many_arguments)]
async fn handle_websocket(
    client_ip: ConnectionInfo,
    session: Session,
    msg_stream: actix_ws::MessageStream,
    registration: kalamdb_core::live::ConnectionRegistration,
    registry: Arc<ConnectionsManager>,
    app_context: Arc<AppContext>,
    rate_limiter: Arc<RateLimiter>,
    live_query_manager: Arc<kalamdb_core::live::LiveQueryManager>,
    user_repo: Arc<dyn UserRepository>,
    max_message_size: usize,
    compression_enabled: bool,
) {
    let mut event_rx = registration.event_rx;
    let mut notification_rx = registration.notification_rx;
    let connection_state = registration.state;
    let connection_id = connection_state.read().connection_id().clone();

    // Scope WebSocket transport so it is dropped before cleanup.
    {
        let mut session = session;
        let mut msg_stream = msg_stream;

        loop {
            tokio::select! {
                biased;

                // Handle control events from registry (highest priority)
                event = event_rx.recv() => {
                    match event {
                        Some(ConnectionEvent::AuthTimeout) => {
                            error!("WebSocket auth timeout: {}", connection_id);
                            let msg = WebSocketMessage::AuthError {
                                message: "Authentication timeout - no auth message received within 3 seconds".to_string(),
                            };
                            let _ = send_json(&mut session, &msg, compression_enabled).await;
                            let _ = session.close(Some(CloseReason {
                                code: CloseCode::Policy,
                                description: Some("Authentication timeout".into()),
                            })).await;
                            break;
                        }
                        Some(ConnectionEvent::HeartbeatTimeout) => {
                            warn!("WebSocket heartbeat timeout: {}", connection_id);
                            let _ = session
                                .close(Some(CloseReason {
                                    code: CloseCode::Normal,
                                    description: Some("Heartbeat timeout".into()),
                                }))
                                .await;
                            break;
                        }
                        Some(ConnectionEvent::Shutdown) => {
                            info!("WebSocket shutdown requested: {}", connection_id);
                            let _ = session.close(Some(CloseReason {
                                code: CloseCode::Away,
                                description: Some("Server shutting down".into()),
                            })).await;
                            break;
                        }
                        None => {
                            // Channel closed
                            break;
                        }
                    }
                }

                // Handle incoming WebSocket messages
                msg = msg_stream.next() => {
                    match msg {
                        Some(Ok(Message::Ping(bytes))) => {
                            connection_state.read().update_heartbeat();
                            if session.pong(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            connection_state.read().update_heartbeat();
                        }
                        Some(Ok(Message::Text(text))) => {
                            connection_state.read().update_heartbeat();

                            // Security: Check message size limit
                            if text.len() > max_message_size {
                                warn!("Message too large from {}: {} bytes (max {})",
                                    connection_id, text.len(), max_message_size);
                                let _ = send_error(&mut session, "protocol", WsErrorCode::MessageTooLarge,
                                    &format!("Message exceeds maximum size of {} bytes", max_message_size), compression_enabled).await;
                                continue;
                            }

                            // Rate limit check
                            if !rate_limiter.check_message_rate(&connection_id) {
                                warn!("Message rate limit exceeded: {}", connection_id);
                                let _ = send_error(&mut session, "rate_limit", WsErrorCode::RateLimitExceeded, "Too many messages", compression_enabled).await;
                                continue;
                            }

                            if let Err(e) = handle_text_message(
                                &connection_state,
                                &client_ip,
                                &text,
                                &mut session,
                                &registry,
                                &app_context,
                                &rate_limiter,
                                &live_query_manager,
                                &user_repo,
                                compression_enabled,
                            ).await {
                                error!("Error handling message: {}", e);
                                let _ = session.close(Some(CloseReason {
                                    code: CloseCode::Error,
                                    description: Some(e),
                                })).await;
                                break;
                            }
                        }
                        Some(Ok(Message::Binary(_))) => {
                            warn!("Binary messages not supported: {}", connection_id);
                            let _ = send_error(&mut session, "protocol", WsErrorCode::UnsupportedData, "Binary not supported", compression_enabled).await;
                        }
                        Some(Ok(Message::Close(reason))) => {
                            debug!("Client requested close: {:?}", reason);
                            let _ = session.close(reason).await;
                            break;
                        }
                        Some(Ok(_)) => {
                            // Continuation, Nop - ignore
                        }
                        Some(Err(e)) => {
                            if is_expected_ws_disconnect(&e) {
                                debug!("WebSocket client disconnected ({}): {}", connection_id, e);
                            } else {
                                error!("WebSocket error ({}): {}", connection_id, e);
                            }
                            break;
                        }
                        None => {
                            // Stream ended
                            debug!("WebSocket stream ended: {}", connection_id);
                            break;
                        }
                    }
                }

                // Handle notifications from live query manager
                notification = notification_rx.recv() => {
                    match notification {
                        Some(notif) => {
                            // Use send_json for automatic compression of large payloads
                            if send_json(&mut session, notif.as_ref(), compression_enabled).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            // Channel closed
                            break;
                        }
                    }
                }
            }
        }
    }

    // Cleanup - ConnectionsManager handles subscription cleanup automatically
    cleanup_connection(&connection_state, &registry, &rate_limiter, &live_query_manager).await;
}

/// Handle text message from client
///
/// Uses connection_id from SharedConnectionState, no separate parameter needed.
#[allow(clippy::too_many_arguments)]
async fn handle_text_message(
    connection_state: &SharedConnectionState,
    client_ip: &ConnectionInfo,
    text: &str,
    session: &mut Session,
    registry: &Arc<ConnectionsManager>,
    app_context: &Arc<AppContext>,
    rate_limiter: &Arc<RateLimiter>,
    live_query_manager: &Arc<kalamdb_core::live::LiveQueryManager>,
    user_repo: &Arc<dyn UserRepository>,
    compression_enabled: bool,
) -> Result<(), String> {
    let msg: ClientMessage =
        serde_json::from_str(text).map_err(|e| format!("Invalid message: {}", e))?;

    match msg {
        ClientMessage::Authenticate { credentials } => {
            connection_state.write().mark_auth_started();
            handle_authenticate(
                connection_state,
                client_ip,
                credentials,
                session,
                registry,
                app_context,
                rate_limiter,
                user_repo,
            )
            .await
        },
        ClientMessage::Subscribe { subscription } => {
            if !connection_state.read().is_authenticated() {
                let _ = send_error(
                    session,
                    "subscribe",
                    WsErrorCode::AuthRequired,
                    "Authentication required before subscribing",
                    compression_enabled,
                )
                .await;
                return Ok(());
            }
            handle_subscribe(
                connection_state,
                subscription,
                session,
                rate_limiter,
                live_query_manager,
                compression_enabled,
            )
            .await
        },
        ClientMessage::NextBatch {
            subscription_id,
            last_seq_id,
        } => {
            if !connection_state.read().is_authenticated() {
                return Ok(());
            }
            handle_next_batch(
                connection_state,
                &subscription_id,
                last_seq_id,
                session,
                live_query_manager,
                compression_enabled,
            )
            .await
        },
        ClientMessage::Unsubscribe { subscription_id } => {
            if !connection_state.read().is_authenticated() {
                return Ok(());
            }
            handle_unsubscribe(connection_state, &subscription_id, rate_limiter, live_query_manager)
                .await
        },
        ClientMessage::Ping => {
            // Application-level keepalive: heartbeat already updated by the
            // text-message arm above; nothing else to do.
            Ok(())
        },
    }
}
