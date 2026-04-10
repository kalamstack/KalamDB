use actix_ws::{CloseCode, CloseReason, Message, Session};
use futures_util::StreamExt;
use kalamdb_commons::{websocket::SerializationType, WebSocketMessage};
use kalamdb_jobs::health_monitor::{
    decrement_websocket_sessions, increment_websocket_sessions, record_activity_now,
};
use kalamdb_live::{ConnectionEvent, ConnectionRegistration};
use log::{debug, error, info, warn};

use super::context::{UpgradeAuth, WsHandlerContext};
use super::events::{
    auth::complete_ws_auth, cleanup::cleanup_connection, send_error, send_json,
    send_wire_notification,
};
use super::messages::{handle_binary_message, handle_text_message};
use super::models::WsErrorCode;
use super::protocol::is_expected_ws_disconnect;

pub(super) async fn run_websocket(
    client_ip: kalamdb_commons::models::ConnectionInfo,
    session: Session,
    msg_stream: actix_ws::MessageStream,
    registration: ConnectionRegistration,
    handler_context: WsHandlerContext,
    pre_auth: Option<UpgradeAuth>,
) {
    record_activity_now();
    increment_websocket_sessions();

    let mut event_rx = registration.event_rx;
    let mut notification_rx = registration.notification_rx;
    let connection_state = registration.state;
    let connection_id = connection_state.connection_id().clone();

    {
        let mut session = session;
        let mut msg_stream = msg_stream;

        if let Some(auth) = pre_auth {
            connection_state.mark_auth_started();
            let _ = complete_ws_auth(
                &connection_state,
                auth.user_id,
                auth.role,
                auth.protocol,
                &mut session,
                handler_context.compression_enabled,
            )
            .await;
            debug!("WebSocket pre-authenticated from header: {}", connection_id);
        }

        loop {
            tokio::select! {
                biased;

                event = event_rx.recv() => {
                    match event {
                        Some(ConnectionEvent::AuthTimeout) => {
                            error!("WebSocket auth timeout: {}", connection_id);
                            let msg = WebSocketMessage::AuthError {
                                message: "Authentication timeout - no auth message received within 3 seconds".to_string(),
                            };
                            let _ = send_json(&mut session, &msg, handler_context.compression_enabled).await;
                            let _ = session.close(Some(CloseReason {
                                code: CloseCode::Policy,
                                description: Some("Authentication timeout".into()),
                            })).await;
                            break;
                        }
                        Some(ConnectionEvent::HeartbeatTimeout) => {
                            warn!("WebSocket heartbeat timeout: {}", connection_id);
                            let _ = session.close(Some(CloseReason {
                                code: CloseCode::Normal,
                                description: Some("Heartbeat timeout".into()),
                            })).await;
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
                        None => break,
                    }
                }

                msg = msg_stream.next() => {
                    match msg {
                        Some(Ok(Message::Ping(bytes))) => {
                            record_activity_now();
                            connection_state.update_heartbeat();
                            if session.pong(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            record_activity_now();
                            connection_state.update_heartbeat();
                        }
                        Some(Ok(Message::Text(text))) => {
                            record_activity_now();
                            connection_state.update_heartbeat();

                            if text.len() > handler_context.max_message_size {
                                warn!(
                                    "Message too large from {}: {} bytes (max {})",
                                    connection_id,
                                    text.len(),
                                    handler_context.max_message_size
                                );
                                let _ = send_error(
                                    &mut session,
                                    "protocol",
                                    WsErrorCode::MessageTooLarge,
                                    &format!(
                                        "Message exceeds maximum size of {} bytes",
                                        handler_context.max_message_size
                                    ),
                                    handler_context.compression_enabled,
                                )
                                .await;
                                continue;
                            }

                            if !handler_context
                                .rate_limiter
                                .check_message_rate(&connection_id)
                            {
                                warn!("Message rate limit exceeded: {}", connection_id);
                                let _ = send_error(
                                    &mut session,
                                    "rate_limit",
                                    WsErrorCode::RateLimitExceeded,
                                    "Too many messages",
                                    handler_context.compression_enabled,
                                )
                                .await;
                                continue;
                            }

                            if let Err(err) = handle_text_message(
                                &connection_state,
                                &client_ip,
                                &text,
                                &mut session,
                                &handler_context.app_context,
                                &handler_context.rate_limiter,
                                &handler_context.live_query_manager,
                                &handler_context.user_repo,
                                handler_context.compression_enabled,
                            )
                            .await
                            {
                                error!("Error handling message: {}", err);
                                let _ = session.close(Some(CloseReason {
                                    code: CloseCode::Error,
                                    description: Some(err),
                                })).await;
                                break;
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            record_activity_now();
                            connection_state.update_heartbeat();

                            if let Err(err) = handle_binary_message(
                                &connection_state,
                                &client_ip,
                                data,
                                &mut session,
                                &handler_context.app_context,
                                &handler_context.rate_limiter,
                                &handler_context.live_query_manager,
                                &handler_context.user_repo,
                                handler_context.compression_enabled,
                            )
                            .await
                            {
                                if connection_state.serialization_type() == SerializationType::MessagePack {
                                    warn!("Invalid msgpack from {}: {}", connection_id, err);
                                    let _ = send_error(
                                        &mut session,
                                        "protocol",
                                        WsErrorCode::UnsupportedData,
                                        &err,
                                        handler_context.compression_enabled,
                                    )
                                    .await;
                                    continue;
                                }

                                error!("Error handling binary message: {}", err);
                                let _ = session.close(Some(CloseReason {
                                    code: CloseCode::Error,
                                    description: Some(err),
                                })).await;
                                break;
                            }
                        }
                        Some(Ok(Message::Close(reason))) => {
                            debug!("Client requested close: {:?}", reason);
                            let _ = session.close(reason).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            if is_expected_ws_disconnect(&err) {
                                debug!("WebSocket client disconnected ({}): {}", connection_id, err);
                            } else {
                                error!("WebSocket error ({}): {}", connection_id, err);
                            }
                            break;
                        }
                        None => {
                            debug!("WebSocket stream ended: {}", connection_id);
                            break;
                        }
                    }
                }

                notification = notification_rx.recv() => {
                    match notification {
                        Some(notif) => {
                            let ser = connection_state.serialization_type();
                            if send_wire_notification(&mut session, notif.as_ref(), ser, handler_context.compression_enabled).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    }

    cleanup_connection(
        &connection_state,
        &handler_context.connection_registry,
        &handler_context.rate_limiter,
        &handler_context.live_query_manager,
    )
    .await;

    decrement_websocket_sessions();
}
