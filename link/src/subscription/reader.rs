//! Background WebSocket reader task.
//!
//! Owns the WebSocket stream and forwards parsed events through a bounded
//! channel.  Handles keepalive pings, decompression, and graceful shutdown.

use crate::{
    connection::{
        decode_ws_payload, parse_message, send_next_batch_request, WebSocketStream, FAR_FUTURE,
        MAX_WS_TEXT_MESSAGE_BYTES,
    },
    error::{KalamLinkError, Result},
    event_handlers::{ConnectionError, DisconnectReason, EventHandlers},
    models::{ChangeEvent, ClientMessage},
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant as TokioInstant;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Best-effort Unsubscribe + Close over a WebSocket stream.
async fn send_unsubscribe_and_close(ws_stream: &mut WebSocketStream, subscription_id: &str) {
    let message = ClientMessage::Unsubscribe {
        subscription_id: subscription_id.to_string(),
    };
    if let Ok(payload) = serde_json::to_string(&message) {
        let _ = ws_stream.send(Message::Text(payload.into())).await;
    }
    let _ = ws_stream.close(None).await;
}

/// Parse a text payload, auto-request the next batch when needed, and forward
/// the parsed event to the bounded channel.
///
/// Returns `true` when the reader loop should exit (channel closed or fatal).
async fn process_and_forward(
    text: &str,
    ws_stream: &mut WebSocketStream,
    event_tx: &mpsc::Sender<Result<ChangeEvent>>,
    subscription_id: &str,
) -> bool {
    match parse_message(text) {
        Ok(Some(event)) => {
            // Auto-request next batch when initial data has more pages
            if let ChangeEvent::InitialDataBatch {
                ref batch_control, ..
            } = event
            {
                if batch_control.has_more {
                    if let Err(e) = send_next_batch_request(
                        ws_stream,
                        subscription_id,
                        batch_control.last_seq_id,
                    )
                    .await
                    {
                        let _ = event_tx.send(Err(e)).await;
                        return true;
                    }
                }
            }
            event_tx.send(Ok(event)).await.is_err()
        },
        Ok(None) => false, // skip auth acks etc.
        Err(e) => event_tx.send(Err(e)).await.is_err(),
    }
}

/// Background task that owns the WebSocket stream and forwards parsed events
/// through a bounded channel.
///
/// Responsibilities:
/// - Read WS frames, decompress binary payloads, parse JSON into `ChangeEvent`
/// - Auto-request next batch when `InitialDataBatch.has_more` is set
/// - Send periodic keepalive pings when idle
/// - Graceful shutdown on close signal or stream end
/// - Emit lifecycle events via `EventHandlers`
pub(crate) async fn ws_reader_loop(
    mut ws_stream: WebSocketStream,
    event_tx: mpsc::Sender<Result<ChangeEvent>>,
    close_rx: oneshot::Receiver<()>,
    subscription_id: String,
    keepalive_interval: Option<Duration>,
    pong_timeout: Duration,
    event_handlers: EventHandlers,
) {
    // NOTE: emit_connect is called by SubscriptionManager::new() before this
    // task is spawned, so we only handle disconnect / error / receive here.
    tokio::pin!(close_rx);

    let keepalive_dur = keepalive_interval.unwrap_or(FAR_FUTURE);
    let has_keepalive = keepalive_interval.is_some();
    // Keep application-level heartbeats periodic even while inbound change
    // events are flowing, otherwise the server can time out a busy stream.
    let mut ping_deadline = TokioInstant::now() + keepalive_dur;

    // Pong timeout tracking
    let has_pong_timeout = has_keepalive && !pong_timeout.is_zero();
    let mut awaiting_pong = false;
    let mut pong_deadline = TokioInstant::now() + FAR_FUTURE;

    loop {
        let ping_sleep = tokio::time::sleep_until(ping_deadline);
        tokio::pin!(ping_sleep);

        let pong_sleep = tokio::time::sleep_until(pong_deadline);
        tokio::pin!(pong_sleep);

        let frame = tokio::select! {
            biased;

            // Highest priority: graceful shutdown requested by close() / Drop.
            _ = &mut close_rx => {
                send_unsubscribe_and_close(&mut ws_stream, &subscription_id).await;
                event_handlers.emit_disconnect(
                    DisconnectReason::with_code("Subscription closed by client".to_string(), 1000),
                );
                return;
            }

            // Pong timeout: no frame arrived since we sent our Ping.
            _ = &mut pong_sleep, if has_pong_timeout && awaiting_pong => {
                log::warn!(
                    "[kalam-link] [{}] Pong timeout ({:?}) — no response to keepalive ping, treating connection as dead",
                    subscription_id, pong_timeout,
                );
                let _ = event_tx
                    .send(Err(KalamLinkError::WebSocketError(format!(
                        "Pong timeout ({:?}) — server unresponsive", pong_timeout
                    ))))
                    .await;
                event_handlers.emit_disconnect(
                    DisconnectReason::new(format!(
                        "Pong timeout ({:?}) — server unresponsive", pong_timeout
                    )),
                );
                return;
            }

            // Keepalive idle timer (only fires when we are NOT already awaiting a pong).
            _ = &mut ping_sleep, if has_keepalive && !awaiting_pong => {
                log::debug!(
                    "[kalam-link] [{}] Keepalive: sending Ping (interval={:?})",
                    subscription_id, keepalive_dur,
                );
                if let Err(e) = ws_stream.send(Message::Ping(Bytes::new())).await {
                    let _ = event_tx
                        .send(Err(KalamLinkError::WebSocketError(format!(
                            "Failed to send keepalive ping: {}", e
                        ))))
                        .await;
                    event_handlers.emit_disconnect(
                        DisconnectReason::new(format!("Keepalive ping failed: {}", e)),
                    );
                    return;
                }
                if let Ok(payload) = serde_json::to_string(&ClientMessage::Ping) {
                    if let Err(e) = ws_stream.send(Message::Text(payload.into())).await {
                        let _ = event_tx
                            .send(Err(KalamLinkError::WebSocketError(format!(
                                "Failed to send keepalive heartbeat: {}", e
                            ))))
                            .await;
                        event_handlers.emit_disconnect(
                            DisconnectReason::new(format!(
                                "Keepalive heartbeat failed: {}",
                                e
                            )),
                        );
                        return;
                    }
                }
                event_handlers.emit_send("[ping]");
                // Arm the pong timeout.
                if has_pong_timeout {
                    awaiting_pong = true;
                    pong_deadline = TokioInstant::now() + pong_timeout;
                    log::debug!(
                        "[kalam-link] [{}] Keepalive: awaiting Pong within {:?}",
                        subscription_id, pong_timeout,
                    );
                }
                ping_deadline = TokioInstant::now() + keepalive_dur;
                continue;
            }

            // Normal path: read the next WebSocket frame.
            msg = ws_stream.next() => {
                // Any frame received proves the connection is alive.
                if awaiting_pong {
                    log::debug!(
                        "[kalam-link] [{}] Keepalive: frame received, clearing pong timeout",
                        subscription_id,
                    );
                    awaiting_pong = false;
                    pong_deadline = TokioInstant::now() + FAR_FUTURE;
                }
                msg
            }
        };

        match frame {
            Some(Ok(Message::Text(text))) => {
                if text.len() > MAX_WS_TEXT_MESSAGE_BYTES {
                    let _ = event_tx
                        .send(Err(KalamLinkError::WebSocketError(format!(
                            "Text message too large ({} bytes > {} bytes)",
                            text.len(),
                            MAX_WS_TEXT_MESSAGE_BYTES
                        ))))
                        .await;
                    return;
                }
                event_handlers.emit_receive(&text);
                if process_and_forward(&text, &mut ws_stream, &event_tx, &subscription_id).await {
                    return;
                }
            },
            Some(Ok(Message::Binary(data))) => match decode_ws_payload(&data) {
                Ok(text) => {
                    event_handlers.emit_receive(&text);
                    if process_and_forward(&text, &mut ws_stream, &event_tx, &subscription_id).await
                    {
                        return;
                    }
                },
                Err(e) => {
                    event_handlers.emit_error(ConnectionError::new(e.to_string(), false));
                    if event_tx.send(Err(e)).await.is_err() {
                        return;
                    }
                },
            },
            Some(Ok(Message::Close(frame))) => {
                let reason = if let Some(f) = frame {
                    DisconnectReason::with_code(f.reason.to_string(), f.code.into())
                } else {
                    DisconnectReason::new("Server closed connection")
                };
                event_handlers.emit_disconnect(reason);
                return;
            },
            Some(Ok(Message::Ping(payload))) => {
                // tokio-tungstenite auto-responds, but be explicit for clarity.
                let _ = ws_stream.send(Message::Pong(payload)).await;
            },
            Some(Ok(Message::Pong(_))) => {
                log::debug!("[kalam-link] [{}] Keepalive: received Pong", subscription_id);
            },
            Some(Ok(Message::Frame(_))) => {},
            Some(Err(e)) => {
                let msg = e.to_string();
                event_handlers.emit_error(ConnectionError::new(&msg, false));
                event_handlers
                    .emit_disconnect(DisconnectReason::new(format!("WebSocket error: {}", msg)));
                let _ = event_tx.send(Err(KalamLinkError::WebSocketError(msg))).await;
                return;
            },
            None => {
                event_handlers.emit_disconnect(DisconnectReason::new("WebSocket stream ended"));
                return;
            },
        }
    }
}
