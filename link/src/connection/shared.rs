//! Shared WebSocket connection manager for real-time subscriptions.
//!
//! Provides a single WebSocket connection multiplexed across multiple
//! subscriptions.  Handles:
//!
//! - Single connection for all subscriptions (no per-subscription connections)
//! - Message routing to the correct subscription by `subscription_id`
//! - Automatic reconnection with exponential backoff
//! - Re-subscription of all active queries after reconnect (resuming from last `seq_id`)
//! - Connection lifecycle events (`on_connect`, `on_disconnect`, `on_error`)
//! - Keepalive pings

use crate::{
    auth::{AuthProvider, ResolvedAuth},
    connection::{
        apply_ws_auth_headers, connect_with_optional_local_bind, decode_ws_payload,
        jitter_keepalive_interval, parse_message, resolve_ws_url, send_auth_and_wait,
        send_next_batch_request, WebSocketStream, DEFAULT_EVENT_CHANNEL_CAPACITY, FAR_FUTURE,
        MAX_WS_TEXT_MESSAGE_BYTES,
    },
    error::{KalamLinkError, Result},
    event_handlers::{ConnectionError, DisconnectReason, EventHandlers},
    models::{
        ChangeEvent, ClientMessage, ConnectionOptions, SubscriptionInfo, SubscriptionOptions,
        SubscriptionRequest,
    },
    seq_id::SeqId,
    seq_tracking,
    timeouts::KalamLinkTimeouts,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, protocol::Message};

/// Current time in millis since Unix epoch.
#[inline]
fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Build a `Vec<SubscriptionInfo>` snapshot from the internal subs map.
fn snapshot_subscriptions(subs: &HashMap<String, SubEntry>) -> Vec<SubscriptionInfo> {
    subs.iter()
        .map(|(id, entry)| SubscriptionInfo {
            id: id.clone(),
            query: entry.sql.clone(),
            last_seq_id: entry.last_seq_id,
            last_event_time_ms: entry.last_event_time_ms,
            created_at_ms: entry.created_at_ms,
            closed: false,
        })
        .collect()
}

fn subscription_start_ready(event: &ChangeEvent) -> bool {
    matches!(
        event,
        ChangeEvent::Ack { batch_control, .. }
            | ChangeEvent::InitialDataBatch { batch_control, .. }
                if batch_control.status == crate::models::BatchStatus::Ready
    )
}

// ── Commands ────────────────────────────────────────────────────────────────

/// Commands sent from the public API to the background connection task.
enum ConnCmd {
    Subscribe {
        id: String,
        sql: String,
        options: SubscriptionOptions,
        event_tx: mpsc::Sender<Result<ChangeEvent>>,
        result_tx: oneshot::Sender<Result<u64>>,
    },
    Unsubscribe {
        id: String,
        generation: Option<u64>,
    },
    ListSubscriptions {
        result_tx: oneshot::Sender<Vec<SubscriptionInfo>>,
    },
    Shutdown,
}

// ── Per-subscription state ──────────────────────────────────────────────────

struct SubEntry {
    sql: String,
    options: SubscriptionOptions,
    event_tx: mpsc::Sender<Result<ChangeEvent>>,
    last_seq_id: Option<SeqId>,
    generation: u64,
    created_at_ms: u64,
    last_event_time_ms: Option<u64>,
    pending_result_tx: Option<oneshot::Sender<Result<u64>>>,
}

// ── SharedConnection (public handle) ────────────────────────────────────────

pub(crate) struct SharedConnection {
    cmd_tx: mpsc::Sender<ConnCmd>,
    unsub_tx: mpsc::Sender<(String, u64)>,
    connected: Arc<AtomicBool>,
    _reconnect_attempts: Arc<AtomicU32>,
    _task: JoinHandle<()>,
    _unsub_bridge: JoinHandle<()>,
}

impl SharedConnection {
    pub async fn connect(
        base_url: String,
        resolved_auth: Arc<RwLock<ResolvedAuth>>,
        timeouts: KalamLinkTimeouts,
        connection_options: ConnectionOptions,
        event_handlers: EventHandlers,
    ) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::channel::<ConnCmd>(256);
        let connected = Arc::new(AtomicBool::new(false));
        let reconnect_attempts = Arc::new(AtomicU32::new(0));

        let connected_clone = connected.clone();
        let reconnect_clone = reconnect_attempts.clone();

        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();

        let task = tokio::spawn(async move {
            connection_task(
                cmd_rx,
                base_url,
                resolved_auth,
                timeouts,
                connection_options,
                event_handlers,
                connected_clone,
                reconnect_clone,
                Some(ready_tx),
            )
            .await;
        });

        match ready_rx.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => {
                log::warn!("Initial shared connection failed: {}", e);
            },
            Err(_) => {
                log::warn!("Connection task exited before signalling readiness");
            },
        }

        let (unsub_tx, mut unsub_rx) = mpsc::channel::<(String, u64)>(256);
        let cmd_tx_bridge = cmd_tx.clone();
        let unsub_bridge = tokio::spawn(async move {
            while let Some((id, generation)) = unsub_rx.recv().await {
                let _ = cmd_tx_bridge
                    .send(ConnCmd::Unsubscribe {
                        id,
                        generation: Some(generation),
                    })
                    .await;
            }
        });

        Ok(Self {
            cmd_tx,
            unsub_tx,
            connected,
            _reconnect_attempts: reconnect_attempts,
            _task: task,
            _unsub_bridge: unsub_bridge,
        })
    }

    pub async fn subscribe(
        &self,
        id: String,
        sql: String,
        options: SubscriptionOptions,
    ) -> Result<(mpsc::Receiver<Result<ChangeEvent>>, u64)> {
        let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_CAPACITY);
        let (result_tx, result_rx) = oneshot::channel();

        self.cmd_tx
            .send(ConnCmd::Subscribe {
                id: id.clone(),
                sql,
                options,
                event_tx,
                result_tx,
            })
            .await
            .map_err(|_| {
                KalamLinkError::WebSocketError("Connection task is not running".to_string())
            })?;

        let generation = result_rx.await.map_err(|_| {
            KalamLinkError::WebSocketError(
                "Connection task died before confirming subscribe".to_string(),
            )
        })??;

        Ok((event_rx, generation))
    }

    pub async fn unsubscribe(&self, id: &str) -> Result<()> {
        self.cmd_tx
            .send(ConnCmd::Unsubscribe {
                id: id.to_string(),
                generation: None,
            })
            .await
            .map_err(|_| {
                KalamLinkError::WebSocketError("Connection task is not running".to_string())
            })?;
        Ok(())
    }

    pub async fn disconnect(&self) {
        let _ = self.cmd_tx.send(ConnCmd::Shutdown).await;

        // Explicit disconnect/connect cycles should wait until the background
        // task marks the shared socket as down before allowing a new connect.
        for _ in 0..50 {
            if !self.connected.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn list_subscriptions(&self) -> Vec<SubscriptionInfo> {
        let (result_tx, result_rx) = oneshot::channel();
        if self.cmd_tx.send(ConnCmd::ListSubscriptions { result_tx }).await.is_err() {
            return Vec::new();
        }
        result_rx.await.unwrap_or_default()
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub(crate) fn unsubscribe_tx(&self) -> mpsc::Sender<(String, u64)> {
        self.unsub_tx.clone()
    }
}

impl Drop for SharedConnection {
    fn drop(&mut self) {
        let _ = self.cmd_tx.try_send(ConnCmd::Shutdown);
    }
}

// ── Background connection task ──────────────────────────────────────────────

async fn establish_ws(
    base_url: &str,
    resolved_auth: &RwLock<ResolvedAuth>,
    timeouts: &KalamLinkTimeouts,
    connection_options: &ConnectionOptions,
    event_handlers: &EventHandlers,
) -> Result<(WebSocketStream, AuthProvider)> {
    log::debug!("[kalam-link] Establishing WebSocket connection to {}", base_url);
    let resolved = resolved_auth.read().unwrap().clone();
    let auth = resolved.resolve().await?;

    let request_url = resolve_ws_url(base_url, None, connection_options.disable_compression)?;

    let mut request = request_url.into_client_request().map_err(|e| {
        KalamLinkError::WebSocketError(format!("Failed to build WebSocket request: {}", e))
    })?;

    apply_ws_auth_headers(&mut request, &auth)?;

    let connect_result = if !KalamLinkTimeouts::is_no_timeout(timeouts.connection_timeout) {
        tokio::time::timeout(
            timeouts.connection_timeout,
            connect_with_optional_local_bind(
                request,
                &connection_options.ws_local_bind_addresses,
                "shared-conn",
            ),
        )
        .await
    } else {
        Ok(connect_with_optional_local_bind(
            request,
            &connection_options.ws_local_bind_addresses,
            "shared-conn",
        )
        .await)
    };

    let mut ws_stream = match connect_result {
        Ok(Ok((stream, _))) => stream,
        Ok(Err(tokio_tungstenite::tungstenite::error::Error::Http(response))) => {
            let status = response.status();
            let body_text = response
                .into_body()
                .as_ref()
                .and_then(|b| {
                    if b.is_empty() {
                        None
                    } else {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                })
                .unwrap_or_default();
            let message = match status.as_u16() {
                401 => "Unauthorized: WebSocket requires valid credentials".to_string(),
                403 => "Forbidden: Access to WebSocket denied".to_string(),
                code => {
                    if body_text.is_empty() {
                        format!("WebSocket HTTP error: {}", code)
                    } else {
                        format!("WebSocket HTTP error {}: {}", code, body_text)
                    }
                },
            };
            event_handlers.emit_error(ConnectionError::new(&message, false));
            return Err(KalamLinkError::WebSocketError(message));
        },
        Ok(Err(e)) => {
            let msg = format!("Connection failed: {}", e);
            event_handlers.emit_error(ConnectionError::new(&msg, true));
            return Err(KalamLinkError::WebSocketError(msg));
        },
        Err(_) => {
            let msg = format!("Connection timeout ({:?})", timeouts.connection_timeout);
            event_handlers.emit_error(ConnectionError::new(&msg, true));
            return Err(KalamLinkError::TimeoutError(msg));
        },
    };

    send_auth_and_wait(&mut ws_stream, &auth, timeouts.auth_timeout).await?;
    log::info!("[kalam-link] WebSocket authenticated successfully");

    Ok((ws_stream, auth))
}

async fn send_subscribe(
    ws: &mut WebSocketStream,
    id: &str,
    sql: &str,
    options: SubscriptionOptions,
) -> Result<()> {
    let msg = ClientMessage::Subscribe {
        subscription: SubscriptionRequest {
            id: id.to_string(),
            sql: sql.to_string(),
            options,
        },
    };
    let payload = serde_json::to_string(&msg).map_err(|e| {
        KalamLinkError::WebSocketError(format!("Failed to serialize subscribe: {}", e))
    })?;
    ws.send(Message::Text(payload.into()))
        .await
        .map_err(|e| KalamLinkError::WebSocketError(format!("Failed to send subscribe: {}", e)))
}

async fn send_unsubscribe(ws: &mut WebSocketStream, id: &str) -> Result<()> {
    let msg = ClientMessage::Unsubscribe {
        subscription_id: id.to_string(),
    };
    let payload = serde_json::to_string(&msg).map_err(|e| {
        KalamLinkError::WebSocketError(format!("Failed to serialize unsubscribe: {}", e))
    })?;
    ws.send(Message::Text(payload.into()))
        .await
        .map_err(|e| KalamLinkError::WebSocketError(format!("Failed to send unsubscribe: {}", e)))
}

async fn route_event(
    event: ChangeEvent,
    ws: &mut WebSocketStream,
    subs: &mut HashMap<String, SubEntry>,
    _event_handlers: &EventHandlers,
) {
    let sub_id = match event.subscription_id() {
        Some(id) => id.to_string(),
        None => return,
    };

    if let ChangeEvent::InitialDataBatch {
        ref batch_control, ..
    }
    | ChangeEvent::Ack {
        ref batch_control, ..
    } = event
    {
        if let Some(seq_id) = batch_control.last_seq_id {
            if let Some(entry) = subs.get_mut(&sub_id) {
                entry.last_seq_id = Some(seq_id);
                entry.last_event_time_ms = Some(now_ms());
            }
        }
        if batch_control.has_more {
            let last_seq = subs.get(&sub_id).and_then(|e| e.last_seq_id);
            if let Err(e) = send_next_batch_request(ws, &sub_id, last_seq).await {
                log::warn!("Failed to send NextBatch for {}: {}", sub_id, e);
            }
        }
    }

    // Track _seq from live change events so reconnects resume correctly.
    // Without this, only the batch_control seq from the initial load is
    // remembered — any Insert/Update/Delete events after that would be
    // replayed on reconnect.
    match &event {
        ChangeEvent::Insert { ref rows, .. }
        | ChangeEvent::Update { ref rows, .. }
        | ChangeEvent::InitialDataBatch { ref rows, .. } => {
            if let Some(entry) = subs.get_mut(&sub_id) {
                seq_tracking::track_rows(&mut entry.last_seq_id, rows);
            }
        },
        ChangeEvent::Delete { ref old_rows, .. } => {
            if let Some(entry) = subs.get_mut(&sub_id) {
                seq_tracking::track_rows(&mut entry.last_seq_id, old_rows);
            }
        },
        _ => {},
    }

    let matched_key = if subs.contains_key(&sub_id) {
        Some(sub_id.clone())
    } else {
        subs.keys().find(|cid| sub_id.ends_with(cid.as_str())).cloned()
    };

    if let Some(key) = matched_key {
        let mut remove_after_send = false;

        if let Some(entry) = subs.get_mut(&key) {
            entry.last_event_time_ms = Some(now_ms());

            match &event {
                _ if subscription_start_ready(&event) => {
                    if let Some(result_tx) = entry.pending_result_tx.take() {
                        let _ = result_tx.send(Ok(entry.generation));
                    }
                },
                ChangeEvent::Error { code, message, .. } => {
                    if let Some(result_tx) = entry.pending_result_tx.take() {
                        let _ = result_tx.send(Err(KalamLinkError::WebSocketError(format!(
                            "Subscription failed ({}): {}",
                            code, message
                        ))));
                        remove_after_send = true;
                    }
                },
                _ => {},
            }

            if !remove_after_send && entry.event_tx.send(Ok(event)).await.is_err() {
                log::debug!("Subscription {} receiver dropped", sub_id);
            }
        }

        if remove_after_send {
            subs.remove(&key);
        }
    } else {
        log::debug!("No subscription found for id: {}", sub_id);
    }
}

async fn resubscribe_all(
    ws: &mut WebSocketStream,
    subs: &HashMap<String, SubEntry>,
    event_handlers: &EventHandlers,
) {
    log::info!(
        "[kalam-link] Re-subscribing {} active subscription(s) after reconnect",
        subs.len()
    );
    for (id, entry) in subs.iter() {
        let mut options = entry.options.clone();
        if let Some(seq_id) = entry.last_seq_id {
            options.from = Some(seq_id);
        }

        log::info!(
            "[kalam-link] Re-subscribing '{}' with from={:?}",
            id,
            entry.last_seq_id.map(|s| s.to_string())
        );

        if let Err(e) = send_subscribe(ws, id, &entry.sql, options).await {
            log::warn!("Failed to re-subscribe {}: {}", id, e);
            event_handlers.emit_error(ConnectionError::new(
                format!("Failed to re-subscribe {}: {}", id, e),
                true,
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connection_task(
    mut cmd_rx: mpsc::Receiver<ConnCmd>,
    base_url: String,
    resolved_auth: Arc<RwLock<ResolvedAuth>>,
    timeouts: KalamLinkTimeouts,
    connection_options: ConnectionOptions,
    event_handlers: EventHandlers,
    connected: Arc<AtomicBool>,
    reconnect_attempts: Arc<AtomicU32>,
    ready_tx: Option<oneshot::Sender<Result<()>>>,
) {
    let mut subs: HashMap<String, SubEntry> = HashMap::new();
    let mut ws_stream: Option<WebSocketStream> = None;
    let mut shutdown_requested = false;
    let mut next_generation: u64 = 1;

    let keepalive_dur = if timeouts.keepalive_interval.is_zero() {
        FAR_FUTURE
    } else {
        jitter_keepalive_interval(timeouts.keepalive_interval, "shared-conn")
    };
    let has_keepalive = !timeouts.keepalive_interval.is_zero();
    let mut idle_deadline = TokioInstant::now() + keepalive_dur;

    let pong_timeout_dur = timeouts.pong_timeout;
    let has_pong_timeout = has_keepalive && !pong_timeout_dur.is_zero();
    let mut awaiting_pong = false;
    let mut pong_deadline = TokioInstant::now() + FAR_FUTURE;

    match establish_ws(&base_url, &resolved_auth, &timeouts, &connection_options, &event_handlers)
        .await
    {
        Ok((stream, _auth)) => {
            ws_stream = Some(stream);
            connected.store(true, Ordering::SeqCst);
            event_handlers.emit_connect();
            idle_deadline = TokioInstant::now() + keepalive_dur;
            if let Some(tx) = ready_tx {
                let _ = tx.send(Ok(()));
            }
        },
        Err(e) => {
            log::warn!("Initial connection failed (will connect on first subscribe): {}", e);
            if let Some(tx) = ready_tx {
                let _ = tx.send(Err(e));
            }
        },
    }

    loop {
        if shutdown_requested {
            if let Some(ref mut ws) = ws_stream {
                for id in subs.keys() {
                    let _ = send_unsubscribe(ws, id).await;
                }
                let _ = ws.close(None).await;
            }
            let was_connected = connected.swap(false, Ordering::SeqCst);
            if was_connected {
                event_handlers.emit_disconnect(DisconnectReason::new("Client disconnected"));
            }
            return;
        }

        if let Some(ref mut ws) = ws_stream {
            let idle_sleep = tokio::time::sleep_until(idle_deadline);
            tokio::pin!(idle_sleep);

            let pong_sleep = tokio::time::sleep_until(pong_deadline);
            tokio::pin!(pong_sleep);

            tokio::select! {
                biased;

                _ = &mut pong_sleep, if has_pong_timeout && awaiting_pong => {
                    log::warn!(
                        "[kalam-link] Pong timeout ({:?}) — server unresponsive",
                        pong_timeout_dur,
                    );
                    event_handlers.emit_disconnect(DisconnectReason::new(format!(
                        "Pong timeout ({:?}) — server unresponsive",
                        pong_timeout_dur,
                    )));
                    connected.store(false, Ordering::SeqCst);
                    awaiting_pong = false;
                    ws_stream = None;
                    continue;
                }

                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ConnCmd::Subscribe { id, sql, options, event_tx, result_tx }) => {
                            if subs.contains_key(&id) {
                                log::debug!(
                                    "[kalam-link] Replacing existing subscription '{}'",
                                    id,
                                );
                                let _ = send_unsubscribe(ws, &id).await;
                                if let Some(mut old_entry) = subs.remove(&id) {
                                    if let Some(old_tx) = old_entry.pending_result_tx.take() {
                                        let _ = old_tx.send(Err(KalamLinkError::Cancelled));
                                    }
                                }
                            }
                            let result = send_subscribe(ws, &id, &sql, options.clone()).await;
                            let gen = next_generation;
                            if result.is_ok() {
                                next_generation += 1;
                                subs.insert(id.clone(), SubEntry {
                                    sql,
                                    options,
                                    event_tx,
                                    last_seq_id: None,
                                    generation: gen,
                                    created_at_ms: now_ms(),
                                    last_event_time_ms: None,
                                    pending_result_tx: Some(result_tx),
                                });
                            } else {
                                let _ = result_tx.send(result.map(|()| gen));
                            }
                        },
                        Some(ConnCmd::Unsubscribe { id, generation }) => {
                            let should_remove = match generation {
                                Some(gen) => subs.get(&id).map_or(false, |e| e.generation == gen),
                                None => true,
                            };
                            if should_remove {
                                if let Some(mut entry) = subs.remove(&id) {
                                    if let Some(result_tx) = entry.pending_result_tx.take() {
                                        let _ = result_tx.send(Err(KalamLinkError::Cancelled));
                                    }
                                }
                                let _ = send_unsubscribe(ws, &id).await;
                            } else {
                                log::debug!(
                                    "[kalam-link] Ignoring stale unsubscribe for '{}' (gen={:?})",
                                    id, generation,
                                );
                            }
                        },
                        Some(ConnCmd::ListSubscriptions { result_tx }) => {
                            let _ = result_tx.send(snapshot_subscriptions(&subs));
                        },
                        Some(ConnCmd::Shutdown) | None => {
                            shutdown_requested = true;
                            continue;
                        },
                    }
                }

                _ = &mut idle_sleep, if has_keepalive && !awaiting_pong => {
                    if let Err(e) = ws.send(Message::Ping(Bytes::new())).await {
                        log::warn!("Keepalive ping failed: {}", e);
                        event_handlers.emit_disconnect(DisconnectReason::new(format!(
                            "Keepalive ping failed: {}", e
                        )));
                        connected.store(false, Ordering::SeqCst);
                        awaiting_pong = false;
                        ws_stream = None;
                        continue;
                    }
                    // Also send an application-level JSON ping so the server's
                    // heartbeat checker is satisfied.  The server tracks
                    // {"type":"ping"} messages, not native WebSocket Ping frames.
                    if let Ok(json_ping) = serde_json::to_string(&ClientMessage::Ping) {
                        let _ = ws.send(Message::Text(json_ping.into())).await;
                    }
                    event_handlers.emit_send("[ping]");
                    if has_pong_timeout {
                        awaiting_pong = true;
                        pong_deadline = TokioInstant::now() + pong_timeout_dur;
                    }
                    idle_deadline = TokioInstant::now() + keepalive_dur;
                }

                frame = ws.next() => {
                    idle_deadline = TokioInstant::now() + keepalive_dur;
                    if awaiting_pong {
                        awaiting_pong = false;
                        pong_deadline = TokioInstant::now() + FAR_FUTURE;
                    }

                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            if text.len() > MAX_WS_TEXT_MESSAGE_BYTES {
                                log::warn!("Text message too large ({} bytes)", text.len());
                                continue;
                            }
                            event_handlers.emit_receive(&text);
                            match parse_message(&text) {
                                Ok(Some(event)) => {
                                    route_event(event, ws, &mut subs, &event_handlers).await;
                                },
                                Ok(None) => {},
                                Err(e) => log::warn!("Failed to parse WS message: {}", e),
                            }
                        },
                        Some(Ok(Message::Binary(data))) => {
                            match decode_ws_payload(&data) {
                                Ok(text) => {
                                    event_handlers.emit_receive(&text);
                                    match parse_message(&text) {
                                        Ok(Some(event)) => {
                                            route_event(event, ws, &mut subs, &event_handlers).await;
                                        },
                                        Ok(None) => {},
                                        Err(e) => log::warn!("Failed to parse decompressed WS message: {}", e),
                                    }
                                },
                                Err(e) => {
                                    event_handlers.emit_error(ConnectionError::new(e.to_string(), false));
                                },
                            }
                        },
                        Some(Ok(Message::Close(frame))) => {
                            let reason = if let Some(f) = frame {
                                DisconnectReason::with_code(f.reason.to_string(), f.code.into())
                            } else {
                                DisconnectReason::new("Server closed connection")
                            };
                            event_handlers.emit_disconnect(reason);
                            connected.store(false, Ordering::SeqCst);
                            ws_stream = None;
                            continue;
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = ws.send(Message::Pong(payload)).await;
                        },
                        Some(Ok(Message::Pong(_))) => {
                            log::debug!("[kalam-link] Keepalive: received Pong");
                        },
                        Some(Ok(Message::Frame(_))) => {},
                        Some(Err(e)) => {
                            let msg = e.to_string();
                            event_handlers.emit_error(ConnectionError::new(&msg, true));
                            event_handlers.emit_disconnect(DisconnectReason::new(format!(
                                "WebSocket error: {}", msg
                            )));
                            connected.store(false, Ordering::SeqCst);
                            ws_stream = None;
                            continue;
                        },
                        None => {
                            event_handlers.emit_disconnect(DisconnectReason::new("WebSocket stream ended"));
                            connected.store(false, Ordering::SeqCst);
                            ws_stream = None;
                            continue;
                        },
                    }
                }
            }
        } else {
            // ── Not connected — reconnect or wait ───────────────────────
            if !connection_options.auto_reconnect || shutdown_requested {
                match cmd_rx.recv().await {
                    Some(ConnCmd::Subscribe { result_tx, .. }) => {
                        let _ = result_tx.send(Err(KalamLinkError::WebSocketError(
                            "Not connected and auto-reconnect is disabled".to_string(),
                        )));
                    },
                    Some(ConnCmd::Unsubscribe { id, generation }) => {
                        let should_remove = match generation {
                            Some(gen) => subs.get(&id).map_or(false, |e| e.generation == gen),
                            None => true,
                        };
                        if should_remove {
                            subs.remove(&id);
                        }
                    },
                    Some(ConnCmd::ListSubscriptions { result_tx }) => {
                        let _ = result_tx.send(snapshot_subscriptions(&subs));
                    },
                    Some(ConnCmd::Shutdown) | None => return,
                }
                continue;
            }

            let attempt = reconnect_attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(max) = connection_options.max_reconnect_attempts {
                if attempt >= max {
                    log::warn!("Max reconnection attempts ({}) reached", max);
                    event_handlers.emit_error(ConnectionError::new(
                        format!("Max reconnection attempts ({}) reached", max),
                        false,
                    ));
                    let err_msg = "Max reconnection attempts reached".to_string();
                    for (_id, mut entry) in subs.drain() {
                        if let Some(result_tx) = entry.pending_result_tx.take() {
                            let _ = result_tx.send(Err(KalamLinkError::WebSocketError(
                                err_msg.clone(),
                            )));
                        }
                        let _ = entry
                            .event_tx
                            .try_send(Err(KalamLinkError::WebSocketError(err_msg.clone())));
                    }
                    loop {
                        match cmd_rx.recv().await {
                            Some(ConnCmd::Subscribe { result_tx, .. }) => {
                                let _ = result_tx.send(Err(KalamLinkError::WebSocketError(
                                    "Max reconnection attempts reached".to_string(),
                                )));
                            },
                            Some(ConnCmd::Unsubscribe { id, .. }) => {
                                subs.remove(&id);
                            },
                            Some(ConnCmd::ListSubscriptions { result_tx }) => {
                                let _ = result_tx.send(snapshot_subscriptions(&subs));
                            },
                            Some(ConnCmd::Shutdown) | None => return,
                        }
                    }
                }
            }

            let delay = std::cmp::min(
                connection_options
                    .reconnect_delay_ms
                    .saturating_mul(2u64.saturating_pow(attempt)),
                connection_options.max_reconnect_delay_ms,
            );

            log::info!("Attempting reconnection in {}ms (attempt {})", delay, attempt + 1);

            let sleep_fut = tokio::time::sleep(Duration::from_millis(delay));
            tokio::pin!(sleep_fut);

            let mut got_shutdown = false;
            loop {
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(ConnCmd::Subscribe { id, sql, options, event_tx, result_tx }) => {
                                if subs.contains_key(&id) {
                                    if let Some(mut old_entry) = subs.remove(&id) {
                                        if let Some(old_tx) = old_entry.pending_result_tx.take() {
                                            let _ = old_tx.send(Err(KalamLinkError::Cancelled));
                                        }
                                    }
                                }
                                let gen = next_generation;
                                next_generation += 1;
                                subs.insert(id, SubEntry {
                                    sql, options, event_tx,
                                    last_seq_id: None,
                                    generation: gen,
                                    created_at_ms: now_ms(),
                                    last_event_time_ms: None,
                                    pending_result_tx: Some(result_tx),
                                });
                            },
                            Some(ConnCmd::Unsubscribe { id, generation }) => {
                                let should_remove = match generation {
                                    Some(gen) => subs.get(&id).map_or(false, |e| e.generation == gen),
                                    None => true,
                                };
                                if should_remove {
                                    if let Some(mut entry) = subs.remove(&id) {
                                        if let Some(result_tx) = entry.pending_result_tx.take() {
                                            let _ = result_tx.send(Err(KalamLinkError::Cancelled));
                                        }
                                    }
                                }
                            },
                            Some(ConnCmd::ListSubscriptions { result_tx }) => {
                                let _ = result_tx.send(snapshot_subscriptions(&subs));
                            },
                            Some(ConnCmd::Shutdown) | None => {
                                got_shutdown = true;
                                break;
                            },
                        }
                    }
                    _ = &mut sleep_fut => { break; }
                }
            }

            if got_shutdown {
                shutdown_requested = true;
                continue;
            }

            match establish_ws(
                &base_url,
                &resolved_auth,
                &timeouts,
                &connection_options,
                &event_handlers,
            )
            .await
            {
                Ok((mut stream, _auth)) => {
                    log::info!("Reconnection successful");
                    reconnect_attempts.store(0, Ordering::SeqCst);
                    connected.store(true, Ordering::SeqCst);
                    event_handlers.emit_connect();
                    resubscribe_all(&mut stream, &subs, &event_handlers).await;
                    ws_stream = Some(stream);
                    idle_deadline = TokioInstant::now() + keepalive_dur;
                    awaiting_pong = false;
                    pong_deadline = TokioInstant::now() + FAR_FUTURE;
                },
                Err(e) => {
                    log::warn!("Reconnection attempt {} failed: {}", attempt + 1, e);
                },
            }
        }
    }
}
