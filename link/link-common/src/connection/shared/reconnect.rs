use super::registry::{
    advance_entry_progress, cache_entry_seq, clear_startup_deadline, effective_entry_seq,
    merge_resume_from, next_startup_deadline, register_subscription_entry,
    remove_subscription_entry, reset_startup_deadline, should_send_subscription_options,
    snapshot_subscriptions, ConnCmd, SubEntry,
};
use super::routing::{route_event, send_subscribe, send_unsubscribe};
use crate::{
    auth::{AuthProvider, ResolvedAuth},
    connection::{
        apply_ws_auth_headers, authenticate_ws, connect_with_optional_local_bind,
        decode_ws_payload, jitter_keepalive_interval, parse_message, parse_message_msgpack,
        resolve_ws_url, send_client_message, WebSocketStream, FAR_FUTURE,
    },
    error::{KalamLinkError, Result},
    event_handlers::{ConnectionError, DisconnectReason, EventHandlers},
    models::{CompressionType, ConnectionOptions, SerializationType},
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
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant as TokioInstant;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, protocol::Message};

async fn establish_ws(
    base_url: &str,
    resolved_auth: &RwLock<ResolvedAuth>,
    timeouts: &KalamLinkTimeouts,
    connection_options: &ConnectionOptions,
    event_handlers: &EventHandlers,
) -> Result<(WebSocketStream, AuthProvider, SerializationType)> {
    log::debug!("[kalam-sdk] Establishing WebSocket connection to {}", base_url);
    let resolved = resolved_auth.read().unwrap().clone();
    let auth = resolved.resolve().await?;

    let uses_header_auth = matches!(&auth, AuthProvider::JwtToken(_));

    let mut request_url = resolve_ws_url(base_url, None, connection_options.disable_compression)?;

    if uses_header_auth {
        let protocol = connection_options.protocol;
        if protocol.serialization != SerializationType::Json {
            let separator = if request_url.contains('?') { "&" } else { "?" };
            request_url.push_str(&format!("{}serialization=msgpack", separator));
        }
        if protocol.compression == CompressionType::None {
            let separator = if request_url.contains('?') { "&" } else { "?" };
            request_url.push_str(&format!("{}compression=none", separator));
        }
    }

    let mut request = request_url.into_client_request().map_err(|error| {
        KalamLinkError::WebSocketError(format!("Failed to build WebSocket request: {}", error))
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
                .and_then(|body| {
                    if body.is_empty() {
                        None
                    } else {
                        Some(String::from_utf8_lossy(body).into_owned())
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
        Ok(Err(error)) => {
            let message = format!("Connection failed: {}", error);
            event_handlers.emit_error(ConnectionError::new(&message, true));
            return Err(KalamLinkError::WebSocketError(message));
        },
        Err(_) => {
            let message = format!("Connection timeout ({:?})", timeouts.connection_timeout);
            event_handlers.emit_error(ConnectionError::new(&message, true));
            return Err(KalamLinkError::TimeoutError(message));
        },
    };

    let serialization = authenticate_ws(
        &mut ws_stream,
        &auth,
        timeouts.auth_timeout,
        connection_options.protocol,
        !uses_header_auth,
    )
    .await?;
    log::info!(
        "[kalam-sdk] WebSocket authenticated successfully (header_auth={})",
        uses_header_auth
    );

    Ok((ws_stream, auth, serialization))
}

async fn resubscribe_all(
    ws: &mut WebSocketStream,
    subs: &mut HashMap<String, SubEntry>,
    timeouts: &KalamLinkTimeouts,
    event_handlers: &EventHandlers,
    serialization: SerializationType,
) {
    log::info!(
        "[kalam-sdk] Re-subscribing {} active subscription(s) after reconnect",
        subs.len()
    );
    for (id, entry) in subs.iter_mut() {
        let mut options = entry.options.clone();
        let was_loading = entry.is_loading;
        entry.batch_seq_id = None;
        entry.is_loading = true;
        reset_startup_deadline(entry, timeouts, true);
        if let Some(seq_id) = effective_entry_seq(entry) {
            options.from = Some(seq_id);
            entry.options.from = Some(seq_id);
        }
        options.snapshot_end_seq = if was_loading {
            entry.snapshot_end_seq
        } else {
            None
        };

        log::info!(
            "[kalam-sdk] Re-subscribing '{}' with from={:?}, snapshot_end={:?}",
            id,
            entry.options.from.map(|seq| seq.to_string()),
            options.snapshot_end_seq.map(|seq| seq.to_string())
        );

        let send_options = should_send_subscription_options(entry.request_initial_data, &options)
            .then_some(options);
        if let Err(error) = send_subscribe(ws, id, &entry.sql, send_options, serialization).await {
            log::warn!("Failed to re-subscribe {}: {}", id, error);
            event_handlers.emit_error(ConnectionError::new(
                format!("Failed to re-subscribe {}: {}", id, error),
                true,
            ));
        }
    }
}

async fn handle_startup_timeouts(
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, crate::seq_id::SeqId>,
    ws_stream: &mut Option<WebSocketStream>,
    connected: &Arc<AtomicBool>,
    timeouts: &KalamLinkTimeouts,
    event_handlers: &EventHandlers,
) -> bool {
    let now = TokioInstant::now();
    let mut timed_out_initial = Vec::new();
    let mut timed_out_resumes = Vec::new();

    for (id, entry) in subs.iter_mut() {
        if entry.ready_deadline.is_some_and(|deadline| deadline <= now) {
            if entry.reconnect_resubscribe_pending {
                timed_out_resumes.push(id.clone());
                clear_startup_deadline(entry);
            } else {
                timed_out_initial.push(id.clone());
            }
        }
    }

    for id in timed_out_initial {
        if let Some(mut entry) = subs.remove(&id) {
            let message = format!(
                "Subscription '{}' did not become ready within {:?}",
                id, timeouts.initial_data_timeout
            );
            log::warn!("[kalam-sdk] {}", message);
            clear_startup_deadline(&mut entry);
            cache_entry_seq(seq_id_cache, id, &entry);
            if let Some(result_tx) = entry.pending_result_tx.take() {
                let _ = result_tx.send(Err(KalamLinkError::TimeoutError(message)));
            }
        }
    }

    if timed_out_resumes.is_empty() {
        return false;
    }

    let message = format!(
        "Reconnected shared WebSocket but subscription resume stalled for {:?}: {}",
        timeouts.initial_data_timeout,
        timed_out_resumes.join(", ")
    );
    log::warn!("[kalam-sdk] {}", message);
    event_handlers.emit_error(ConnectionError::new(&message, true));

    if let Some(mut ws) = ws_stream.take() {
        let _ = ws.close(None).await;
    }

    let was_connected = connected.swap(false, Ordering::SeqCst);
    if was_connected {
        event_handlers.emit_disconnect(DisconnectReason::new(
            "Subscription resume timed out; forcing reconnect",
        ));
    }

    true
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn connection_task(
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
    let mut seq_id_cache: HashMap<String, crate::seq_id::SeqId> = HashMap::new();
    let mut ws_stream: Option<WebSocketStream> = None;
    let mut shutdown_requested = false;
    let mut next_generation: u64 = 1;
    let mut negotiated_ser = SerializationType::Json;

    let keepalive_duration = if timeouts.keepalive_interval.is_zero() {
        FAR_FUTURE
    } else {
        jitter_keepalive_interval(timeouts.keepalive_interval, "shared-conn")
    };
    let has_keepalive = !timeouts.keepalive_interval.is_zero();
    let mut ping_deadline = TokioInstant::now() + keepalive_duration;

    let pong_timeout_duration = timeouts.pong_timeout;
    let has_pong_timeout = has_keepalive && !pong_timeout_duration.is_zero();
    let mut awaiting_pong = false;
    let mut pong_deadline = TokioInstant::now() + FAR_FUTURE;

    match establish_ws(&base_url, &resolved_auth, &timeouts, &connection_options, &event_handlers)
        .await
    {
        Ok((stream, _auth, serialization)) => {
            ws_stream = Some(stream);
            negotiated_ser = serialization;
            connected.store(true, Ordering::SeqCst);
            event_handlers.emit_connect();
            ping_deadline = TokioInstant::now() + keepalive_duration;
            if let Some(tx) = ready_tx {
                let _ = tx.send(Ok(()));
            }
        },
        Err(error) => {
            log::warn!("Initial connection failed (will connect on first subscribe): {}", error);
            if let Some(tx) = ready_tx {
                let _ = tx.send(Err(error));
            }
        },
    }

    loop {
        if shutdown_requested {
            if let Some(ref mut ws) = ws_stream {
                for id in subs.keys() {
                    let _ = send_unsubscribe(ws, id, negotiated_ser).await;
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
            let ping_sleep = tokio::time::sleep_until(ping_deadline);
            tokio::pin!(ping_sleep);

            let pong_sleep = tokio::time::sleep_until(pong_deadline);
            tokio::pin!(pong_sleep);

            let startup_sleep = tokio::time::sleep_until(next_startup_deadline(&subs));
            tokio::pin!(startup_sleep);

            tokio::select! {
                biased;

                _ = &mut startup_sleep => {
                    if handle_startup_timeouts(
                        &mut subs,
                        &mut seq_id_cache,
                        &mut ws_stream,
                        &connected,
                        &timeouts,
                        &event_handlers,
                    ).await {
                        continue;
                    }
                }

                _ = &mut pong_sleep, if has_pong_timeout && awaiting_pong => {
                    log::warn!(
                        "[kalam-sdk] Pong timeout ({:?}) - server unresponsive",
                        pong_timeout_duration,
                    );
                    event_handlers.emit_disconnect(DisconnectReason::new(format!(
                        "Pong timeout ({:?}) — server unresponsive",
                        pong_timeout_duration,
                    )));
                    connected.store(false, Ordering::SeqCst);
                    awaiting_pong = false;
                    ws_stream = None;
                    continue;
                }

                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ConnCmd::Subscribe { id, sql, options, request_initial_data, event_tx, result_tx }) => {
                            if subs.contains_key(&id) {
                                log::debug!("[kalam-sdk] Replacing existing subscription '{}'", id);
                                let _ = send_unsubscribe(ws, &id, negotiated_ser).await;
                                if let Some(mut old_entry) =
                                    remove_subscription_entry(&mut subs, &mut seq_id_cache, &id, None)
                                {
                                    if let Some(old_tx) = old_entry.pending_result_tx.take() {
                                        let _ = old_tx.send(Err(KalamLinkError::Cancelled));
                                    }
                                }
                            }
                            let inherited_seq = seq_id_cache.get(&id).copied();
                            let mut send_options = options.clone();
                            let effective_from = merge_resume_from(&mut send_options, inherited_seq);
                            let wire_options = should_send_subscription_options(
                                request_initial_data,
                                &send_options,
                            )
                            .then_some(send_options);
                            let result = send_subscribe(ws, &id, &sql, wire_options, negotiated_ser).await;
                            if result.is_ok() {
                                register_subscription_entry(
                                    &mut subs,
                                    &mut seq_id_cache,
                                    &mut next_generation,
                                    &timeouts,
                                    id.clone(),
                                    sql,
                                    options,
                                    request_initial_data,
                                    event_tx,
                                    result_tx,
                                );
                            } else {
                                let _ = result_tx.send(result.map(|()| (next_generation, effective_from)));
                            }
                        },
                        Some(ConnCmd::Unsubscribe { id, generation }) => {
                            if let Some(mut entry) =
                                remove_subscription_entry(&mut subs, &mut seq_id_cache, &id, generation)
                            {
                                if let Some(result_tx) = entry.pending_result_tx.take() {
                                    let _ = result_tx.send(Err(KalamLinkError::Cancelled));
                                }
                                let _ = send_unsubscribe(ws, &id, negotiated_ser).await;
                            } else {
                                log::debug!(
                                    "[kalam-sdk] Ignoring stale unsubscribe for '{}' (gen={:?})",
                                    id,
                                    generation,
                                );
                            }
                        },
                        Some(ConnCmd::Progress { id, generation, seq_id, advance_resume }) => {
                            if let Some(entry) = subs.get_mut(&id) {
                                advance_entry_progress(entry, generation, seq_id, advance_resume);
                            }
                        },
                        Some(ConnCmd::ListSubscriptions { result_tx }) => {
                            let _ = result_tx.send(snapshot_subscriptions(&subs, &seq_id_cache));
                        },
                        Some(ConnCmd::Shutdown) | None => {
                            shutdown_requested = true;
                            continue;
                        },
                    }
                }

                _ = &mut ping_sleep, if has_keepalive && !awaiting_pong => {
                    if let Err(error) = ws.send(Message::Ping(Bytes::new())).await {
                        log::warn!("Keepalive ping failed: {}", error);
                        event_handlers.emit_disconnect(DisconnectReason::new(format!(
                            "Keepalive ping failed: {}",
                            error,
                        )));
                        connected.store(false, Ordering::SeqCst);
                        awaiting_pong = false;
                        ws_stream = None;
                        continue;
                    }
                    let _ = send_client_message(ws, &crate::models::ClientMessage::Ping, negotiated_ser).await;
                    event_handlers.emit_send("[ping]");
                    if has_pong_timeout {
                        awaiting_pong = true;
                        pong_deadline = TokioInstant::now() + pong_timeout_duration;
                    }
                    ping_deadline = TokioInstant::now() + keepalive_duration;
                }

                frame = ws.next() => {
                    if awaiting_pong {
                        awaiting_pong = false;
                        pong_deadline = TokioInstant::now() + FAR_FUTURE;
                    }

                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            if text.len() > crate::connection::MAX_WS_TEXT_MESSAGE_BYTES {
                                log::warn!("Text message too large ({} bytes)", text.len());
                                continue;
                            }
                            event_handlers.emit_receive(&text);
                            match parse_message(&text) {
                                Ok(Some(event)) => {
                                    route_event(
                                        event,
                                        ws,
                                        &mut subs,
                                        &mut seq_id_cache,
                                        &timeouts,
                                        negotiated_ser,
                                    )
                                    .await;
                                },
                                Ok(None) => {},
                                Err(error) => log::warn!("Failed to parse WS message: {}", error),
                            }
                        },
                        Some(Ok(Message::Binary(data))) => {
                            match negotiated_ser {
                                SerializationType::MessagePack => {
                                    let raw = if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
                                        match crate::compression::decompress_gzip_with_limit(
                                            &data,
                                            crate::connection::MAX_WS_DECOMPRESSED_MESSAGE_BYTES,
                                        ) {
                                            Ok(decoded) => decoded,
                                            Err(error) => {
                                                log::warn!("Failed to decompress msgpack: {}", error);
                                                continue;
                                            },
                                        }
                                    } else {
                                        data.to_vec()
                                    };
                                    match parse_message_msgpack(&raw) {
                                        Ok(Some(event)) => {
                                            route_event(
                                                event,
                                                ws,
                                                &mut subs,
                                                &mut seq_id_cache,
                                                &timeouts,
                                                negotiated_ser,
                                            )
                                            .await;
                                        },
                                        Ok(None) => {},
                                        Err(error) => log::warn!("Failed to parse msgpack message: {}", error),
                                    }
                                },
                                SerializationType::Json => {
                                    match decode_ws_payload(&data) {
                                        Ok(text) => {
                                            event_handlers.emit_receive(&text);
                                            match parse_message(&text) {
                                                Ok(Some(event)) => {
                                                    route_event(
                                                        event,
                                                        ws,
                                                        &mut subs,
                                                        &mut seq_id_cache,
                                                        &timeouts,
                                                        negotiated_ser,
                                                    )
                                                    .await;
                                                },
                                                Ok(None) => {},
                                                Err(error) => log::warn!(
                                                    "Failed to parse decompressed WS message: {}",
                                                    error,
                                                ),
                                            }
                                        },
                                        Err(error) => {
                                            event_handlers.emit_error(ConnectionError::new(error.to_string(), false));
                                        },
                                    }
                                },
                            }
                        },
                        Some(Ok(Message::Close(frame))) => {
                            let reason = if let Some(frame) = frame {
                                DisconnectReason::with_code(frame.reason.to_string(), frame.code.into())
                            } else {
                                DisconnectReason::new("Server closed connection")
                            };
                            event_handlers.emit_disconnect(reason);
                            connected.store(false, Ordering::SeqCst);
                            negotiated_ser = SerializationType::Json;
                            ws_stream = None;
                            continue;
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = ws.send(Message::Pong(payload)).await;
                        },
                        Some(Ok(Message::Pong(_))) => {
                            log::debug!("[kalam-sdk] Keepalive: received Pong");
                        },
                        Some(Ok(Message::Frame(_))) => {},
                        Some(Err(error)) => {
                            let message = error.to_string();
                            event_handlers.emit_error(ConnectionError::new(&message, true));
                            event_handlers.emit_disconnect(DisconnectReason::new(format!(
                                "WebSocket error: {}",
                                message,
                            )));
                            connected.store(false, Ordering::SeqCst);
                            negotiated_ser = SerializationType::Json;
                            ws_stream = None;
                            continue;
                        },
                        None => {
                            event_handlers.emit_disconnect(DisconnectReason::new("WebSocket stream ended"));
                            connected.store(false, Ordering::SeqCst);
                            negotiated_ser = SerializationType::Json;
                            ws_stream = None;
                            continue;
                        },
                    }
                }
            }
        } else {
            if !connection_options.auto_reconnect || shutdown_requested {
                match cmd_rx.recv().await {
                    Some(ConnCmd::Subscribe { result_tx, .. }) => {
                        let _ = result_tx.send(Err(KalamLinkError::WebSocketError(
                            "Not connected and auto-reconnect is disabled".to_string(),
                        )));
                    },
                    Some(ConnCmd::Unsubscribe { id, generation }) => {
                        let _ = remove_subscription_entry(
                            &mut subs,
                            &mut seq_id_cache,
                            &id,
                            generation,
                        );
                    },
                    Some(ConnCmd::Progress {
                        id,
                        generation,
                        seq_id,
                        advance_resume,
                    }) => {
                        if let Some(entry) = subs.get_mut(&id) {
                            advance_entry_progress(entry, generation, seq_id, advance_resume);
                        }
                    },
                    Some(ConnCmd::ListSubscriptions { result_tx }) => {
                        let _ = result_tx.send(snapshot_subscriptions(&subs, &seq_id_cache));
                    },
                    Some(ConnCmd::Shutdown) | None => return,
                }
                continue;
            }

            let attempt = reconnect_attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(max_attempts) = connection_options.max_reconnect_attempts {
                if attempt >= max_attempts {
                    log::warn!("Max reconnection attempts ({}) reached", max_attempts);
                    event_handlers.emit_error(ConnectionError::new(
                        format!("Max reconnection attempts ({}) reached", max_attempts),
                        false,
                    ));
                    let error_message = "Max reconnection attempts reached".to_string();
                    for (id, mut entry) in subs.drain() {
                        cache_entry_seq(&mut seq_id_cache, id, &entry);
                        if let Some(result_tx) = entry.pending_result_tx.take() {
                            let _ = result_tx
                                .send(Err(KalamLinkError::WebSocketError(error_message.clone())));
                        }
                        let _ = entry
                            .event_tx
                            .try_send(Err(KalamLinkError::WebSocketError(error_message.clone())));
                    }
                    loop {
                        match cmd_rx.recv().await {
                            Some(ConnCmd::Subscribe { result_tx, .. }) => {
                                let _ = result_tx.send(Err(KalamLinkError::WebSocketError(
                                    "Max reconnection attempts reached".to_string(),
                                )));
                            },
                            Some(ConnCmd::Unsubscribe { id, .. }) => {
                                let _ = remove_subscription_entry(
                                    &mut subs,
                                    &mut seq_id_cache,
                                    &id,
                                    None,
                                );
                            },
                            Some(ConnCmd::Progress {
                                id,
                                generation,
                                seq_id,
                                advance_resume,
                            }) => {
                                if let Some(entry) = subs.get_mut(&id) {
                                    advance_entry_progress(
                                        entry,
                                        generation,
                                        seq_id,
                                        advance_resume,
                                    );
                                }
                            },
                            Some(ConnCmd::ListSubscriptions { result_tx }) => {
                                let _ =
                                    result_tx.send(snapshot_subscriptions(&subs, &seq_id_cache));
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

            let sleep_future = tokio::time::sleep(Duration::from_millis(delay));
            tokio::pin!(sleep_future);

            let mut got_shutdown = false;
            loop {
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(ConnCmd::Subscribe { id, sql, options, request_initial_data, event_tx, result_tx }) => {
                                if subs.contains_key(&id) {
                                    if let Some(mut old_entry) =
                                        remove_subscription_entry(&mut subs, &mut seq_id_cache, &id, None)
                                    {
                                        if let Some(old_tx) = old_entry.pending_result_tx.take() {
                                            let _ = old_tx.send(Err(KalamLinkError::Cancelled));
                                        }
                                    }
                                }
                                register_subscription_entry(
                                    &mut subs,
                                    &mut seq_id_cache,
                                    &mut next_generation,
                                    &timeouts,
                                    id,
                                    sql,
                                    options,
                                    request_initial_data,
                                    event_tx,
                                    result_tx,
                                );
                            },
                            Some(ConnCmd::Unsubscribe { id, generation }) => {
                                if let Some(mut entry) =
                                    remove_subscription_entry(&mut subs, &mut seq_id_cache, &id, generation)
                                {
                                    if let Some(result_tx) = entry.pending_result_tx.take() {
                                        let _ = result_tx.send(Err(KalamLinkError::Cancelled));
                                    }
                                }
                            },
                            Some(ConnCmd::Progress { id, generation, seq_id, advance_resume }) => {
                                if let Some(entry) = subs.get_mut(&id) {
                                    advance_entry_progress(entry, generation, seq_id, advance_resume);
                                }
                            },
                            Some(ConnCmd::ListSubscriptions { result_tx }) => {
                                let _ = result_tx.send(snapshot_subscriptions(&subs, &seq_id_cache));
                            },
                            Some(ConnCmd::Shutdown) | None => {
                                got_shutdown = true;
                                break;
                            },
                        }
                    }
                    _ = &mut sleep_future => { break; }
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
                Ok((mut stream, _auth, serialization)) => {
                    log::info!("Reconnection successful");
                    negotiated_ser = serialization;
                    reconnect_attempts.store(0, Ordering::SeqCst);
                    connected.store(true, Ordering::SeqCst);
                    event_handlers.emit_connect();
                    resubscribe_all(
                        &mut stream,
                        &mut subs,
                        &timeouts,
                        &event_handlers,
                        negotiated_ser,
                    )
                    .await;
                    ws_stream = Some(stream);
                    ping_deadline = TokioInstant::now() + keepalive_duration;
                    awaiting_pong = false;
                    pong_deadline = TokioInstant::now() + FAR_FUTURE;
                },
                Err(error) => {
                    log::warn!("Reconnection attempt {} failed: {}", attempt + 1, error);
                },
            }
        }
    }
}
