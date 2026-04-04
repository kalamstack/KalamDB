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
        apply_ws_auth_headers, authenticate_ws, connect_with_optional_local_bind,
        decode_ws_payload, jitter_keepalive_interval, parse_message, parse_message_msgpack,
        resolve_ws_url, send_client_message, send_next_batch_request_with_format, WebSocketStream,
        DEFAULT_EVENT_CHANNEL_CAPACITY, FAR_FUTURE, MAX_WS_TEXT_MESSAGE_BYTES,
    },
    error::{KalamLinkError, Result},
    event_handlers::{ConnectionError, DisconnectReason, EventHandlers},
    models::{
        ChangeEvent, ClientMessage, CompressionType, ConnectionOptions, SerializationType,
        SubscriptionInfo, SubscriptionOptions, SubscriptionRequest,
    },
    seq_id::SeqId,
    seq_tracking,
    subscription::{
        batch_envelope, filter_replayed_event, final_resume_seq, subscription_start_ready,
    },
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

/// Build a `Vec<SubscriptionInfo>` snapshot from the internal subs map,
/// including closed subscriptions whose `last_seq_id` was cached.
fn snapshot_subscriptions(
    subs: &HashMap<String, SubEntry>,
    seq_id_cache: &HashMap<String, SeqId>,
) -> Vec<SubscriptionInfo> {
    let mut out: Vec<SubscriptionInfo> = subs
        .iter()
        .map(|(id, entry)| SubscriptionInfo {
            id: id.clone(),
            query: entry.sql.clone(),
            last_seq_id: effective_entry_seq(entry),
            last_event_time_ms: entry.last_event_time_ms,
            created_at_ms: entry.created_at_ms,
            closed: false,
        })
        .collect();

    // Include cached seq IDs for recently closed subscriptions so that
    // callers (e.g. Dart `_lookupSubscriptionLastSeqId`) can resume from
    // the correct point even after the SubEntry was removed.
    for (id, &seq) in seq_id_cache {
        if !subs.contains_key(id) {
            out.push(SubscriptionInfo {
                id: id.clone(),
                query: String::new(),
                last_seq_id: Some(seq),
                last_event_time_ms: None,
                created_at_ms: 0,
                closed: true,
            });
        }
    }

    out
}

fn effective_entry_seq(entry: &SubEntry) -> Option<SeqId> {
    final_resume_seq(entry.last_seq_id, entry.consumed_seq_id)
}

fn cache_entry_seq(
    seq_id_cache: &mut HashMap<String, SeqId>,
    id: impl Into<String>,
    entry: &SubEntry,
) {
    if let Some(seq) = effective_entry_seq(entry) {
        seq_id_cache.insert(id.into(), seq);
    }
}

fn merge_resume_from(
    options: &mut SubscriptionOptions,
    inherited_seq: Option<SeqId>,
) -> Option<SeqId> {
    let effective_from = match (options.from, inherited_seq) {
        (Some(explicit), Some(cached)) => Some(explicit.max(cached)),
        (explicit, cached) => explicit.or(cached),
    };
    options.from = effective_from;
    effective_from
}

fn should_send_subscription_options(
    request_initial_data: bool,
    options: &SubscriptionOptions,
) -> bool {
    request_initial_data
        || options.batch_size.is_some()
        || options.last_rows.is_some()
        || options.from.is_some()
        || options.snapshot_end_seq.is_some()
}

fn register_subscription_entry(
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
    next_generation: &mut u64,
    timeouts: &KalamLinkTimeouts,
    id: String,
    sql: String,
    mut options: SubscriptionOptions,
    request_initial_data: bool,
    event_tx: mpsc::Sender<Result<ChangeEvent>>,
    result_tx: oneshot::Sender<Result<(u64, Option<SeqId>)>>,
) -> (u64, Option<SeqId>) {
    let effective_from = merge_resume_from(&mut options, seq_id_cache.remove(&id));
    let generation = *next_generation;
    *next_generation += 1;

    subs.insert(
        id,
        SubEntry {
            sql,
            options,
            request_initial_data,
            event_tx,
            last_seq_id: effective_from,
            consumed_seq_id: effective_from,
            batch_seq_id: None,
            snapshot_end_seq: None,
            is_loading: true,
            generation,
            created_at_ms: now_ms(),
            last_event_time_ms: None,
            pending_result_tx: Some(result_tx),
            ready_deadline: startup_deadline(timeouts),
            reconnect_resubscribe_pending: false,
        },
    );

    (generation, effective_from)
}

fn remove_subscription_entry(
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
    id: &str,
    generation: Option<u64>,
) -> Option<SubEntry> {
    let should_remove = match generation {
        Some(expected_generation) => {
            subs.get(id).is_some_and(|entry| entry.generation == expected_generation)
        },
        None => true,
    };
    if !should_remove {
        return None;
    }

    subs.remove(id)
        .inspect(|entry| cache_entry_seq(seq_id_cache, id.to_string(), entry))
}

fn advance_entry_progress(
    entry: &mut SubEntry,
    generation: u64,
    seq_id: SeqId,
    advance_resume: bool,
) {
    if entry.generation != generation {
        return;
    }

    seq_tracking::advance_seq(&mut entry.consumed_seq_id, seq_id);
    if advance_resume {
        seq_tracking::advance_seq(&mut entry.last_seq_id, seq_id);
    }
    entry.last_event_time_ms = Some(now_ms());
}

fn startup_deadline(timeouts: &KalamLinkTimeouts) -> Option<TokioInstant> {
    if KalamLinkTimeouts::is_no_timeout(timeouts.initial_data_timeout) {
        None
    } else {
        Some(TokioInstant::now() + timeouts.initial_data_timeout)
    }
}

fn reset_startup_deadline(entry: &mut SubEntry, timeouts: &KalamLinkTimeouts, is_resume: bool) {
    entry.ready_deadline = startup_deadline(timeouts);
    entry.reconnect_resubscribe_pending = is_resume;
}

fn refresh_startup_deadline(entry: &mut SubEntry, timeouts: &KalamLinkTimeouts) {
    if entry.ready_deadline.is_some() {
        entry.ready_deadline = startup_deadline(timeouts);
    }
}

fn clear_startup_deadline(entry: &mut SubEntry) {
    entry.ready_deadline = None;
    entry.reconnect_resubscribe_pending = false;
}

fn next_startup_deadline(subs: &HashMap<String, SubEntry>) -> TokioInstant {
    subs.values()
        .filter_map(|entry| entry.ready_deadline)
        .min()
        .unwrap_or_else(|| TokioInstant::now() + FAR_FUTURE)
}

fn resolve_subscription_key(sub_id: &str, subs: &HashMap<String, SubEntry>) -> Option<String> {
    if subs.contains_key(sub_id) {
        Some(sub_id.to_string())
    } else {
        subs.keys().find(|client_id| sub_id.ends_with(client_id.as_str())).cloned()
    }
}

// ── Commands ────────────────────────────────────────────────────────────────

/// Commands sent from the public API to the background connection task.
enum ConnCmd {
    Subscribe {
        id: String,
        sql: String,
        options: SubscriptionOptions,
        request_initial_data: bool,
        event_tx: mpsc::Sender<Result<ChangeEvent>>,
        result_tx: oneshot::Sender<Result<(u64, Option<SeqId>)>>,
    },
    Unsubscribe {
        id: String,
        generation: Option<u64>,
    },
    Progress {
        id: String,
        generation: u64,
        seq_id: SeqId,
        advance_resume: bool,
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
    request_initial_data: bool,
    event_tx: mpsc::Sender<Result<ChangeEvent>>,
    last_seq_id: Option<SeqId>,
    consumed_seq_id: Option<SeqId>,
    batch_seq_id: Option<SeqId>,
    snapshot_end_seq: Option<SeqId>,
    is_loading: bool,
    generation: u64,
    created_at_ms: u64,
    last_event_time_ms: Option<u64>,
    pending_result_tx: Option<oneshot::Sender<Result<(u64, Option<SeqId>)>>>,
    ready_deadline: Option<TokioInstant>,
    reconnect_resubscribe_pending: bool,
}

// ── SharedConnection (public handle) ────────────────────────────────────────

pub(crate) struct SharedConnection {
    cmd_tx: mpsc::Sender<ConnCmd>,
    unsub_tx: mpsc::Sender<(String, u64)>,
    progress_tx: mpsc::Sender<(String, u64, SeqId, bool)>,
    connected: Arc<AtomicBool>,
    _reconnect_attempts: Arc<AtomicU32>,
    _task: JoinHandle<()>,
    _unsub_bridge: JoinHandle<()>,
    _progress_bridge: JoinHandle<()>,
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

        let (progress_tx, mut progress_rx) = mpsc::channel::<(String, u64, SeqId, bool)>(256);
        let cmd_tx_progress = cmd_tx.clone();
        let progress_bridge = tokio::spawn(async move {
            while let Some((id, generation, seq_id, advance_resume)) = progress_rx.recv().await {
                let _ = cmd_tx_progress
                    .send(ConnCmd::Progress {
                        id,
                        generation,
                        seq_id,
                        advance_resume,
                    })
                    .await;
            }
        });

        Ok(Self {
            cmd_tx,
            unsub_tx,
            progress_tx,
            connected,
            _reconnect_attempts: reconnect_attempts,
            _task: task,
            _unsub_bridge: unsub_bridge,
            _progress_bridge: progress_bridge,
        })
    }

    pub async fn subscribe(
        &self,
        id: String,
        sql: String,
        options: Option<SubscriptionOptions>,
    ) -> Result<(mpsc::Receiver<Result<ChangeEvent>>, u64, Option<SeqId>)> {
        let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_CAPACITY);
        let (result_tx, result_rx) = oneshot::channel();
        let request_initial_data = options.is_some();
        let options = options.unwrap_or_default();

        self.cmd_tx
            .send(ConnCmd::Subscribe {
                id: id.clone(),
                sql,
                options,
                request_initial_data,
                event_tx,
                result_tx,
            })
            .await
            .map_err(|_| {
                KalamLinkError::WebSocketError("Connection task is not running".to_string())
            })?;

        let (generation, resume_from) = result_rx.await.map_err(|_| {
            KalamLinkError::WebSocketError(
                "Connection task died before confirming subscribe".to_string(),
            )
        })??;

        Ok((event_rx, generation, resume_from))
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

    pub(crate) fn progress_tx(&self) -> mpsc::Sender<(String, u64, SeqId, bool)> {
        self.progress_tx.clone()
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
) -> Result<(WebSocketStream, AuthProvider, SerializationType)> {
    log::debug!("[kalam-link] Establishing WebSocket connection to {}", base_url);
    let resolved = resolved_auth.read().unwrap().clone();
    let auth = resolved.resolve().await?;

    let uses_header_auth = matches!(&auth, AuthProvider::JwtToken(_));

    let mut request_url = resolve_ws_url(base_url, None, connection_options.disable_compression)?;

    // For header-auth fast path, include protocol preferences in query params
    // so the server can negotiate without a separate Authenticate message.
    if uses_header_auth {
        let protocol = connection_options.protocol;
        if protocol.serialization != SerializationType::Json {
            let sep = if request_url.contains('?') { "&" } else { "?" };
            request_url.push_str(&format!("{}serialization=msgpack", sep));
        }
        if protocol.compression == CompressionType::None {
            let sep = if request_url.contains('?') { "&" } else { "?" };
            request_url.push_str(&format!("{}compression=none", sep));
        }
    }

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

    let ser = if uses_header_auth {
        // Header-auth fast path: JWT was sent in the upgrade request's
        // Authorization header. The server validates it during the HTTP
        // upgrade and sends AuthSuccess proactively — no explicit
        // Authenticate message needed, saving a full round-trip.
        authenticate_ws(
            &mut ws_stream,
            &auth,
            timeouts.auth_timeout,
            connection_options.protocol,
            false,
        )
        .await?
    } else {
        // Fallback: send an explicit Authenticate message and wait.
        authenticate_ws(
            &mut ws_stream,
            &auth,
            timeouts.auth_timeout,
            connection_options.protocol,
            true,
        )
        .await?
    };
    log::info!(
        "[kalam-link] WebSocket authenticated successfully (header_auth={})",
        uses_header_auth
    );

    Ok((ws_stream, auth, ser))
}

async fn send_subscribe(
    ws: &mut WebSocketStream,
    id: &str,
    sql: &str,
    options: Option<SubscriptionOptions>,
    serialization: SerializationType,
) -> Result<()> {
    let msg = ClientMessage::Subscribe {
        subscription: SubscriptionRequest {
            id: id.to_string(),
            sql: sql.to_string(),
            options,
        },
    };
    send_client_message(ws, &msg, serialization).await
}

async fn send_unsubscribe(
    ws: &mut WebSocketStream,
    id: &str,
    serialization: SerializationType,
) -> Result<()> {
    let msg = ClientMessage::Unsubscribe {
        subscription_id: id.to_string(),
    };
    send_client_message(ws, &msg, serialization).await
}

async fn route_event(
    event: ChangeEvent,
    ws: &mut WebSocketStream,
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
    timeouts: &KalamLinkTimeouts,
    serialization: SerializationType,
) {
    let incoming_sub_id = match event.subscription_id() {
        Some(id) => id.to_string(),
        None => return,
    };
    let matched_key = resolve_subscription_key(&incoming_sub_id, subs);
    let resume_from = matched_key
        .as_ref()
        .and_then(|key| subs.get(key.as_str()))
        .and_then(effective_entry_seq);
    let Some(event) = filter_replayed_event(event, resume_from) else {
        return;
    };

    let auto_request_next_batch = matches!(event, ChangeEvent::InitialDataBatch { .. });

    if let Some(batch) = batch_envelope(&event) {
        if let Some(key) = matched_key.as_ref() {
            if let Some(entry) = subs.get_mut(key) {
                if let Some(seq_id) = batch.last_seq_id {
                    entry.batch_seq_id = Some(seq_id);
                }
                if let Some(snapshot_end_seq) = batch.snapshot_end_seq {
                    entry.snapshot_end_seq = Some(snapshot_end_seq);
                }
                entry.is_loading = batch.status != crate::models::BatchStatus::Ready;
                entry.last_event_time_ms = Some(now_ms());
                if entry.is_loading {
                    refresh_startup_deadline(entry, timeouts);
                }
            }
        }
        if auto_request_next_batch && batch.has_more {
            let last_seq = matched_key
                .as_ref()
                .and_then(|key| subs.get(key))
                .and_then(|entry| entry.batch_seq_id.or(entry.last_seq_id));
            if let Err(e) =
                send_next_batch_request_with_format(ws, &incoming_sub_id, last_seq, serialization)
                    .await
            {
                log::warn!("Failed to send NextBatch for {}: {}", incoming_sub_id, e);
            }
        }
    }

    // Track _seq from live change events so reconnects resume correctly.
    // Without this, only the batch_control seq from the initial load is
    // remembered — any Insert/Update/Delete events after that would be
    // replayed on reconnect.
    match &event {
        ChangeEvent::Insert { .. } | ChangeEvent::Update { .. } => {
            if let Some(key) = matched_key.as_ref() {
                if let Some(entry) = subs.get_mut(key) {
                    entry.last_event_time_ms = Some(now_ms());
                }
            }
        },
        ChangeEvent::InitialDataBatch { .. } => {},
        ChangeEvent::Delete { .. } => {
            if let Some(key) = matched_key.as_ref() {
                if let Some(entry) = subs.get_mut(key) {
                    entry.last_event_time_ms = Some(now_ms());
                }
            }
        },
        _ => {},
    }

    if let Some(key) = matched_key {
        let mut remove_after_send = false;

        if let Some(entry) = subs.get_mut(&key) {
            entry.last_event_time_ms = Some(now_ms());

            match &event {
                _ if subscription_start_ready(&event) => {
                    clear_startup_deadline(entry);
                    if let Some(result_tx) = entry.pending_result_tx.take() {
                        let _ = result_tx.send(Ok((entry.generation, entry.options.from)));
                    }
                },
                ChangeEvent::Error { code, message, .. } => {
                    clear_startup_deadline(entry);
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

            if !subscription_start_ready(&event) {
                if entry.is_loading {
                    refresh_startup_deadline(entry, timeouts);
                } else if entry.reconnect_resubscribe_pending {
                    clear_startup_deadline(entry);
                }
            }

            if !remove_after_send && entry.event_tx.send(Ok(event)).await.is_err() {
                log::debug!("Subscription {} receiver dropped", incoming_sub_id);
            }
        }

        if remove_after_send {
            if let Some(entry) = subs.remove(&key) {
                cache_entry_seq(seq_id_cache, key, &entry);
            }
        }
    } else {
        log::debug!("No subscription found for id: {}", incoming_sub_id);
    }
}

async fn resubscribe_all(
    ws: &mut WebSocketStream,
    subs: &mut HashMap<String, SubEntry>,
    timeouts: &KalamLinkTimeouts,
    event_handlers: &EventHandlers,
    serialization: SerializationType,
) {
    log::info!(
        "[kalam-link] Re-subscribing {} active subscription(s) after reconnect",
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
            "[kalam-link] Re-subscribing '{}' with from={:?}, snapshot_end={:?}",
            id,
            entry.options.from.map(|s| s.to_string()),
            options.snapshot_end_seq.map(|s| s.to_string())
        );

        let send_options = should_send_subscription_options(entry.request_initial_data, &options)
            .then_some(options);
        if let Err(e) = send_subscribe(ws, id, &entry.sql, send_options, serialization).await {
            log::warn!("Failed to re-subscribe {}: {}", id, e);
            event_handlers.emit_error(ConnectionError::new(
                format!("Failed to re-subscribe {}: {}", id, e),
                true,
            ));
        }
    }
}

async fn handle_startup_timeouts(
    subs: &mut HashMap<String, SubEntry>,
    seq_id_cache: &mut HashMap<String, SeqId>,
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
            log::warn!("[kalam-link] {}", message);
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
    log::warn!("[kalam-link] {}", message);
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
    // Preserve last_seq_id for subscriptions that were closed, so that a
    // re-subscribe with the same ID can resume from the correct point
    // instead of replaying from the beginning.
    let mut seq_id_cache: HashMap<String, SeqId> = HashMap::new();
    let mut ws_stream: Option<WebSocketStream> = None;
    let mut shutdown_requested = false;
    let mut next_generation: u64 = 1;
    // Negotiated serialization format for the current connection.
    // Reset to Json on disconnect; updated after each successful auth.
    let mut negotiated_ser = SerializationType::Json;

    let keepalive_dur = if timeouts.keepalive_interval.is_zero() {
        FAR_FUTURE
    } else {
        jitter_keepalive_interval(timeouts.keepalive_interval, "shared-conn")
    };
    let has_keepalive = !timeouts.keepalive_interval.is_zero();
    // Keep heartbeats on a fixed cadence even when the server is actively
    // streaming data, otherwise sustained inbound traffic can indefinitely
    // postpone the JSON ping the server expects for liveness.
    let mut ping_deadline = TokioInstant::now() + keepalive_dur;

    let pong_timeout_dur = timeouts.pong_timeout;
    let has_pong_timeout = has_keepalive && !pong_timeout_dur.is_zero();
    let mut awaiting_pong = false;
    let mut pong_deadline = TokioInstant::now() + FAR_FUTURE;

    match establish_ws(&base_url, &resolved_auth, &timeouts, &connection_options, &event_handlers)
        .await
    {
        Ok((stream, _auth, ser)) => {
            ws_stream = Some(stream);
            negotiated_ser = ser;
            connected.store(true, Ordering::SeqCst);
            event_handlers.emit_connect();
            ping_deadline = TokioInstant::now() + keepalive_dur;
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
                        Some(ConnCmd::Subscribe { id, sql, options, request_initial_data, event_tx, result_tx }) => {
                            if subs.contains_key(&id) {
                                log::debug!(
                                    "[kalam-link] Replacing existing subscription '{}'",
                                    id,
                                );
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
                                    "[kalam-link] Ignoring stale unsubscribe for '{}' (gen={:?})",
                                    id, generation,
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
                    // Also send an application-level ping so the server's
                    // heartbeat checker is satisfied.  The server tracks
                    // {"type":"ping"} messages, not native WebSocket Ping frames.
                    let _ = send_client_message(ws, &ClientMessage::Ping, negotiated_ser).await;
                    event_handlers.emit_send("[ping]");
                    if has_pong_timeout {
                        awaiting_pong = true;
                        pong_deadline = TokioInstant::now() + pong_timeout_dur;
                    }
                    ping_deadline = TokioInstant::now() + keepalive_dur;
                }

                frame = ws.next() => {
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
                                    route_event(event, ws, &mut subs, &mut seq_id_cache, &timeouts, negotiated_ser).await;
                                },
                                Ok(None) => {},
                                Err(e) => log::warn!("Failed to parse WS message: {}", e),
                            }
                        },
                        Some(Ok(Message::Binary(data))) => {
                            match negotiated_ser {
                                SerializationType::MessagePack => {
                                    // Msgpack binary frame (possibly gzip-compressed)
                                    let raw = if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
                                        match crate::compression::decompress_gzip_with_limit(
                                            &data,
                                            crate::connection::MAX_WS_DECOMPRESSED_MESSAGE_BYTES,
                                        ) {
                                            Ok(d) => d,
                                            Err(e) => {
                                                log::warn!("Failed to decompress msgpack: {}", e);
                                                continue;
                                            },
                                        }
                                    } else {
                                        data.to_vec()
                                    };
                                    match parse_message_msgpack(&raw) {
                                        Ok(Some(event)) => {
                                            route_event(event, ws, &mut subs, &mut seq_id_cache, &timeouts, negotiated_ser).await;
                                        },
                                        Ok(None) => {},
                                        Err(e) => log::warn!("Failed to parse msgpack message: {}", e),
                                    }
                                },
                                SerializationType::Json => {
                                    // Legacy: gzip-compressed JSON binary frame
                                    match decode_ws_payload(&data) {
                                        Ok(text) => {
                                            event_handlers.emit_receive(&text);
                                            match parse_message(&text) {
                                                Ok(Some(event)) => {
                                                    route_event(event, ws, &mut subs, &mut seq_id_cache, &timeouts, negotiated_ser).await;
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
                            negotiated_ser = SerializationType::Json;
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
            // ── Not connected — reconnect or wait ───────────────────────
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
            if let Some(max) = connection_options.max_reconnect_attempts {
                if attempt >= max {
                    log::warn!("Max reconnection attempts ({}) reached", max);
                    event_handlers.emit_error(ConnectionError::new(
                        format!("Max reconnection attempts ({}) reached", max),
                        false,
                    ));
                    let err_msg = "Max reconnection attempts reached".to_string();
                    for (id, mut entry) in subs.drain() {
                        cache_entry_seq(&mut seq_id_cache, id, &entry);
                        if let Some(result_tx) = entry.pending_result_tx.take() {
                            let _ = result_tx
                                .send(Err(KalamLinkError::WebSocketError(err_msg.clone())));
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

            let sleep_fut = tokio::time::sleep(Duration::from_millis(delay));
            tokio::pin!(sleep_fut);

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
                Ok((mut stream, _auth, ser)) => {
                    log::info!("Reconnection successful");
                    negotiated_ser = ser;
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
                    ping_deadline = TokioInstant::now() + keepalive_dur;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    #[test]
    fn startup_deadline_disabled_when_initial_timeout_is_zero() {
        let mut timeouts = KalamLinkTimeouts::default();
        timeouts.initial_data_timeout = Duration::ZERO;
        assert_eq!(startup_deadline(&timeouts), None);
    }

    #[test]
    fn startup_deadline_helpers_toggle_resume_state() {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (result_tx, _result_rx) = oneshot::channel();
        let mut entry = SubEntry {
            sql: "SELECT 1".to_string(),
            options: SubscriptionOptions::default(),
            request_initial_data: true,
            event_tx,
            last_seq_id: None,
            consumed_seq_id: None,
            batch_seq_id: None,
            snapshot_end_seq: None,
            is_loading: true,
            generation: 1,
            created_at_ms: 0,
            last_event_time_ms: None,
            pending_result_tx: Some(result_tx),
            ready_deadline: None,
            reconnect_resubscribe_pending: false,
        };

        reset_startup_deadline(&mut entry, &KalamLinkTimeouts::default(), true);
        assert!(entry.ready_deadline.is_some());
        assert!(entry.reconnect_resubscribe_pending);

        clear_startup_deadline(&mut entry);
        assert!(entry.ready_deadline.is_none());
        assert!(!entry.reconnect_resubscribe_pending);
    }
}
