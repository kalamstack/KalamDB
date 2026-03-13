//! `SubscriptionManager` – consumer handle for a single subscription.
//!
//! Can operate in two modes:
//! - **Per-subscription WebSocket** (legacy) – owns its WS stream via a
//!   background reader task.
//! - **Shared connection** – receives events routed by
//!   [`SharedConnection`](crate::connection::SharedConnection).

use crate::{
    auth::AuthProvider,
    connection::{
        apply_ws_auth_headers, connect_with_optional_local_bind, jitter_keepalive_interval,
        resolve_ws_url, send_auth_and_wait, send_subscription_request,
        DEFAULT_EVENT_CHANNEL_CAPACITY,
    },
    error::{KalamLinkError, Result},
    event_handlers::{ConnectionError, EventHandlers},
    models::{ChangeEvent, ConnectionOptions, SubscriptionConfig},
    seq_id::SeqId,
    subscription::buffer_event,
    subscription::{event_progress, ws_reader_loop},
    timeouts::KalamLinkTimeouts,
};
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, error::Error as WsError};

/// Manages WebSocket subscriptions for real-time change notifications.
///
/// # Examples
///
/// ```rust,no_run
/// use kalam_link::KalamLinkClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = KalamLinkClient::builder()
///     .base_url("http://localhost:3000")
///     .build()?;
///
/// let mut subscription = client.subscribe("SELECT * FROM messages").await?;
///
/// while let Some(event) = subscription.next().await {
///     match event {
///         Ok(change) => println!("Change detected: {:?}", change),
///         Err(e) => eprintln!("Error: {}", e),
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct SubscriptionManager {
    subscription_id: String,
    /// Receives parsed events from the background WS reader task.
    event_rx: mpsc::Receiver<Result<ChangeEvent>>,
    /// Signal the background task to initiate graceful shutdown.
    /// `None` after `close()` has been called (or consumed by `Drop`).
    /// Only used for per-subscription connections (legacy mode).
    close_tx: Option<oneshot::Sender<()>>,
    /// Handle to the background reader task.
    /// Only used for per-subscription connections (legacy mode).
    _reader_handle: Option<JoinHandle<()>>,
    /// When using a shared connection, this sender lets us unsubscribe.
    /// The channel carries `(subscription_id, generation)`.
    shared_unsubscribe_tx: Option<mpsc::Sender<(String, u64)>>,
    /// Sends consumer-observed checkpoint progress back to the shared connection.
    shared_progress_tx: Option<mpsc::Sender<(String, u64, SeqId, bool)>>,
    /// Generation tag assigned by the shared `connection_task`.
    generation: u64,
    /// Local event buffer for yielding batched events from a single WS message.
    event_queue: VecDeque<ChangeEvent>,
    /// Changes received while initial data is still loading.
    buffered_changes: Vec<ChangeEvent>,
    /// Whether initial data is still loading.
    is_loading: bool,
    /// Original `from` cursor used to open this subscription, if any.
    resume_from: Option<SeqId>,
    timeouts: KalamLinkTimeouts,
    closed: bool,
}

impl SubscriptionManager {
    /// Create a new WebSocket subscription.
    pub(crate) async fn new(
        base_url: &str,
        config: SubscriptionConfig,
        auth: &AuthProvider,
        timeouts: &KalamLinkTimeouts,
        connection_options: &ConnectionOptions,
        event_handlers: &EventHandlers,
    ) -> Result<Self> {
        let SubscriptionConfig {
            id,
            sql,
            options,
            ws_url,
        } = config;
        let resume_from = options.as_ref().and_then(|opts| opts.from);

        let request_url =
            resolve_ws_url(base_url, ws_url.as_deref(), connection_options.disable_compression)?;

        let mut request = request_url.into_client_request().map_err(|e| {
            KalamLinkError::WebSocketError(format!("Failed to build WebSocket request: {}", e))
        })?;

        apply_ws_auth_headers(&mut request, auth)?;

        let connect_result = if !KalamLinkTimeouts::is_no_timeout(timeouts.connection_timeout) {
            tokio::time::timeout(
                timeouts.connection_timeout,
                connect_with_optional_local_bind(
                    request,
                    &connection_options.ws_local_bind_addresses,
                    &id,
                ),
            )
            .await
        } else {
            Ok(connect_with_optional_local_bind(
                request,
                &connection_options.ws_local_bind_addresses,
                &id,
            )
            .await)
        };

        let ws_stream = match connect_result {
            Ok(Ok((stream, _))) => stream,
            Ok(Err(WsError::Http(response))) => {
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

        let mut ws_stream = ws_stream;

        // Send authentication message and wait for AuthSuccess
        send_auth_and_wait(&mut ws_stream, auth, timeouts.auth_timeout).await?;

        // Emit on_connect event — WebSocket is established and authenticated
        event_handlers.emit_connect();

        let subscription_id = id;
        let keepalive_interval = if timeouts.keepalive_interval.is_zero() {
            None
        } else {
            Some(jitter_keepalive_interval(timeouts.keepalive_interval, &subscription_id))
        };

        // Send subscription request
        send_subscription_request(&mut ws_stream, &subscription_id, &sql, options).await?;

        // Spawn background reader task
        let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_CAPACITY);
        let (close_tx, close_rx) = oneshot::channel();
        let reader_handle = tokio::spawn(ws_reader_loop(
            ws_stream,
            event_tx,
            close_rx,
            subscription_id.clone(),
            keepalive_interval,
            timeouts.pong_timeout,
            event_handlers.clone(),
        ));

        Ok(Self {
            subscription_id,
            event_rx,
            close_tx: Some(close_tx),
            _reader_handle: Some(reader_handle),
            shared_unsubscribe_tx: None,
            shared_progress_tx: None,
            generation: 0,
            event_queue: VecDeque::new(),
            buffered_changes: Vec::new(),
            is_loading: true,
            resume_from,
            timeouts: timeouts.clone(),
            closed: false,
        })
    }

    /// Create a `SubscriptionManager` that receives events from a
    /// [`SharedConnection`](crate::connection::SharedConnection) rather than
    /// owning its own WebSocket.
    pub(crate) fn from_shared(
        subscription_id: String,
        event_rx: mpsc::Receiver<Result<ChangeEvent>>,
        unsubscribe_tx: mpsc::Sender<(String, u64)>,
        progress_tx: mpsc::Sender<(String, u64, SeqId, bool)>,
        generation: u64,
        resume_from: Option<SeqId>,
        timeouts: &KalamLinkTimeouts,
    ) -> Self {
        Self {
            subscription_id,
            event_rx,
            close_tx: None,
            _reader_handle: None,
            shared_unsubscribe_tx: Some(unsubscribe_tx),
            shared_progress_tx: Some(progress_tx),
            generation,
            event_queue: VecDeque::new(),
            buffered_changes: Vec::new(),
            is_loading: true,
            resume_from,
            timeouts: timeouts.clone(),
            closed: false,
        }
    }

    async fn report_shared_progress(&self, event: &ChangeEvent) {
        let Some(progress_tx) = self.shared_progress_tx.as_ref() else {
            return;
        };
        let Some((seq_id, advance_resume)) = event_progress(event) else {
            return;
        };

        let _ = progress_tx
            .send((self.subscription_id.clone(), self.generation, seq_id, advance_resume))
            .await;
    }

    /// Buffer incoming events: hold live changes while initial data is loading,
    /// then flush them in order once the snapshot is complete.
    fn apply_buffering(&mut self, event: ChangeEvent) {
        buffer_event(
            &mut self.event_queue,
            &mut self.buffered_changes,
            &mut self.is_loading,
            self.resume_from,
            event,
        );
    }

    /// Receive the next change event from the subscription.
    ///
    /// Returns `None` when the connection is closed.
    pub async fn next(&mut self) -> Option<Result<ChangeEvent>> {
        loop {
            // 1. Drain local event queue first
            if let Some(event) = self.event_queue.pop_front() {
                self.report_shared_progress(&event).await;
                return Some(Ok(event));
            }

            // 2. If already closed, signal end-of-stream
            if self.closed {
                return None;
            }

            // 3. Read next parsed event from the background reader task
            match self.event_rx.recv().await {
                Some(Ok(event)) => {
                    self.apply_buffering(event);
                    // Loop back to drain event_queue
                },
                Some(Err(e)) => return Some(Err(e)),
                None => {
                    self.closed = true;
                    return None;
                },
            }
        }
    }

    /// Get the subscription ID assigned by the server
    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    /// Get the configured timeouts
    pub fn timeouts(&self) -> &KalamLinkTimeouts {
        &self.timeouts
    }

    /// Close the subscription gracefully.
    ///
    /// Safe to call multiple times — subsequent calls are no-ops.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        // Shared connection path: tell the connection task to unsubscribe us.
        if let Some(tx) = self.shared_unsubscribe_tx.take() {
            let _ = tx.send((self.subscription_id.clone(), self.generation)).await;
        }

        // Per-subscription path: signal the background reader task.
        if let Some(tx) = self.close_tx.take() {
            let _ = tx.send(());
        }

        Ok(())
    }

    /// Returns `true` if `close()` has been called or `Drop` has run.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

impl Drop for SubscriptionManager {
    fn drop(&mut self) {
        // Shared connection path: fire-and-forget unsubscribe.
        if let Some(tx) = self.shared_unsubscribe_tx.take() {
            let id = self.subscription_id.clone();
            let gen = self.generation;
            let _ = tx.try_send((id, gen));
        }

        // Per-subscription path: signal the background reader task.
        if let Some(tx) = self.close_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a minimal `SubscriptionManager` with no real WebSocket for
    /// testing state-flag logic without a network connection.
    async fn make_test_sub() -> SubscriptionManager {
        let (_tx, rx) = mpsc::channel(1);
        let (close_tx, close_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = close_rx.await;
        });
        SubscriptionManager {
            subscription_id: "unit-test-id".to_string(),
            event_rx: rx,
            close_tx: Some(close_tx),
            _reader_handle: Some(handle),
            shared_unsubscribe_tx: None,
            shared_progress_tx: None,
            generation: 0,
            event_queue: VecDeque::new(),
            buffered_changes: Vec::new(),
            is_loading: false,
            resume_from: None,
            timeouts: KalamLinkTimeouts::default(),
            closed: false,
        }
    }

    #[tokio::test]
    async fn test_is_not_closed_initially() {
        let sub = make_test_sub().await;
        assert!(!sub.is_closed(), "subscription should start as open");
    }

    #[tokio::test]
    async fn test_close_marks_subscription_as_closed() {
        let mut sub = make_test_sub().await;
        assert!(!sub.is_closed());
        sub.close().await.expect("close should succeed on a stream-less sub");
        assert!(sub.is_closed(), "subscription should be closed after close()");
    }

    #[tokio::test]
    async fn test_close_is_idempotent() {
        let mut sub = make_test_sub().await;
        sub.close().await.expect("first close should succeed");
        sub.close().await.expect("second close should also succeed (no-op)");
        assert!(sub.is_closed());
    }

    #[tokio::test]
    async fn test_next_returns_none_when_stream_is_none() {
        let mut sub = make_test_sub().await;
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), sub.next())
            .await
            .expect("next() should complete quickly when stream is None");
        assert!(result.is_none(), "next() should return None when stream is None");
    }

    #[tokio::test]
    async fn test_next_returns_none_after_close() {
        let mut sub = make_test_sub().await;
        sub.close().await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), sub.next())
            .await
            .expect("next() should complete quickly after close");
        assert!(result.is_none());
    }

    #[test]
    fn test_drop_without_runtime_does_not_panic() {
        let sub = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { make_test_sub().await })
        };
        drop(sub);
    }

    #[tokio::test]
    async fn test_drop_inside_runtime_does_not_panic() {
        let sub = make_test_sub().await;
        drop(sub);
        tokio::task::yield_now().await;
    }
}
