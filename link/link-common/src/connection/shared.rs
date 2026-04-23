//! Shared WebSocket connection manager for real-time subscriptions.
//!
//! Provides a single WebSocket connection multiplexed across multiple
//! subscriptions. Handles a shared connection handle, subscription registry,
//! event routing, and reconnect behavior.

use crate::{
    auth::ResolvedAuth,
    error::{KalamLinkError, Result},
    event_handlers::EventHandlers,
    models::{ChangeEvent, ConnectionOptions, SubscriptionInfo, SubscriptionOptions},
    seq_id::SeqId,
    timeouts::KalamLinkTimeouts,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

mod reconnect;
mod registry;
mod routing;

use reconnect::connection_task;
use registry::ConnCmd;

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
            Ok(Err(error)) => {
                log::warn!("Initial shared connection failed: {}", error);
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

    /// Send a subscribe command without waiting for the server Ready ack.
    ///
    /// Returns the event receiver and a oneshot that resolves when the server
    /// confirms the subscription is ready. Callers can drop their lock on the
    /// connection before awaiting the oneshot, allowing other subscribes to
    /// pipeline through the same shared connection concurrently.
    pub async fn subscribe_send(
        &self,
        id: String,
        sql: String,
        options: Option<SubscriptionOptions>,
    ) -> Result<(
        mpsc::Receiver<Result<ChangeEvent>>,
        oneshot::Receiver<Result<(u64, Option<SeqId>)>>,
    )> {
        let (event_tx, event_rx) = mpsc::channel(crate::connection::DEFAULT_EVENT_CHANNEL_CAPACITY);
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

        Ok((event_rx, result_rx))
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

#[cfg(test)]
mod tests {
    use super::registry::{
        clear_startup_deadline, reset_startup_deadline, resume_startup_deadline,
        startup_deadline, SubEntry,
    };
    use super::*;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::Instant as TokioInstant;

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

    #[test]
    fn resume_startup_deadline_uses_subscribe_timeout_window() {
        let timeouts = KalamLinkTimeouts {
            subscribe_timeout: Duration::from_secs(5),
            initial_data_timeout: Duration::from_secs(30),
            ..KalamLinkTimeouts::default()
        };

        let deadline = resume_startup_deadline(&timeouts).expect("resume deadline should exist");
        let remaining = deadline.saturating_duration_since(TokioInstant::now());

        assert!(remaining <= Duration::from_millis(5_100));
        assert!(remaining > Duration::from_secs(4));
    }
}
