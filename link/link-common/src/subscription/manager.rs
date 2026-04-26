//! `SubscriptionManager` – consumer handle for a single subscription.
//!
//! Receives events routed by the shared
//! [`SharedConnection`](crate::connection::SharedConnection).

use std::collections::VecDeque;

use tokio::sync::mpsc;

use crate::{
    error::Result,
    models::ChangeEvent,
    seq_id::SeqId,
    subscription::{buffer_event, event_progress},
    timeouts::KalamLinkTimeouts,
};

/// Manages WebSocket subscriptions for real-time change notifications.
///
/// # Examples
///
/// ```rust,no_run
/// use kalam_client::KalamLinkClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = KalamLinkClient::builder().base_url("http://localhost:3000").build()?;
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
    /// Receives parsed events from the shared connection task.
    event_rx: mpsc::Receiver<Result<ChangeEvent>>,
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
        let Some(progress) = event_progress(event) else {
            return;
        };

        let _ = progress_tx
            .send((
                self.subscription_id.clone(),
                self.generation,
                progress.seq_id,
                progress.advance_resume,
            ))
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

            // 3. Read next parsed event from the shared connection task
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

        if let Some(tx) = self.shared_unsubscribe_tx.take() {
            let _ = tx.send((self.subscription_id.clone(), self.generation)).await;
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
        if let Some(tx) = self.shared_unsubscribe_tx.take() {
            let id = self.subscription_id.clone();
            let gen = self.generation;
            let _ = tx.try_send((id, gen));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a minimal `SubscriptionManager` with no live shared connection
    /// for testing state-flag logic without a network dependency.
    fn make_test_sub() -> SubscriptionManager {
        let (event_tx, event_rx) = mpsc::channel(1);
        let (unsubscribe_tx, _unsubscribe_rx) = mpsc::channel(1);
        let (progress_tx, _progress_rx) = mpsc::channel(1);
        drop(event_tx);

        let mut subscription = SubscriptionManager::from_shared(
            "unit-test-id".to_string(),
            event_rx,
            unsubscribe_tx,
            progress_tx,
            0,
            None,
            &KalamLinkTimeouts::default(),
        );
        subscription.is_loading = false;
        subscription
    }

    #[tokio::test]
    async fn test_is_not_closed_initially() {
        let sub = make_test_sub();
        assert!(!sub.is_closed(), "subscription should start as open");
    }

    #[tokio::test]
    async fn test_close_marks_subscription_as_closed() {
        let mut sub = make_test_sub();
        assert!(!sub.is_closed());
        sub.close().await.expect("close should succeed on a stream-less sub");
        assert!(sub.is_closed(), "subscription should be closed after close()");
    }

    #[tokio::test]
    async fn test_close_is_idempotent() {
        let mut sub = make_test_sub();
        sub.close().await.expect("first close should succeed");
        sub.close().await.expect("second close should also succeed (no-op)");
        assert!(sub.is_closed());
    }

    #[tokio::test]
    async fn test_next_returns_none_when_stream_is_none() {
        let mut sub = make_test_sub();
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), sub.next())
            .await
            .expect("next() should complete quickly when stream is None");
        assert!(result.is_none(), "next() should return None when stream is None");
    }

    #[tokio::test]
    async fn test_next_returns_none_after_close() {
        let mut sub = make_test_sub();
        sub.close().await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), sub.next())
            .await
            .expect("next() should complete quickly after close");
        assert!(result.is_none());
    }

    #[test]
    fn test_drop_without_runtime_does_not_panic() {
        let sub = make_test_sub();
        drop(sub);
    }

    #[tokio::test]
    async fn test_drop_inside_runtime_does_not_panic() {
        let sub = make_test_sub();
        drop(sub);
        tokio::task::yield_now().await;
    }
}
