use std::time::{Duration, Instant};

use crate::auth::AuthProvider;
use crate::client::KalamLinkClientBuilder;
use crate::consumer::core::offset_manager::OffsetManager;
use crate::consumer::core::poller::{AckRequest, ConsumeRequest, ConsumeResponse, ConsumerPoller};
use crate::consumer::models::{AutoOffsetReset, CommitResult, ConsumerConfig, ConsumerRecord};
use crate::error::{KalamLinkError, Result};
use crate::models::ConnectionOptions;
use crate::timeouts::KalamLinkTimeouts;
use crate::KalamLinkClient;

pub struct TopicConsumer {
    #[allow(dead_code)] // retained for lifetime — owns the reqwest::Client
    config: ConsumerConfig,
    poller: ConsumerPoller,
    offsets: OffsetManager,
    last_auto_commit: Instant,
    closed: bool,
}

impl TopicConsumer {
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::new()
    }

    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        self.ensure_open()?;
        self.maybe_auto_commit().await?;

        let request = self.build_consume_request(None);
        let response = self.poller.consume(request).await?;
        self.apply_consume_response(response)
    }

    pub async fn poll_with_timeout(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        self.ensure_open()?;
        self.maybe_auto_commit().await?;

        let request = self.build_consume_request(Some(timeout));
        let response = self.poller.consume(request).await?;
        self.apply_consume_response(response)
    }

    pub async fn commit_sync(&mut self) -> Result<CommitResult> {
        self.ensure_open()?;
        let offset = self.offsets.commit_offset().ok_or_else(|| {
            KalamLinkError::ConfigurationError("No processed offsets to commit".into())
        })?;

        let request = AckRequest {
            topic_id: self.config.topic.clone(),
            group_id: self.config.group_id.clone(),
            partition_id: self.config.partition_id,
            upto_offset: offset,
        };

        let result = self.poller.ack(request).await?;
        self.offsets.set_last_committed(result.acknowledged_offset);
        self.last_auto_commit = Instant::now();
        Ok(result)
    }

    pub async fn commit_async(&mut self) -> Result<()> {
        self.ensure_open()?;
        let offset = match self.offsets.commit_offset() {
            Some(value) => value,
            None => return Ok(()),
        };

        let poller = self.poller.clone();
        let topic_id = self.config.topic.clone();
        let group_id = self.config.group_id.clone();
        let partition_id = self.config.partition_id;

        tokio::spawn(async move {
            let _ = poller
                .ack(AckRequest {
                    topic_id,
                    group_id,
                    partition_id,
                    upto_offset: offset,
                })
                .await;
        });

        Ok(())
    }

    pub fn seek(&mut self, offset: u64) {
        self.offsets.reset_position(offset);
    }

    pub fn position(&self) -> u64 {
        self.offsets.position().unwrap_or(0)
    }

    pub fn mark_processed(&mut self, record: &ConsumerRecord) {
        self.offsets.mark_processed(record.offset);
    }

    pub fn offsets(&self) -> crate::consumer::models::ConsumerOffsets {
        self.offsets.snapshot()
    }

    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if self.config.enable_auto_commit {
            let _ = self.commit_sync().await;
        }
        self.closed = true;
        Ok(())
    }

    /// Returns `true` if `close()` has been called or `Drop` has run.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn build_consume_request(&self, timeout_override: Option<Duration>) -> ConsumeRequest {
        let start = match self.offsets.position() {
            Some(position) => AutoOffsetReset::Offset(position),
            None => self.config.auto_offset_reset.clone(),
        };

        ConsumeRequest {
            topic_id: self.config.topic.clone(),
            group_id: self.config.group_id.clone(),
            start,
            limit: self.config.max_poll_records,
            partition_id: self.config.partition_id,
            timeout_seconds: timeout_override
                .or(Some(self.config.poll_timeout))
                .map(|value| value.as_secs()),
        }
    }

    fn apply_consume_response(&mut self, response: ConsumeResponse) -> Result<Vec<ConsumerRecord>> {
        self.offsets.set_position(response.next_offset);
        let topic_name = self.config.topic.clone();
        let records = response
            .messages
            .into_iter()
            .map(|message| message.into_record(topic_name.clone()))
            .collect::<Vec<_>>();
        Ok(records)
    }

    async fn maybe_auto_commit(&mut self) -> Result<()> {
        if !self.config.enable_auto_commit {
            return Ok(());
        }
        if self.last_auto_commit.elapsed() < self.config.auto_commit_interval {
            return Ok(());
        }
        if self.offsets.commit_offset().is_none() {
            return Ok(());
        }
        let _ = self.commit_sync().await;
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            return Err(KalamLinkError::Cancelled);
        }
        Ok(())
    }
}

impl Drop for TopicConsumer {
    fn drop(&mut self) {
        if self.closed || !self.config.enable_auto_commit {
            return;
        }
        // Best-effort async commit of any unacknowledged offsets
        if let Some(offset) = self.offsets.commit_offset() {
            let poller = self.poller.clone();
            let topic_id = self.config.topic.clone();
            let group_id = self.config.group_id.clone();
            let partition_id = self.config.partition_id;

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = poller
                        .ack(AckRequest {
                            topic_id,
                            group_id,
                            partition_id,
                            upto_offset: offset,
                        })
                        .await;
                });
            }
        }
    }
}

pub struct ConsumerBuilder {
    client: Option<KalamLinkClient>,
    base_url: Option<String>,
    auth: AuthProvider,
    timeouts: KalamLinkTimeouts,
    connection_options: ConnectionOptions,
    group_id: Option<String>,
    client_id: Option<String>,
    topic: Option<String>,
    auto_offset_reset: Option<AutoOffsetReset>,
    enable_auto_commit: Option<bool>,
    auto_commit_interval: Option<Duration>,
    max_poll_records: Option<u32>,
    poll_timeout: Option<Duration>,
    partition_id: Option<u32>,
    request_timeout: Option<Duration>,
    retry_backoff: Option<Duration>,
}

impl ConsumerBuilder {
    pub(crate) fn new() -> Self {
        Self {
            client: None,
            base_url: None,
            auth: AuthProvider::none(),
            timeouts: KalamLinkTimeouts::default(),
            connection_options: ConnectionOptions::default(),
            group_id: None,
            client_id: None,
            topic: None,
            auto_offset_reset: None,
            enable_auto_commit: None,
            auto_commit_interval: None,
            max_poll_records: None,
            poll_timeout: None,
            partition_id: None,
            request_timeout: None,
            retry_backoff: None,
        }
    }

    pub(crate) fn from_client(client: KalamLinkClient) -> Self {
        Self {
            client: Some(client),
            ..Self::new()
        }
    }

    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    pub fn auth(mut self, auth: AuthProvider) -> Self {
        self.auth = auth;
        self
    }

    pub fn jwt_token(mut self, token: impl Into<String>) -> Self {
        self.auth = AuthProvider::jwt_token(token.into());
        self
    }

    pub fn timeouts(mut self, timeouts: KalamLinkTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub fn connection_options(mut self, options: ConnectionOptions) -> Self {
        self.connection_options = options;
        self
    }

    pub fn group_id(mut self, value: impl Into<String>) -> Self {
        self.group_id = Some(value.into());
        self
    }

    pub fn client_id(mut self, value: impl Into<String>) -> Self {
        self.client_id = Some(value.into());
        self
    }

    pub fn topic(mut self, value: impl Into<String>) -> Self {
        self.topic = Some(value.into());
        self
    }

    pub fn auto_offset_reset(mut self, value: AutoOffsetReset) -> Self {
        self.auto_offset_reset = Some(value);
        self
    }

    pub fn enable_auto_commit(mut self, value: bool) -> Self {
        self.enable_auto_commit = Some(value);
        self
    }

    pub fn auto_commit_interval(mut self, value: Duration) -> Self {
        self.auto_commit_interval = Some(value);
        self
    }

    pub fn max_poll_records(mut self, value: u32) -> Self {
        self.max_poll_records = Some(value);
        self
    }

    pub fn poll_timeout(mut self, value: Duration) -> Self {
        self.poll_timeout = Some(value);
        self
    }

    pub fn partition_id(mut self, value: u32) -> Self {
        self.partition_id = Some(value);
        self
    }

    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.request_timeout = Some(value);
        self
    }

    pub fn retry_backoff(mut self, value: Duration) -> Self {
        self.retry_backoff = Some(value);
        self
    }

    pub fn from_properties(
        mut self,
        props: &std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let config = ConsumerConfig::from_map(props)?;
        self.group_id = Some(config.group_id);
        self.client_id = config.client_id;
        self.topic = Some(config.topic);
        self.auto_offset_reset = Some(config.auto_offset_reset);
        self.enable_auto_commit = Some(config.enable_auto_commit);
        self.auto_commit_interval = Some(config.auto_commit_interval);
        self.max_poll_records = Some(config.max_poll_records);
        self.poll_timeout = Some(config.poll_timeout);
        self.partition_id = Some(config.partition_id);
        self.request_timeout = Some(config.request_timeout);
        self.retry_backoff = Some(config.retry_backoff);
        Ok(self)
    }

    pub fn build(self) -> Result<TopicConsumer> {
        let group_id = self
            .group_id
            .ok_or_else(|| KalamLinkError::ConfigurationError("group_id is required".into()))?;
        let topic = self
            .topic
            .ok_or_else(|| KalamLinkError::ConfigurationError("topic is required".into()))?;

        let mut config = ConsumerConfig::new(group_id, topic);
        if let Some(value) = self.client_id {
            config.client_id = Some(value);
        }
        if let Some(value) = self.auto_offset_reset {
            config.auto_offset_reset = value;
        }
        if let Some(value) = self.enable_auto_commit {
            config.enable_auto_commit = value;
        }
        if let Some(value) = self.auto_commit_interval {
            config.auto_commit_interval = value;
        }
        if let Some(value) = self.max_poll_records {
            config.max_poll_records = value;
        }
        if let Some(value) = self.poll_timeout {
            config.poll_timeout = value;
        }
        if let Some(value) = self.partition_id {
            config.partition_id = value;
        }
        if let Some(value) = self.request_timeout {
            config.request_timeout = value;
        }
        if let Some(value) = self.retry_backoff {
            config.retry_backoff = value;
        }

        let client = match self.client {
            Some(client) => client,
            None => {
                let base_url = self.base_url.ok_or_else(|| {
                    KalamLinkError::ConfigurationError("base_url is required".into())
                })?;
                KalamLinkClientBuilder::new()
                    .base_url(base_url)
                    .auth(self.auth)
                    .timeouts(self.timeouts)
                    .connection_options(self.connection_options)
                    .build()?
            },
        };

        let poller = ConsumerPoller::new(
            client.base_url(),
            client.http_client(),
            client.auth().clone(),
            config.request_timeout,
            config.retry_backoff,
        );

        // `client` is consumed here — the poller holds clones of the
        // http_client and auth, so the KalamLinkClient is no longer needed.
        drop(client);

        Ok(TopicConsumer {
            config,
            poller,
            offsets: OffsetManager::new(),
            last_auto_commit: Instant::now(),
            closed: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::core::poller::ConsumerPoller;
    use crate::consumer::models::ConsumerConfig;

    /// Build a minimal `TopicConsumer` without a real server so we can test
    /// state-flag behaviour purely in-memory.
    fn make_test_consumer(enable_auto_commit: bool) -> TopicConsumer {
        let mut config = ConsumerConfig::new("test-group", "test.topic");
        config.enable_auto_commit = enable_auto_commit;

        let poller = ConsumerPoller::new(
            "http://127.0.0.1:1", // unreachable — calls must not be made in these tests
            reqwest::Client::new(),
            AuthProvider::None,
            Duration::from_millis(100),
            Duration::from_millis(50),
        );

        TopicConsumer {
            config,
            poller,
            offsets: OffsetManager::new(),
            last_auto_commit: Instant::now(),
            closed: false,
        }
    }

    // ── is_closed state ─────────────────────────────────────────────────────

    #[test]
    fn test_is_not_closed_initially() {
        let consumer = make_test_consumer(false);
        assert!(!consumer.is_closed(), "consumer should start open");
    }

    #[tokio::test]
    async fn test_close_marks_as_closed() {
        let mut consumer = make_test_consumer(false);
        consumer.close().await.expect("close should succeed");
        assert!(consumer.is_closed(), "consumer should be closed after close()");
    }

    #[tokio::test]
    async fn test_close_is_idempotent() {
        let mut consumer = make_test_consumer(false);
        consumer.close().await.expect("first close should succeed");
        consumer.close().await.expect("second close should be a no-op");
        assert!(consumer.is_closed());
    }

    #[tokio::test]
    async fn test_poll_fails_after_close() {
        let mut consumer = make_test_consumer(false);
        consumer.close().await.unwrap();
        let result = consumer.poll().await;
        assert!(result.is_err(), "poll() after close should return an error");
    }

    /// Drop without runtime present must not panic.
    #[test]
    fn test_drop_without_runtime_does_not_panic() {
        let consumer = make_test_consumer(false);
        drop(consumer);
    }

    /// Drop with a tokio runtime and auto_commit disabled must not panic or
    /// attempt any network I/O.
    #[tokio::test]
    async fn test_drop_no_commit_when_auto_commit_disabled() {
        let consumer = make_test_consumer(false);
        drop(consumer); // should silently no-op
        tokio::task::yield_now().await;
    }

    /// Drop with auto_commit enabled but no processed offsets should be a
    /// silent no-op (nothing to commit).
    #[tokio::test]
    async fn test_drop_no_commit_when_no_processed_offsets() {
        let consumer = make_test_consumer(true);
        // No records processed, so commit_offset() returns None → Drop is a no-op.
        drop(consumer);
        tokio::task::yield_now().await;
    }
}
