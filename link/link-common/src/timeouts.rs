//! Timeout configuration for KalamDB client operations.
//!
//! Provides centralized timeout management for all client operations
//! including HTTP requests, WebSocket connections, and subscriptions.

use std::time::Duration;

/// Timeout configuration for KalamDB client operations.
///
/// All timeout values are optional and will use sensible defaults if not specified.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use kalam_client::KalamLinkTimeouts;
///
/// // Use defaults (recommended for most cases)
/// let timeouts = KalamLinkTimeouts::default();
///
/// // Custom timeouts for high-latency environments
/// let timeouts = KalamLinkTimeouts::builder()
///     .connection_timeout(Duration::from_secs(60))
///     .receive_timeout(Duration::from_secs(120))
///     .build();
///
/// // Aggressive timeouts for local development
/// let timeouts = KalamLinkTimeouts::fast();
/// ```
#[derive(Debug, Clone)]
pub struct KalamLinkTimeouts {
    /// Timeout for establishing connections (TCP + TLS handshake).
    /// Default: 10 seconds
    pub connection_timeout: Duration,

    /// Timeout for receiving data after a request is sent.
    /// Also used as the interval for detecting stale WebSocket connections.
    /// Default: 30 seconds
    pub receive_timeout: Duration,

    /// Timeout for sending data to the server.
    /// Default: 10 seconds
    pub send_timeout: Duration,

    /// Timeout for waiting for subscription acknowledgment (SUBSCRIBED ack).
    /// Default: 5 seconds
    pub subscribe_timeout: Duration,

    /// Timeout for authentication handshake (WebSocket auth message exchange).
    /// Default: 5 seconds
    pub auth_timeout: Duration,

    /// Timeout for waiting for initial data batch after subscription.
    /// Set to 0 to wait indefinitely (useful for live-only subscriptions).
    /// Default: 30 seconds
    pub initial_data_timeout: Duration,

    /// Idle timeout - close connection if no subscriptions are active.
    /// Set to 0 to disable idle timeout.
    /// Default: 0 (disabled)
    pub idle_timeout: Duration,

    /// Keep-alive ping interval for WebSocket connections.
    /// Set to 0 to disable keep-alive pings.
    /// Default: 10 seconds
    pub keepalive_interval: Duration,

    /// Maximum time to wait for a Pong response after sending a keepalive Ping.
    /// If no Pong (or any other frame) arrives within this window, the
    /// connection is considered dead and will be torn down / reconnected.
    /// Set to 0 to disable pong timeout checking.
    /// Default: 5 seconds
    pub pong_timeout: Duration,
}

impl Default for KalamLinkTimeouts {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(10),
            receive_timeout: Duration::from_secs(30),
            send_timeout: Duration::from_secs(10),
            subscribe_timeout: Duration::from_secs(5),
            auth_timeout: Duration::from_secs(5),
            initial_data_timeout: Duration::from_secs(30),
            idle_timeout: Duration::ZERO, // Disabled by default
            keepalive_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(5),
        }
    }
}

impl KalamLinkTimeouts {
    /// Create a new builder for custom timeout configuration.
    pub fn builder() -> KalamLinkTimeoutsBuilder {
        KalamLinkTimeoutsBuilder::new()
    }

    /// Create timeouts optimized for fast local development.
    ///
    /// Uses shorter timeouts suitable for localhost connections.
    pub fn fast() -> Self {
        Self {
            connection_timeout: Duration::from_secs(2),
            receive_timeout: Duration::from_secs(5),
            send_timeout: Duration::from_secs(2),
            subscribe_timeout: Duration::from_secs(2),
            auth_timeout: Duration::from_secs(2),
            initial_data_timeout: Duration::from_secs(10),
            idle_timeout: Duration::ZERO,
            keepalive_interval: Duration::from_secs(15),
            pong_timeout: Duration::from_secs(5),
        }
    }

    /// Create timeouts optimized for high-latency or unreliable networks.
    ///
    /// Uses longer timeouts suitable for cloud/remote connections.
    pub fn relaxed() -> Self {
        Self {
            connection_timeout: Duration::from_secs(30),
            receive_timeout: Duration::from_secs(120),
            send_timeout: Duration::from_secs(30),
            subscribe_timeout: Duration::from_secs(15),
            auth_timeout: Duration::from_secs(15),
            initial_data_timeout: Duration::from_secs(120),
            idle_timeout: Duration::ZERO,
            keepalive_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(10),
        }
    }

    /// Create timeouts suitable for testing with a specific subscription timeout.
    ///
    /// This is useful for CLI integration tests that need to exit after receiving
    /// initial data rather than waiting forever for changes.
    pub fn for_testing(subscription_timeout_secs: u64) -> Self {
        Self {
            connection_timeout: Duration::from_secs(5),
            receive_timeout: Duration::from_secs(10),
            send_timeout: Duration::from_secs(5),
            subscribe_timeout: Duration::from_secs(3),
            auth_timeout: Duration::from_secs(3),
            initial_data_timeout: Duration::from_secs(subscription_timeout_secs),
            idle_timeout: Duration::from_secs(subscription_timeout_secs),
            keepalive_interval: Duration::from_secs(5),
            pong_timeout: Duration::from_secs(3),
        }
    }

    /// Check if a duration represents "no timeout" (zero or very large).
    pub fn is_no_timeout(duration: Duration) -> bool {
        duration.is_zero() || duration > Duration::from_secs(86400 * 365) // > 1 year
    }
}

/// Builder for creating custom [`KalamLinkTimeouts`] configurations.
#[derive(Debug, Clone)]
pub struct KalamLinkTimeoutsBuilder {
    timeouts: KalamLinkTimeouts,
}

impl KalamLinkTimeoutsBuilder {
    fn new() -> Self {
        Self {
            timeouts: KalamLinkTimeouts::default(),
        }
    }

    /// Set the connection timeout (TCP + TLS handshake).
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.connection_timeout = timeout;
        self
    }

    /// Set the connection timeout in seconds.
    pub fn connection_timeout_secs(self, secs: u64) -> Self {
        self.connection_timeout(Duration::from_secs(secs))
    }

    /// Set the receive timeout (waiting for data after request).
    pub fn receive_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.receive_timeout = timeout;
        self
    }

    /// Set the receive timeout in seconds.
    pub fn receive_timeout_secs(self, secs: u64) -> Self {
        self.receive_timeout(Duration::from_secs(secs))
    }

    /// Set the send timeout (writing data to socket).
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.send_timeout = timeout;
        self
    }

    /// Set the send timeout in seconds.
    pub fn send_timeout_secs(self, secs: u64) -> Self {
        self.send_timeout(Duration::from_secs(secs))
    }

    /// Set the subscription acknowledgment timeout.
    pub fn subscribe_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.subscribe_timeout = timeout;
        self
    }

    /// Set the subscription acknowledgment timeout in seconds.
    pub fn subscribe_timeout_secs(self, secs: u64) -> Self {
        self.subscribe_timeout(Duration::from_secs(secs))
    }

    /// Set the authentication handshake timeout.
    pub fn auth_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.auth_timeout = timeout;
        self
    }

    /// Set the authentication handshake timeout in seconds.
    pub fn auth_timeout_secs(self, secs: u64) -> Self {
        self.auth_timeout(Duration::from_secs(secs))
    }

    /// Set the initial data batch timeout.
    /// Set to 0 to wait indefinitely.
    pub fn initial_data_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.initial_data_timeout = timeout;
        self
    }

    /// Set the initial data batch timeout in seconds.
    /// Set to 0 to wait indefinitely.
    pub fn initial_data_timeout_secs(self, secs: u64) -> Self {
        self.initial_data_timeout(Duration::from_secs(secs))
    }

    /// Set the idle timeout (close connection if no active subscriptions).
    /// Set to 0 to disable.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.idle_timeout = timeout;
        self
    }

    /// Set the idle timeout in seconds.
    /// Set to 0 to disable.
    pub fn idle_timeout_secs(self, secs: u64) -> Self {
        self.idle_timeout(Duration::from_secs(secs))
    }

    /// Set the keepalive ping interval.
    /// Set to 0 to disable keepalive pings.
    pub fn keepalive_interval(mut self, interval: Duration) -> Self {
        self.timeouts.keepalive_interval = interval;
        self
    }

    /// Set the keepalive ping interval in seconds.
    /// Set to 0 to disable keepalive pings.
    pub fn keepalive_interval_secs(self, secs: u64) -> Self {
        self.keepalive_interval(Duration::from_secs(secs))
    }

    /// Set the pong timeout (max wait for Pong after sending a Ping).
    /// Set to 0 to disable pong timeout checking.
    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.pong_timeout = timeout;
        self
    }

    /// Set the pong timeout in seconds.
    /// Set to 0 to disable pong timeout checking.
    pub fn pong_timeout_secs(self, secs: u64) -> Self {
        self.pong_timeout(Duration::from_secs(secs))
    }

    /// Build the timeout configuration.
    pub fn build(self) -> KalamLinkTimeouts {
        self.timeouts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_timeouts() {
        let timeouts = KalamLinkTimeouts::default();
        assert_eq!(timeouts.connection_timeout, Duration::from_secs(10));
        assert_eq!(timeouts.receive_timeout, Duration::from_secs(30));
        assert_eq!(timeouts.subscribe_timeout, Duration::from_secs(5));
        assert!(timeouts.idle_timeout.is_zero());
    }

    #[test]
    fn test_builder() {
        let timeouts = KalamLinkTimeouts::builder()
            .connection_timeout_secs(60)
            .receive_timeout_secs(120)
            .idle_timeout_secs(300)
            .build();

        assert_eq!(timeouts.connection_timeout, Duration::from_secs(60));
        assert_eq!(timeouts.receive_timeout, Duration::from_secs(120));
        assert_eq!(timeouts.idle_timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_fast_preset() {
        let timeouts = KalamLinkTimeouts::fast();
        assert!(timeouts.connection_timeout <= Duration::from_secs(5));
        assert!(timeouts.subscribe_timeout <= Duration::from_secs(5));
    }

    #[test]
    fn test_relaxed_preset() {
        let timeouts = KalamLinkTimeouts::relaxed();
        assert!(timeouts.connection_timeout >= Duration::from_secs(30));
        assert!(timeouts.receive_timeout >= Duration::from_secs(60));
    }

    #[test]
    fn test_for_testing() {
        let timeouts = KalamLinkTimeouts::for_testing(10);
        assert_eq!(timeouts.initial_data_timeout, Duration::from_secs(10));
        assert_eq!(timeouts.idle_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_is_no_timeout() {
        assert!(KalamLinkTimeouts::is_no_timeout(Duration::ZERO));
        assert!(!KalamLinkTimeouts::is_no_timeout(Duration::from_secs(1)));
        assert!(!KalamLinkTimeouts::is_no_timeout(Duration::from_secs(3600)));
    }
}
