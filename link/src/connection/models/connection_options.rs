use serde::{Deserialize, Serialize};

use crate::timestamp::{TimestampFormat, TimestampFormatter};

use super::http_version::HttpVersion;

/// Connection-level options for the WebSocket/HTTP client.
///
/// These options control connection behavior including:
/// - HTTP protocol version (HTTP/1.1 or HTTP/2)
/// - Automatic reconnection on connection loss
/// - Reconnection timing and retry limits
///
/// Separate from SubscriptionOptions which control individual subscriptions.
///
/// # Example
///
/// ```rust
/// use kalam_link::{ConnectionOptions, HttpVersion};
///
/// let options = ConnectionOptions::default()
///     .with_http_version(HttpVersion::Http2)
///     .with_auto_reconnect(true)
///     .with_reconnect_delay_ms(2000)
///     .with_max_reconnect_attempts(Some(10));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionOptions {
    /// HTTP protocol version to use for connections
    /// Default: Http1 (HTTP/1.1) for maximum compatibility
    /// Use Http2 for better performance with multiple concurrent requests
    #[serde(default)]
    pub http_version: HttpVersion,

    /// Enable automatic reconnection on connection loss
    /// Default: true - automatically attempts to reconnect
    #[serde(default = "default_auto_reconnect")]
    pub auto_reconnect: bool,

    /// Initial delay in milliseconds between reconnection attempts
    /// Default: 1000ms (1 second)
    /// Uses exponential backoff up to max_reconnect_delay_ms
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,

    /// Maximum delay between reconnection attempts (for exponential backoff)
    /// Default: 30000ms (30 seconds)
    #[serde(default = "default_max_reconnect_delay_ms")]
    pub max_reconnect_delay_ms: u64,

    /// Maximum number of reconnection attempts before giving up
    /// Default: None (infinite retries)
    /// Set to Some(0) to disable reconnection entirely
    #[serde(default)]
    pub max_reconnect_attempts: Option<u32>,

    /// Timestamp format to use for displaying timestamp columns
    /// Default: Iso8601 (2024-12-14T15:30:45.123Z)
    /// This allows clients to control how timestamps are displayed
    #[serde(default)]
    pub timestamp_format: TimestampFormat,

    /// Application-level keepalive ping interval in milliseconds.
    ///
    /// Browser WebSocket APIs do not expose protocol-level Ping frames, so
    /// the WASM client sends a JSON `{"type":"ping"}` message at this
    /// interval to prevent the server-side heartbeat timeout from firing
    /// on idle connections.
    ///
    /// Set to `0` to disable.  Default: `30_000` (30 seconds).
    #[serde(default = "default_ping_interval_ms")]
    pub ping_interval_ms: u64,

    /// Optional local source IP addresses for outbound WebSocket connections.
    ///
    /// When non-empty, each subscription connection binds to one of these
    /// addresses before dialing the server. This enables source-IP sharding
    /// (for example `127.0.0.1,127.0.0.2,...`) to increase per-host
    /// concurrency under local ephemeral-port limits.
    ///
    /// Values must be IP literals (IPv4 or IPv6), not hostnames.
    #[serde(default)]
    pub ws_local_bind_addresses: Vec<String>,

    /// Disable server-side gzip compression on this WebSocket connection.
    ///
    /// When `true` the client appends `?compress=false` to the WebSocket URL.
    /// The server will then send all messages as plain JSON text frames instead
    /// of gzip-compressed binary frames, making traffic inspectable in browser
    /// DevTools or packet-capture tools.
    ///
    /// **Do not enable in production** — compression reduces bandwidth by ~70%
    /// for large payloads.  Default: `false`.
    #[serde(default)]
    pub disable_compression: bool,

    /// Defer the WebSocket connection until the first subscription is created.
    ///
    /// When `true` (the default), calling `connect()` is not required — the
    /// client will automatically open the shared WebSocket connection the
    /// first time `subscribe()` (or `subscribe_with_config()`) is called.
    /// The same `AuthProvider` used for HTTP queries is reused for the
    /// WebSocket handshake.
    ///
    /// When `false`, the caller is expected to establish the WebSocket
    /// connection explicitly by calling `connect()` before subscribing.
    ///
    /// Default: `true`.
    #[serde(default = "default_ws_lazy_connect")]
    pub ws_lazy_connect: bool,
}

fn default_auto_reconnect() -> bool {
    true
}

fn default_reconnect_delay_ms() -> u64 {
    1000
}

fn default_max_reconnect_delay_ms() -> u64 {
    30000
}

fn default_ping_interval_ms() -> u64 {
    30000
}

fn default_ws_lazy_connect() -> bool {
    true
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            http_version: HttpVersion::default(),
            auto_reconnect: true,
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 30000,
            max_reconnect_attempts: None,
            timestamp_format: TimestampFormat::Iso8601,
            ping_interval_ms: 30000,
            ws_local_bind_addresses: Vec::new(),
            disable_compression: false,
            ws_lazy_connect: true,
        }
    }
}

impl ConnectionOptions {
    /// Create new connection options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the HTTP protocol version to use
    ///
    /// - `HttpVersion::Http1` - HTTP/1.1 (default, maximum compatibility)
    /// - `HttpVersion::Http2` - HTTP/2 (better performance for concurrent requests)
    /// - `HttpVersion::Auto` - Let the client negotiate with the server
    pub fn with_http_version(mut self, version: HttpVersion) -> Self {
        self.http_version = version;
        self
    }

    /// Set whether to automatically reconnect on connection loss
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Set the initial delay between reconnection attempts (in milliseconds)
    pub fn with_reconnect_delay_ms(mut self, delay_ms: u64) -> Self {
        self.reconnect_delay_ms = delay_ms;
        self
    }

    /// Set the maximum delay between reconnection attempts (in milliseconds)
    pub fn with_max_reconnect_delay_ms(mut self, max_delay_ms: u64) -> Self {
        self.max_reconnect_delay_ms = max_delay_ms;
        self
    }

    /// Set the maximum number of reconnection attempts
    /// Pass None for infinite retries, Some(0) to disable reconnection
    pub fn with_max_reconnect_attempts(mut self, max_attempts: Option<u32>) -> Self {
        self.max_reconnect_attempts = max_attempts;
        self
    }

    /// Set the timestamp format for displaying timestamp columns
    pub fn with_timestamp_format(mut self, format: TimestampFormat) -> Self {
        self.timestamp_format = format;
        self
    }

    /// Create a timestamp formatter from this configuration
    pub fn create_formatter(&self) -> TimestampFormatter {
        TimestampFormatter::new(self.timestamp_format)
    }

    /// Set the application-level keepalive ping interval in milliseconds.
    ///
    /// Set to `0` to disable pings.
    pub fn with_ping_interval_ms(mut self, ms: u64) -> Self {
        self.ping_interval_ms = ms;
        self
    }

    /// Set local source IP addresses for outbound WebSocket connections.
    pub fn with_ws_local_bind_addresses(mut self, addresses: Vec<String>) -> Self {
        self.ws_local_bind_addresses = addresses;
        self
    }

    /// Disable server-side gzip compression for debugging.
    ///
    /// **Do not enable in production.**
    pub fn with_disable_compression(mut self, disable: bool) -> Self {
        self.disable_compression = disable;
        self
    }

    /// Control lazy WebSocket connections.
    ///
    /// When `true` (the default), the shared WebSocket connection is
    /// deferred until the first `subscribe()` call, removing the need to
    /// call `connect()` explicitly.  Authentication uses the same provider
    /// configured for HTTP queries.
    ///
    /// Set to `false` to require an explicit `connect()` call before
    /// subscribing.
    pub fn with_ws_lazy_connect(mut self, lazy: bool) -> Self {
        self.ws_lazy_connect = lazy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_ws_lazy_connect_is_true() {
        let opts = ConnectionOptions::default();
        assert!(opts.ws_lazy_connect);
    }

    #[test]
    fn with_ws_lazy_connect_sets_field() {
        let opts = ConnectionOptions::default().with_ws_lazy_connect(true);
        assert!(opts.ws_lazy_connect);
    }

    #[test]
    fn serde_roundtrip_ws_lazy_connect() {
        let opts = ConnectionOptions::default().with_ws_lazy_connect(true);
        let json = serde_json::to_string(&opts).unwrap();
        let deserialized: ConnectionOptions = serde_json::from_str(&json).unwrap();
        assert!(deserialized.ws_lazy_connect);
    }

    #[test]
    fn serde_missing_ws_lazy_connect_defaults_true() {
        // Simulates receiving JSON from an older client that doesn't send this field
        let json = r#"{"auto_reconnect": true}"#;
        let deserialized: ConnectionOptions = serde_json::from_str(json).unwrap();
        assert!(deserialized.ws_lazy_connect);
    }
}
