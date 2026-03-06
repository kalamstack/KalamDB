//! Connection lifecycle event handlers for KalamDB client.
//!
//! Provides callback-based hooks for monitoring WebSocket connection events:
//!
//! - [`on_connect`](EventHandlers::on_connect): Fired when WebSocket connection is established
//! - [`on_disconnect`](EventHandlers::on_disconnect): Fired when WebSocket connection closes
//! - [`on_error`](EventHandlers::on_error): Fired on connection or protocol errors
//! - [`on_receive`](EventHandlers::on_receive): Optional debug hook for all incoming messages
//! - [`on_send`](EventHandlers::on_send): Optional debug hook for all outgoing messages
//!
//! # Example
//!
//! ```rust,no_run
//! use kalam_link::{KalamLinkClient, EventHandlers};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let handlers = EventHandlers::new()
//!     .on_connect(|| {
//!         println!("Connected to KalamDB!");
//!     })
//!     .on_disconnect(|reason| {
//!         println!("Disconnected: {}", reason.unwrap_or("unknown".to_string()));
//!     })
//!     .on_error(|error| {
//!         eprintln!("Connection error: {}", error);
//!     });
//!
//! let client = KalamLinkClient::builder()
//!     .base_url("http://localhost:3000")
//!     .event_handlers(handlers)
//!     .build()?;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::sync::Arc;

/// Reason for a disconnect event.
#[derive(Debug, Clone)]
pub struct DisconnectReason {
    /// Human-readable description of why the connection closed.
    pub message: String,
    /// WebSocket close code, if available (e.g. 1000 = normal, 1006 = abnormal).
    pub code: Option<u16>,
}

impl DisconnectReason {
    /// Create a new disconnect reason with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
        }
    }

    /// Create a new disconnect reason with a message and close code.
    pub fn with_code(message: impl Into<String>, code: u16) -> Self {
        Self {
            message: message.into(),
            code: Some(code),
        }
    }
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code {
            write!(f, "{} (code: {})", self.message, code)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

/// Error information passed to the `on_error` handler.
#[derive(Debug, Clone)]
pub struct ConnectionError {
    /// Human-readable error message.
    pub message: String,
    /// Whether this error is recoverable (i.e. auto-reconnect may succeed).
    pub recoverable: bool,
}

impl ConnectionError {
    /// Create a new connection error.
    pub fn new(message: impl Into<String>, recoverable: bool) -> Self {
        Self {
            message: message.into(),
            recoverable,
        }
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Direction of a message (for `on_receive` / `on_send` debug hooks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDirection {
    /// Message received from the server.
    Inbound,
    /// Message sent to the server.
    Outbound,
}

impl fmt::Display for MessageDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageDirection::Inbound => write!(f, "inbound"),
            MessageDirection::Outbound => write!(f, "outbound"),
        }
    }
}

/// Type alias for the on_connect callback.
pub type OnConnectCallback = Arc<dyn Fn() + Send + Sync>;

/// Type alias for the on_disconnect callback.
pub type OnDisconnectCallback = Arc<dyn Fn(DisconnectReason) + Send + Sync>;

/// Type alias for the on_error callback.
pub type OnErrorCallback = Arc<dyn Fn(ConnectionError) + Send + Sync>;

/// Type alias for the on_receive callback (debug hook for all inbound messages).
pub type OnReceiveCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Type alias for the on_send callback (debug hook for all outbound messages).
pub type OnSendCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Connection lifecycle event handlers.
///
/// All handlers are optional. The builder pattern makes it easy to register
/// only the handlers you need. Handlers are `Send + Sync` so they work with
/// the async tokio runtime.
///
/// These handlers are invoked across all SDK platforms (Rust, TypeScript, Dart)
/// with the same semantics.
#[derive(Clone, Default)]
pub struct EventHandlers {
    /// Called when the WebSocket connection is successfully established.
    pub(crate) on_connect: Option<OnConnectCallback>,

    /// Called when the WebSocket connection is closed (intentionally or not).
    pub(crate) on_disconnect: Option<OnDisconnectCallback>,

    /// Called when a connection or protocol error occurs.
    pub(crate) on_error: Option<OnErrorCallback>,

    /// Called for every raw message received from the server (debug/tracing).
    pub(crate) on_receive: Option<OnReceiveCallback>,

    /// Called for every raw message sent to the server (debug/tracing).
    pub(crate) on_send: Option<OnSendCallback>,
}

impl fmt::Debug for EventHandlers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventHandlers")
            .field("on_connect", &self.on_connect.is_some())
            .field("on_disconnect", &self.on_disconnect.is_some())
            .field("on_error", &self.on_error.is_some())
            .field("on_receive", &self.on_receive.is_some())
            .field("on_send", &self.on_send.is_some())
            .finish()
    }
}

impl EventHandlers {
    /// Create a new empty `EventHandlers` (no callbacks registered).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a callback invoked when the WebSocket connection is established.
    ///
    /// # Example
    /// ```rust
    /// use kalam_link::EventHandlers;
    ///
    /// let handlers = EventHandlers::new()
    ///     .on_connect(|| println!("Connected!"));
    /// ```
    pub fn on_connect(mut self, f: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// Register a callback invoked when the WebSocket connection is closed.
    ///
    /// The callback receives a [`DisconnectReason`] with details about why
    /// the connection was closed.
    ///
    /// # Example
    /// ```rust
    /// use kalam_link::EventHandlers;
    ///
    /// let handlers = EventHandlers::new()
    ///     .on_disconnect(|reason| println!("Disconnected: {}", reason));
    /// ```
    pub fn on_disconnect(mut self, f: impl Fn(DisconnectReason) + Send + Sync + 'static) -> Self {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    /// Register a callback invoked when a connection error occurs.
    ///
    /// The callback receives a [`ConnectionError`] indicating whether the
    /// error is recoverable (auto-reconnect may help) or fatal.
    ///
    /// # Example
    /// ```rust
    /// use kalam_link::EventHandlers;
    ///
    /// let handlers = EventHandlers::new()
    ///     .on_error(|err| eprintln!("Error (recoverable={}): {}", err.recoverable, err));
    /// ```
    pub fn on_error(mut self, f: impl Fn(ConnectionError) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }

    /// Register a callback invoked for every raw message received from the server.
    ///
    /// This is a **debug/tracing hook** — it receives the raw JSON string of
    /// every inbound WebSocket message before parsing. Useful for logging or
    /// diagnostics. Not needed for normal operation.
    ///
    /// # Example
    /// ```rust
    /// use kalam_link::EventHandlers;
    ///
    /// let handlers = EventHandlers::new()
    ///     .on_receive(|msg| println!("[RECV] {}", msg));
    /// ```
    pub fn on_receive(mut self, f: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.on_receive = Some(Arc::new(f));
        self
    }

    /// Register a callback invoked for every raw message sent to the server.
    ///
    /// This is a **debug/tracing hook** — it receives the raw JSON string of
    /// every outbound WebSocket message. Useful for logging or diagnostics.
    ///
    /// # Example
    /// ```rust
    /// use kalam_link::EventHandlers;
    ///
    /// let handlers = EventHandlers::new()
    ///     .on_send(|msg| println!("[SEND] {}", msg));
    /// ```
    pub fn on_send(mut self, f: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.on_send = Some(Arc::new(f));
        self
    }

    /// Returns `true` if any handler is registered.
    pub fn has_any(&self) -> bool {
        self.on_connect.is_some()
            || self.on_disconnect.is_some()
            || self.on_error.is_some()
            || self.on_receive.is_some()
            || self.on_send.is_some()
    }

    // ---------------------------------------------------------------
    // Internal dispatch helpers
    // ---------------------------------------------------------------

    #[cfg(feature = "tokio-runtime")]
    /// Dispatch the on_connect event.
    pub(crate) fn emit_connect(&self) {
        if let Some(cb) = &self.on_connect {
            cb();
        }
    }

    #[cfg(feature = "tokio-runtime")]
    /// Dispatch the on_disconnect event.
    pub(crate) fn emit_disconnect(&self, reason: DisconnectReason) {
        if let Some(cb) = &self.on_disconnect {
            cb(reason);
        }
    }

    #[cfg(feature = "tokio-runtime")]
    /// Dispatch the on_error event.
    pub(crate) fn emit_error(&self, error: ConnectionError) {
        if let Some(cb) = &self.on_error {
            cb(error);
        }
    }

    #[cfg(feature = "tokio-runtime")]
    /// Dispatch the on_receive event.
    pub(crate) fn emit_receive(&self, raw: &str) {
        if let Some(cb) = &self.on_receive {
            cb(raw);
        }
    }

    #[cfg(feature = "tokio-runtime")]
    /// Dispatch the on_send event.
    pub(crate) fn emit_send(&self, raw: &str) {
        if let Some(cb) = &self.on_send {
            cb(raw);
        }
    }
}
