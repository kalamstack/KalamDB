//! WebSocket connection management.
//!
//! This module contains:
//! - [`models`]: Connection-level data models (always available)
//! - [`websocket`]: Low-level WebSocket helpers (URL resolution, auth headers,
//!   message parsing, keepalive jitter, local bind addresses, decompression)
//! - [`shared`]: Shared multiplexed WebSocket connection with auto-reconnect

pub mod models;

#[cfg(feature = "tokio-runtime")]
pub mod shared;
#[cfg(feature = "tokio-runtime")]
pub mod websocket;

// Re-export the shared connection type for crate-internal use.
#[cfg(feature = "tokio-runtime")]
pub(crate) use shared::SharedConnection;
#[cfg(feature = "tokio-runtime")]
pub(crate) use websocket::{
    apply_ws_auth_headers, connect_with_optional_local_bind, decode_ws_payload,
    jitter_keepalive_interval, parse_message, resolve_ws_url, send_auth_and_wait,
    send_next_batch_request, WebSocketStream,
};

#[cfg(feature = "tokio-runtime")]
/// Default capacity for subscription event channels.
pub(crate) const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 8192;

#[cfg(feature = "tokio-runtime")]
/// Maximum text message size (64 MiB).
pub(crate) const MAX_WS_TEXT_MESSAGE_BYTES: usize = 64 << 20;

#[cfg(feature = "tokio-runtime")]
/// Maximum binary message size before decompression (16 MiB).
pub(crate) const MAX_WS_BINARY_MESSAGE_BYTES: usize = 16 << 20;

#[cfg(feature = "tokio-runtime")]
/// Maximum decompressed message size (64 MiB).
pub(crate) const MAX_WS_DECOMPRESSED_MESSAGE_BYTES: usize = 64 << 20;

#[cfg(feature = "tokio-runtime")]
/// A duration far enough in the future (~100 years) to act as "never" for
/// deadline calculations without overflowing `Instant::now() + dur`.
pub(crate) const FAR_FUTURE: std::time::Duration =
    std::time::Duration::from_secs(100 * 365 * 24 * 3600);
