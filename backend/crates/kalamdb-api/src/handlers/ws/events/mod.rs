//! WebSocket event handlers
//!
//! This module provides handlers for different WebSocket message types:
//! - Authentication (auth.rs)
//! - Subscription management (subscription.rs, unsubscribe.rs)
//! - Batch fetching (batch.rs)
//! - Connection cleanup (cleanup.rs)
//!
//! All handlers use SharedConnectionState which contains the connection_id,
//! eliminating the need to pass connection_id as a separate parameter.
//!
//! Messages are compressed with gzip when they exceed 512 bytes.
//! When the client negotiates MessagePack, payloads are sent as binary frames.

pub mod auth;
pub mod batch;
pub mod cleanup;
pub mod subscription;
pub mod unsubscribe;

use actix_ws::{CloseCode, CloseReason, Session};
use kalamdb_commons::websocket::SerializationType;
use kalamdb_commons::WebSocketMessage;

use crate::compression::{is_gzip, maybe_compress};
use crate::handlers::ws::models::{Notification, WsErrorCode};

/// Send auth error and close (takes ownership of session to close it)
pub async fn send_auth_error(mut session: Session, message: &str) -> Result<(), ()> {
    let msg = WebSocketMessage::AuthError {
        message: message.to_string(),
    };
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = send_data(&mut session, json.as_bytes(), false).await;
    }
    session
        .close(Some(CloseReason {
            code: CloseCode::Policy,
            description: Some("Authentication failed".into()),
        }))
        .await
        .map_err(|_| ())
}

/// Send error notification
pub async fn send_error(
    session: &mut Session,
    id: &str,
    code: WsErrorCode,
    message: &str,
    compress: bool,
) -> Result<(), ()> {
    let msg = Notification::error(id.to_string(), code.to_string(), message.to_string());
    send_json(session, &msg, compress).await
}

/// Send JSON message with optional compression for large payloads.
///
/// When `compress` is `false` the payload is always sent as a text frame
/// regardless of size, which is useful during development.
pub async fn send_json<T: serde::Serialize>(
    session: &mut Session,
    msg: &T,
    compress: bool,
) -> Result<(), ()> {
    if let Ok(json) = serde_json::to_string(msg) {
        send_data(session, json.as_bytes(), compress).await
    } else {
        Err(())
    }
}

/// Protocol-aware message sender.
///
/// Serializes `msg` using the connection's negotiated serialization type:
/// - **Json**: serialized to JSON text, optionally gzip-compressed.
/// - **MessagePack**: serialized to msgpack binary, optionally gzip-compressed.
///
/// The `compress` flag respects the `?compress=false` query-parameter override
/// and the negotiated `CompressionType`.
pub async fn send_message<T: serde::Serialize>(
    session: &mut Session,
    msg: &T,
    serialization: SerializationType,
    compress: bool,
) -> Result<(), ()> {
    match serialization {
        SerializationType::Json => send_json(session, msg, compress).await,
        SerializationType::MessagePack => {
            let bytes = rmp_serde::to_vec_named(msg).map_err(|_| ())?;
            send_data_binary(session, &bytes, compress).await
        },
    }
}

/// Send raw data with optional compression.
///
/// When `compress` is `true`, messages over 512 bytes are gzip compressed and
/// sent as binary frames.  When `false`, the raw payload is always sent as a
/// text frame, which is easier to inspect during development.
async fn send_data(session: &mut Session, data: &[u8], compress: bool) -> Result<(), ()> {
    if !compress {
        let text = String::from_utf8_lossy(data);
        return session.text(text.into_owned()).await.map_err(|_| ());
    }

    let (payload, compressed) = maybe_compress(data);

    if compressed && is_gzip(&payload) {
        // Send compressed data as binary frame
        session.binary(payload).await.map_err(|_| ())
    } else {
        // Send uncompressed data as text frame
        // Safe to convert since original data was valid JSON string
        let text = String::from_utf8_lossy(&payload);
        session.text(text.into_owned()).await.map_err(|_| ())
    }
}

/// Send binary data (msgpack or already-binary) with optional gzip compression.
///
/// Always sends as a binary WebSocket frame (never text).
async fn send_data_binary(
    session: &mut Session,
    data: &[u8],
    compress: bool,
) -> Result<(), ()> {
    if !compress {
        return session.binary(data.to_vec()).await.map_err(|_| ());
    }

    let (payload, _compressed) = maybe_compress(data);
    session.binary(payload).await.map_err(|_| ())
}
