//! Low-level WebSocket helpers for shared connections.
//!
//! Contains URL resolution, authentication header application, WS connection
//! with optional local bind addresses, message parsing, keepalive jitter,
//! decompression, and protocol message helpers.

use crate::{
    auth::AuthProvider,
    error::{KalamLinkError, Result},
    models::{ChangeEvent, ClientMessage, ServerMessage, WsAuthCredentials},
};
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Error as IoError, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::time::Instant as TokioInstant;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async,
    tungstenite::{
        error::Error as WsError,
        error::UrlError,
        handshake::client::Response as WsResponse,
        http::header::{HeaderValue, AUTHORIZATION},
        protocol::Message,
    },
};

use super::{MAX_WS_BINARY_MESSAGE_BYTES, MAX_WS_DECOMPRESSED_MESSAGE_BYTES};

/// The concrete WebSocket stream type used throughout kalam-link.
pub(crate) type WebSocketStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

// ── URL Resolution ──────────────────────────────────────────────────────────

/// Resolve the WebSocket URL from a base HTTP URL.
///
/// Converts `http(s)://` to `ws(s)://` and appends `/v1/ws`.
/// Optionally accepts an override URL for custom WebSocket endpoints.
pub(crate) fn resolve_ws_url(
    base_url: &str,
    override_url: Option<&str>,
    disable_compression: bool,
) -> Result<String> {
    let base = Url::parse(base_url.trim()).map_err(|e| {
        KalamLinkError::ConfigurationError(format!("Invalid base_url '{}': {}", base_url, e))
    })?;

    validate_ws_url(&base, false, "base_url")?;

    if let Some(url) = override_url {
        let override_parsed = Url::parse(url.trim()).map_err(|e| {
            KalamLinkError::ConfigurationError(format!(
                "Invalid WebSocket override URL '{}': {}",
                url, e
            ))
        })?;

        validate_ws_url(&override_parsed, true, "WebSocket override URL")?;

        if base.scheme() == "https" && override_parsed.scheme() == "ws" {
            return Err(KalamLinkError::ConfigurationError(
                "Refusing insecure ws:// override when base_url uses https://".to_string(),
            ));
        }

        let mut result = override_parsed.to_string();
        if disable_compression {
            result.push_str("?compress=false");
        }
        return Ok(result);
    }

    let mut ws_url = base.clone();
    let ws_scheme = match base.scheme() {
        "http" | "ws" => "ws",
        "https" | "wss" => "wss",
        other => {
            return Err(KalamLinkError::ConfigurationError(format!(
                "Unsupported base_url scheme '{}'; expected http(s) or ws(s)",
                other
            )));
        },
    };

    ws_url.set_scheme(ws_scheme).map_err(|_| {
        KalamLinkError::ConfigurationError("Failed to set WebSocket URL scheme".to_string())
    })?;
    ws_url.set_fragment(None);
    ws_url.set_path("/v1/ws");
    if disable_compression {
        ws_url.set_query(Some("compress=false"));
    } else {
        ws_url.set_query(None);
    }

    Ok(ws_url.to_string())
}

fn validate_ws_url(url: &Url, require_ws_scheme: bool, context: &str) -> Result<()> {
    if url.host_str().is_none() {
        return Err(KalamLinkError::ConfigurationError(format!("{} must include a host", context)));
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(KalamLinkError::ConfigurationError(format!(
            "{} must not include username/password credentials",
            context
        )));
    }

    if require_ws_scheme {
        match url.scheme() {
            "ws" | "wss" => {},
            other => {
                return Err(KalamLinkError::ConfigurationError(format!(
                    "{} must use ws:// or wss:// (found '{}')",
                    context, other
                )));
            },
        }
    }

    if url.query().is_some() || url.fragment().is_some() {
        return Err(KalamLinkError::ConfigurationError(format!(
            "{} must not include query parameters or fragments",
            context
        )));
    }

    Ok(())
}

// ── Connection with optional local bind ─────────────────────────────────────

/// Connect to a WebSocket endpoint, optionally binding to specific local addresses.
///
/// When `local_bind_addresses` is empty, falls back to the default `connect_async`.
/// Otherwise iterates through the configured addresses (starting at a deterministic
/// offset derived from `subscription_id`) to spread connections across interfaces.
pub(crate) async fn connect_with_optional_local_bind(
    request: tokio_tungstenite::tungstenite::http::Request<()>,
    local_bind_addresses: &[String],
    subscription_id: &str,
) -> std::result::Result<(WebSocketStream, WsResponse), WsError> {
    if local_bind_addresses.is_empty() {
        return connect_async(request).await;
    }

    let host = request.uri().host().ok_or(WsError::Url(UrlError::NoHostName))?;
    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(WsError::Url(UrlError::UnsupportedUrlScheme))?;

    let remote_addrs: Vec<SocketAddr> =
        lookup_host((host, port)).await.map_err(WsError::Io)?.collect();
    if remote_addrs.is_empty() {
        return Err(WsError::Io(IoError::new(
            ErrorKind::AddrNotAvailable,
            format!("No resolved addresses for {}:{}", host, port),
        )));
    }

    let bind_ips = parse_local_bind_addresses(local_bind_addresses)?;
    if bind_ips.is_empty() {
        return Err(WsError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "ws_local_bind_addresses is configured but empty after parsing",
        )));
    }

    let mut last_error: Option<IoError> = None;
    let mut attempted_connections = 0usize;
    let start = hash_start_index(subscription_id, bind_ips.len());

    for local_offset in 0..bind_ips.len() {
        let local_ip = bind_ips[(start + local_offset) % bind_ips.len()];
        let bind_addr = SocketAddr::new(local_ip, 0);

        for remote_addr in remote_addrs.iter().copied() {
            if remote_addr.is_ipv4() != local_ip.is_ipv4() {
                continue;
            }

            attempted_connections += 1;

            let socket = if remote_addr.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(WsError::Io)?;

            if let Err(bind_err) = socket.bind(bind_addr) {
                last_error = Some(bind_err);
                continue;
            }

            match socket.connect(remote_addr).await {
                Ok(stream) => {
                    return client_async_tls_with_config(request, stream, None, None).await;
                },
                Err(connect_err) => {
                    last_error = Some(connect_err);
                },
            }
        }
    }

    if attempted_connections == 0 {
        return Err(WsError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "No compatible ws_local_bind_addresses for resolved target address family",
        )));
    }

    Err(WsError::Io(last_error.unwrap_or_else(|| {
        IoError::new(
            ErrorKind::AddrNotAvailable,
            format!(
                "Failed to connect using configured ws_local_bind_addresses ({})",
                local_bind_addresses.join(", ")
            ),
        )
    })))
}

/// Deterministic start index for round-robin bind address selection.
#[inline]
fn hash_start_index(key: &str, len: usize) -> usize {
    if len == 0 {
        return 0;
    }

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % len
}

/// Parse and deduplicate local bind addresses.
fn parse_local_bind_addresses(addresses: &[String]) -> std::result::Result<Vec<IpAddr>, WsError> {
    let mut parsed = Vec::with_capacity(addresses.len());
    for raw in addresses {
        let candidate = raw.trim();
        if candidate.is_empty() {
            continue;
        }
        let ip: IpAddr = candidate.parse().map_err(|e| {
            WsError::Io(IoError::new(
                ErrorKind::InvalidInput,
                format!("Invalid ws_local_bind_addresses entry '{}': {}", candidate, e),
            ))
        })?;
        if !parsed.contains(&ip) {
            parsed.push(ip);
        }
    }
    Ok(parsed)
}

// ── Auth Header Application ─────────────────────────────────────────────────

/// Apply authentication headers to a WebSocket upgrade request.
///
/// Only JWT token auth is supported for WebSocket connections.
/// Basic auth returns an error — callers must login first to get a JWT.
pub(crate) fn apply_ws_auth_headers(
    request: &mut tokio_tungstenite::tungstenite::http::Request<()>,
    auth: &AuthProvider,
) -> Result<()> {
    match auth {
        AuthProvider::BasicAuth(_, _) => Err(KalamLinkError::AuthenticationError(
            "WebSocket authentication requires a JWT token. Use AuthProvider::jwt_token or login first.".to_string(),
        )),
        AuthProvider::JwtToken(token) => {
            let value = format!("Bearer {}", token);
            let header_value = HeaderValue::from_str(&value).map_err(|e| {
                KalamLinkError::ConfigurationError(format!(
                    "Invalid JWT token for Authorization header: {}",
                    e
                ))
            })?;
            request.headers_mut().insert(AUTHORIZATION, header_value);
            Ok(())
        },
        AuthProvider::None => Ok(()),
    }
}

// ── Authentication Handshake ────────────────────────────────────────────────

/// Authenticate the WebSocket connection.
///
/// When `send_credentials` is `true` (message-auth fallback), sends an explicit
/// `Authenticate` message carrying the JWT and protocol options, then waits for
/// the server's `AuthSuccess`.
///
/// When `send_credentials` is `false` (header-auth fast path), the JWT was
/// already in the HTTP upgrade request header. The server validates it during
/// the upgrade and proactively sends `AuthSuccess` as the first frame — no
/// `Authenticate` message is sent, saving a full round-trip.
///
/// Returns the negotiated `SerializationType` from the server's `AuthSuccess`.
pub(crate) async fn authenticate_ws(
    ws_stream: &mut WebSocketStream,
    auth: &AuthProvider,
    auth_timeout: Duration,
    protocol: crate::models::ProtocolOptions,
    send_credentials: bool,
) -> Result<crate::models::SerializationType> {
    if send_credentials {
        send_authenticate_message(ws_stream, auth, protocol).await?;
    }
    await_auth_response(ws_stream, auth_timeout).await
}

/// Send the explicit `Authenticate` client message on the WebSocket.
async fn send_authenticate_message(
    ws_stream: &mut WebSocketStream,
    auth: &AuthProvider,
    protocol: crate::models::ProtocolOptions,
) -> Result<()> {
    let credentials = match auth {
        AuthProvider::BasicAuth(_, _) => {
            return Err(KalamLinkError::AuthenticationError(
                "WebSocket authentication requires a JWT token. Use AuthProvider::jwt_token or login first.".to_string(),
            ));
        },
        AuthProvider::JwtToken(token) => WsAuthCredentials::Jwt {
            token: token.clone(),
        },
        AuthProvider::None => {
            return Err(KalamLinkError::AuthenticationError(
                "Authentication required for WebSocket subscriptions".to_string(),
            ));
        },
    };

    let auth_message = ClientMessage::Authenticate {
        credentials,
        protocol,
    };
    let payload = serde_json::to_string(&auth_message).map_err(|e| {
        KalamLinkError::WebSocketError(format!("Failed to serialize auth message: {}", e))
    })?;

    ws_stream.send(Message::Text(payload.into())).await.map_err(|e| {
        KalamLinkError::WebSocketError(format!("Failed to send auth message: {}", e))
    })?;

    Ok(())
}

/// Wait for the server's `AuthSuccess` or `AuthError` response.
///
/// Shared by both the message-auth and header-auth paths. Tolerates
/// Ping/Pong frames during the handshake window.
async fn await_auth_response(
    ws_stream: &mut WebSocketStream,
    auth_timeout: Duration,
) -> Result<crate::models::SerializationType> {
    let deadline = TokioInstant::now() + auth_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(TokioInstant::now());
        if remaining.is_zero() {
            return Err(KalamLinkError::TimeoutError(format!(
                "Authentication timeout ({:?})",
                auth_timeout
            )));
        }

        match tokio::time::timeout(remaining, ws_stream.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                match serde_json::from_str::<ServerMessage>(&text) {
                    Ok(ServerMessage::AuthSuccess {
                        protocol: negotiated,
                        ..
                    }) => {
                        return Ok(negotiated.serialization);
                    },
                    Ok(ServerMessage::AuthError { message }) => {
                        return Err(KalamLinkError::AuthenticationError(format!(
                            "WebSocket authentication failed: {}",
                            message
                        )));
                    },
                    Ok(_) => continue,
                    Err(e) => {
                        return Err(KalamLinkError::WebSocketError(format!(
                            "Failed to parse auth response: {}",
                            e
                        )));
                    },
                }
            },
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = ws_stream.send(Message::Pong(payload)).await;
            },
            Ok(Some(Ok(Message::Pong(_) | Message::Binary(_) | Message::Frame(_)))) => {
                continue;
            },
            Ok(Some(Ok(Message::Close(_)))) => {
                return Err(KalamLinkError::WebSocketError(
                    "Connection closed during authentication".to_string(),
                ));
            },
            Ok(Some(Err(e))) => {
                return Err(KalamLinkError::WebSocketError(format!(
                    "WebSocket error during authentication: {}",
                    e
                )));
            },
            Ok(None) => {
                return Err(KalamLinkError::WebSocketError(
                    "Connection closed before authentication completed".to_string(),
                ));
            },
            Err(_) => {
                return Err(KalamLinkError::TimeoutError(format!(
                    "Authentication timeout ({:?})",
                    auth_timeout
                )));
            },
        }
    }
}

// ── Message Parsing ─────────────────────────────────────────────────────────

/// Parse a text WebSocket message into a `ChangeEvent`.
///
/// Returns `Ok(None)` for messages that should be silently skipped
/// (e.g. AuthSuccess/AuthError which are handled during the handshake).
pub(crate) fn parse_message(text: &str) -> Result<Option<ChangeEvent>> {
    let msg: ServerMessage = serde_json::from_str(text).map_err(|e| {
        KalamLinkError::SerializationError(format!(
            "Failed to parse message as ServerMessage: {}",
            e
        ))
    })?;

    Ok(ChangeEvent::from_server_message(msg))
}

/// Parse a binary MessagePack payload into a `ChangeEvent`.
pub(crate) fn parse_message_msgpack(data: &[u8]) -> Result<Option<ChangeEvent>> {
    let msg: ServerMessage = rmp_serde::from_slice(data).map_err(|e| {
        KalamLinkError::SerializationError(format!(
            "Failed to parse msgpack as ServerMessage: {}",
            e
        ))
    })?;

    Ok(ChangeEvent::from_server_message(msg))
}

// ── Keepalive Jitter ────────────────────────────────────────────────────────

/// Spread keepalive pings across connections to avoid synchronized bursts.
///
/// Uses deterministic jitter (0-20% earlier than the base interval) derived
/// from `subscription_id` so reconnecting preserves phase, avoids
/// thundering-herd effects, and never exceeds the configured heartbeat budget.
pub(crate) fn jitter_keepalive_interval(base: Duration, subscription_id: &str) -> Duration {
    if base.is_zero() {
        return base;
    }

    let base_ms = base.as_millis() as u64;
    if base_ms <= 1 {
        return base;
    }

    let jitter_span = (base_ms / 5).max(1);
    let mut hasher = DefaultHasher::new();
    subscription_id.hash(&mut hasher);
    let hashed = hasher.finish();

    let offset = (hashed % jitter_span).saturating_add(1);
    let jittered_ms = base_ms.saturating_sub(offset).max(1);

    Duration::from_millis(jittered_ms)
}

// ── Payload Helpers ─────────────────────────────────────────────────────────

/// Decode a binary (gzip-compressed) WebSocket payload into a UTF-8 string.
pub(crate) fn decode_ws_payload(data: &[u8]) -> Result<String> {
    if data.len() > MAX_WS_BINARY_MESSAGE_BYTES {
        return Err(KalamLinkError::WebSocketError(format!(
            "Binary WebSocket message too large ({} bytes > {} bytes)",
            data.len(),
            MAX_WS_BINARY_MESSAGE_BYTES
        )));
    }

    let decompressed =
        crate::compression::decompress_gzip_with_limit(data, MAX_WS_DECOMPRESSED_MESSAGE_BYTES)
            .map_err(|e| {
                KalamLinkError::WebSocketError(format!("Failed to decompress message: {}", e))
            })?;

    String::from_utf8(decompressed).map_err(|e| {
        KalamLinkError::WebSocketError(format!("Invalid UTF-8 in decompressed message: {}", e))
    })
}

/// Send a `NextBatch` request using the negotiated serialization format.
pub(crate) async fn send_next_batch_request_with_format(
    ws_stream: &mut WebSocketStream,
    subscription_id: &str,
    last_seq_id: Option<crate::seq_id::SeqId>,
    serialization: crate::models::SerializationType,
) -> Result<()> {
    let message = ClientMessage::NextBatch {
        subscription_id: subscription_id.to_string(),
        last_seq_id,
    };
    send_client_message(ws_stream, &message, serialization).await
}

/// Encode and send a `ClientMessage` using the given serialization format.
pub(crate) async fn send_client_message(
    ws_stream: &mut WebSocketStream,
    msg: &ClientMessage,
    serialization: crate::models::SerializationType,
) -> Result<()> {
    match serialization {
        crate::models::SerializationType::Json => {
            let payload = serde_json::to_string(msg).map_err(|e| {
                KalamLinkError::WebSocketError(format!("Failed to serialize message: {}", e))
            })?;
            ws_stream.send(Message::Text(payload.into())).await.map_err(|e| {
                KalamLinkError::WebSocketError(format!("Failed to send message: {}", e))
            })
        },
        crate::models::SerializationType::MessagePack => {
            let payload = rmp_serde::to_vec_named(msg).map_err(|e| {
                KalamLinkError::WebSocketError(format!("Failed to serialize msgpack: {}", e))
            })?;
            ws_stream.send(Message::Binary(payload.into())).await.map_err(|e| {
                KalamLinkError::WebSocketError(format!("Failed to send binary message: {}", e))
            })
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthProvider;
    use crate::error::KalamLinkError;
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::header::AUTHORIZATION};

    #[test]
    fn test_ws_url_conversion() {
        assert_eq!(
            resolve_ws_url("http://localhost:3000", None, false).unwrap(),
            "ws://localhost:3000/v1/ws"
        );
        assert_eq!(
            resolve_ws_url("https://api.example.com", None, false).unwrap(),
            "wss://api.example.com/v1/ws"
        );
        assert_eq!(
            resolve_ws_url("http://localhost:3000", Some("ws://override/ws"), false).unwrap(),
            "ws://override/ws"
        );
    }

    #[test]
    fn test_ws_url_trailing_slash_stripped() {
        assert_eq!(
            resolve_ws_url("http://localhost:3000/", None, false).unwrap(),
            "ws://localhost:3000/v1/ws"
        );
    }

    #[test]
    fn test_ws_url_rejects_query_and_fragment() {
        assert!(resolve_ws_url(
            "http://localhost:3000",
            Some("wss://api.example.com/v1/ws?token=secret"),
            false
        )
        .is_err());
        assert!(resolve_ws_url(
            "http://localhost:3000",
            Some("wss://api.example.com/v1/ws#frag"),
            false
        )
        .is_err());
    }

    #[test]
    fn test_ws_url_rejects_userinfo() {
        assert!(resolve_ws_url(
            "http://localhost:3000",
            Some("wss://user:pass@api.example.com/v1/ws"),
            false
        )
        .is_err());
    }

    #[test]
    fn test_ws_url_rejects_https_downgrade() {
        assert!(resolve_ws_url(
            "https://api.example.com",
            Some("ws://api.example.com/v1/ws"),
            false
        )
        .is_err());
    }

    #[test]
    fn test_ws_url_rejects_unsupported_scheme() {
        assert!(resolve_ws_url(
            "http://localhost:3000",
            Some("ftp://api.example.com/v1/ws"),
            false
        )
        .is_err());
    }

    #[test]
    fn test_apply_ws_auth_headers_sets_bearer_header_for_jwt() {
        let mut request = "ws://localhost:3000/v1/ws".into_client_request().unwrap();

        apply_ws_auth_headers(&mut request, &AuthProvider::jwt_token("token-123".to_string()))
            .expect("jwt auth should be applied via Authorization header");

        assert_eq!(request.headers().get(AUTHORIZATION).unwrap(), "Bearer token-123");
    }

    #[test]
    fn test_apply_ws_auth_headers_rejects_basic_auth() {
        let mut request = "ws://localhost:3000/v1/ws".into_client_request().unwrap();

        let err = apply_ws_auth_headers(
            &mut request,
            &AuthProvider::basic_auth("admin".to_string(), "secret".to_string()),
        )
        .expect_err("basic auth should not be used for websocket upgrades");

        assert!(matches!(
            err,
            KalamLinkError::AuthenticationError(message)
            if message.contains("requires a JWT token")
        ));
    }

    #[test]
    fn test_keepalive_jitter_is_deterministic() {
        let base = Duration::from_secs(20);
        let a = jitter_keepalive_interval(base, "sub-a");
        let b = jitter_keepalive_interval(base, "sub-a");
        assert_eq!(a, b, "jitter must be stable for the same subscription");
    }

    #[test]
    fn test_keepalive_jitter_stays_within_bounds() {
        let base = Duration::from_secs(20);
        let jittered = jitter_keepalive_interval(base, "sub-b");
        let min = Duration::from_secs(16); // -20%
        let max = Duration::from_secs(20);
        assert!(
            jittered >= min && jittered < max,
            "jittered interval {:?} must be within [{:?}, {:?})",
            jittered,
            min,
            max
        );
    }

    #[test]
    fn test_parse_message_msgpack_server_message() {
        use crate::models::{ProtocolOptions, SerializationType, ServerMessage};

        let msg = ServerMessage::AuthSuccess {
            user_id: "user-1".to_string(),
            role: "admin".to_string(),
            protocol: ProtocolOptions {
                serialization: SerializationType::MessagePack,
                compression: crate::models::CompressionType::Gzip,
            },
        };
        let bytes = rmp_serde::to_vec_named(&msg).unwrap();
        let result = parse_message_msgpack(&bytes).unwrap();
        // AuthSuccess is not a ChangeEvent, so parse_message_msgpack should return None
        // (it only handles subscription events)
        assert!(result.is_none());
    }

    #[test]
    fn test_msgpack_client_message_roundtrip() {
        use crate::models::{ClientMessage, SubscriptionOptions, SubscriptionRequest};

        let msg = ClientMessage::Subscribe {
            subscription: SubscriptionRequest {
                id: "sub-1".to_string(),
                sql: "SELECT * FROM test".to_string(),
                options: Some(SubscriptionOptions::default()),
            },
        };
        let bytes = rmp_serde::to_vec_named(&msg).unwrap();
        let parsed: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
        match parsed {
            ClientMessage::Subscribe { subscription } => {
                assert_eq!(subscription.id, "sub-1");
                assert_eq!(subscription.sql, "SELECT * FROM test");
            },
            _ => panic!("Expected Subscribe"),
        }
    }
}
