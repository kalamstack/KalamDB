use actix_web::{HttpRequest, HttpResponse};
use actix_ws::ProtocolError;
use kalamdb_auth::{authenticate, AuthRequest, UserRepository};
use kalamdb_commons::websocket::{CompressionType, ProtocolOptions, SerializationType};
use std::sync::Arc;

use super::context::UpgradeAuth;

pub(super) fn parse_protocol_from_query(query: &str) -> ProtocolOptions {
    let mut protocol = ProtocolOptions::default();
    for kv in query.split('&') {
        if let Some((key, value)) = kv.split_once('=') {
            match key {
                "serialization" => {
                    if value.eq_ignore_ascii_case("msgpack") {
                        protocol.serialization = SerializationType::MessagePack;
                    }
                },
                "compression" => {
                    if value.eq_ignore_ascii_case("none") {
                        protocol.compression = CompressionType::None;
                    }
                },
                _ => {},
            }
        }
    }
    protocol
}

pub(super) fn compression_enabled_from_query(req: &HttpRequest) -> bool {
    !req.query_string()
        .split('&')
        .any(|kv| kv.eq_ignore_ascii_case("compress=false"))
}

pub(super) fn validate_origin(
    req: &HttpRequest,
    app_context: &kalamdb_core::app_context::AppContext,
) -> Result<(), HttpResponse> {
    let config = app_context.config();
    let allowed_origins = &config.security.cors.allowed_origins;

    if allowed_origins.is_empty() || allowed_origins.contains(&"*".to_string()) {
        return Ok(());
    }

    if let Some(origin) = req.headers().get("Origin") {
        if let Ok(origin_str) = origin.to_str() {
            if allowed_origins.iter().any(|allowed| allowed == origin_str) {
                return Ok(());
            }
            log::warn!("WebSocket connection rejected: invalid origin '{}'", origin_str);
            return Err(HttpResponse::Forbidden().body("Origin not allowed"));
        }
    }

    if config.security.strict_ws_origin_check {
        log::warn!("WebSocket connection rejected: missing Origin header");
        return Err(HttpResponse::Forbidden().body("Origin header required"));
    }

    Ok(())
}

pub(super) async fn authenticate_upgrade(
    req: &HttpRequest,
    user_repo: &Arc<dyn UserRepository>,
) -> Result<Option<UpgradeAuth>, HttpResponse> {
    let Some(auth_header) = req.headers().get("Authorization") else {
        return Ok(None);
    };

    let Ok(auth_str) = auth_header.to_str() else {
        return Ok(None);
    };

    let Some(token) = auth_str.strip_prefix("Bearer ") else {
        return Ok(None);
    };

    let client_ip_for_auth = kalamdb_auth::extract_client_ip_secure(req);
    let auth_request = AuthRequest::Jwt {
        token: token.to_string(),
    };

    match authenticate(auth_request, &client_ip_for_auth, user_repo).await {
        Ok(result) => Ok(Some(UpgradeAuth {
            user_id: result.user.user_id,
            role: result.user.role,
            protocol: parse_protocol_from_query(req.query_string()),
        })),
        Err(_) => {
            log::warn!("WebSocket upgrade rejected: invalid Bearer token");
            Err(HttpResponse::Unauthorized().body("Invalid token"))
        },
    }
}

pub(super) fn is_expected_ws_disconnect(error: &ProtocolError) -> bool {
    match error {
        ProtocolError::Io(io_err) => {
            use std::io::ErrorKind::*;
            if matches!(
                io_err.kind(),
                BrokenPipe | ConnectionReset | ConnectionAborted | UnexpectedEof
            ) {
                return true;
            }

            let msg = io_err.to_string().to_ascii_lowercase();
            msg.contains("eof")
                || msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection aborted")
                || msg.contains("payload reached eof")
                || msg.contains("connection closed")
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_protocol_from_query, validate_origin};
    use actix_web::{http::StatusCode, test::TestRequest};
    use kalamdb_commons::websocket::{CompressionType, SerializationType};
    use kalamdb_commons::NodeId;
    use kalamdb_configs::ServerConfig;
    use kalamdb_core::app_context::AppContext;
    use kalamdb_store::test_utils::InMemoryBackend;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_app_context_with_origin_policy(
        cors_allowed_origins: Vec<String>,
        strict_ws_origin_check: bool,
    ) -> Arc<AppContext> {
        let mut config = ServerConfig::default();
        config.security.cors.allowed_origins = cors_allowed_origins;
        config.security.strict_ws_origin_check = strict_ws_origin_check;

        AppContext::init_test(
            Arc::new(InMemoryBackend::new()),
            NodeId::new(91),
            format!("/tmp/kalamdb-ws-origin-{}", Uuid::new_v4()),
            config,
        )
    }

    #[test]
    fn parse_protocol_defaults_when_empty() {
        let proto = parse_protocol_from_query("");
        assert_eq!(proto.serialization, SerializationType::Json);
        assert_eq!(proto.compression, CompressionType::Gzip);
    }

    #[test]
    fn parse_protocol_msgpack_serialization() {
        let proto = parse_protocol_from_query("serialization=msgpack");
        assert_eq!(proto.serialization, SerializationType::MessagePack);
        assert_eq!(proto.compression, CompressionType::Gzip);
    }

    #[test]
    fn parse_protocol_compression_none() {
        let proto = parse_protocol_from_query("compression=none");
        assert_eq!(proto.serialization, SerializationType::Json);
        assert_eq!(proto.compression, CompressionType::None);
    }

    #[test]
    fn parse_protocol_both_options() {
        let proto = parse_protocol_from_query("serialization=msgpack&compression=none");
        assert_eq!(proto.serialization, SerializationType::MessagePack);
        assert_eq!(proto.compression, CompressionType::None);
    }

    #[test]
    fn parse_protocol_mixed_with_compress_false() {
        let proto = parse_protocol_from_query("compress=false&serialization=msgpack");
        assert_eq!(proto.serialization, SerializationType::MessagePack);
        assert_eq!(proto.compression, CompressionType::Gzip);
    }

    #[test]
    fn parse_protocol_case_insensitive() {
        let proto = parse_protocol_from_query("serialization=MSGPACK&compression=NONE");
        assert_eq!(proto.serialization, SerializationType::MessagePack);
        assert_eq!(proto.compression, CompressionType::None);
    }

    #[test]
    fn parse_protocol_unknown_values_keep_defaults() {
        let proto = parse_protocol_from_query("serialization=avro&compression=lz4");
        assert_eq!(proto.serialization, SerializationType::Json);
        assert_eq!(proto.compression, CompressionType::Gzip);
    }

    #[actix_rt::test]
    async fn validate_origin_rejects_unlisted_origin() {
        let app_context = test_app_context_with_origin_policy(
            vec!["https://admin.example.com".to_string()],
            false,
        );
        let request = TestRequest::default()
            .insert_header(("Origin", "https://evil.example.com"))
            .to_http_request();

        let response = validate_origin(&request, app_context.as_ref())
            .expect_err("unexpectedly allowed an unlisted origin");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[actix_rt::test]
    async fn validate_origin_rejects_missing_origin_when_strict() {
        let app_context = test_app_context_with_origin_policy(
            vec!["https://admin.example.com".to_string()],
            true,
        );
        let request = TestRequest::default().to_http_request();

        let response = validate_origin(&request, app_context.as_ref())
            .expect_err("strict origin checking should require Origin header");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[actix_rt::test]
    async fn validate_origin_allows_configured_cors_origin() {
        let app_context =
            test_app_context_with_origin_policy(vec!["https://app.example.com".to_string()], true);
        let request = TestRequest::default()
            .insert_header(("Origin", "https://app.example.com"))
            .to_http_request();

        validate_origin(&request, app_context.as_ref())
            .expect("configured CORS origin should also be allowed for WebSocket upgrades");
    }
}
