//! WebSocket handlers for live query subscriptions
//!
//! ## Endpoints
//! - GET /v1/ws - Establish WebSocket connection for live queries
//!
//! ## Connection Lifecycle
//! 1. Client connects to /v1/ws (unauthenticated initially)
//! 2. Client sends Authenticate message within 3 seconds
//! 3. Server validates credentials and marks connection as authenticated
//! 4. Client can then subscribe to live queries
//! 5. Server pushes notifications when data changes
//!
//! ## Message Types
//! - Authenticate: JWT/token-based authentication
//! - Subscribe: Subscribe to a live query
//! - Unsubscribe: Unsubscribe from a live query
//! - NextBatch: Request next batch of results
//!
//! ## Security Features
//! - Origin header validation (configurable)
//! - Message size limits (configurable, default 64KB)
//! - Rate limiting per connection
//! - Authentication timeout (3 seconds)
//! - Heartbeat monitoring

pub mod events;
pub mod models;

mod compression;
mod context;
mod handler;
mod messages;
mod protocol;
mod runtime;

pub(crate) use handler::websocket_handler;

#[cfg(test)]
mod tests {
    use super::websocket_handler;
    use crate::limiter::RateLimiter;
    use actix_web::{web, App, HttpServer};
    use futures_util::StreamExt;
    use kalamdb_auth::{create_and_sign_token, CoreUsersRepo, UserRepository};
    use kalamdb_commons::models::{KalamCellValue, UserId};
    use kalamdb_commons::websocket::{ChangeType, SharedChangePayload, WireNotification};
    use kalamdb_commons::Role;
    use kalamdb_core::app_context::AppContext;
    use kalamdb_core::test_helpers::test_app_context_simple;
    use kalamdb_live::ConnectionsManager;
    use kalamdb_system::providers::storages::models::StorageMode;
    use kalamdb_system::{AuthType, User};
    use std::collections::HashMap;
    use std::net::TcpListener;
    use std::sync::Arc;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    struct WsTestContext {
        server: actix_web::dev::ServerHandle,
        base_url: String,
        registry: Arc<ConnectionsManager>,
        user_id: UserId,
        token: String,
    }

    fn test_user() -> User {
        let now = chrono::Utc::now().timestamp_millis();
        User {
            user_id: UserId::new("ws-test-user"),
            password_hash: "$2b$12$hash".to_string(),
            role: Role::Dba,
            email: Some("ws-test@example.com".to_string()),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(kalamdb_commons::StorageId::new("local")),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: now,
            updated_at: now,
            last_seen: None,
            deleted_at: None,
        }
    }

    async fn start_ws_test_server() -> WsTestContext {
        let app_context: Arc<AppContext> = test_app_context_simple();
        let user = test_user();
        app_context.system_tables().users().create_user(user.clone()).unwrap();

        let secret = app_context.config().auth.jwt_secret.clone();
        let (token, _) =
            create_and_sign_token(&user.user_id, &user.role, user.email.as_deref(), None, &secret)
                .unwrap();

        let rate_limiter = Arc::new(RateLimiter::new());
        let live_query_manager = app_context.live_query_manager();
        let registry = app_context.connection_registry();
        let user_repo: Arc<dyn UserRepository> =
            Arc::new(CoreUsersRepo::new(app_context.system_tables().users()));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let app_context_data = Arc::clone(&app_context);
        let rate_limiter_data = Arc::clone(&rate_limiter);
        let live_query_manager_data = Arc::clone(&live_query_manager);
        let registry_data = Arc::clone(&registry);
        let user_repo_data = Arc::clone(&user_repo);

        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(Arc::clone(&app_context_data)))
                .app_data(web::Data::new(Arc::clone(&rate_limiter_data)))
                .app_data(web::Data::new(Arc::clone(&live_query_manager_data)))
                .app_data(web::Data::new(Arc::clone(&user_repo_data)))
                .app_data(web::Data::new(Arc::clone(&registry_data)))
                .service(websocket_handler)
        })
        .listen(listener)
        .unwrap()
        .run();

        let handle = server.handle();
        actix_web::rt::spawn(server);

        WsTestContext {
            server: handle,
            base_url: format!("ws://{}/ws", addr),
            registry,
            user_id: user.user_id,
            token,
        }
    }

    fn make_notification(subscription_id: &str) -> WireNotification {
        let mut row = HashMap::new();
        row.insert("id".to_string(), KalamCellValue::int(7));
        row.insert("body".to_string(), KalamCellValue::text("hello"));

        WireNotification {
            subscription_id: Arc::from(subscription_id),
            payload: Arc::new(SharedChangePayload::new(ChangeType::Insert, Some(vec![row]), None)),
        }
    }

    async fn connect(
        base_url: &str,
        token: &str,
        query: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let mut request = format!("{}{}", base_url, query).into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {}", token).parse().unwrap());

        let (stream, _) = connect_async(request).await.unwrap();
        stream
    }

    async fn expect_auth_success(
        stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) {
        let frame = stream.next().await.unwrap().unwrap();
        let Message::Text(text) = frame else {
            panic!("expected auth success text frame, got {frame:?}");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["type"], "auth_success");
    }

    #[actix_rt::test]
    async fn websocket_handler_sends_json_notification_frames_for_json_connections() {
        let ctx = start_ws_test_server().await;
        let mut stream = connect(&ctx.base_url, &ctx.token, "?compress=false").await;

        expect_auth_success(&mut stream).await;
        assert!(ctx
            .registry
            .notify_connections_for_user(&ctx.user_id, make_notification("sub-json")));

        let frame = stream.next().await.unwrap().unwrap();
        let Message::Text(text) = frame else {
            panic!("expected JSON text frame, got {frame:?}");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["type"], "change");
        assert_eq!(value["subscription_id"], "sub-json");
        assert_eq!(value["change_type"], "insert");
        assert_eq!(value["rows"][0]["body"], "hello");

        let _ = stream.close(None).await;
        ctx.server.stop(true).await;
    }

    #[actix_rt::test]
    async fn websocket_handler_sends_binary_notification_frames_for_msgpack_connections() {
        let ctx = start_ws_test_server().await;
        let mut stream =
            connect(&ctx.base_url, &ctx.token, "?serialization=msgpack&compress=false").await;

        expect_auth_success(&mut stream).await;
        assert!(ctx
            .registry
            .notify_connections_for_user(&ctx.user_id, make_notification("sub-msgpack")));

        let frame = stream.next().await.unwrap().unwrap();
        let Message::Binary(bytes) = frame else {
            panic!("expected MessagePack binary frame, got {frame:?}");
        };
        let value: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(value["type"], "change");
        assert_eq!(value["subscription_id"], "sub-msgpack");
        assert_eq!(value["change_type"], "insert");
        assert_eq!(value["rows"][0]["body"], "hello");

        let _ = stream.close(None).await;
        ctx.server.stop(true).await;
    }
}
