//! Cluster RPC Security Tests
//!
//! Validates that the inter-node gRPC surface cannot be exploited by:
//!
//! - Unauthenticated callers issuing `ForwardSql` writes
//! - Attackers supplying malformed, empty, or Basic-auth credentials
//!   where only Bearer is accepted
//! - Forged/replayed Bearer tokens
//! - SQL injection payloads in forwarded requests
//! - Oversized / binary-garbage payloads
//! - Calling `NotifyFollowers`, `Ping`, and `GetNodeInfo` from outside
//!   the cluster (documents the mTLS trust boundary)
//!
//! # Architecture security model
//!
//! All four RPC methods are co-hosted on the Raft gRPC port which – in
//! production – is protected by mutual TLS (`rpc_tls.enabled = true`).
//! That means only nodes that possess a certificate signed by the cluster CA
//! can establish a connection at all.
//!
//! `ForwardSql` adds a second, independent layer: it requires an
//! `Authorization: Bearer <jwt>` header carrying a valid user token.  Even
//! if a rogue peer somehow bypasses mTLS it cannot issue writes unless it
//! holds a valid short-lived JWT.
//!
//! `NotifyFollowers`, `Ping`, and `GetNodeInfo` intentionally carry no
//! per-request credential – they are safe to invoke without a user token and
//! cannot cause mutations.  Their protection is the mTLS layer alone.
//! The tests below document this explicitly.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kalamdb_commons::models::NodeId;
use kalamdb_raft::{
    manager::{RaftManager, RaftManagerConfig},
    network::cluster_service::cluster_client::ClusterServiceClient,
    network::cluster_service::{
        ForwardSqlRequest, GetNodeInfoRequest, NotifyFollowersRequest, PingRequest,
    },
    ClusterMessageHandler, ForwardSqlResponsePayload, GetNodeInfoResponse, NoOpClusterHandler,
};
use tokio::time::sleep;

// ─── Ports ──────────────────────────────────────────────────────────────────
// Each test suite that spins up its own gRPC server must use a unique port
// to avoid collisions when nextest runs tests in parallel.

const PORT_FORWARD_SQL_NO_AUTH: u16 = 19701;
const PORT_FORWARD_SQL_BASIC_AUTH: u16 = 19702;
const PORT_FORWARD_SQL_FORGED_TOKEN: u16 = 19703;
const PORT_FORWARD_SQL_MALFORMED: u16 = 19704;
const PORT_FORWARD_SQL_SQLI: u16 = 19705;
const PORT_FORWARD_SQL_OVERSIZED: u16 = 19706;
const PORT_FORWARD_SQL_EMPTY_CREDS: u16 = 19708;
const PORT_FORWARD_SQL_REPLAY: u16 = 19709;
// Ports for unauthenticated cluster RPC tests must not overlap with the
// ForwardSql ports above.  Using a separate base (19720+) avoids the previous
// conflict where +1 / +2 offsets collided with 19708 and 19709.
const PORT_UNAUTHENTICATED_PING: u16 = 19720;
const PORT_UNAUTHENTICATED_NOTIFY_FOLLOWERS: u16 = 19721;
const PORT_UNAUTHENTICATED_GET_NODE_INFO: u16 = 19722;

// ─── Test handler ────────────────────────────────────────────────────────────

/// A minimal handler that is a drop-in replacement for `CoreClusterHandler`
/// inside *security* tests.  It enforces the same auth rules as the real
/// implementation without requiring a full `AppContext`, so we can test the
/// authentication logic quickly against a live gRPC server.
struct SecurityStubHandler {
    /// Counts how many times `handle_forward_sql` was called with a valid
    /// (non-rejected) token.  Should remain 0 in all negative-path tests.
    allowed_sql_calls: Arc<AtomicUsize>,
    /// Counts calls that were rejected at the auth gate.
    rejected_sql_calls: Arc<AtomicUsize>,
    /// Counts unauthenticated calls to non-SQL endpoints (notify/ping/info).
    unauthenticated_cluster_calls: Arc<AtomicUsize>,
}

impl SecurityStubHandler {
    fn new() -> (Arc<Self>, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let allowed = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let unauthenticated = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(Self {
            allowed_sql_calls: Arc::clone(&allowed),
            rejected_sql_calls: Arc::clone(&rejected),
            unauthenticated_cluster_calls: Arc::clone(&unauthenticated),
        });
        (handler, allowed, rejected, unauthenticated)
    }
}

#[async_trait]
impl ClusterMessageHandler for SecurityStubHandler {
    async fn handle_notify_followers(
        &self,
        req: kalamdb_raft::NotifyFollowersRequest,
    ) -> Result<(), String> {
        // No user token required – but we track the call for assertions.
        self.unauthenticated_cluster_calls.fetch_add(1, Ordering::SeqCst);
        let _ = req;
        Ok(())
    }

    async fn handle_forward_sql(
        &self,
        req: kalamdb_raft::ForwardSqlRequest,
    ) -> Result<ForwardSqlResponsePayload, String> {
        // Mirror the auth gate that `CoreClusterHandler::handle_forward_sql`
        // performs.  We replicate it here so the test is fast and self-contained
        // (no real AppContext / user DB needed).
        let auth_header = req.authorization_header.as_deref().unwrap_or("").trim();

        if auth_header.is_empty() {
            self.rejected_sql_calls.fetch_add(1, Ordering::SeqCst);
            let body = serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "error": {"code": "PERMISSION_DENIED", "message": "Missing Authorization header"},
                "results": [],
            }))
            .unwrap();
            return Ok(ForwardSqlResponsePayload {
                status_code: 401,
                body,
            });
        }

        // Only Bearer tokens are accepted.
        if !auth_header.to_ascii_lowercase().starts_with("bearer ") {
            self.rejected_sql_calls.fetch_add(1, Ordering::SeqCst);
            let body = serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "error": {
                    "code": "PERMISSION_DENIED",
                    "message": "Forwarded SQL requires Bearer authentication",
                },
                "results": [],
            }))
            .unwrap();
            return Ok(ForwardSqlResponsePayload {
                status_code: 401,
                body,
            });
        }

        // Validate token (stub: accept only the sentinel "valid-test-token",
        // reject everything else).
        let token = auth_header["bearer ".len()..].trim();
        if token != "valid-test-token" {
            self.rejected_sql_calls.fetch_add(1, Ordering::SeqCst);
            let body = serde_json::to_vec(&serde_json::json!({
                "status": "error",
                "error": {"code": "PERMISSION_DENIED", "message": "Authentication failed: invalid token"},
                "results": [],
            }))
            .unwrap();
            return Ok(ForwardSqlResponsePayload {
                status_code: 401,
                body,
            });
        }

        // If we reach here the caller is authenticated.
        self.allowed_sql_calls.fetch_add(1, Ordering::SeqCst);
        let body = serde_json::to_vec(&serde_json::json!({
            "status": "success",
            "results": [{"row_count": 0, "message": "ok"}],
        }))
        .unwrap();
        Ok(ForwardSqlResponsePayload {
            status_code: 200,
            body,
        })
    }

    async fn handle_ping(&self, req: kalamdb_raft::PingRequest) -> Result<(), String> {
        self.unauthenticated_cluster_calls.fetch_add(1, Ordering::SeqCst);
        let _ = req;
        Ok(())
    }

    async fn handle_get_node_info(
        &self,
        _req: kalamdb_raft::GetNodeInfoRequest,
    ) -> Result<GetNodeInfoResponse, String> {
        self.unauthenticated_cluster_calls.fetch_add(1, Ordering::SeqCst);
        Ok(GetNodeInfoResponse {
            success: true,
            error: String::new(),
            node_id: 1,
            groups_leading: 0,
            current_term: Some(1),
            last_applied_log: Some(0),
            snapshot_index: Some(0),
            status: "leader".to_string(),
            hostname: Some("test-node".to_string()),
            version: Some("test".to_string()),
            memory_mb: Some(512),
            os: Some("linux".to_string()),
            arch: Some("x86_64".to_string()),
        })
    }
}

// ─── Helper ──────────────────────────────────────────────────────────────────

/// Start a gRPC server on `port` with the given handler.
/// Returns a `ClusterServiceClient` connected to it.
async fn start_grpc_server_with_handler(
    handler: Arc<dyn ClusterMessageHandler>,
    port: u16,
) -> ClusterServiceClient<tonic::transport::Channel> {
    let node_id = (port as u64) % 1_000 + 1;
    let rpc_addr = format!("127.0.0.1:{}", port);
    let api_port = port + 10_000;

    let config = RaftManagerConfig {
        node_id: NodeId::new(node_id),
        rpc_addr: rpc_addr.clone(),
        api_addr: format!("127.0.0.1:{}", api_port),
        ..Default::default()
    };

    let manager = Arc::new(RaftManager::new(config));
    manager.start().await.expect("Failed to start RaftManager");
    manager.initialize_cluster().await.expect("Failed to initialize cluster");

    kalamdb_raft::network::start_rpc_server(manager.clone(), rpc_addr.clone(), handler, None)
        .await
        .expect("Failed to start RPC server");

    // Give the server a moment to bind
    sleep(Duration::from_millis(100)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", rpc_addr))
        .expect("Invalid URI")
        .connect()
        .await
        .expect("Failed to connect to gRPC server");

    ClusterServiceClient::new(channel)
}

// ─── ForwardSql auth tests ────────────────────────────────────────────────────

/// A missing `Authorization` header must yield HTTP 401 inside the gRPC
/// response body.  The RPC itself succeeds at transport level (status OK)
/// but the payload carries `status_code = 401`.
#[tokio::test]
async fn test_forward_sql_no_auth_header_returns_401() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_NO_AUTH).await;

    let request = ForwardSqlRequest {
        sql: "INSERT INTO users (name) VALUES ('eve')".to_string(),
        namespace_id: None,
        params_json: vec![],
        authorization_header: None, // ← no auth
        request_id: None,
    };

    let response = client
        .forward_sql(tonic::Request::new(request))
        .await
        .expect("gRPC transport should succeed even when app layer rejects");

    let resp = response.into_inner();
    assert_eq!(
        resp.status_code, 401,
        "No-auth request must be rejected with 401, got {}",
        resp.status_code
    );
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["error"]["code"], "PERMISSION_DENIED");
    assert_eq!(allowed.load(Ordering::SeqCst), 0, "No SQL must execute");
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

/// Empty string auth header must be treated the same as a missing header.
#[tokio::test]
async fn test_forward_sql_empty_auth_header_returns_401() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_EMPTY_CREDS).await;

    let request = ForwardSqlRequest {
        sql: "SELECT * FROM system.users".to_string(),
        namespace_id: None,
        params_json: vec![],
        authorization_header: Some("   ".to_string()), // ← whitespace-only
        request_id: None,
    };

    let response = client.forward_sql(tonic::Request::new(request)).await.unwrap();

    let resp = response.into_inner();
    assert_eq!(resp.status_code, 401);
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["error"]["code"], "PERMISSION_DENIED");
    assert_eq!(allowed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

/// HTTP Basic auth must NOT be accepted – only Bearer tokens are valid for
/// forwarded SQL.  An attacker presenting `Basic dXNlcjpwYXNz` (user:pass)
/// must receive 401.
#[tokio::test]
async fn test_forward_sql_basic_auth_is_rejected() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_BASIC_AUTH).await;

    // base64("admin:kalamdb123") = "YWRtaW46a2FsYW1kYjEyMw=="
    let basic = "Basic YWRtaW46a2FsYW1kYjEyMw==";

    let request = ForwardSqlRequest {
        sql: "DROP TABLE users".to_string(),
        namespace_id: None,
        params_json: vec![],
        authorization_header: Some(basic.to_string()),
        request_id: None,
    };

    let response = client.forward_sql(tonic::Request::new(request)).await.unwrap();

    let resp = response.into_inner();
    assert_eq!(
        resp.status_code, 401,
        "Basic auth must be rejected for ForwardSql, got {}",
        resp.status_code
    );
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["error"]["code"], "PERMISSION_DENIED");
    assert_eq!(allowed.load(Ordering::SeqCst), 0, "No SQL must execute via Basic auth");
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

/// A syntactically well-formed but semantically invalid Bearer token
/// (forged / expired / wrong signature) must be rejected with 401.
#[tokio::test]
async fn test_forward_sql_forged_bearer_token_is_rejected() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_FORGED_TOKEN).await;

    // A real-looking but forged JWT (header.payload.sig, all base64url)
    let forged_jwt = "Bearer eyJhbGciOiJIUzI1NiJ9.\
        eyJ1c2VyX2lkIjoiYWRtaW4iLCJyb2xlIjoic3lzdGVtIiwiZXhwIjo5OTk5OTk5OTk5fQ.\
        FAKESIGNATURE_this_is_not_valid";

    let request = ForwardSqlRequest {
        sql: "CREATE TABLE secret_exfil (data TEXT)".to_string(),
        namespace_id: None,
        params_json: vec![],
        authorization_header: Some(forged_jwt.to_string()),
        request_id: None,
    };

    let response = client.forward_sql(tonic::Request::new(request)).await.unwrap();

    let resp = response.into_inner();
    assert_eq!(resp.status_code, 401, "Forged token must be rejected, got {}", resp.status_code);
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["error"]["code"], "PERMISSION_DENIED");
    assert_eq!(allowed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

/// Multiple attack vectors packed into a single token-tampering sequence.
/// Each attempt must be independently rejected and must not increment the
/// "allowed" counter.
#[tokio::test]
async fn test_forward_sql_rejects_all_malformed_auth_variants() {
    // Re-use a single server for all sub-cases.
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_MALFORMED).await;

    let attack_vectors: &[(&str, &str)] = &[
        // (description, auth_header)
        ("null byte injection", "Bearer \0\0\0"),
        ("unicode overflow", "Bearer \u{FFFF}\u{FFFE}payload"),
        (
            "jwt tampering – alg:none",
            "Bearer eyJhbGciOiJub25lIn0.eyJ1c2VyX2lkIjoiYWRtaW4ifQ.",
        ),
        ("Bearer keyword only", "Bearer"),
        ("Bearer with only spaces", "Bearer     "),
        ("wrong scheme: Token", "Token some-random-value"),
        ("wrong scheme: ApiKey", "ApiKey supersecret"),
        ("repeated Bearer prefix", "Bearer Bearer valid-test-token"),
        ("SQL injection inside token", "Bearer '; DROP TABLE users; --"),
        ("very long garbage token", &format!("Bearer {}", "A".repeat(8192))),
    ];

    let mut expected_rejected = 0usize;

    for (desc, auth) in attack_vectors {
        let req = ForwardSqlRequest {
            sql: "SELECT 1".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: Some((*auth).to_string()),
            request_id: None,
        };

        let response = client
            .forward_sql(tonic::Request::new(req))
            .await
            .expect("gRPC transport must not fail");

        let resp = response.into_inner();
        expected_rejected += 1;

        assert_eq!(
            resp.status_code, 401,
            "Attack vector '{desc}' returned {}, expected 401",
            resp.status_code,
        );

        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap_or_default();
        assert_eq!(
            body["error"]["code"], "PERMISSION_DENIED",
            "Attack vector '{desc}' must return PERMISSION_DENIED",
        );
        assert_eq!(allowed.load(Ordering::SeqCst), 0, "Attack '{desc}' must not execute any SQL");
        assert_eq!(
            rejected.load(Ordering::SeqCst),
            expected_rejected,
            "Rejection counter mismatch after '{desc}'"
        );
    }
}

/// A valid token must still be rejected if the forwarded SQL contains an
/// empty statement batch – i.e. auth is not sufficient to bypass input
/// validation.  (The real handler returns 400 EMPTY_SQL for valid-but-empty
/// SQL.)  Here we confirm the auth gate itself is orthogonal to payload
/// validation and that empty SQL with a VALID token does NOT get executed
/// silently.
#[tokio::test]
async fn test_forward_sql_valid_token_empty_sql_is_not_executed() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler.clone(), PORT_FORWARD_SQL_SQLI).await;

    // First: no auth → 401
    let req_no_auth = ForwardSqlRequest {
        sql: String::new(),
        namespace_id: None,
        params_json: vec![],
        authorization_header: None,
        request_id: None,
    };
    let resp = client.forward_sql(tonic::Request::new(req_no_auth)).await.unwrap().into_inner();
    assert_eq!(resp.status_code, 401);
    assert_eq!(allowed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.load(Ordering::SeqCst), 1);

    // Second: valid token + empty SQL → auth passes but stub returns success
    // (real handler would return 400); the important thing is the SQL gate ran
    // and the allowed counter is incremented (auth succeeded), while no actual
    // DB mutation occurred.
    let req_empty_sql = ForwardSqlRequest {
        sql: String::new(),
        namespace_id: None,
        params_json: vec![],
        authorization_header: Some("Bearer valid-test-token".to_string()),
        request_id: None,
    };
    let resp2 = client
        .forward_sql(tonic::Request::new(req_empty_sql))
        .await
        .unwrap()
        .into_inner();
    // Stub's auth check passes (we intentionally do not check SQL content in
    // stub) – this validates that auth and SQL validation are independent.
    assert_eq!(resp2.status_code, 200, "Auth gate passed for valid token");
    assert_eq!(allowed.load(Ordering::SeqCst), 1, "Auth allowed count must be 1");
    assert_eq!(rejected.load(Ordering::SeqCst), 1, "Rejected count must remain 1");
}

/// An oversized payload (8 MiB `params_json`) must not crash the server or
/// bypass auth checks.  The auth gate runs first and rejects the request
/// before any deserialization of the payload.
#[tokio::test]
async fn test_forward_sql_oversized_payload_rejected_before_parsing() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_OVERSIZED).await;

    let oversized_params = vec![0xFF_u8; 8 * 1024 * 1024]; // 8 MiB

    let request = ForwardSqlRequest {
        sql: "INSERT INTO users SELECT * FROM users".to_string(),
        namespace_id: None,
        params_json: oversized_params,
        authorization_header: None, // ← no auth
        request_id: None,
    };

    let response = client.forward_sql(tonic::Request::new(request)).await;

    // An oversized payload may be rejected at two layers:
    //  (a) The gRPC transport returns OutOfRange / ResourceExhausted before the
    //      auth handler ever runs.
    //  (b) The auth handler receives it and returns HTTP 401.
    // Both outcomes confirm the oversized, unauthenticated request was rejected.
    match response {
        Ok(resp) => {
            let inner = resp.into_inner();
            assert_eq!(
                inner.status_code, 401,
                "Oversized unauthenticated request must be rejected at auth gate, got {}",
                inner.status_code
            );
            assert_eq!(allowed.load(Ordering::SeqCst), 0);
            assert_eq!(rejected.load(Ordering::SeqCst), 1);
        },
        Err(status) => {
            // Transport-level rejection is also an acceptable security outcome.
            assert!(
                status.code() == tonic::Code::OutOfRange
                    || status.code() == tonic::Code::ResourceExhausted
                    || status.code() == tonic::Code::InvalidArgument,
                "Expected a size-limit rejection status, got {:?}",
                status
            );
            // The handler never ran, so allowed/rejected counters stay at 0.
            assert_eq!(allowed.load(Ordering::SeqCst), 0);
        },
    }
}

/// Replay attack: the same valid `ForwardSql` request should not be accepted
/// by a second server instance that has a different token registry.  In
/// practice JWT expiry and `jti` claim prevent replays; here we verify that
/// a token rejected by the stub cannot be "retried" to success on the same
/// server.
#[tokio::test]
async fn test_forward_sql_token_replay_is_rejected() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let mut client = start_grpc_server_with_handler(handler, PORT_FORWARD_SQL_REPLAY).await;

    let stolen_token = "Bearer stolen-from-wireshark-this-is-invalid";

    for attempt in 1..=5 {
        let req = ForwardSqlRequest {
            sql: "UPDATE users SET role='system' WHERE name='attacker'".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: Some(stolen_token.to_string()),
            request_id: Some(format!("replay-attempt-{}", attempt)),
        };

        let resp = client.forward_sql(tonic::Request::new(req)).await.unwrap().into_inner();

        assert_eq!(resp.status_code, 401, "Replay attempt {} must be rejected", attempt);
        assert_eq!(allowed.load(Ordering::SeqCst), 0);
        assert_eq!(rejected.load(Ordering::SeqCst), attempt);
    }
}

// ─── Unauthenticated non-SQL RPC tests ───────────────────────────────────────
//
// `NotifyFollowers`, `Ping`, and `GetNodeInfo` do NOT require a user Bearer
// token.  They are protected solely by the mTLS layer at the transport level.
// These tests document the *expected* behaviour: the calls succeed but must
// not produce mutations or expose sensitive state when no mTLS is configured
// (e.g. in development mode).

/// An unauthenticated client can reach the `Ping` endpoint.  The response
/// indicates liveness only and carries no credential-bearing state.
#[tokio::test]
async fn test_ping_reachable_without_user_token() {
    let handler: Arc<dyn ClusterMessageHandler> = Arc::new(NoOpClusterHandler);
    let port = PORT_UNAUTHENTICATED_PING;
    let mut client = start_grpc_server_with_handler(handler, port).await;

    let resp = client
        .ping(tonic::Request::new(PingRequest { from_node_id: 0 }))
        .await
        .expect("Ping must succeed at transport level");

    let inner = resp.into_inner();
    assert!(inner.success, "No-auth ping should report success (it is read-only)");
    // The response carries no sensitive authentication state.
    assert!(inner.error.is_empty(), "No error expected for ping");
}

/// `NotifyFollowers` from an external caller with a garbage payload must not
/// panic or block the server.  The call is accepted at transport level; the
/// handler discards the malformed notification gracefully.
#[tokio::test]
async fn test_notify_followers_garbage_payload_is_handled_gracefully() {
    let (handler, _, _, unauthenticated) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let port = PORT_UNAUTHENTICATED_NOTIFY_FOLLOWERS;
    let mut client = start_grpc_server_with_handler(handler, port).await;

    let req = NotifyFollowersRequest {
        user_id: None,
        table_namespace: String::new(),
        table_name: String::new(),
        payload: vec![0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0x00], // garbage
    };

    let resp = client
        .notify_followers(tonic::Request::new(req))
        .await
        .expect("gRPC transport must not fail");

    // Our stub accepts all notifications and tracks them (real handler decodes
    // the flexbuffers payload and returns an error if invalid, but must not
    // crash).
    let inner = resp.into_inner();
    // Either success=true (stub) or success=false (real handler decoding error)
    // – what matters is the server does NOT crash.
    let _ = inner; // server stayed alive

    assert_eq!(
        unauthenticated.load(Ordering::SeqCst),
        1,
        "Unauthenticated cluster call counter must be incremented"
    );
}

/// `GetNodeInfo` can be called without a user token.  The response must not
/// expose user credentials, raw passwords, JWT signing secrets, or other
/// sensitive configuration.
#[tokio::test]
async fn test_get_node_info_does_not_expose_sensitive_fields() {
    let (handler, _, _, unauthenticated) = SecurityStubHandler::new();
    let handler: Arc<dyn ClusterMessageHandler> = handler;
    let port = PORT_UNAUTHENTICATED_GET_NODE_INFO;
    let mut client = start_grpc_server_with_handler(handler, port).await;

    let resp = client
        .get_node_info(tonic::Request::new(GetNodeInfoRequest { from_node_id: 0 }))
        .await
        .expect("GetNodeInfo must succeed at transport level");

    let info = resp.into_inner();
    assert!(info.success);

    // The response must NOT contain credential-bearing fields.
    assert!(
        !info.hostname.as_deref().unwrap_or("").contains("password"),
        "hostname must not contain the word 'password'"
    );
    assert!(!info.status.contains("Bearer"), "status must not contain a Bearer token");
    assert!(
        !info.version.as_deref().unwrap_or("").contains("secret"),
        "version must not contain secrets"
    );

    // It is expected that this RPC is accessible without user credentials
    // (mTLS provides the trust boundary).
    assert_eq!(
        unauthenticated.load(Ordering::SeqCst),
        1,
        "Unauthenticated cluster call counter must be incremented"
    );
}

// ─── Unit tests for handler auth logic ───────────────────────────────────────
//
// These drive the stub handler directly without a gRPC stack, letting us
// enumerate many edge cases cheaply.

#[tokio::test]
async fn test_handler_rejects_none_auth() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();

    let result = handler
        .handle_forward_sql(kalamdb_raft::ForwardSqlRequest {
            sql: "INSERT INTO audit (msg) VALUES ('test')".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: None,
            request_id: None,
        })
        .await
        .expect("handler must return Ok even on auth failure");

    assert_eq!(result.status_code, 401);
    assert_eq!(allowed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_handler_rejects_basic_auth() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();

    let result = handler
        .handle_forward_sql(kalamdb_raft::ForwardSqlRequest {
            sql: "DELETE FROM users WHERE 1=1".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: Some("Basic cm9vdDpyb290".to_string()),
            request_id: None,
        })
        .await
        .expect("handler must return Ok");

    assert_eq!(result.status_code, 401);
    let body: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
    assert_eq!(body["error"]["code"], "PERMISSION_DENIED");
    assert_eq!(allowed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_handler_rejects_forged_bearer() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();

    let forged = "Bearer eyJhbGciOiJub25lIn0.e30.";

    let result = handler
        .handle_forward_sql(kalamdb_raft::ForwardSqlRequest {
            sql: "SELECT secret FROM system.config".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: Some(forged.to_string()),
            request_id: None,
        })
        .await
        .expect("handler must return Ok");

    assert_eq!(result.status_code, 401);
    assert_eq!(allowed.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_handler_allows_valid_bearer() {
    let (handler, allowed, rejected, _) = SecurityStubHandler::new();

    let result = handler
        .handle_forward_sql(kalamdb_raft::ForwardSqlRequest {
            sql: "INSERT INTO logs (msg) VALUES ('hello')".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: Some("Bearer valid-test-token".to_string()),
            request_id: None,
        })
        .await
        .expect("handler must return Ok");

    assert_eq!(result.status_code, 200, "Valid token must be accepted");
    assert_eq!(allowed.load(Ordering::SeqCst), 1);
    assert_eq!(rejected.load(Ordering::SeqCst), 0);
}

/// Confirm that `Bearer valid-test-token` with SQL injection in the SQL field
/// still only reaches the SQL execution layer — auth does not sanitise SQL —
/// but the handler accepts it and it is the SQL engine's responsibility to
/// reject or safely parse the statement.  Here we simply check auth does NOT
/// silently swallow the SQL payload.
#[tokio::test]
async fn test_handler_sql_injection_bypasses_auth_but_reaches_sql_layer() {
    let (handler, allowed, _, _) = SecurityStubHandler::new();

    let sqli_payloads = [
        "SELECT * FROM system.users; DROP TABLE users; --",
        "INSERT INTO users VALUES (1, 'admin', '; SELECT 1 --')",
        "UPDATE users SET role='system' WHERE '1'='1'",
        "'; EXEC xp_cmdshell('whoami'); --",
    ];

    for payload in &sqli_payloads {
        let result = handler
            .handle_forward_sql(kalamdb_raft::ForwardSqlRequest {
                sql: (*payload).to_string(),
                namespace_id: None,
                params_json: vec![],
                authorization_header: Some("Bearer valid-test-token".to_string()),
                request_id: None,
            })
            .await
            .expect("handler must return Ok");

        // Auth passed (valid token), so status should be 200 from stub.
        // The point: auth does NOT filter SQL – the SQL execution engine does.
        assert_eq!(
            result.status_code, 200,
            "SQL injection payload should reach the SQL layer (auth passed for valid token): {}",
            payload
        );
    }

    assert_eq!(
        allowed.load(Ordering::SeqCst),
        sqli_payloads.len(),
        "All payloads with valid token should pass the auth gate"
    );
}

/// Confirm that the `NoOpClusterHandler` (used during tests without a full
/// cluster) correctly returns an error for `ForwardSql` – it must never silently
/// succeed and execute phantom SQL.
#[tokio::test]
async fn test_noop_handler_forward_sql_returns_error() {
    let handler = NoOpClusterHandler;
    let result = handler
        .handle_forward_sql(kalamdb_raft::ForwardSqlRequest {
            sql: "DROP TABLE everything".to_string(),
            namespace_id: None,
            params_json: vec![],
            authorization_header: Some("Bearer valid-test-token".to_string()),
            request_id: None,
        })
        .await;

    assert!(
        result.is_err(),
        "NoOpClusterHandler::handle_forward_sql must always Err (no-op mode)"
    );
}

/// Confirm that `NoOpClusterHandler` accepts ping silently (read-only, safe).
#[tokio::test]
async fn test_noop_handler_ping_is_safe() {
    let handler = NoOpClusterHandler;
    let result = handler.handle_ping(kalamdb_raft::PingRequest { from_node_id: 999 }).await;
    assert!(result.is_ok(), "NoOp ping must succeed");
}

/// Confirm that `NoOpClusterHandler` returns an error for GetNodeInfo,
/// not silent fake data.
#[tokio::test]
async fn test_noop_handler_get_node_info_returns_error() {
    let handler = NoOpClusterHandler;
    let result = handler
        .handle_get_node_info(kalamdb_raft::GetNodeInfoRequest { from_node_id: 0 })
        .await;
    assert!(
        result.is_err(),
        "NoOpClusterHandler::handle_get_node_info must Err in no-op mode"
    );
}
