use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::RemoteServerConfig;
use kalamdb_pg::{KalamPgService, PgServiceServer};
use std::time::Duration;
use tokio::net::TcpListener;

/// Helper: start a gRPC PgService on an ephemeral port and return a connected client.
async fn start_server_and_client() -> RemoteKalamClient {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let service = KalamPgService::new(false, None);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PgServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .expect("serve pg grpc");
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    RemoteKalamClient::connect(RemoteServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        ..Default::default()
    })
    .await
    .expect("connect client")
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn remote_client_connects_and_opens_session() {
    let client = start_server_and_client().await;

    client.ping().await.expect("ping");
    let session = client
        .open_session("pg-backend-1", Some("tenant_app"))
        .await
        .expect("open session");

    assert_eq!(session.session_id, "pg-backend-1");
    assert_eq!(session.current_schema.as_deref(), Some("tenant_app"));
}

/// Regression test: sequential begin→commit cycles must not leave stale
/// transactions. Previously, the FDW xact_callback consumed transaction state
/// on PRE_COMMIT, causing the server to see stale active transactions.
#[tokio::test]
#[ntest::timeout(10000)]
async fn sequential_transactions_commit_cleanly() {
    let client = start_server_and_client().await;

    client.open_session("pg-seq-tx", None).await.expect("open session");

    for i in 0..5 {
        let tx_id = client
            .begin_transaction("pg-seq-tx")
            .await
            .unwrap_or_else(|e| panic!("begin_transaction #{i} failed: {e}"));

        client
            .commit_transaction("pg-seq-tx", &tx_id)
            .await
            .unwrap_or_else(|e| panic!("commit_transaction #{i} ({tx_id}) failed: {e}"));
    }
}

/// Verify that begin_transaction auto-rollbacks a stale active transaction
/// when a new one is started (server-side safety net).
#[tokio::test]
#[ntest::timeout(10000)]
async fn stale_transaction_auto_rollback_on_new_begin() {
    let client = start_server_and_client().await;

    client.open_session("pg-stale-tx", None).await.expect("open session");

    // Begin a transaction and intentionally skip commit/rollback
    let _tx_id1 = client.begin_transaction("pg-stale-tx").await.expect("begin tx1");

    // Beginning a new transaction should succeed (auto-rollback of stale tx1)
    let tx_id2 = client
        .begin_transaction("pg-stale-tx")
        .await
        .expect("begin tx2 should auto-rollback stale tx1");

    client.commit_transaction("pg-stale-tx", &tx_id2).await.expect("commit tx2");
}

/// Verify close_session removes the session from the server registry.
#[tokio::test]
#[ntest::timeout(10000)]
async fn close_session_removes_server_state() {
    let client = start_server_and_client().await;

    client.open_session("pg-close-test", None).await.expect("open session");

    // Close the session — should succeed
    client.close_session("pg-close-test").await.expect("close session");

    // Closing again should also succeed (idempotent remove)
    client
        .close_session("pg-close-test")
        .await
        .expect("close session again (idempotent)");
}

/// Verify that connecting to a non-existent server with a timeout fails quickly.
#[tokio::test]
#[ntest::timeout(10000)]
async fn connect_with_timeout_fails_on_unreachable_server() {
    // Bind and immediately drop to guarantee the port is free and nothing listens.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let free_port = listener.local_addr().expect("addr").port();
    drop(listener);

    let config = RemoteServerConfig {
        host: "127.0.0.1".to_string(),
        port: free_port,
        timeout_ms: 1000,
        ..Default::default()
    };

    let client = RemoteKalamClient::connect(config).await;
    // connect() may succeed (lazy connect), so try a ping
    match client {
        Err(_) => {
            // Connection failed immediately — this is acceptable
        },
        Ok(c) => {
            // Connection appeared to succeed (tonic uses lazy connect).
            // The first RPC call should fail with timeout.
            let result = c.ping().await;
            assert!(result.is_err(), "ping to unreachable server should fail");
        },
    }
}
