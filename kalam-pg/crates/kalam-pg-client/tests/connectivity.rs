use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::RemoteServerConfig;
use kalamdb_pg::{KalamPgService, PgServiceServer};
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::test]
#[ntest::timeout(10000)]
async fn remote_client_connects_with_auth_and_opens_session() {
    let bind_addr: SocketAddr = "127.0.0.1:59971".parse().expect("bind addr");
    let service = KalamPgService::new(Some("Bearer test-token".to_string()));

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PgServiceServer::new(service))
            .serve(bind_addr)
            .await
            .expect("serve pg grpc");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = RemoteKalamClient::connect(
        RemoteServerConfig {
            host: "127.0.0.1".to_string(),
            port: 59971,
        },
        Some("Bearer test-token".to_string()),
    )
    .await
    .expect("connect client");

    client.ping().await.expect("ping");
    let session = client
        .open_session("pg-backend-1", Some("tenant_app"))
        .await
        .expect("open session");

    assert_eq!(session.session_id, "pg-backend-1");
    assert_eq!(session.current_schema.as_deref(), Some("tenant_app"));
}
