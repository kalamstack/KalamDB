use std::sync::Arc;
use std::time::Duration;

use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::RemoteServerConfig;
use kalamdb_commons::models::NodeId;
use kalamdb_pg::KalamPgService;
use kalamdb_raft::manager::{RaftManager, RaftManagerConfig};
use kalamdb_raft::{network::cluster_handler::NoOpClusterHandler, network::start_rpc_server};

#[tokio::test]
#[ntest::timeout(10000)]
async fn shared_rpc_server_hosts_pg_service() {
    let rpc_addr = "127.0.0.1:19741".to_string();
    let config = RaftManagerConfig {
        node_id: NodeId::new(1),
        rpc_addr: rpc_addr.clone(),
        api_addr: "127.0.0.1:29741".to_string(),
        peers: Vec::new(),
        user_shards: 1,
        shared_shards: 1,
        ..Default::default()
    };

    let manager = Arc::new(RaftManager::new(config));
    let cluster_handler: Arc<dyn kalamdb_raft::ClusterMessageHandler> =
        Arc::new(NoOpClusterHandler);
    let pg_service = Arc::new(KalamPgService::new(false, None));

    start_rpc_server(manager, rpc_addr, cluster_handler, Some(pg_service))
        .await
        .expect("start shared rpc server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = RemoteKalamClient::connect(
        RemoteServerConfig {
            host: "127.0.0.1".to_string(),
            port: 19741,
            ..Default::default()
        },
    )
    .await
    .expect("connect remote client");

    client.ping().await.expect("ping shared rpc service");
    let session = client
        .open_session("pg-backend-raft", Some("tenant_a"))
        .await
        .expect("open session over shared rpc server");

    assert_eq!(session.session_id, "pg-backend-raft");
    assert_eq!(session.current_schema.as_deref(), Some("tenant_a"));
}
