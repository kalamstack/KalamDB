use anyhow::Result;

use super::test_support::http_server::start_http_test_server;

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cluster_health_exposes_runtime_metrics() -> Result<()> {
    let server = start_http_test_server().await?;

    let result = async {
        let client = server.link_client("root");
        let response = client.cluster_health_check().await?;

        anyhow::ensure!(!response.nodes.is_empty(), "cluster health returned no nodes");

        let self_node = response
            .nodes
            .iter()
            .find(|node| node.is_self)
            .ok_or_else(|| anyhow::anyhow!("cluster health missing self node"))?;

        anyhow::ensure!(
            self_node.memory_usage_mb.is_some(),
            "cluster health missing memory_usage_mb"
        );
        anyhow::ensure!(
            self_node.cpu_usage_percent.is_some(),
            "cluster health missing cpu_usage_percent"
        );
        anyhow::ensure!(
            self_node.uptime_seconds.is_some(),
            "cluster health missing uptime_seconds"
        );
        anyhow::ensure!(
            self_node.uptime_human.as_deref().is_some(),
            "cluster health missing uptime_human"
        );

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}