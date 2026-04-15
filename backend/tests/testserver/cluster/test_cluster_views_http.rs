use anyhow::Result;
use kalam_client::models::ResponseStatus;

use super::test_support::http_server::start_http_test_server;

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_system_cluster_views_over_http() -> Result<()> {
    let server = start_http_test_server().await?;

    let result = async {
        let resp = server.execute_sql("SELECT * FROM system.cluster").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "system.cluster failed: {:?}",
            resp.error
        );
        let rows = resp.rows_as_maps();
        anyhow::ensure!(!rows.is_empty(), "system.cluster returned no rows");
        let first = &rows[0];
        anyhow::ensure!(first.contains_key("cluster_id"), "system.cluster missing cluster_id");
        anyhow::ensure!(first.contains_key("node_id"), "system.cluster missing node_id");
        anyhow::ensure!(first.contains_key("role"), "system.cluster missing role");
        anyhow::ensure!(first.contains_key("status"), "system.cluster missing status");
        anyhow::ensure!(
            first.contains_key("memory_usage_mb"),
            "system.cluster missing memory_usage_mb"
        );
        anyhow::ensure!(
            first.contains_key("cpu_usage_percent"),
            "system.cluster missing cpu_usage_percent"
        );
        anyhow::ensure!(
            first.contains_key("uptime_seconds"),
            "system.cluster missing uptime_seconds"
        );
        anyhow::ensure!(first.contains_key("uptime_human"), "system.cluster missing uptime_human");

        let resp = server.execute_sql("SELECT * FROM system.cluster_groups").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "system.cluster_groups failed: {:?}",
            resp.error
        );
        let rows = resp.rows_as_maps();
        anyhow::ensure!(!rows.is_empty(), "system.cluster_groups returned no rows");
        let first = &rows[0];
        anyhow::ensure!(first.contains_key("group_id"), "system.cluster_groups missing group_id");
        anyhow::ensure!(
            first.contains_key("group_type"),
            "system.cluster_groups missing group_type"
        );

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}
