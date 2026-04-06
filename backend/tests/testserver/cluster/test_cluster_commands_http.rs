use anyhow::Result;
use kalam_client::models::ResponseStatus;

use super::test_support::http_server::start_http_test_server;

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cluster_commands_over_http() -> Result<()> {
    let server = start_http_test_server().await?;

    let result = async {
        let resp = server.execute_sql("CLUSTER LIST").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CLUSTER LIST failed: {:?}",
            resp.error
        );

        let resp = server.execute_sql("CLUSTER SNAPSHOT").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CLUSTER SNAPSHOT failed: {:?}",
            resp.error
        );

        let resp = server.execute_sql("CLUSTER CLEAR").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CLUSTER CLEAR failed: {:?}",
            resp.error
        );

        // CLUSTER JOIN and CLUSTER LEAVE were removed from the command set.
        // Verify they fail gracefully with an informative error rather than panicking.
        let resp = server.execute_sql("CLUSTER JOIN 127.0.0.1:9001").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Error,
            "CLUSTER JOIN should return Error (command removed), got {:?}",
            resp.status
        );

        let resp = server.execute_sql("CLUSTER LEAVE").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Error,
            "CLUSTER LEAVE should return Error (command removed), got {:?}",
            resp.status
        );

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}
