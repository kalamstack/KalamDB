use anyhow::Result;
use kalam_client::models::{QueryResult, ResponseStatus};
use kalamdb_raft::GroupId;
use tokio::time::{sleep, Duration, Instant};

use super::test_support::{
    cluster::ClusterTestServer,
    consolidated_helpers::{get_count_value, unique_namespace},
};

fn result_i64(result: &QueryResult, column: &str) -> Result<i64> {
    let row = result
        .row_as_map(0)
        .ok_or_else(|| anyhow::anyhow!("missing first row for column {}", column))?;

    row.get(column)
        .and_then(|value| {
            value.as_i64().or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        })
        .ok_or_else(|| anyhow::anyhow!("missing numeric column {} in row {:?}", column, row))
}

async fn wait_for_cluster_roles(cluster: &ClusterTestServer) -> Result<(usize, usize, usize)> {
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        let mut meta_leader_index = None;
        let mut shared_leader_index = None;
        let mut follower_index = None;

        for (index, node) in cluster.nodes.iter().enumerate() {
            let executor = node.app_context().executor();
            let node_id = executor.node_id();
            let meta_leader = executor.get_leader(GroupId::Meta).await;
            let shared_leader = executor.get_leader(GroupId::DataSharedShard(0)).await;

            if let Some(meta_leader) = meta_leader {
                if meta_leader == node_id {
                    meta_leader_index = Some(index);
                } else if follower_index.is_none() {
                    follower_index = Some(index);
                }
            }

            if let Some(shared_leader) = shared_leader {
                if shared_leader == node_id {
                    shared_leader_index = Some(index);
                }
            }
        }

        if let (Some(follower_index), Some(meta_leader_index), Some(shared_leader_index)) =
            (follower_index, meta_leader_index, shared_leader_index)
        {
            return Ok((follower_index, meta_leader_index, shared_leader_index));
        }

        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for follower/meta-leader/shared-leader discovery");
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_table_visible(
    server: &super::test_support::http_server::HttpTestServer,
    namespace: &str,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let query = format!("SELECT COUNT(*) AS cnt FROM {}.items", namespace);

    loop {
        match server.execute_sql(&query).await {
            Ok(response) if response.status == ResponseStatus::Success => return Ok(()),
            Ok(_) | Err(_) if Instant::now() < deadline => {
                sleep(Duration::from_millis(50)).await;
            },
            Ok(response) => {
                anyhow::bail!(
                    "table {}.items did not become visible: {:?}",
                    namespace,
                    response.error
                )
            },
            Err(error) => {
                anyhow::bail!("table {}.items did not become visible: {}", namespace, error)
            },
        }
    }
}

#[tokio::test]
#[ntest::timeout(3600)]
async fn test_sql_transaction_forwarded_from_follower_preserves_atomic_staging() -> Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let cluster = super::test_support::http_server::get_cluster_server().await;
    let (follower_index, meta_leader_index, shared_leader_index) =
        wait_for_cluster_roles(cluster).await?;

    anyhow::ensure!(
        follower_index != meta_leader_index,
        "expected a follower/frontdoor distinct from the meta leader"
    );

    let follower = cluster.get_node(follower_index)?;
    let meta_leader = cluster.get_node(meta_leader_index)?;
    let shared_leader = cluster.get_node(shared_leader_index)?;

    let namespace = unique_namespace("tx_cluster_http_forward");

    let create_namespace = meta_leader
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .await?;
    anyhow::ensure!(
        create_namespace.status == ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        create_namespace.error
    );

    let create_table = meta_leader
        .execute_sql(&format!(
            "CREATE TABLE {}.items (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED', \
             STORAGE_ID='local')",
            namespace
        ))
        .await?;
    anyhow::ensure!(
        create_table.status == ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_table.error
    );

    wait_for_table_visible(shared_leader, &namespace).await?;

    let batch = format!(
        "/* strict */ BEGIN; INSERT INTO {}.items (id, name) VALUES (4101, 'alpha'); INSERT INTO \
         {}.items (id, name) VALUES (4102, 'beta'); SELECT COUNT(*) AS visible_rows FROM \
         {}.items; SELECT write_count AS staged_writes, touched_tables_count AS staged_tables \
         FROM system.transactions WHERE origin = 'SqlBatch'; COMMIT;",
        namespace, namespace, namespace
    );

    let response = follower.execute_sql(&batch).await?;
    anyhow::ensure!(
        response.status == ResponseStatus::Success,
        "forwarded transaction batch failed: {:?}",
        response.error
    );

    let visible_rows = response
        .results
        .iter()
        .find(|result| result.row_as_map(0).is_some_and(|row| row.contains_key("visible_rows")))
        .map(|result| result_i64(result, "visible_rows"))
        .transpose()?
        .ok_or_else(|| {
            anyhow::anyhow!("missing visible_rows result set: {:?}", response.results)
        })?;
    anyhow::ensure!(
        visible_rows == 2,
        "expected in-transaction visibility of 2 rows, got {}",
        visible_rows
    );

    let staged_result = response
        .results
        .iter()
        .find(|result| result.row_as_map(0).is_some_and(|row| row.contains_key("staged_writes")))
        .ok_or_else(|| {
            anyhow::anyhow!("missing staged_writes result set: {:?}", response.results)
        })?;
    anyhow::ensure!(result_i64(staged_result, "staged_writes")? == 2);
    anyhow::ensure!(result_i64(staged_result, "staged_tables")? == 1);

    let insert_messages = response
        .results
        .iter()
        .filter(|result| result.message.as_deref() == Some("Inserted 1 row(s)"))
        .count();
    anyhow::ensure!(
        insert_messages == 2,
        "expected exactly two inserted-row messages in response: {:?}",
        response.results
    );

    let committed = shared_leader
        .execute_sql(&format!("SELECT COUNT(*) AS committed_rows FROM {}.items", namespace))
        .await?;
    anyhow::ensure!(
        committed.status == ResponseStatus::Success,
        "post-commit count failed: {:?}",
        committed.error
    );
    anyhow::ensure!(
        get_count_value(&committed, 0) == 2,
        "expected 2 committed rows after atomic commit"
    );

    let active_transactions = shared_leader
        .execute_sql("SELECT COUNT(*) AS cnt FROM system.transactions WHERE origin = 'SqlBatch'")
        .await?;
    anyhow::ensure!(
        active_transactions.status == ResponseStatus::Success,
        "system.transactions verification failed: {:?}",
        active_transactions.error
    );
    anyhow::ensure!(
        get_count_value(&active_transactions, -1) == 0,
        "expected no lingering SqlBatch transactions"
    );

    Ok(())
}
