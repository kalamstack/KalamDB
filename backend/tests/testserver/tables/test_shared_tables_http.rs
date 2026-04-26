//! Shared table lifecycle tests over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;
use tokio::time::Duration;

use super::test_support::{
    consolidated_helpers::unique_namespace,
    flush::{flush_table_and_wait, wait_for_parquet_files_for_table},
    jobs::{extract_cleanup_job_id, wait_for_job_completion, wait_for_path_absent},
};

#[tokio::test]
async fn test_shared_tables_lifecycle_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("st");
    let table = "audit";

    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let _ = server.execute_sql(&format!("DROP TABLE {}.{}", ns, table)).await;

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (id TEXT PRIMARY KEY, entry TEXT, level INT) WITH (TYPE='SHARED', \
             STORAGE_ID='local', FLUSH_POLICY='rows:50')",
            ns, table
        ))
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        resp.error
    );

    let resp = server
        .execute_sql(&format!(
            "INSERT INTO {}.{} (id, entry, level) VALUES ('a1','hello',1)",
            ns, table
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "INSERT INTO {}.{} (id, entry, level) VALUES ('a2','world',2)",
            ns, table
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    // SELECT
    let resp = server
        .execute_sql(&format!("SELECT id FROM {}.{} ORDER BY id", ns, table))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    anyhow::ensure!(rows.len() == 2);

    // UPDATE
    let resp = server
        .execute_sql(&format!("UPDATE {}.{} SET entry = 'updated' WHERE id = 'a1'", ns, table))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!("SELECT entry FROM {}.{} WHERE id = 'a1'", ns, table))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    let row = resp
        .results
        .first()
        .and_then(|r| r.row_as_map(0))
        .ok_or_else(|| anyhow::anyhow!("Missing row"))?;
    anyhow::ensure!(row.get("entry").and_then(|v| v.as_str()) == Some("updated"));

    // DELETE
    let resp = server
        .execute_sql(&format!("DELETE FROM {}.{} WHERE id = 'a2'", ns, table))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    // Flush + parquet exists
    flush_table_and_wait(server, &ns, table).await?;
    let parquet =
        wait_for_parquet_files_for_table(server, &ns, table, 1, Duration::from_secs(20)).await?;
    let parquet_dir = parquet
        .first()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| anyhow::anyhow!("missing parquet parent dir"))?;

    // DROP TABLE cleanup
    let drop_resp = server.execute_sql(&format!("DROP TABLE {}.{}", ns, table)).await?;
    anyhow::ensure!(drop_resp.status == ResponseStatus::Success);

    let msg = drop_resp.results.first().and_then(|r| r.message.as_deref()).unwrap_or("");

    if let Some(job_id) = extract_cleanup_job_id(msg) {
        wait_for_job_completion(server, &job_id, Duration::from_secs(15)).await?;
    }

    anyhow::ensure!(
        wait_for_path_absent(&parquet_dir, Duration::from_secs(5)).await,
        "Parquet dir still exists after drop: {}",
        parquet_dir.display()
    );
    Ok(())
}
