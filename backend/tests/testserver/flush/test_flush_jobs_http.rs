//! Flush-related SQL tests over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;
use tokio::time::{sleep, Duration, Instant};

use super::test_support::consolidated_helpers::unique_namespace;

#[tokio::test]
#[ntest::timeout(120000)] // 120 seconds - allow for server startup + job persistence
async fn test_flush_table_persists_job_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("app_flush_jobs");
    let create_ns = format!("CREATE NAMESPACE IF NOT EXISTS {}", ns);
    let resp = server.execute_sql(&create_ns).await?;
    assert_eq!(resp.status, ResponseStatus::Success, "CREATE NAMESPACE failed");

    let create_table = format!(
        "CREATE TABLE {}.logs (log_id TEXT PRIMARY KEY, message TEXT) WITH (TYPE = 'USER')",
        ns
    );
    let resp = server.execute_sql(&create_table).await?;
    assert_eq!(resp.status, ResponseStatus::Success, "CREATE TABLE failed");

    let insert_sql =
        format!("INSERT INTO {}.logs (log_id, message) VALUES ('log1', 'Test message')", ns);
    let resp = server.execute_sql(&insert_sql).await?;
    assert_eq!(resp.status, ResponseStatus::Success, "INSERT failed");

    let flush_sql = format!("STORAGE FLUSH TABLE {}.logs", ns);
    let resp = server.execute_sql(&flush_sql).await?;
    assert_eq!(resp.status, ResponseStatus::Success, "STORAGE FLUSH TABLE failed");

    // Jobs may be persisted asynchronously. Poll briefly.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let resp = server
            .execute_sql(
                "SELECT job_type, status, parameters FROM system.jobs WHERE job_type = 'flush'",
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);
        let rows = resp.rows_as_maps();
        if !rows.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("Expected at least one flush job in system.jobs");
        }
        sleep(Duration::from_millis(50)).await;
    }

    let _ = server.execute_sql(&format!("DROP NAMESPACE {}", ns)).await;
    Ok(())
}
