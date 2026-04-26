//! Production-readiness observability checks over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;
use tokio::time::{sleep, Duration, Instant};

use super::test_support::{
    auth_helper::create_user_auth_header,
    consolidated_helpers::{unique_namespace, unique_table},
};

#[tokio::test]
#[ntest::timeout(60000)] // 60 seconds - observability test with job polling
async fn test_observability_system_tables_and_jobs_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;

    // system.schemas reflects created tables
    let ns_tables = unique_namespace("app_systab");
    let resp = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns_tables))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.messages (id TEXT PRIMARY KEY, content TEXT NOT NULL, timestamp \
             BIGINT) WITH (TYPE = 'USER')",
            ns_tables
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "SELECT namespace_id, table_name, table_type FROM system.schemas WHERE namespace_id = \
             '{}' AND table_name = 'messages' AND is_latest = true",
            ns_tables
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("table_type").unwrap().as_str().unwrap(), "user");

    // system.jobs tracks flush operations
    let ns_jobs = unique_namespace("app_jobs");
    let resp = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns_jobs))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.logs (log_id TEXT PRIMARY KEY, message TEXT) WITH (TYPE = 'USER')",
            ns_jobs
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let username = unique_table("user1");
    let password = "UserPass123!";
    let user_auth = create_user_auth_header(server, &username, password, &Role::User).await?;

    let resp = server
        .execute_sql_with_auth(
            &format!(
                "INSERT INTO {}.logs (log_id, message) VALUES ('log1', 'Test message')",
                ns_jobs
            ),
            &user_auth,
        )
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.logs", ns_jobs)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let deadline = Instant::now() + Duration::from_secs(5);
    let flush_job = loop {
        let resp_after = server
            .execute_sql(
                "SELECT job_type, status, parameters FROM system.jobs WHERE job_type = 'flush'",
            )
            .await?;
        assert_eq!(resp_after.status, ResponseStatus::Success);
        let rows = resp_after.rows_as_maps();

        if let Some(job) = rows.iter().find(|r| {
            r.get("parameters")
                .and_then(|v| v.as_str())
                .map(|s| s.contains(&ns_jobs) && s.contains("logs"))
                .unwrap_or(false)
        }) {
            break job.clone();
        }

        if Instant::now() >= deadline {
            anyhow::bail!("Timed out waiting for flush job for {}.logs", ns_jobs);
        }
        sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(flush_job.get("job_type").and_then(|v| v.as_str()).unwrap_or(""), "flush");

    // system tables queryable on startup
    for table in [
        "system.users",
        "system.namespaces",
        "system.schemas",
        "system.storages",
        "system.jobs",
        "system.live",
    ] {
        let resp = server.execute_sql(&format!("SELECT * FROM {} LIMIT 1", table)).await?;
        assert_eq!(resp.status, ResponseStatus::Success);
    }
    Ok(())
}
