//! User table lifecycle + isolation tests over the real HTTP SQL API.

use super::test_support::auth_helper::create_user_auth_header_default;
use super::test_support::consolidated_helpers::{unique_namespace, unique_table};
use super::test_support::flush::{flush_table_and_wait, wait_for_parquet_files_for_user_table};
use super::test_support::http_server::HttpTestServer;
use super::test_support::jobs::{
    extract_cleanup_job_id, wait_for_job_completion, wait_for_path_absent,
};
use kalam_client::models::ResponseStatus;
use tokio::time::Duration;

async fn lookup_user_id(server: &HttpTestServer, username: &str) -> anyhow::Result<String> {
    let resp = server
        .execute_sql(&format!("SELECT user_id FROM system.users WHERE user_id='{}'", username))
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "SELECT system.users failed: {:?}",
        resp.error
    );

    let row = resp
        .results
        .first()
        .and_then(|r| r.row_as_map(0))
        .ok_or_else(|| anyhow::anyhow!("Missing system.users row for {}", username))?;

    row.get("user_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing user_id for {}", username))
}

#[tokio::test]
async fn test_user_tables_lifecycle_and_isolation_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("ut");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let user1 = unique_table("user1");
    let user2 = unique_table("user2");
    let auth1 = create_user_auth_header_default(server, &user1).await?;
    let auth2 = create_user_auth_header_default(server, &user2).await?;
    let user1_id = lookup_user_id(server, &user1).await?;
    let user2_id = lookup_user_id(server, &user2).await?;

    // -----------------------------------------------------------------
    // Create + insert
    // -----------------------------------------------------------------
    {
        let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.notes (id TEXT PRIMARY KEY, content TEXT, priority INT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:50')",
                            ns
                        ),
                        &auth1,
                    )
                    .await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CREATE TABLE failed: {:?}",
            resp.error
        );

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "INSERT INTO {}.notes (id, content, priority) VALUES ('note1','User1 note',1)",
                    ns
                ),
                &auth1,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "INSERT INTO {}.notes (id, content, priority) VALUES ('note2','User2 note',2)",
                    ns
                ),
                &auth2,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
    }

    // -----------------------------------------------------------------
    // Isolation: each user sees only their own rows
    // -----------------------------------------------------------------
    {
        let resp = server
            .execute_sql_with_auth(&format!("SELECT id, content FROM {}.notes", ns), &auth1)
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        let rows = resp.rows_as_maps();
        anyhow::ensure!(rows.len() == 1);
        anyhow::ensure!(rows[0].get("id").and_then(|v| v.as_str()) == Some("note1"));

        let resp = server
            .execute_sql_with_auth(&format!("SELECT id, content FROM {}.notes", ns), &auth2)
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        let rows = resp.rows_as_maps();
        anyhow::ensure!(rows.len() == 1);
        anyhow::ensure!(rows[0].get("id").and_then(|v| v.as_str()) == Some("note2"));
    }

    // -----------------------------------------------------------------
    // UPDATE/DELETE isolation
    // -----------------------------------------------------------------
    {
        let resp = server
            .execute_sql_with_auth(
                &format!("UPDATE {}.notes SET content = 'Updated by user1' WHERE id = 'note1'", ns),
                &auth1,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server
            .execute_sql_with_auth(
                &format!("SELECT content FROM {}.notes WHERE id = 'note1'", ns),
                &auth1,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        let row = resp
            .results
            .first()
            .and_then(|r| r.row_as_map(0))
            .ok_or_else(|| anyhow::anyhow!("Missing row"))?;
        anyhow::ensure!(row.get("content").and_then(|v| v.as_str()) == Some("Updated by user1"));

        let resp = server
            .execute_sql_with_auth(&format!("DELETE FROM {}.notes WHERE id = 'note1'", ns), &auth1)
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server
            .execute_sql_with_auth(&format!("SELECT id FROM {}.notes", ns), &auth1)
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        anyhow::ensure!(resp.rows_as_maps().is_empty());

        let resp = server
            .execute_sql_with_auth(&format!("SELECT id FROM {}.notes", ns), &auth2)
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        let rows = resp.rows_as_maps();
        anyhow::ensure!(rows.len() == 1);
        anyhow::ensure!(rows[0].get("id").and_then(|v| v.as_str()) == Some("note2"));
    }

    // -----------------------------------------------------------------
    // DROP TABLE cleanup smoke: parquet dirs removed
    // -----------------------------------------------------------------
    {
        let table = "notes_drop";
        let _ = server.execute_sql(&format!("DROP TABLE {}.{}", ns, table)).await;

        let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id TEXT PRIMARY KEY, content TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:2')",
                            ns, table
                        ),
                        &auth1,
                    )
                    .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server
            .execute_sql_with_auth(
                &format!("INSERT INTO {}.{} (id, content) VALUES ('a1','hello')", ns, table),
                &auth1,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server
            .execute_sql_with_auth(
                &format!("INSERT INTO {}.{} (id, content) VALUES ('b1','world')", ns, table),
                &auth2,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        flush_table_and_wait(server, &ns, table).await?;

        let files_user1 = wait_for_parquet_files_for_user_table(
            server,
            &ns,
            table,
            &user1_id,
            1,
            Duration::from_secs(40),
        )
        .await?;
        let files_user2 = wait_for_parquet_files_for_user_table(
            server,
            &ns,
            table,
            &user2_id,
            1,
            Duration::from_secs(40),
        )
        .await?;

        let dir_user1 = files_user1
            .first()
            .and_then(|p| p.parent())
            .map(|p| p.to_path_buf())
            .ok_or_else(|| anyhow::anyhow!("No parent dir for user1 parquet file"))?;
        let dir_user2 = files_user2
            .first()
            .and_then(|p| p.parent())
            .map(|p| p.to_path_buf())
            .ok_or_else(|| anyhow::anyhow!("No parent dir for user2 parquet file"))?;

        let drop_resp = server.execute_sql(&format!("DROP TABLE {}.{}", ns, table)).await?;
        anyhow::ensure!(drop_resp.status == ResponseStatus::Success);

        let msg = drop_resp.results.first().and_then(|r| r.message.as_deref()).unwrap_or("");

        if let Some(job_id) = extract_cleanup_job_id(msg) {
            let _ = wait_for_job_completion(server, &job_id, Duration::from_secs(15)).await?;
        }

        anyhow::ensure!(
            wait_for_path_absent(&dir_user1, Duration::from_secs(5)).await,
            "User1 parquet dir still exists after drop: {}",
            dir_user1.display()
        );
        anyhow::ensure!(
            wait_for_path_absent(&dir_user2, Duration::from_secs(5)).await,
            "User2 parquet dir still exists after drop: {}",
            dir_user2.display()
        );
    }
    Ok(())
}
