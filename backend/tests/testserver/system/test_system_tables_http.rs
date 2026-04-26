//! System tables smoke coverage over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;

use super::test_support::{
    auth_helper::create_user_auth_header_default,
    consolidated_helpers::{unique_namespace, unique_table},
    flush::flush_table_and_wait,
};

#[tokio::test]
async fn test_system_tables_queryable_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("sys");
    let table_user = "ut";
    let table_shared = "st";

    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let user = unique_table("sys_user");
    let auth = create_user_auth_header_default(server, &user).await?;

    let resp = server
        .execute_sql_with_auth(
            &format!(
                "CREATE TABLE {}.{} (id INT PRIMARY KEY, v TEXT) WITH (TYPE='USER', \
                 STORAGE_ID='local', FLUSH_POLICY='rows:5')",
                ns, table_user
            ),
            &auth,
        )
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (id INT PRIMARY KEY, v TEXT) WITH (TYPE='SHARED', \
             STORAGE_ID='local', FLUSH_POLICY='rows:5')",
            ns, table_shared
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql_with_auth(
            &format!("INSERT INTO {}.{} (id, v) VALUES (1, 'x')", ns, table_user),
            &auth,
        )
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!("INSERT INTO {}.{} (id, v) VALUES (1, 'y')", ns, table_shared))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    flush_table_and_wait(server, &ns, table_user).await?;

    // system.namespaces
    let resp = server
        .execute_sql(&format!(
            "SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'",
            ns
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    anyhow::ensure!(!resp.rows_as_maps().is_empty());

    // system.schemas
    let resp = server
        .execute_sql(&format!(
            "SELECT table_name, table_type FROM system.schemas WHERE namespace_id = '{}' ORDER BY \
             table_name",
            ns
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    anyhow::ensure!(rows.len() >= 2);

    // system.users
    let resp = server
        .execute_sql(&format!("SELECT user_id, role FROM system.users WHERE user_id = '{}'", user))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    anyhow::ensure!(!resp.rows_as_maps().is_empty());

    // system.storages
    let resp = server
        .execute_sql("SELECT storage_id, storage_type FROM system.storages")
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    anyhow::ensure!(rows
        .iter()
        .any(|r| r.get("storage_id").and_then(|v| v.as_str()) == Some("local")));

    // system.jobs (flush should have created at least one record)
    let resp = server
        .execute_sql("SELECT job_type, status FROM system.jobs WHERE job_type = 'flush'")
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);
    anyhow::ensure!(!resp.rows_as_maps().is_empty());
    Ok(())
}
