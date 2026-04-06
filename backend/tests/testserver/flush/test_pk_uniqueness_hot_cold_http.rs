//! Primary key uniqueness checks in hot storage and after flush (cold Parquet), over HTTP.

use super::test_support::auth_helper::create_user_auth_header_default;
use super::test_support::consolidated_helpers::{unique_namespace, unique_table};
use super::test_support::flush::{flush_table_and_wait, wait_for_parquet_files_for_table};
use kalam_client::models::ResponseStatus;
use tokio::time::Duration;

async fn count_rows(
    server: &super::test_support::http_server::HttpTestServer,
    auth: &str,
    ns: &str,
    table: &str,
) -> anyhow::Result<i64> {
    let resp = server
        .execute_sql_with_auth(&format!("SELECT COUNT(*) AS cnt FROM {}.{}", ns, table), auth)
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success, "COUNT failed: {:?}", resp.error);

    let row = resp
        .results
        .first()
        .and_then(|r| r.row_as_map(0))
        .ok_or_else(|| anyhow::anyhow!("Missing COUNT row"))?;

    row.get("cnt")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_u64().map(|u| u as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .ok_or_else(|| anyhow::anyhow!("COUNT value not an integer: {:?}", row.get("cnt")))
}

async fn get_name_for_id(
    server: &super::test_support::http_server::HttpTestServer,
    ns: &str,
    table: &str,
    id: i64,
) -> anyhow::Result<String> {
    let resp = server
        .execute_sql(&format!("SELECT name FROM {}.{} WHERE id = {} LIMIT 1", ns, table, id))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success, "SELECT failed: {:?}", resp.error);

    let row = resp
        .results
        .first()
        .and_then(|r| r.row_as_map(0))
        .ok_or_else(|| anyhow::anyhow!("Missing SELECT row"))?;

    row.get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing/invalid name field: {:?}", row.get("name")))
}

#[tokio::test]
#[ntest::timeout(180000)] // 3 minutes max for comprehensive PK uniqueness test
async fn test_pk_uniqueness_hot_and_cold_over_http() {
    (async {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("pk");
            let table_user = "items_user";
            let table_shared = "items_shared";

            let resp = server
                .execute_sql(&format!("CREATE NAMESPACE {}", ns))
                .await?;
            assert_eq!(resp.status, ResponseStatus::Success);

            let auth_a = create_user_auth_header_default(server, &unique_table("user_a")).await?;

            // -------------------------
            // USER table: hot duplicate
            // -------------------------
            {
                let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id INT PRIMARY KEY, name TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:100')",
                            ns, table_user
                        ),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (1, 'first')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (1, 'dup')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Error);

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (2, 'second')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);
            }

            // -------------------------
            // USER table: cold duplicate
            // -------------------------
            {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (10, 'cold')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                flush_table_and_wait(server, &ns, table_user).await?;
                let _ = wait_for_parquet_files_for_table(server, &ns, table_user, 1, Duration::from_secs(20)).await?;

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (10, 'dup_cold')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Error);

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (11, 'ok')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);
            }

            // -------------------------
            // USER table: across segments
            // -------------------------
            {
                // Insert + flush two separate segments; ensure duplicate is rejected.
                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (20, 'seg1')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);
                flush_table_and_wait(server, &ns, table_user).await?;

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (30, 'seg2')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);
                flush_table_and_wait(server, &ns, table_user).await?;

                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (20, 'dup_seg')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Error);
            }

            // -------------------------
            // Shared table: hot+cold dup
            // -------------------------
            {
                let resp = server
                    .execute_sql(&format!(
                        "CREATE TABLE {}.{} (id INT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED', STORAGE_ID='local', FLUSH_POLICY='rows:100')",
                        ns, table_shared
                    ))
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                let resp = server
                    .execute_sql(&format!(
                        "INSERT INTO {}.{} (id, name) VALUES (100, 'first')",
                        ns, table_shared
                    ))
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                let resp = server
                    .execute_sql(&format!(
                        "INSERT INTO {}.{} (id, name) VALUES (100, 'dup')",
                        ns, table_shared
                    ))
                    .await?;
                anyhow::ensure!(
                    matches!(resp.status, ResponseStatus::Success | ResponseStatus::Error),
                    "Unexpected response status: {:?}",
                    resp.status
                );

                // Regardless of immediate response status, the shared table should not allow
                // a visible overwrite of the existing PK.
                let name = get_name_for_id(server, &ns, table_shared, 100).await?;
                anyhow::ensure!(name == "first", "expected name='first', got '{}'", name);

                flush_table_and_wait(server, &ns, table_shared).await?;
                let _ = wait_for_parquet_files_for_table(server, &ns, table_shared, 1, Duration::from_secs(20)).await?;

                let resp = server
                    .execute_sql(&format!(
                        "INSERT INTO {}.{} (id, name) VALUES (100, 'dup_cold')",
                        ns, table_shared
                    ))
                    .await?;
                anyhow::ensure!(
                    matches!(resp.status, ResponseStatus::Success | ResponseStatus::Error),
                    "Unexpected response status: {:?}",
                    resp.status
                );
                let name = get_name_for_id(server, &ns, table_shared, 100).await?;
                anyhow::ensure!(name == "first", "expected name='first', got '{}'", name);
            }

            // -------------------------
            // UPDATE changing PK to duplicate should error
            // -------------------------
            {
                // Make sure ids 1 and 2 exist.
                let resp = server
                    .execute_sql_with_auth(
                        &format!("INSERT INTO {}.{} (id, name) VALUES (40, 'x')", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                // Try to change 40 -> 2 (duplicate)
                let resp = server
                    .execute_sql_with_auth(
                        &format!("UPDATE {}.{} SET id = 2 WHERE id = 40", ns, table_user),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Error);
            }

            // -------------------------
            // AUTO_INCREMENT PK should allow inserts without id conflicts
            // -------------------------
            {
                let table_auto = "items_auto";
                let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY AUTO_INCREMENT, name TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:100')",
                            ns, table_auto
                        ),
                        &auth_a,
                    )
                    .await?;
                assert_eq!(resp.status, ResponseStatus::Success);

                for _ in 0..3 {
                    let resp = server
                        .execute_sql_with_auth(
                            &format!("INSERT INTO {}.{} (name) VALUES ('n')", ns, table_auto),
                            &auth_a,
                        )
                        .await?;
                    assert_eq!(resp.status, ResponseStatus::Success);
                }

                let cnt = count_rows(server, &auth_a, &ns, table_auto).await?;
                anyhow::ensure!(cnt == 3, "expected 3 rows in auto table, got {}", cnt);
            }

    Ok(())
    })
        .await
        .expect("test_pk_uniqueness_hot_and_cold_over_http");
}
