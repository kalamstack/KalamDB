//! Stress / concurrency smoke tests over the real HTTP SQL API.
//!
//! These are intentionally short (seconds, not minutes) and run through the real
//! HTTP surface to cover business logic without flaking CI.

use futures_util::future::try_join_all;
use kalam_client::models::ResponseStatus;
use serial_test::serial;

use super::test_support::{
    auth_helper::create_user_auth_header_default,
    consolidated_helpers::{unique_namespace, unique_table},
};

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

#[tokio::test]
#[ntest::timeout(60000)] // 60 seconds - stress HTTP smoke test
#[serial]
async fn test_stress_smoke_over_http() {
    (async {
        if std::env::var("KALAMDB_RUN_STRESS_TESTS").as_deref() != Ok("1") {
            eprintln!("Skipping stress smoke test. Set KALAMDB_RUN_STRESS_TESTS=1 to enable.");
            return Ok(());
        }

        let server = super::test_support::http_server::get_global_server().await;
        let ns = unique_namespace("stress");

        let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CREATE NAMESPACE failed: {:?}",
            resp.error
        );

        let user = unique_table("stress_user");
        let auth = create_user_auth_header_default(server, &user).await?;

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "CREATE TABLE {}.stress_data (id INT PRIMARY KEY, value TEXT) WITH \
                     (TYPE='USER', STORAGE_ID='local')",
                    ns
                ),
                &auth,
            )
            .await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CREATE TABLE failed: {:?}",
            resp.error
        );

        // Concurrent writers (small, deterministic).
        let writer_futures = (0..3).map(|writer| {
            let ns = ns.clone();
            let auth = auth.clone();
            async move {
                for j in 0..8 {
                    let id = writer * 100 + j;
                    let sql = format!(
                        "INSERT INTO {}.stress_data (id, value) VALUES ({}, 'w{}-{}')",
                        ns, id, writer, j
                    );
                    let resp = server.execute_sql_with_auth(&sql, &auth).await?;
                    anyhow::ensure!(
                        resp.status == ResponseStatus::Success,
                        "insert failed: {:?}",
                        resp.error
                    );
                }
                anyhow::Ok(())
            }
        });
        try_join_all(writer_futures).await?;

        let cnt = count_rows(server, &auth, &ns, "stress_data").await?;
        anyhow::ensure!(cnt == 24, "expected 24 rows, got {}", cnt);

        // Basic cleanup: DROP TABLE should succeed.
        // Note: DROP TABLE currently requires admin privileges.
        let resp = server.execute_sql(&format!("DROP TABLE {}.stress_data", ns)).await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "DROP TABLE failed: {:?}",
            resp.error
        );

        Ok(())
    })
    .await
    .expect("test_stress_smoke_over_http");
}
