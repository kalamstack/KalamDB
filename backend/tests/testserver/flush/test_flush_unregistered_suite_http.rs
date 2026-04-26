//! Flush concurrency + correctness tests over the real HTTP SQL API.
//!
//! These tests were previously in `tests/integration/flush/*` but were not
//! registered in `backend/Cargo.toml`, so they never ran. This suite migrates
//! them to the near-production HTTP harness.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;
use tokio::time::{sleep, Duration, Instant};

use super::test_support::{
    auth_helper::create_user_auth_header,
    consolidated_helpers::{unique_namespace, unique_table},
    http_server::HttpTestServer,
};

async fn wait_for_flush_jobs_settled(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
) -> anyhow::Result<()> {
    super::test_support::flush::wait_for_flush_jobs_settled(server, ns, table).await
}

async fn count_rows(
    server: &HttpTestServer,
    auth: &str,
    ns: &str,
    table: &str,
) -> anyhow::Result<i64> {
    let resp = server
        .execute_sql_with_auth(
            &format!(
                "SELECT COUNT(DISTINCT id) AS cnt FROM {}.{} WHERE _deleted = false",
                ns, table
            ),
            auth,
        )
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success, "COUNT failed: {:?}", resp.error);
    let count = resp
        .results
        .first()
        .and_then(|r| r.row_as_map(0))
        .and_then(|row| row.get("cnt").cloned())
        .and_then(|value| {
            value.as_i64().or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to parse COUNT(DISTINCT id) result"))?;
    Ok(count)
}

async fn wait_for_id_absent(
    server: &HttpTestServer,
    auth: &str,
    ns: &str,
    table: &str,
    id: i64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let resp = server
            .execute_sql_with_auth(&format!("SELECT id FROM {ns}.{table} WHERE id = {id}"), auth)
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "select failed: {:?}", resp.error);
        let rows = resp.results.first().and_then(|r| r.rows.as_ref()).map(|r| r.len()).unwrap_or(0);
        if rows == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!("expected id {} to be deleted", id));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_row_count_in_range(
    server: &HttpTestServer,
    auth: &str,
    ns: &str,
    table: &str,
    min_expected: i64,
    max_expected: i64,
    timeout: Duration,
) -> anyhow::Result<i64> {
    let deadline = Instant::now() + timeout;
    loop {
        let cnt = count_rows(server, auth, ns, table).await?;
        if (min_expected..=max_expected).contains(&cnt) {
            return Ok(cnt);
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "expected {}-{} rows, got {}",
                min_expected,
                max_expected,
                cnt
            ));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn create_user_table(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
    flush_policy: &str,
) -> anyhow::Result<()> {
    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        resp.error
    );

    let create_sql = format!(
        r#"CREATE TABLE {ns}.{table} (
			id INT PRIMARY KEY,
			data TEXT
		) WITH (
			TYPE = 'USER',
			STORAGE_ID = 'local',
			FLUSH_POLICY = '{flush_policy}'
		)"#
    );

    let resp = server.execute_sql(&create_sql).await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        resp.error
    );
    Ok(())
}

async fn flush_table_and_wait(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
) -> anyhow::Result<()> {
    super::test_support::flush::flush_table_and_wait(server, ns, table).await
}

#[tokio::test]
#[ntest::timeout(45000)] // observed up to ~15s in stress runs; keep 3x headroom
async fn test_flush_concurrency_and_correctness_over_http() {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    (async {
        let server = super::test_support::http_server::get_global_server().await;
        let user_a = unique_table("user_a");
        let user_b = unique_table("user_b");
        let password = "UserPass123!";
        let auth_a = create_user_auth_header(server, &user_a, password, &Role::User).await?;
        let auth_b = create_user_auth_header(server, &user_b, password, &Role::User).await?;

        // -----------------------------------------------------------------
        // test_flush_concurrent_dml
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_concurrent_ns");
            let table = "items";
            create_user_table(server, &ns, table, "rows:50").await?;

            let writer = async {
                for i in 1..=30 {
                    let insert_sql = format!(
                        "INSERT INTO {}.{} (id, data) VALUES ({}, 'value_{}')",
                        ns, table, i, i
                    );
                    let resp = server.execute_sql_with_auth(&insert_sql, &auth_a).await?;
                    anyhow::ensure!(
                        resp.status == ResponseStatus::Success,
                        "insert {} failed: {:?}",
                        i,
                        resp.error
                    );

                    if i % 5 == 0 {
                        let update_sql = format!(
                            "UPDATE {}.{} SET data = 'updated_{}' WHERE id = {}",
                            ns, table, i, i
                        );
                        let resp = server.execute_sql_with_auth(&update_sql, &auth_a).await?;
                        anyhow::ensure!(
                            resp.status == ResponseStatus::Success,
                            "update {} failed: {:?}",
                            i,
                            resp.error
                        );
                    }

                    if i % 10 == 0 {
                        let delete_id = i - 5;
                        let delete_sql =
                            format!("DELETE FROM {}.{} WHERE id = {}", ns, table, delete_id);
                        let resp = server.execute_sql_with_auth(&delete_sql, &auth_a).await?;
                        anyhow::ensure!(
                            resp.status == ResponseStatus::Success,
                            "delete {} failed: {:?}",
                            delete_id,
                            resp.error
                        );
                    }
                }
                anyhow::Ok(())
            };

            let flusher = async {
                for _ in 0..2 {
                    flush_table_and_wait(server, &ns, table).await?;
                    sleep(Duration::from_millis(10)).await;
                }
                anyhow::Ok(())
            };

            let (w, f) = tokio::join!(writer, flusher);
            w?;
            f?;

            flush_table_and_wait(server, &ns, table).await?;

            wait_for_row_count_in_range(
                server,
                &auth_a,
                &ns,
                table,
                27,
                28,
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| anyhow::anyhow!("expected 27-28 rows after deletes, {}", e))?;

            for deleted_id in [5, 15, 25] {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("DELETE FROM {}.{} WHERE id = {}", ns, table, deleted_id),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "delete {} failed: {:?}",
                    deleted_id,
                    resp.error
                );
            }
            flush_table_and_wait(server, &ns, table).await?;
            let delete_timeout = Duration::from_secs(20);
            tokio::try_join!(
                wait_for_id_absent(server, &auth_a, &ns, table, 5, delete_timeout),
                wait_for_id_absent(server, &auth_a, &ns, table, 15, delete_timeout),
                wait_for_id_absent(server, &auth_a, &ns, table, 25, delete_timeout)
            )?;

            for deleted_id in [5, 15, 25] {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("SELECT id FROM {}.{} WHERE id = {}", ns, table, deleted_id),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "SELECT failed: {:?}",
                    resp.error
                );
                let rows = resp
                    .results
                    .first()
                    .and_then(|r| r.rows.as_ref())
                    .map(|r| r.len())
                    .unwrap_or(0);
                anyhow::ensure!(rows == 0, "expected id {} to be deleted", deleted_id);
            }

            for updated_id in [10, 20, 30] {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("SELECT data FROM {}.{} WHERE id = {}", ns, table, updated_id),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "SELECT failed: {:?}",
                    resp.error
                );
                let row = resp
                    .results
                    .first()
                    .and_then(|r| r.row_as_map(0))
                    .ok_or_else(|| anyhow::anyhow!("Missing row for id {}", updated_id))?;
                let val = row
                    .get("data")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing data for id {}", updated_id))?;
                anyhow::ensure!(
                    val == format!("updated_{}", updated_id),
                    "unexpected value for id {}: {}",
                    updated_id,
                    val
                );
            }
        }

        // -----------------------------------------------------------------
        // test_queries_remain_consistent_during_flush
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_consistency_ns");
            let table = "items";
            create_user_table(server, &ns, table, "rows:8").await?;

            for i in 1..=10 {
                let sql = format!("INSERT INTO {ns}.{table} (id, data) VALUES ({i}, 'v{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "INSERT failed: {:?}",
                    resp.error
                );
            }

            let flusher = async {
                for _ in 0..1 {
                    flush_table_and_wait(server, &ns, table).await?;
                    sleep(Duration::from_millis(10)).await;
                }
                anyhow::Ok(())
            };

            let reader = async {
                for _ in 0..2 {
                    let cnt = count_rows(server, &auth_a, &ns, table).await?;
                    anyhow::ensure!(cnt == 10, "expected count=10 during flush, got {}", cnt);
                    sleep(Duration::from_millis(5)).await;
                }
                anyhow::Ok(())
            };

            let (f, r) = tokio::join!(flusher, reader);
            f?;
            r?;

            wait_for_flush_jobs_settled(server, &ns, table).await?;
        }

        // -----------------------------------------------------------------
        // test_flush_preserves_read_visibility
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_vis_ns");
            let table = "items";
            create_user_table(server, &ns, table, "rows:20").await?;

            for i in 1..=10 {
                let insert_sql =
                    format!("INSERT INTO {ns}.{table} (id, data) VALUES ({i}, 'val_{i}')");
                let resp = server.execute_sql_with_auth(&insert_sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert {} failed: {:?}",
                    i,
                    resp.error
                );
            }

            flush_table_and_wait(server, &ns, table).await?;

            for i in 11..=13 {
                let insert_sql =
                    format!("INSERT INTO {ns}.{table} (id, data) VALUES ({i}, 'val_{i}')");
                let resp = server.execute_sql_with_auth(&insert_sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert {} failed: {:?}",
                    i,
                    resp.error
                );
            }

            flush_table_and_wait(server, &ns, table).await?;

            let cnt = count_rows(server, &auth_a, &ns, table).await?;
            anyhow::ensure!(
                cnt == 13,
                "expected all rows visible after multiple flushes, got {}",
                cnt
            );
        }

        // -----------------------------------------------------------------
        // test_updates_persist_across_flushes
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_update_ns");
            let table = "items";
            create_user_table(server, &ns, table, "rows:10").await?;

            let insert = format!("INSERT INTO {ns}.{table} (id, data) VALUES (1, 'old')");
            let resp = server.execute_sql_with_auth(&insert, &auth_a).await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "insert failed: {:?}",
                resp.error
            );

            flush_table_and_wait(server, &ns, table).await?;

            let update = format!("UPDATE {ns}.{table} SET data = 'new' WHERE id = 1");
            let resp = server.execute_sql_with_auth(&update, &auth_a).await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "update failed: {:?}",
                resp.error
            );

            flush_table_and_wait(server, &ns, table).await?;

            let resp = server
                .execute_sql_with_auth(
                    &format!("SELECT data FROM {ns}.{table} WHERE id = 1"),
                    &auth_a,
                )
                .await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "select failed: {:?}",
                resp.error
            );
            let row = resp
                .results
                .first()
                .and_then(|r| r.row_as_map(0))
                .ok_or_else(|| anyhow::anyhow!("Missing row"))?;
            let val = row.get("data").and_then(|v| v.as_str()).unwrap_or("");
            anyhow::ensure!(val == "new", "expected 'new', got '{}'", val);
        }

        // -----------------------------------------------------------------
        // test_deletes_persist_across_flushes
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_delete_ns");
            let table = "items";
            create_user_table(server, &ns, table, "rows:15").await?;

            for i in 1..=5 {
                let sql = format!("INSERT INTO {ns}.{table} (id, data) VALUES ({i}, 'v{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert failed: {:?}",
                    resp.error
                );
            }

            flush_table_and_wait(server, &ns, table).await?;

            let delete_sql = format!("DELETE FROM {ns}.{table} WHERE id = 3");
            let resp = server.execute_sql_with_auth(&delete_sql, &auth_a).await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "delete failed: {:?}",
                resp.error
            );

            flush_table_and_wait(server, &ns, table).await?;

            let cnt = count_rows(server, &auth_a, &ns, table).await?;
            anyhow::ensure!(cnt == 4, "delete should persist after flush, got {}", cnt);

            let resp = server
                .execute_sql_with_auth(
                    &format!("SELECT id FROM {ns}.{table} WHERE id = 3"),
                    &auth_a,
                )
                .await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "select failed: {:?}",
                resp.error
            );
            let rows =
                resp.results.first().and_then(|r| r.rows.as_ref()).map(|r| r.len()).unwrap_or(0);
            anyhow::ensure!(rows == 0, "expected id=3 to be deleted");
        }

        // -----------------------------------------------------------------
        // test_interleaved_batches_and_flushes
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_interleave_ns");
            let table = "items";
            create_user_table(server, &ns, table, "rows:25").await?;

            for i in 1..=10 {
                let sql = format!("INSERT INTO {ns}.{table} (id, data) VALUES ({i}, 'p{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert failed: {:?}",
                    resp.error
                );
            }
            flush_table_and_wait(server, &ns, table).await?;

            for i in 11..=15 {
                let sql = format!("INSERT INTO {ns}.{table} (id, data) VALUES ({i}, 'p{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert failed: {:?}",
                    resp.error
                );
            }
            for id in [5] {
                let sql = format!("UPDATE {ns}.{table} SET data = 'upd_{id}' WHERE id = {id}");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "update failed: {:?}",
                    resp.error
                );
            }
            flush_table_and_wait(server, &ns, table).await?;

            for id in [3] {
                let sql = format!("DELETE FROM {ns}.{table} WHERE id = {id}");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "delete failed: {:?}",
                    resp.error
                );
            }
            flush_table_and_wait(server, &ns, table).await?;

            let cnt = count_rows(server, &auth_a, &ns, table).await?;
            anyhow::ensure!(cnt == 14, "should reflect inserts minus deletions, got {}", cnt);

            for id in [5] {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("SELECT data FROM {ns}.{table} WHERE id = {id}"),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "select failed: {:?}",
                    resp.error
                );
                let row = resp
                    .results
                    .first()
                    .and_then(|r| r.row_as_map(0))
                    .ok_or_else(|| anyhow::anyhow!("Missing row for id {}", id))?;
                let val = row.get("data").and_then(|v| v.as_str()).unwrap_or("");
                anyhow::ensure!(
                    val == format!("upd_{}", id),
                    "unexpected value for id {}: {}",
                    id,
                    val
                );
            }

            for id in [3] {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("SELECT id FROM {ns}.{table} WHERE id = {id}"),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "select failed: {:?}",
                    resp.error
                );
                let rows = resp
                    .results
                    .first()
                    .and_then(|r| r.rows.as_ref())
                    .map(|r| r.len())
                    .unwrap_or(0);
                anyhow::ensure!(rows == 0, "expected id {} to be deleted", id);
            }
        }

        // -----------------------------------------------------------------
        // test_flush_isolation_between_tables
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_iso_ns");
            let t1 = "alpha";
            let t2 = "beta";
            create_user_table(server, &ns, t1, "rows:5").await?;
            create_user_table(server, &ns, t2, "rows:5").await?;

            for i in 1..=4 {
                let sql = format!("INSERT INTO {ns}.{t1} (id, data) VALUES ({i}, 'a{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert failed: {:?}",
                    resp.error
                );
            }
            for i in 1..=3 {
                let sql = format!("INSERT INTO {ns}.{t2} (id, data) VALUES ({i}, 'b{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert failed: {:?}",
                    resp.error
                );
            }

            flush_table_and_wait(server, &ns, t1).await?;

            for i in 4..=5 {
                let sql = format!("INSERT INTO {ns}.{t2} (id, data) VALUES ({i}, 'b{i}')");
                let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "insert failed: {:?}",
                    resp.error
                );
            }

            flush_table_and_wait(server, &ns, t2).await?;

            let cnt1 = count_rows(server, &auth_a, &ns, t1).await?;
            let cnt2 = count_rows(server, &auth_a, &ns, t2).await?;
            anyhow::ensure!(cnt1 == 4, "expected t1 count=4, got {}", cnt1);
            anyhow::ensure!(cnt2 == 5, "expected t2 count=5, got {}", cnt2);
        }

        // -----------------------------------------------------------------
        // test_concurrent_flushes_across_tables
        // -----------------------------------------------------------------
        {
            let ns = unique_namespace("flush_concurrent_multi_ns");
            let t1 = "alpha";
            let t2 = "beta";
            create_user_table(server, &ns, t1, "rows:30").await?;
            create_user_table(server, &ns, t2, "rows:30").await?;

            let writer1 = async {
                for i in 1..=30 {
                    let sql = format!("INSERT INTO {ns}.{t1} (id, data) VALUES ({i}, 'a{i}')");
                    let resp = server.execute_sql_with_auth(&sql, &auth_a).await?;
                    anyhow::ensure!(
                        resp.status == ResponseStatus::Success,
                        "t1 insert {} failed: {:?}",
                        i,
                        resp.error
                    );
                    if i % 10 == 0 {
                        let upd = format!("UPDATE {ns}.{t1} SET data = 'upd_{i}' WHERE id = {i}");
                        let resp = server.execute_sql_with_auth(&upd, &auth_a).await?;
                        anyhow::ensure!(
                            resp.status == ResponseStatus::Success,
                            "t1 update {} failed: {:?}",
                            i,
                            resp.error
                        );
                    }
                }
                anyhow::Ok(())
            };

            let writer2 = async {
                for i in 1..=30 {
                    let sql = format!("INSERT INTO {ns}.{t2} (id, data) VALUES ({i}, 'b{i}')");
                    let resp = server.execute_sql_with_auth(&sql, &auth_b).await?;
                    anyhow::ensure!(
                        resp.status == ResponseStatus::Success,
                        "t2 insert {} failed: {:?}",
                        i,
                        resp.error
                    );
                    if i % 10 == 0 {
                        let del = format!("DELETE FROM {ns}.{t2} WHERE id = {}", i - 5);
                        let resp = server.execute_sql_with_auth(&del, &auth_b).await?;
                        anyhow::ensure!(
                            resp.status == ResponseStatus::Success,
                            "t2 delete failed: {:?}",
                            resp.error
                        );
                    }
                }
                anyhow::Ok(())
            };

            let flusher = async {
                for _ in 0..2 {
                    flush_table_and_wait(server, &ns, t1).await?;
                    flush_table_and_wait(server, &ns, t2).await?;
                    sleep(Duration::from_millis(10)).await;
                }
                anyhow::Ok(())
            };

            let (w1, w2, f) = tokio::join!(writer1, writer2, flusher);
            w1?;
            w2?;
            f?;

            flush_table_and_wait(server, &ns, t1).await?;
            flush_table_and_wait(server, &ns, t2).await?;

            let cnt1 = count_rows(server, &auth_a, &ns, t1).await?;
            anyhow::ensure!(cnt1 == 30, "t1 expected 30 rows, got {}", cnt1);

            for id in [5, 15, 25] {
                let resp = server
                    .execute_sql_with_auth(
                        &format!("DELETE FROM {ns}.{t2} WHERE id = {id}"),
                        &auth_b,
                    )
                    .await?;
                anyhow::ensure!(
                    resp.status == ResponseStatus::Success,
                    "t2 delete {} failed: {:?}",
                    id,
                    resp.error
                );
            }
            flush_table_and_wait(server, &ns, t2).await?;

            let delete_timeout = Duration::from_secs(20);
            tokio::try_join!(
                wait_for_id_absent(server, &auth_b, &ns, t2, 5, delete_timeout),
                wait_for_id_absent(server, &auth_b, &ns, t2, 15, delete_timeout),
                wait_for_id_absent(server, &auth_b, &ns, t2, 25, delete_timeout)
            )?;

            wait_for_row_count_in_range(server, &auth_b, &ns, t2, 27, 28, Duration::from_secs(5))
                .await
                .map_err(|e| anyhow::anyhow!("expected 27-28 rows after deletes, {}", e))?;

            let resp = server
                .execute_sql_with_auth(
                    &format!("SELECT data FROM {ns}.{t1} WHERE id = 30"),
                    &auth_a,
                )
                .await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "select failed: {:?}",
                resp.error
            );
            let row = resp
                .results
                .first()
                .and_then(|r| r.row_as_map(0))
                .ok_or_else(|| anyhow::anyhow!("Missing row"))?;
            let val = row.get("data").and_then(|v| v.as_str()).unwrap_or("");
            anyhow::ensure!(val == "upd_30", "expected upd_30, got {}", val);

            let resp = server
                .execute_sql_with_auth(&format!("SELECT id FROM {ns}.{t2} WHERE id = 15"), &auth_b)
                .await?;
            anyhow::ensure!(
                resp.status == ResponseStatus::Success,
                "select failed: {:?}",
                resp.error
            );
            let rows =
                resp.results.first().and_then(|r| r.rows.as_ref()).map(|r| r.len()).unwrap_or(0);
            anyhow::ensure!(rows == 0, "expected id=15 to be deleted");
        }

        Ok(())
    })
    .await
    .expect("test_flush_concurrency_and_correctness_over_http");
}
