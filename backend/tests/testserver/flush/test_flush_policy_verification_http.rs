//! Flush policy + Parquet output verification over the real HTTP SQL API.
//!
//! Migrates coverage from:
//! - tests/integration/flush/test_manual_flush_verification.rs
//! - tests/integration/flush/test_automatic_flushing.rs
//! - tests/integration/flush/test_automatic_flushing_comprehensive.rs
//! - tests/integration/flush/test_flush_operations.rs

use super::test_support::auth_helper::create_user_auth_header_with_id;
use super::test_support::consolidated_helpers::{unique_namespace, unique_table};
use super::test_support::flush::{
    count_parquet_files_for_table, flush_table_and_wait, wait_for_parquet_files_for_table,
    wait_for_parquet_files_for_user_table,
};
use super::test_support::jobs::{
    extract_cleanup_job_id, wait_for_job_completion, wait_for_path_absent,
};
use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_jobs::FlushScheduler;
use tokio::time::Duration;

async fn count_matching_flush_jobs(
    server: &super::test_support::http_server::HttpTestServer,
    ns: &str,
    table: &str,
) -> anyhow::Result<usize> {
    let resp = server
        .execute_sql(
            "SELECT parameters FROM system.jobs WHERE job_type = 'flush' ORDER BY created_at DESC LIMIT 500",
        )
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "failed to query system.jobs: {:?}",
        resp.error
    );

    Ok(resp
        .rows_as_maps()
        .into_iter()
        .filter(|row| {
            row.get("parameters")
                .and_then(|value| value.as_str())
                .map(|params| params.contains(ns) && params.contains(table))
                .unwrap_or(false)
        })
        .count())
}

fn count_parquet_files_for_user(
    storage_root: &std::path::Path,
    ns: &str,
    table: &str,
    user_id: &str,
) -> usize {
    super::test_support::flush::find_parquet_files(storage_root)
        .into_iter()
        .filter(|path| {
            let text = path.to_string_lossy();
            text.contains(ns) && text.contains(table) && text.contains(user_id)
        })
        .count()
}

async fn query_count_with_auth(
    server: &super::test_support::http_server::HttpTestServer,
    auth_header: &str,
    ns: &str,
    table: &str,
) -> anyhow::Result<i64> {
    let resp = server
        .execute_sql_with_auth(
            &format!("SELECT COUNT(*) AS count FROM {}.{}", ns, table),
            auth_header,
        )
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success, "count query failed: {:?}", resp.error);

    let row = resp
        .results
        .first()
        .and_then(|result| result.row_as_map(0))
        .ok_or_else(|| anyhow::anyhow!("missing COUNT(*) row for {}.{}", ns, table))?;

    let value = row
        .get("count")
        .ok_or_else(|| anyhow::anyhow!("missing COUNT(*) value for {}.{}", ns, table))?;

    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|count| i64::try_from(count).ok()))
        .or_else(|| value.as_f64().map(|count| count as i64))
        .or_else(|| value.as_str().and_then(|count| count.parse::<i64>().ok()))
        .ok_or_else(|| anyhow::anyhow!("missing integer count for {}.{}", ns, table))
}

#[tokio::test]
#[ntest::timeout(180000)] // 3 minutes max for comprehensive flush policy test
async fn test_flush_policy_and_parquet_output_over_http() {
    (async {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("flush_policy");

    let resp = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let user_a = unique_table("alice");
    let user_b = unique_table("bob");
    let (auth_a, user_a_id) =
        create_user_auth_header_with_id(server, &user_a, "UserPass123!", &Role::User).await?;
    let (auth_b, user_b_id) =
        create_user_auth_header_with_id(server, &user_b, "UserPass123!", &Role::User).await?;

            // -----------------------------------------------------------------
            // USER table: manual flush creates parquet + respects row threshold
            // -----------------------------------------------------------------
            {
                let table = "messages";
                let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, content TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:25')",
                            ns, table
                        ),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE TABLE failed: {:?}", resp.error);

                for i in 0..25 {
                    let resp = server
                        .execute_sql_with_auth(
                            &format!("INSERT INTO {}.{} (id, content) VALUES ({}, 'msg-{}')", ns, table, i, i),
                            &auth_a,
                        )
                        .await?;
                    anyhow::ensure!(resp.status == ResponseStatus::Success, "insert failed: {:?}", resp.error);
                }

                flush_table_and_wait(server, &ns, table).await?;
                let _ = wait_for_parquet_files_for_user_table(server, &ns, table, &user_a_id, 1, Duration::from_secs(40)).await?;
            }

            // -----------------------------------------------------------------
            // USER table: multiple flush batches produce additional parquet
            // -----------------------------------------------------------------
            {
                let table = "events";
                let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, payload TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:100,interval:30')",
                            ns, table
                        ),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(resp.status == ResponseStatus::Success);

                for batch in 0..3 {
                    for i in 0..10 {
                        let id = batch * 100 + i;
                        let resp = server
                            .execute_sql_with_auth(
                                &format!("INSERT INTO {}.{} (id, payload) VALUES ({}, 'p-{}')", ns, table, id, id),
                                &auth_a,
                            )
                            .await?;
                        anyhow::ensure!(resp.status == ResponseStatus::Success);
                    }

                    flush_table_and_wait(server, &ns, table).await?;
                    let _ = wait_for_parquet_files_for_table(
                        server,
                        &ns,
                        table,
                        1,
                        Duration::from_secs(60),
                    )
                    .await?;
                }
            }

            // -----------------------------------------------------------------
            // USER table: multi-user partitions produce per-user parquet output
            // -----------------------------------------------------------------
            {
                let table = "inbox";
                let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, body TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:20')",
                            ns, table
                        ),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(resp.status == ResponseStatus::Success);

                for i in 0..15 {
                    let resp = server
                        .execute_sql_with_auth(
                            &format!("INSERT INTO {}.{} (id, body) VALUES ({}, '{}-msg-{}')", ns, table, i, user_a, i),
                            &auth_a,
                        )
                        .await?;
                    anyhow::ensure!(resp.status == ResponseStatus::Success);
                }
                for i in 100..115 {
                    let resp = server
                        .execute_sql_with_auth(
                            &format!("INSERT INTO {}.{} (id, body) VALUES ({}, '{}-msg-{}')", ns, table, i, user_b, i),
                            &auth_b,
                        )
                        .await?;
                    anyhow::ensure!(resp.status == ResponseStatus::Success);
                }

                flush_table_and_wait(server, &ns, table).await?;

                let _ = wait_for_parquet_files_for_user_table(server, &ns, table, &user_a_id, 1, Duration::from_secs(40)).await?;
                let _ = wait_for_parquet_files_for_user_table(server, &ns, table, &user_b_id, 1, Duration::from_secs(40)).await?;
            }

            // -----------------------------------------------------------------
            // SHARED table: manual flush creates parquet
            // -----------------------------------------------------------------
            {
                let table = "audit_events";
                let resp = server
                    .execute_sql(&format!(
                        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, entry TEXT) WITH (TYPE='SHARED', STORAGE_ID='local', FLUSH_POLICY='rows:5')",
                        ns, table
                    ))
                    .await?;
                anyhow::ensure!(resp.status == ResponseStatus::Success);

                for i in 0..8 {
                    let resp = server
                        .execute_sql(&format!(
                            "INSERT INTO {}.{} (id, entry) VALUES ({}, 'entry-{}')",
                            ns, table, i, i
                        ))
                        .await?;
                    anyhow::ensure!(resp.status == ResponseStatus::Success);
                }

                flush_table_and_wait(server, &ns, table).await?;
                let _ = wait_for_parquet_files_for_table(server, &ns, table, 1, Duration::from_secs(40)).await?;
            }

            // -----------------------------------------------------------------
            // DROP TABLE: wait for cleanup job + parquet removal (smoke)
            // -----------------------------------------------------------------
            {
                let table = "drop_cleanup";
                let resp = server
                    .execute_sql_with_auth(
                        &format!(
                            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, body TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:2')",
                            ns, table
                        ),
                        &auth_a,
                    )
                    .await?;
                anyhow::ensure!(resp.status == ResponseStatus::Success);

                for i in 0..2 {
                    let resp = server
                        .execute_sql_with_auth(
                            &format!("INSERT INTO {}.{} (id, body) VALUES ({}, 'x')", ns, table, i),
                            &auth_a,
                        )
                        .await?;
                    anyhow::ensure!(resp.status == ResponseStatus::Success);
                }

                flush_table_and_wait(server, &ns, table).await?;
                let parquet = wait_for_parquet_files_for_user_table(
                    server,
                    &ns,
                    table,
                    &user_a_id,
                    1,
                    Duration::from_secs(90),
                )
                .await?;
                let parquet_dir = parquet
                    .first()
                    .and_then(|p| p.parent())
                    .map(|p| p.to_path_buf())
                    .ok_or_else(|| anyhow::anyhow!("missing parquet parent dir"))?;

                let drop_resp = server
                    .execute_sql(&format!("DROP TABLE {}.{}", ns, table))
                    .await?;
                anyhow::ensure!(drop_resp.status == ResponseStatus::Success);

                let msg = drop_resp
                    .results
                    .first()
                    .and_then(|r| r.message.as_deref())
                    .unwrap_or("");

                if let Some(job_id) = extract_cleanup_job_id(msg) {
                    let _ = wait_for_job_completion(server, &job_id, Duration::from_secs(15)).await?;
                }

                // Allow async filestore cleanup to finish.
                anyhow::ensure!(
                    wait_for_path_absent(&parquet_dir, Duration::from_secs(5)).await,
                    "expected parquet dir removed after drop: {}",
                    parquet_dir.display()
                );
            }

    Ok(())
    })
        .await
        .expect("test_flush_policy_and_parquet_output_over_http");
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_automatic_flush_waits_for_row_limit_before_writing_parquet_over_http() {
    (async {
        let _guard = super::test_support::http_server::acquire_test_lock().await;
        let server = super::test_support::http_server::get_global_server().await;
        let app_context = server.app_context();
        let jobs_manager = app_context.job_manager();
        let ns = unique_namespace("flush_row_limit");
        let table = "threshold_guard";

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE NAMESPACE failed");

        let resp = server
            .execute_sql(&format!(
                "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, payload TEXT) WITH (TYPE='SHARED', STORAGE_ID='local', FLUSH_POLICY='rows:5')",
                ns, table
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE TABLE failed: {:?}", resp.error);

        let storage_root = server.storage_root();
        anyhow::ensure!(count_parquet_files_for_table(&storage_root, &ns, table) == 0);

        for i in 0..4 {
            let resp = server
                .execute_sql(&format!(
                    "INSERT INTO {}.{} (id, payload) VALUES ({}, 'row-{}')",
                    ns, table, i, i
                ))
                .await?;
            anyhow::ensure!(resp.status == ResponseStatus::Success, "INSERT failed: {:?}", resp.error);
        }

        FlushScheduler::check_and_schedule(&app_context, jobs_manager.as_ref()).await?;

        anyhow::ensure!(
            count_matching_flush_jobs(server, &ns, table).await? == 0,
            "flush job should not be created below the row threshold"
        );
        anyhow::ensure!(
            count_parquet_files_for_table(&storage_root, &ns, table) == 0,
            "parquet files should not exist below the row threshold"
        );

        let resp = server
            .execute_sql(&format!(
                "INSERT INTO {}.{} (id, payload) VALUES (4, 'row-4')",
                ns, table
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "threshold INSERT failed: {:?}", resp.error);

        FlushScheduler::check_and_schedule(&app_context, jobs_manager.as_ref()).await?;

        let job_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if count_matching_flush_jobs(server, &ns, table).await? > 0 {
                break;
            }
            if tokio::time::Instant::now() >= job_deadline {
                anyhow::bail!("expected a flush job to be created once the row limit was reached");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let flush_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if count_parquet_files_for_table(&storage_root, &ns, table) > 0 {
                break;
            }

            let _ = jobs_manager.run_once_for_tests().await?;

            if tokio::time::Instant::now() >= flush_deadline {
                anyhow::bail!("expected parquet files to be written after the threshold flush job executed");
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let parquet_files =
            wait_for_parquet_files_for_table(server, &ns, table, 1, Duration::from_secs(20))
                .await?;
        anyhow::ensure!(
            !parquet_files.is_empty(),
            "expected at least one parquet file after threshold flush"
        );
        for file in &parquet_files {
            let metadata = std::fs::metadata(file)?;
            anyhow::ensure!(
                metadata.len() > 0,
                "expected non-empty parquet file: {}",
                file.display()
            );
        }

        Ok(())
    })
    .await
    .expect("test_automatic_flush_waits_for_row_limit_before_writing_parquet_over_http");
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_automatic_user_flush_waits_for_row_limit_and_writes_only_user_files_over_http() {
    (async {
        let _guard = super::test_support::http_server::acquire_test_lock().await;
        let server = super::test_support::http_server::get_global_server().await;
        let app_context = server.app_context();
        let jobs_manager = app_context.job_manager();
        let ns = unique_namespace("user_flush_row_limit");
        let table = "user_threshold_guard";
        let user_a = unique_table("threshold_alice");
        let user_b = unique_table("threshold_bob");

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE NAMESPACE failed");

        let (auth_a, user_a_id) =
            create_user_auth_header_with_id(server, &user_a, "UserPass123!", &Role::User)
                .await?;
        let (auth_b, user_b_id) =
            create_user_auth_header_with_id(server, &user_b, "UserPass123!", &Role::User)
                .await?;

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, payload TEXT) WITH (TYPE='USER', STORAGE_ID='local', FLUSH_POLICY='rows:5')",
                    ns, table
                ),
                &auth_a,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE TABLE failed: {:?}", resp.error);

        let storage_root = server.storage_root();
        let initial_job_count = count_matching_flush_jobs(server, &ns, table).await?;
        anyhow::ensure!(count_parquet_files_for_user(&storage_root, &ns, table, &user_a_id) == 0);
        anyhow::ensure!(count_parquet_files_for_user(&storage_root, &ns, table, &user_b_id) == 0);

        for i in 0..4 {
            let resp = server
                .execute_sql_with_auth(
                    &format!(
                        "INSERT INTO {}.{} (id, payload) VALUES ({}, 'user-a-{}')",
                        ns, table, i, i
                    ),
                    &auth_a,
                )
                .await?;
            anyhow::ensure!(resp.status == ResponseStatus::Success, "INSERT failed: {:?}", resp.error);
        }

        FlushScheduler::check_and_schedule(&app_context, jobs_manager.as_ref()).await?;

        anyhow::ensure!(
            count_matching_flush_jobs(server, &ns, table).await? == initial_job_count,
            "user-table flush job should not be created below the row threshold"
        );
        anyhow::ensure!(
            count_parquet_files_for_user(&storage_root, &ns, table, &user_a_id) == 0,
            "no parquet files should exist for user A below the row threshold"
        );
        anyhow::ensure!(
            count_parquet_files_for_user(&storage_root, &ns, table, &user_b_id) == 0,
            "no parquet files should exist for user B below the row threshold"
        );
        anyhow::ensure!(query_count_with_auth(server, &auth_a, &ns, table).await? == 4);
        anyhow::ensure!(query_count_with_auth(server, &auth_b, &ns, table).await? == 0);

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "INSERT INTO {}.{} (id, payload) VALUES (4, 'user-a-4')",
                    ns, table
                ),
                &auth_a,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "threshold INSERT failed: {:?}", resp.error);

        FlushScheduler::check_and_schedule(&app_context, jobs_manager.as_ref()).await?;

        let job_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if count_matching_flush_jobs(server, &ns, table).await? > initial_job_count {
                break;
            }
            if tokio::time::Instant::now() >= job_deadline {
                anyhow::bail!("expected a user-table flush job once the row limit was reached");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let flush_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if count_parquet_files_for_user(&storage_root, &ns, table, &user_a_id) > 0 {
                break;
            }

            let _ = jobs_manager.run_once_for_tests().await?;

            if tokio::time::Instant::now() >= flush_deadline {
                anyhow::bail!("expected user-table parquet files after the threshold flush job executed");
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let parquet_files = wait_for_parquet_files_for_user_table(
            server,
            &ns,
            table,
            &user_a_id,
            1,
            Duration::from_secs(20),
        )
        .await?;
        anyhow::ensure!(
            count_parquet_files_for_user(&storage_root, &ns, table, &user_b_id) == 0,
            "flushing user A's rows should not create parquet files for user B"
        );
        for file in &parquet_files {
            let metadata = std::fs::metadata(file)?;
            anyhow::ensure!(
                metadata.len() > 0,
                "expected non-empty user parquet file: {}",
                file.display()
            );
        }
        anyhow::ensure!(query_count_with_auth(server, &auth_a, &ns, table).await? == 5);
        anyhow::ensure!(query_count_with_auth(server, &auth_b, &ns, table).await? == 0);

        let files_after_first_flush = count_parquet_files_for_user(&storage_root, &ns, table, &user_a_id);
        let jobs_after_first_flush = count_matching_flush_jobs(server, &ns, table).await?;

        for i in 100..104 {
            let resp = server
                .execute_sql_with_auth(
                    &format!(
                        "INSERT INTO {}.{} (id, payload) VALUES ({}, 'user-a-{}')",
                        ns, table, i, i
                    ),
                    &auth_a,
                )
                .await?;
            anyhow::ensure!(resp.status == ResponseStatus::Success, "second-batch INSERT failed: {:?}", resp.error);
        }

        FlushScheduler::check_and_schedule(&app_context, jobs_manager.as_ref()).await?;

        anyhow::ensure!(
            count_matching_flush_jobs(server, &ns, table).await? == jobs_after_first_flush,
            "row threshold should reset after a flush and not schedule a new job at 4 rows"
        );
        anyhow::ensure!(
            count_parquet_files_for_user(&storage_root, &ns, table, &user_a_id) == files_after_first_flush,
            "no additional parquet files should be created for the next batch below threshold"
        );
        anyhow::ensure!(query_count_with_auth(server, &auth_a, &ns, table).await? == 9);

        Ok(())
    })
    .await
    .expect("test_automatic_user_flush_waits_for_row_limit_and_writes_only_user_files_over_http");
}
