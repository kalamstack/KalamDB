//! Integration tests for STORAGE COMPACT commands
//!
//! Covers:
//! - STORAGE COMPACT TABLE creates and completes a compaction job
//! - STORAGE COMPACT ALL IN <namespace> creates jobs for user/shared tables only
//! - Unsupported table types return validation errors

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use anyhow::Result;
use kalam_client::models::ResponseStatus;
use kalamdb_system::Manifest;
use tokio::time::{sleep, Instant};

use super::test_support::{fixtures, TestServer};

fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}_{}", prefix, std::process::id(), id)
}

fn rocksdb_dir(server: &TestServer) -> PathBuf {
    let data_path = server.app_context.config().storage.data_path.clone();
    PathBuf::from(data_path).join("rocksdb")
}

fn latest_sst_mtime(root: &Path) -> Option<SystemTime> {
    fn recurse(dir: &Path, latest: &mut Option<SystemTime>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                recurse(&path, latest);
            } else if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(modified) = meta.modified() {
                        match latest {
                            Some(current) if modified > *current => *latest = Some(modified),
                            None => *latest = Some(modified),
                            _ => {},
                        }
                    }
                }
            }
        }
    }

    let mut latest = None;
    recurse(root, &mut latest);
    latest
}

fn find_manifest_files(root: &Path) -> Vec<PathBuf> {
    fn recurse(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                recurse(&path, out);
            } else if path.file_name().and_then(|name| name.to_str()) == Some("manifest.json") {
                out.push(path);
            }
        }
    }

    let mut out = Vec::new();
    recurse(root, &mut out);
    out
}

fn find_batch_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };

    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("batch-") && name.ends_with(".parquet"))
                .unwrap_or(false)
        })
        .collect()
}

async fn wait_for_compact_jobs(
    server: &TestServer,
    namespace: &str,
    tables: &[String],
    timeout: Duration,
) -> Result<Vec<String>> {
    let deadline = Instant::now() + timeout;

    loop {
        let resp = server
            .execute_sql(
                "SELECT job_id, status, parameters, message FROM system.jobs WHERE job_type = \
                 'compact' ORDER BY created_at DESC LIMIT 50",
            )
            .await;

        if resp.status != ResponseStatus::Success {
            if Instant::now() >= deadline {
                anyhow::bail!("Timed out waiting for system.jobs to be queryable");
            }
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        let rows = resp.results.first().map(|r| r.rows_as_maps()).unwrap_or_default();
        let mut job_ids = Vec::new();
        let mut all_completed = true;

        for table in tables {
            let needle = format!("{}.{}", namespace, table);
            let Some(row) = rows.iter().find(|r| {
                r.get("parameters")
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains(&needle))
                    .unwrap_or(false)
            }) else {
                all_completed = false;
                continue;
            };

            let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");

            match status {
                "completed" => {
                    if let Some(result) = row.get("result").and_then(|v| v.as_str()) {
                        if !result.contains("Compaction completed") {
                            anyhow::bail!(
                                "Compaction job result missing completion message for {}: {}",
                                needle,
                                result
                            );
                        }
                    }
                },
                "failed" | "cancelled" => {
                    anyhow::bail!("Compaction job for {} ended with status {}", needle, status);
                },
                _ => {
                    all_completed = false;
                },
            }

            if let Some(job_id) = row.get("job_id").and_then(|v| v.as_str()) {
                job_ids.push(job_id.to_string());
            }
        }

        if all_completed && job_ids.len() == tables.len() {
            return Ok(job_ids);
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out waiting for compact jobs for {:?} in namespace {}",
                tables,
                namespace
            );
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_flush_job_completed(
    server: &TestServer,
    namespace: &str,
    table: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;

    loop {
        let resp = server
            .execute_sql(
                "SELECT job_type, status, parameters FROM system.jobs WHERE job_type = 'flush'",
            )
            .await;

        if resp.status == ResponseStatus::Success {
            if let Some(first) = resp.results.first() {
                let maps = first.rows_as_maps();
                let maybe_job = maps.iter().find(|row| {
                    row.get("parameters")
                        .and_then(|v| v.as_str())
                        .map(|s| s.contains(namespace) && s.contains(table))
                        .unwrap_or(false)
                });

                if let Some(job) = maybe_job {
                    let status = job.get("status").and_then(|v| v.as_str()).unwrap_or("");
                    if status == "completed" {
                        return Ok(());
                    }
                }
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out waiting for flush job to complete for {}.{}",
                namespace,
                table
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_storage_compact_table_and_all_commands() -> Result<()> {
    let server = TestServer::new_shared().await;
    let namespace = unique_name("compact_ns");
    let user_table = unique_name("compact_user");
    let shared_table = unique_name("compact_shared");

    fixtures::create_namespace(&server, &namespace).await;

    let create_user = format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'USER')",
        namespace, user_table
    );
    let resp = server.execute_sql(&create_user).await;
    assert_eq!(resp.status, ResponseStatus::Success, "create user table failed");

    let create_shared = format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'SHARED')",
        namespace, shared_table
    );
    let resp = server.execute_sql(&create_shared).await;
    assert_eq!(resp.status, ResponseStatus::Success, "create shared table failed");

    for i in 1..=30 {
        let insert = format!(
            "INSERT INTO {}.{} (id, value) VALUES ({}, 'value_{}')",
            namespace, shared_table, i, i
        );
        let resp = server.execute_sql(&insert).await;
        assert_eq!(resp.status, ResponseStatus::Success, "insert shared failed");
    }

    for i in 1..=30 {
        let insert = format!(
            "INSERT INTO {}.{} (id, value) VALUES ({}, 'value_{}')",
            namespace, user_table, i, i
        );
        let resp = server.execute_sql_as_user(&insert, "compact_user").await;
        assert_eq!(resp.status, ResponseStatus::Success, "insert user failed");
    }

    let delete_shared = format!("DELETE FROM {}.{} WHERE id <= 20", namespace, shared_table);
    let resp = server.execute_sql(&delete_shared).await;
    assert_eq!(resp.status, ResponseStatus::Success, "delete shared failed");

    let delete_user = format!("DELETE FROM {}.{} WHERE id <= 20", namespace, user_table);
    let resp = server.execute_sql_as_user(&delete_user, "compact_user").await;
    assert_eq!(resp.status, ResponseStatus::Success, "delete user failed");

    let rocksdb_path = rocksdb_dir(&server);
    let before_compact = latest_sst_mtime(&rocksdb_path);

    let compact_user_sql = format!("STORAGE COMPACT TABLE {}.{}", namespace, user_table);
    let resp = server.execute_sql(&compact_user_sql).await;
    assert_eq!(resp.status, ResponseStatus::Success, "compact user table failed");

    wait_for_compact_jobs(&server, &namespace, &[user_table.clone()], Duration::from_secs(20))
        .await?;

    let compact_all_sql = format!("STORAGE COMPACT ALL IN {}", namespace);
    let resp = server.execute_sql(&compact_all_sql).await;
    assert_eq!(resp.status, ResponseStatus::Success, "compact all failed");

    wait_for_compact_jobs(
        &server,
        &namespace,
        &[user_table.clone(), shared_table.clone()],
        Duration::from_secs(30),
    )
    .await?;

    if let (Some(before), Some(after)) = (before_compact, latest_sst_mtime(&rocksdb_path)) {
        assert!(after >= before, "Expected RocksDB SST files to update after compaction");
    }

    Ok(())
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_storage_compact_rejects_stream_and_empty_namespace() -> Result<()> {
    let server = TestServer::new_shared().await;
    let namespace = unique_name("compact_stream_ns");
    let stream_table = unique_name("compact_stream");

    fixtures::create_namespace(&server, &namespace).await;

    let create_stream = format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'STREAM', TTL_SECONDS = \
         3600)",
        namespace, stream_table
    );
    let resp = server.execute_sql(&create_stream).await;
    assert_eq!(resp.status, ResponseStatus::Success, "create stream table failed");

    let compact_stream = format!("STORAGE COMPACT TABLE {}.{}", namespace, stream_table);
    let resp = server.execute_sql(&compact_stream).await;
    assert_eq!(resp.status, ResponseStatus::Error, "compact stream should fail");
    let message = resp.error.as_ref().map(|e| e.message.clone()).unwrap_or_default();
    assert!(message.contains("STREAM"), "expected STREAM error, got: {}", message);

    let compact_all = format!("STORAGE COMPACT ALL IN {}", namespace);
    let resp = server.execute_sql(&compact_all).await;
    assert_eq!(resp.status, ResponseStatus::Error, "compact all should fail");
    let message = resp.error.as_ref().map(|e| e.message.clone()).unwrap_or_default();
    assert!(
        message.contains("No compactable tables"),
        "expected no compactable tables error, got: {}",
        message
    );

    Ok(())
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn test_storage_compact_cleans_empty_shared_segments_and_parquet_files() -> Result<()> {
    let server = TestServer::new_shared().await;
    let namespace = unique_name("compact_cleanup_ns");
    let table = unique_name("compact_cleanup_shared");

    fixtures::create_namespace(&server, &namespace).await;

    let create_shared = format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'SHARED', FLUSH_POLICY \
         = 'rows:5')",
        namespace, table
    );
    let resp = server.execute_sql(&create_shared).await;
    assert_eq!(resp.status, ResponseStatus::Success, "create shared table failed");

    for i in 1..=6 {
        let insert = format!(
            "INSERT INTO {}.{} (id, value) VALUES ({}, 'value_{}')",
            namespace, table, i, i
        );
        let resp = server.execute_sql(&insert).await;
        assert_eq!(resp.status, ResponseStatus::Success, "insert shared failed");
    }

    let flush_sql = format!("STORAGE FLUSH TABLE {}.{}", namespace, table);
    let resp = server.execute_sql(&flush_sql).await;
    assert_eq!(resp.status, ResponseStatus::Success, "flush shared table failed");

    wait_for_flush_job_completed(&server, &namespace, &table, Duration::from_secs(30)).await?;

    let storage_root =
        PathBuf::from(server.app_context.config().storage.data_path.clone()).join("storage");
    let manifest_path = find_manifest_files(&storage_root)
        .into_iter()
        .find(|path| {
            path.to_string_lossy().contains(&namespace) && path.to_string_lossy().contains(&table)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Expected manifest.json for {}.{} under {}",
                namespace,
                table,
                storage_root.display()
            )
        })?;

    let table_dir = manifest_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("manifest.json missing parent directory"))?;
    let batch_files_before = find_batch_files(table_dir);
    assert!(
        !batch_files_before.is_empty(),
        "Expected flushed parquet files before compaction cleanup in {}",
        table_dir.display()
    );

    let delete_sql = format!("DELETE FROM {}.{} WHERE id >= 1", namespace, table);
    let resp = server.execute_sql(&delete_sql).await;
    assert_eq!(resp.status, ResponseStatus::Success, "delete shared failed");

    let compact_sql = format!("STORAGE COMPACT TABLE {}.{}", namespace, table);
    let resp = server.execute_sql(&compact_sql).await;
    assert_eq!(resp.status, ResponseStatus::Success, "compact shared table failed");

    wait_for_compact_jobs(
        &server,
        &namespace,
        std::slice::from_ref(&table),
        Duration::from_secs(20),
    )
    .await?;

    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let manifest_json = std::fs::read_to_string(&manifest_path)?;
        let manifest: Manifest = serde_json::from_str(&manifest_json)?;
        let batch_files_after = find_batch_files(table_dir);

        if manifest.segments.is_empty() && batch_files_after.is_empty() {
            return Ok(());
        }

        if Instant::now() >= cleanup_deadline {
            anyhow::bail!(
                "Expected empty manifest and no parquet files after compaction cleanup for {}.{} \
                 (segments={}, files={:?})",
                namespace,
                table,
                manifest.segments.len(),
                batch_files_after
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}
