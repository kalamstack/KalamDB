//! Manifest flush behavior over the real HTTP SQL API.

use super::test_support::consolidated_helpers::unique_namespace;
use kalam_client::models::ResponseStatus;
use kalamdb_system::Manifest;
use tokio::time::{sleep, Duration, Instant};

fn find_manifest_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn recurse(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                recurse(&path, out);
            } else if path.file_name().and_then(|n| n.to_str()) == Some("manifest.json") {
                out.push(path);
            }
        }
    }

    let mut out = Vec::new();
    recurse(root, &mut out);
    out
}

fn find_batch_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
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

async fn wait_for_flush_job_completed(
    server: &super::test_support::http_server::HttpTestServer,
    ns: &str,
    table: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resp = server
            .execute_sql(
                "SELECT job_type, status, parameters FROM system.jobs WHERE job_type = 'flush'",
            )
            .await?;

        if resp.status == ResponseStatus::Success {
            if let Some(first) = resp.results.first() {
                let maps = first.rows_as_maps();
                let maybe_job = maps.iter().find(|row| {
                    row.get("parameters")
                        .and_then(|v| v.as_str())
                        .map(|s| s.contains(ns) && s.contains(table))
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
            anyhow::bail!("Timed out waiting for flush job to complete for {}.{}", ns, table);
        }
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_shared_flush_creates_manifest_json_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("test_manifest_flush");
    let table = "products";

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
                .execute_sql(&format!(
                    "CREATE TABLE {}.{} (id INT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED', FLUSH_POLICY = 'rows:5')",
                    namespace, table
                ))
                .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    for i in 1..=7 {
        let resp = server
            .execute_sql(&format!(
                "INSERT INTO {}.{} (id, name) VALUES ({}, 'item_{}')",
                namespace, table, i, i
            ))
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    let resp = server
        .execute_sql(&format!("STORAGE FLUSH TABLE {}.{}", namespace, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    wait_for_flush_job_completed(server, &namespace, table).await?;

    let storage_root = server.storage_root();
    let deadline = Instant::now() + Duration::from_secs(5);
    let manifest_path = loop {
        let candidates = find_manifest_files(&storage_root);
        if let Some(path) = candidates.iter().find(|p| {
            p.to_string_lossy().contains(&namespace) && p.to_string_lossy().contains(table)
        }) {
            break path.to_path_buf();
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Expected manifest.json for {}.{} under {} (found: {:?})",
                namespace,
                table,
                storage_root.display(),
                candidates
            );
        }

        sleep(Duration::from_millis(50)).await;
    };

    let manifest_json = std::fs::read_to_string(&manifest_path)?;
    let manifest: Manifest = serde_json::from_str(&manifest_json)?;

    assert_eq!(manifest.table_id.namespace_id().as_str(), namespace);
    assert_eq!(manifest.table_id.table_name().as_str(), table);
    assert!(manifest.user_id.is_none(), "Shared table should have no user_id");
    assert!(!manifest.segments.is_empty(), "Should have at least one segment");

    let resp = server
        .execute_sql(&format!("SELECT COUNT(*) AS count FROM {}.{}", namespace, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    Ok(())
}

#[tokio::test]
async fn test_shared_flush_cleans_empty_segments_and_parquet_files() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("test_manifest_empty_cleanup");
    let table = "metrics";

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (id INT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED', FLUSH_POLICY = 'rows:5')",
            namespace, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    for i in 1..=6 {
        let resp = server
            .execute_sql(&format!(
                "INSERT INTO {}.{} (id, name) VALUES ({}, 'item_{}')",
                namespace, table, i, i
            ))
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    let resp = server
        .execute_sql(&format!("STORAGE FLUSH TABLE {}.{}", namespace, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    wait_for_flush_job_completed(server, &namespace, table).await?;

    let storage_root = server.storage_root();
    let deadline = Instant::now() + Duration::from_secs(5);
    let manifest_path = loop {
        let candidates = find_manifest_files(&storage_root);
        if let Some(path) = candidates.iter().find(|p| {
            p.to_string_lossy().contains(&namespace) && p.to_string_lossy().contains(table)
        }) {
            break path.to_path_buf();
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Expected manifest.json for {}.{} under {} (found: {:?})",
                namespace,
                table,
                storage_root.display(),
                candidates
            );
        }

        sleep(Duration::from_millis(50)).await;
    };

    let table_dir = manifest_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("manifest.json missing parent directory"))?
        .to_path_buf();
    let batch_files_before = find_batch_files(&table_dir);
    assert!(
        !batch_files_before.is_empty(),
        "Expected flushed parquet files before cleanup in {}",
        table_dir.display()
    );

    let resp = server
        .execute_sql(&format!("DELETE FROM {}.{} WHERE id >= 1", namespace, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!("STORAGE FLUSH TABLE {}.{}", namespace, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    wait_for_flush_job_completed(server, &namespace, table).await?;

    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let manifest_json = std::fs::read_to_string(&manifest_path)?;
        let manifest: Manifest = serde_json::from_str(&manifest_json)?;
        let batch_files_after = find_batch_files(&table_dir);

        if manifest.segments.is_empty() && batch_files_after.is_empty() {
            return Ok(());
        }

        if Instant::now() >= cleanup_deadline {
            anyhow::bail!(
                "Expected empty manifest and no parquet files after cleanup for {}.{} (segments={}, files={:?})",
                namespace,
                table,
                manifest.segments.len(),
                batch_files_after
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}
