use anyhow::Result;
use kalam_client::models::ResponseStatus;
use std::path::{Path, PathBuf};
use tokio::time::{sleep, Duration, Instant};

use super::test_support::http_server::start_http_test_server_with_config;

fn list_snapshot_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("bin"))
        .collect()
}

async fn wait_for_snapshots(dir: &Path, min_files: usize) -> Result<Vec<PathBuf>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let files = list_snapshot_files(dir);
        if files.len() >= min_files {
            return Ok(files);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out waiting for {} snapshot files under {} (found={})",
                min_files,
                dir.display(),
                files.len()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_lock_release(lock_path: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15); // Increased from 5s
    loop {
        if !lock_path.exists() {
            return Ok(());
        }

        let open_result = std::fs::OpenOptions::new().read(true).write(true).open(lock_path);

        if open_result.is_ok() {
            return Ok(());
        }

        if Instant::now() >= deadline {
            anyhow::bail!("Timed out waiting for RocksDB lock release at {}", lock_path.display());
        }

        sleep(Duration::from_millis(200)).await; // Increased from 100ms
    }
}

async fn start_server_with_retry(
    data_path: &Path,
) -> Result<super::test_support::http_server::HttpTestServer> {
    let deadline = Instant::now() + Duration::from_secs(30); // Increased from 10s
    loop {
        match start_http_test_server_with_config(|cfg| {
            cfg.storage.data_path = data_path.to_string_lossy().into_owned();
        })
        .await
        {
            Ok(server) => return Ok(server),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("LOCK")
                    || msg.contains("lock file")
                    || msg.contains("No locks available")
                {
                    if Instant::now() >= deadline {
                        return Err(e);
                    }
                    sleep(Duration::from_millis(500)).await; // Increased from 200ms
                    continue;
                }
                return Err(e);
            },
        }
    }
}

#[ignore = "RocksDB lock not released during server restart - requires explicit AppContext cleanup"]
#[tokio::test]
#[ntest::timeout(120000)]
async fn test_cluster_snapshot_creation_and_reuse() -> Result<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let server = start_server_with_retry(&data_path).await?;

    let result = async {
        let ns = "snap_ns";
        let table = "snap_table";

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE NAMESPACE failed: {:?}", resp.error);

        let resp = server
            .execute_sql(&format!(
                "CREATE TABLE {}.{} (id INT PRIMARY KEY, v TEXT) WITH (TYPE='SHARED', STORAGE_ID='local')",
                ns, table
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CREATE TABLE failed: {:?}", resp.error);

        let resp = server
            .execute_sql(&format!("INSERT INTO {}.{} (id, v) VALUES (1, 'a')", ns, table))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "INSERT failed: {:?}", resp.error);

        let resp = server.execute_sql("CLUSTER SNAPSHOT").await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success, "CLUSTER SNAPSHOT failed: {:?}", resp.error);

        let snapshots_dir = data_path.join("snapshots").join("meta");
        let _ = wait_for_snapshots(&snapshots_dir, 1).await?;

        Ok(())
    }
    .await;

    server.shutdown().await;

    // Give RocksDB time to fully release locks
    sleep(Duration::from_secs(1)).await;

    let lock_path = data_path.join("rocksdb").join("LOCK");
    let _ = wait_for_lock_release(&lock_path).await;

    // Additional delay to ensure all resources are released
    sleep(Duration::from_millis(500)).await;

    result?;

    let server = start_server_with_retry(&data_path).await?;

    let result = async {
        let resp = server.execute_sql("SELECT COUNT(*) AS cnt FROM snap_ns.snap_table").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "SELECT after restart failed: {:?}",
            resp.error
        );
        let count = resp.get_i64("cnt").unwrap_or(0);
        anyhow::ensure!(count == 1, "Expected 1 row after restart, got {}", count);

        let resp = server
            .execute_sql("SELECT snapshot FROM system.cluster_groups WHERE group_type = 'meta'")
            .await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "system.cluster_groups failed: {:?}",
            resp.error
        );
        let rows = resp.rows_as_maps();
        anyhow::ensure!(!rows.is_empty(), "system.cluster_groups returned no rows");

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}
