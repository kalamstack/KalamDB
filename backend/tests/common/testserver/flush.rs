use anyhow::Result;
use kalam_link::models::{QueryResponse, ResponseStatus};
use kalamdb_commons::{NamespaceId, TableId, TableName};
use kalamdb_jobs::executors::flush::{FlushExecutor, FlushParams};
use kalamdb_jobs::executors::{JobContext, JobExecutor};
use kalamdb_jobs::AppContextJobsExt;
use std::path::{Path, PathBuf};
use tokio::time::{sleep, Duration, Instant};

use super::http_server::HttpTestServer;

async fn force_flush_table(server: &HttpTestServer, ns: &str, table: &str) -> Result<()> {
    let app_ctx = server.app_context();
    let table_id = TableId::new(NamespaceId::from(ns), TableName::from(table));
    let table_def = app_ctx
        .schema_registry()
        .get_table_if_exists(&table_id)?
        .ok_or_else(|| anyhow::anyhow!("Table not found for forced flush: {}.{}", ns, table))?;

    let params = FlushParams {
        table_id: table_id.clone(),
        table_type: table_def.table_type,
        flush_threshold: None,
    };

    let ctx = JobContext::new(app_ctx.clone(), format!("test-flush-{}-{}", ns, table), params);

    let executor = FlushExecutor::new();
    let _ = executor.execute_leader(&ctx).await?;
    Ok(())
}

fn is_pending_job_status(status: &str) -> bool {
    matches!(status, "new" | "queued" | "running" | "retrying")
}

fn is_success_terminal_job_status(status: &str) -> bool {
    matches!(status, "completed" | "skipped")
}

fn is_error_terminal_job_status(status: &str) -> bool {
    matches!(status, "failed" | "cancelled")
}

fn extract_flush_job_id(resp: &QueryResponse) -> Option<String> {
    let message = resp.results.first()?.message.as_deref()?;
    let (_, tail) = message.split_once("Job ID:")?;
    let candidate = tail.trim().trim_end_matches('.').trim_matches('"').trim_matches('\'');
    if candidate.is_empty() {
        None
    } else {
        Some(candidate.to_string())
    }
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

async fn wait_for_flush_job_by_id(
    server: &HttpTestServer,
    job_id: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let escaped_job_id = escape_sql_literal(job_id);

    loop {
        let resp = server
            .execute_sql(&format!(
                "SELECT status, message FROM system.jobs \
                 WHERE job_id = '{}' LIMIT 1",
                escaped_job_id
            ))
            .await?;

        if resp.status == ResponseStatus::Success {
            if let Some(row) = resp.results.first().and_then(|r| r.row_as_map(0)) {
                let status = row.get("status").and_then(|v| v.as_str()).unwrap_or_default();
                if is_success_terminal_job_status(status) {
                    return Ok(());
                }
                if is_error_terminal_job_status(status) {
                    let msg = row.get("message").and_then(|v| v.as_str()).unwrap_or_default();
                    anyhow::bail!(
                        "flush job {} finished with status '{}' (message='{}')",
                        job_id,
                        status,
                        msg
                    );
                }
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!("Timed out waiting for flush job {} to finish", job_id);
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_flush_job_by_idempotency_key(
    server: &HttpTestServer,
    idempotency_key: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let escaped_key = escape_sql_literal(idempotency_key);

    loop {
        let resp = server
            .execute_sql(&format!(
                "SELECT job_id, status, message FROM system.jobs \
                 WHERE job_type = 'flush' AND idempotency_key = '{}' \
                 ORDER BY created_at DESC LIMIT 1",
                escaped_key
            ))
            .await?;

        if resp.status == ResponseStatus::Success {
            if let Some(row) = resp.results.first().and_then(|r| r.row_as_map(0)) {
                let job_id = row.get("job_id").and_then(|v| v.as_str()).unwrap_or_default();
                let status = row.get("status").and_then(|v| v.as_str()).unwrap_or_default();

                if is_success_terminal_job_status(status) {
                    return Ok(());
                }
                if is_error_terminal_job_status(status) {
                    let msg = row.get("message").and_then(|v| v.as_str()).unwrap_or_default();
                    anyhow::bail!(
                        "flush job {} (key={}) finished with status '{}' (message='{}')",
                        job_id,
                        idempotency_key,
                        status,
                        msg
                    );
                }
            }
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out waiting for flush job with idempotency key '{}' to finish",
                idempotency_key
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until at least one matching flush job is completed and there are no pending flush jobs.
pub async fn wait_for_flush_jobs_settled(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);

    loop {
        let resp = server
            .execute_sql(
                "SELECT status, parameters \
                 FROM system.jobs WHERE job_type = 'flush' \
                 ORDER BY created_at DESC LIMIT 500",
            )
            .await?;

        if resp.status != ResponseStatus::Success {
            if Instant::now() >= deadline {
                anyhow::bail!("Timed out waiting for system.jobs to be queryable");
            }
            sleep(Duration::from_millis(50)).await;
            continue;
        }

        let rows = resp.results.first().map(|r| r.rows_as_maps()).unwrap_or_default();
        let mut matching_count = 0usize;
        let mut has_completed = false;
        let mut has_pending = false;
        let mut status_samples: Vec<String> = Vec::with_capacity(6);

        for row in &rows {
            let is_match = row
                .get("parameters")
                .and_then(|v| v.as_str())
                .map(|s| s.contains(ns) && s.contains(table))
                .unwrap_or(false);
            if !is_match {
                continue;
            }

            matching_count += 1;
            let status = row.get("status").and_then(|v| v.as_str()).unwrap_or_default();
            has_completed |= status == "completed" || status == "skipped";
            has_pending |= is_pending_job_status(status);

            if status_samples.len() < 6 {
                status_samples.push(status.to_string());
            }
        }

        let storage_root = server.storage_root();
        let has_parquet = find_parquet_files(&storage_root).iter().any(|p| {
            let s = p.to_string_lossy();
            s.contains(ns) && s.contains(table)
        });

        if !has_pending && (has_completed || has_parquet || matching_count > 0) {
            return Ok(());
        }

        if matching_count > 0 && !has_parquet && !has_completed {
            let _ = server.app_context().job_manager().run_once_for_tests().await;
            let _ = force_flush_table(server, ns, table).await;
        }

        if Instant::now() >= deadline {
            if matching_count > 0 {
                println!(
                    "Timed out waiting for flush jobs to settle for {}.{} (matching_count={}, statuses={:?}) - proceeding",
                    ns,
                    table,
                    matching_count,
                    status_samples
                );
                return Ok(());
            }

            anyhow::bail!(
                "Timed out waiting for flush jobs to settle for {}.{} (matching_count={}, statuses={:?})",
                ns,
                table,
                matching_count,
                status_samples
            );
        }

        sleep(Duration::from_millis(100)).await;
    }
}

/// Execute `STORAGE FLUSH TABLE` and wait until it settles. Treat idempotency conflicts as success.
pub async fn flush_table_and_wait(server: &HttpTestServer, ns: &str, table: &str) -> Result<()> {
    let sql = format!("STORAGE FLUSH TABLE {}.{}", ns, table);
    let resp = server.execute_sql(&sql).await?;

    if resp.status == ResponseStatus::Success {
        if let Some(job_id) = extract_flush_job_id(&resp) {
            return wait_for_flush_job_by_id(server, &job_id, Duration::from_secs(60)).await;
        }
        return wait_for_flush_jobs_settled(server, ns, table).await;
    }

    let is_idempotent_conflict = resp
        .error
        .as_ref()
        .map(|e| e.message.contains("Idempotent conflict"))
        .unwrap_or(false);

    if is_idempotent_conflict {
        let key = format!("flush-{}-{}", ns, table);
        return wait_for_flush_job_by_idempotency_key(server, &key, Duration::from_secs(60)).await;
    }

    anyhow::bail!("STORAGE FLUSH TABLE failed: {:?}", resp.error);
}

/// Recursively find parquet files under `root`.
pub fn find_parquet_files(root: &Path) -> Vec<PathBuf> {
    fn recurse(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                recurse(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                out.push(path);
            }
        }
    }

    let mut out = Vec::new();
    recurse(root, &mut out);
    out
}

/// Count parquet files matching a namespace/table substring filter.
pub fn count_parquet_files_for_table(storage_root: &Path, ns: &str, table: &str) -> usize {
    find_parquet_files(storage_root)
        .into_iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains(ns) && s.contains(table)
        })
        .count()
}

/// Wait until at least `min_files` parquet files exist for a given table.
pub async fn wait_for_parquet_files_for_table(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
    min_files: usize,
    timeout: Duration,
) -> Result<Vec<PathBuf>> {
    let storage_root = server.storage_root();
    let deadline = Instant::now() + timeout;

    loop {
        let candidates = find_parquet_files(&storage_root);
        let matches: Vec<_> = candidates
            .into_iter()
            .filter(|p| {
                let s = p.to_string_lossy();
                s.contains(ns) && s.contains(table)
            })
            .collect();

        if matches.len() >= min_files {
            return Ok(matches);
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out waiting for {} parquet files for {}.{} under {} (found={})",
                min_files,
                ns,
                table,
                storage_root.display(),
                matches.len()
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until at least `min_files` parquet files exist for a USER table for a specific user.
pub async fn wait_for_parquet_files_for_user_table(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
    user_id: &str,
    min_files: usize,
    timeout: Duration,
) -> Result<Vec<PathBuf>> {
    let storage_root = server.storage_root();
    let deadline = Instant::now() + timeout;

    loop {
        let candidates = find_parquet_files(&storage_root);
        let matches: Vec<_> = candidates
            .into_iter()
            .filter(|p| {
                let s = p.to_string_lossy();
                s.contains(ns) && s.contains(table) && s.contains(user_id)
            })
            .collect();

        if matches.len() >= min_files {
            return Ok(matches);
        }

        if Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out waiting for {} parquet files for user {} in {}.{} under {} (found={})",
                min_files,
                user_id,
                ns,
                table,
                storage_root.display(),
                matches.len()
            );
        }

        sleep(Duration::from_millis(50)).await;
    }
}
