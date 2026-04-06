use anyhow::Result;
use kalam_client::models::ResponseStatus;
use tokio::time::{sleep, Duration, Instant};

use super::http_server::HttpTestServer;

fn is_pending_job_status(status: &str) -> bool {
    matches!(status, "new" | "queued" | "running" | "retrying")
}

/// Extract cleanup job ID from DROP TABLE response message.
///
/// Message format (current): "Table ns.table dropped successfully. Cleanup job: CL-xxxxxxxx"
pub fn extract_cleanup_job_id(message: &str) -> Option<String> {
    message.split("Cleanup job: ").nth(1).map(|s| s.trim().to_string())
}

/// Wait for a system job (by job_id) to finish.
pub async fn wait_for_job_completion(
    server: &HttpTestServer,
    job_id: &str,
    max_wait: Duration,
) -> Result<String> {
    let deadline = Instant::now() + max_wait;

    loop {
        if Instant::now() >= deadline {
            anyhow::bail!("Timeout waiting for job {} to complete after {:?}", job_id, max_wait);
        }

        let query = format!("SELECT status, message FROM system.jobs WHERE job_id = '{}'", job_id);
        let resp = server.execute_sql(&query).await?;

        if resp.status != ResponseStatus::Success {
            sleep(Duration::from_millis(200)).await;
            continue;
        }

        let row = resp
            .results
            .first()
            .and_then(|r| r.row_as_map(0))
            .ok_or_else(|| anyhow::anyhow!("job {} not found in system.jobs yet", job_id))?;

        let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");

        match status {
            s if is_pending_job_status(s) => {
                sleep(Duration::from_millis(200)).await;
            },
            "completed" => {
                return Ok(row
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("completed")
                    .to_string());
            },
            "failed" | "cancelled" => {
                let error = row.get("message").and_then(|v| v.as_str()).unwrap_or("unknown error");
                anyhow::bail!("Job {} {}: {}", job_id, status, error);
            },
            other => anyhow::bail!("Unknown job status for {}: {}", job_id, other),
        }
    }
}

/// Wait for a path to be removed from filesystem (for cleanup verification).
pub async fn wait_for_path_absent(path: &std::path::Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while path.exists() {
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
    true
}
