//! Scenario-specific test helpers and utilities.
//!
//! This module extends the consolidated helpers from `test_support::consolidated_helpers`
//! with scenario-specific utilities for things like:
//! - User isolation assertions
//! - Query result assertions
//! - Flush/storage artifact validation
//! - Parallel test utilities

use anyhow::Result;
use kalam_link::models::{ChangeEvent, ResponseStatus};
use kalam_link::{KalamCellValue, SubscriptionManager};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

use crate::test_support::consolidated_helpers as helpers;
use crate::test_support::http_server::HttpTestServer;

// Re-export commonly used consolidated helpers
pub use helpers::{
    assert_error_contains, assert_manifest_exists, assert_min_row_count, assert_no_duplicates,
    assert_parquet_exists_and_nonempty, assert_query_has_results, assert_query_success,
    assert_row_count, assert_success, create_test_users, create_user_and_client,
    drain_initial_data, ensure_user_exists, find_files_recursive, get_column_index, get_count,
    get_count_value, get_first_i64, get_first_string, get_i64_value, get_response_rows, get_rows,
    get_string_value, json_to_i64, run_parallel_users, unique_namespace, unique_table,
    wait_for_ack,
};

// =============================================================================
// Scenario-Specific Helpers
// =============================================================================

/// Alias for unique_namespace - generates unique namespace for test isolation
pub fn unique_ns(prefix: &str) -> String {
    unique_namespace(prefix)
}

/// Alias for wait_for_flush_jobs_settled - waits for flush jobs to settle
pub async fn wait_for_flush_complete(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
    _timeout_duration: Duration,
) -> Result<()> {
    crate::test_support::flush::wait_for_flush_jobs_settled(server, ns, table).await
}

// =============================================================================
// User Isolation Assertions
// =============================================================================

/// Assert that user isolation is maintained: user_a cannot see user_b's data
pub async fn assert_user_isolation(
    server: &HttpTestServer,
    ns: &str,
    table: &str,
    user_a: &str,
    user_b: &str,
) -> Result<()> {
    let client_a = server.link_client(user_a);
    let client_b = server.link_client(user_b);

    // Query as user A
    let sql = format!("SELECT * FROM {}.{}", ns, table);
    let resp_a = client_a.execute_query(&sql, None, None, None).await?;

    // Query as user B
    let resp_b = client_b.execute_query(&sql, None, None, None).await?;

    // Extract user_id values from each response
    let get_user_ids = |resp: &kalam_link::models::QueryResponse| -> HashSet<String> {
        resp.rows_as_maps()
            .iter()
            .filter_map(|row| {
                row.get("user_id")
                    .or_else(|| row.get("_user_id"))
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
            })
            .collect()
    };

    let user_ids_a = get_user_ids(&resp_a);
    let user_ids_b = get_user_ids(&resp_b);

    // User A should only see their own data
    for uid in &user_ids_a {
        assert!(uid == user_a || uid.is_empty(), "User {} saw data from user {}", user_a, uid);
    }

    // User B should only see their own data
    for uid in &user_ids_b {
        assert!(uid == user_b || uid.is_empty(), "User {} saw data from user {}", user_b, uid);
    }

    Ok(())
}

// =============================================================================
// Subscription Assertions
// =============================================================================

// Scenario-specific subscription helpers
/// Wait for N insert events and return the total rows inserted
pub async fn wait_for_inserts(
    subscription: &mut SubscriptionManager,
    expected_count: usize,
    timeout_duration: Duration,
) -> Result<Vec<HashMap<String, KalamCellValue>>> {
    let deadline = Instant::now() + timeout_duration;
    let mut all_rows = Vec::new();

    while Instant::now() < deadline && all_rows.len() < expected_count {
        match timeout(Duration::from_millis(100), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                all_rows.extend(rows);
            },
            Ok(Some(Ok(ChangeEvent::Ack { .. })))
            | Ok(Some(Ok(ChangeEvent::InitialDataBatch { .. }))) => {
                continue;
            },
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!("Subscription error: {:?}", e)),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    if all_rows.len() < expected_count {
        return Err(anyhow::anyhow!(
            "Timed out waiting for {} inserts, got {}",
            expected_count,
            all_rows.len()
        ));
    }

    Ok(all_rows)
}

/// Wait for update events
pub async fn wait_for_updates(
    subscription: &mut SubscriptionManager,
    expected_count: usize,
    timeout_duration: Duration,
) -> Result<Vec<HashMap<String, KalamCellValue>>> {
    let deadline = Instant::now() + timeout_duration;
    let mut all_rows = Vec::new();

    while Instant::now() < deadline && all_rows.len() < expected_count {
        match timeout(Duration::from_millis(100), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Update { rows, .. }))) => {
                all_rows.extend(rows);
            },
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!("Subscription error: {:?}", e)),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    if all_rows.len() < expected_count {
        return Err(anyhow::anyhow!(
            "Timed out waiting for {} updates, got {}",
            expected_count,
            all_rows.len()
        ));
    }

    Ok(all_rows)
}

/// Wait for delete events
pub async fn wait_for_deletes(
    subscription: &mut SubscriptionManager,
    expected_count: usize,
    timeout_duration: Duration,
) -> Result<Vec<HashMap<String, KalamCellValue>>> {
    let deadline = Instant::now() + timeout_duration;
    let mut all_rows = Vec::new();

    while Instant::now() < deadline && all_rows.len() < expected_count {
        match timeout(Duration::from_millis(100), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Delete { old_rows, .. }))) => {
                all_rows.extend(old_rows);
            },
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!("Subscription error: {:?}", e)),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    if all_rows.len() < expected_count {
        return Err(anyhow::anyhow!(
            "Timed out waiting for {} deletes, got {}",
            expected_count,
            all_rows.len()
        ));
    }

    Ok(all_rows)
}

/// Collect all change events for a duration
pub async fn collect_events(
    subscription: &mut SubscriptionManager,
    duration: Duration,
) -> Vec<ChangeEvent> {
    let deadline = Instant::now() + duration;
    let mut events = Vec::new();

    while Instant::now() < deadline {
        match timeout(Duration::from_millis(50), subscription.next()).await {
            Ok(Some(Ok(event))) => events.push(event),
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue,
        }
    }

    events
}

// =============================================================================
// Scenario-Specific Flush & Storage Helpers
// =============================================================================

/// Trigger flush and wait for completion
pub async fn flush_and_wait(server: &HttpTestServer, ns: &str, table: &str) -> Result<()> {
    crate::test_support::flush::flush_table_and_wait(server, ns, table).await
}

// =============================================================================
// Job Assertions
// =============================================================================

/// Wait for a specific job to reach a terminal state
pub async fn wait_for_job_terminal(
    server: &HttpTestServer,
    job_id: &str,
    timeout_duration: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        let sql = format!("SELECT status FROM system.jobs WHERE job_id = '{}'", job_id);
        let resp = server.execute_sql(&sql).await?;

        if resp.status == ResponseStatus::Success {
            if let Some(rows) = resp.results.first().and_then(|r| r.rows.as_ref()) {
                if let Some(row) = rows.first() {
                    if let Some(status) = row.first().and_then(|v| v.as_str()) {
                        match status {
                            "completed" | "failed" | "cancelled" => {
                                return Ok(status.to_string());
                            },
                            _ => {},
                        }
                    }
                }
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!("Timed out waiting for job {} to reach terminal state", job_id))
}

/// Assert job completed successfully
pub async fn assert_job_completed(
    server: &HttpTestServer,
    job_id: &str,
    timeout_duration: Duration,
) -> Result<()> {
    let status = wait_for_job_terminal(server, job_id, timeout_duration).await?;
    if status != "completed" {
        return Err(anyhow::anyhow!("Expected job {} to be completed, got {}", job_id, status));
    }
    Ok(())
}

/// Assert job failed
pub async fn assert_job_failed(
    server: &HttpTestServer,
    job_id: &str,
    timeout_duration: Duration,
) -> Result<()> {
    let status = wait_for_job_terminal(server, job_id, timeout_duration).await?;
    if status != "failed" {
        return Err(anyhow::anyhow!("Expected job {} to be failed, got {}", job_id, status));
    }
    Ok(())
}
