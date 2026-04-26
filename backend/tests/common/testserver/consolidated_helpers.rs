//! Consolidated test helper functions shared across all test suites.
//!
//! This module consolidates common test utilities that were previously duplicated
//! across scenario tests, misc tests, and testserver tests. All helpers use
//! kalam-client's QueryResponse methods where possible.
//!
//! # Organization
//!
//! - **Query Result Extraction**: Safe extraction of values from QueryResponse
//! - **Assertion Helpers**: Common assertions for test validation
//! - **User/Auth Helpers**: User creation and authentication
//! - **Storage/Validation**: File operations and data validation
//! - **Subscription Helpers**: WebSocket subscription utilities
//! - **Parallel Testing**: Multi-user and concurrency helpers

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use kalam_client::{
    models::{ChangeEvent, QueryResponse, ResponseStatus},
    KalamCellValue, SubscriptionManager,
};
use kalamdb_commons::Role;
use tokio::time::{timeout, Instant};

use super::http_server::HttpTestServer;

// =============================================================================
// Query Result Extraction Helpers
// =============================================================================

/// Get count from a SELECT COUNT(*) query response.
///
/// Tries multiple common column names: "count", "COUNT(*)", "cnt", "total"
/// Returns the provided default if no count is found.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT COUNT(*) as total FROM users").await;
/// let count = get_count_value(&response, 0);
/// assert_eq!(count, 10);
/// ```
pub fn get_count_value(response: &QueryResponse, default: i64) -> i64 {
    // Try multiple column name variants
    for col in &["count", "COUNT(*)", "cnt", "total"] {
        if let Some(value) = response.get_i64(col) {
            return value;
        }
    }

    // Try from first row as map
    if let Some(row) = response.first_row_as_map() {
        for (_, v) in row {
            if let Some(n) = extract_i64(&v) {
                return n;
            }
        }
    }

    default
}

/// Get string value from first row (wrapper around kalam-client method).
pub fn get_first_string(resp: &QueryResponse, column: &str) -> Option<String> {
    resp.get_string(column)
}

/// Get i64 value from first row (wrapper around kalam-client method).
pub fn get_first_i64(resp: &QueryResponse, column: &str) -> Option<i64> {
    resp.get_i64(column)
}

/// Get count from a SELECT COUNT(*) AS cnt query response.
pub fn get_count(resp: &QueryResponse) -> i64 {
    resp.get_i64("cnt").unwrap_or(0)
}

/// Get all rows as HashMaps (wrapper around kalam-client method).
pub fn get_response_rows(resp: &QueryResponse) -> Vec<HashMap<String, KalamCellValue>> {
    resp.rows_as_maps()
}

/// Extract i64 from cell value, handling both Number and String types.
pub fn json_to_i64(v: &KalamCellValue) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

/// Extract i64 from JSON value (alternative name for compatibility).
fn extract_i64(v: &KalamCellValue) -> Option<i64> {
    json_to_i64(v)
}

/// Get all rows as vectors of JSON values.
pub fn get_rows(resp: &QueryResponse) -> Vec<Vec<KalamCellValue>> {
    resp.rows()
}

/// Get column index by name from schema (wrapper around kalam-client method).
pub fn get_column_index(resp: &QueryResponse, column_name: &str) -> Option<usize> {
    resp.column_index(column_name)
}

/// Extract a string column value from a row.
pub fn get_string_value(row: &[KalamCellValue], idx: usize) -> Option<String> {
    row.get(idx).and_then(|v| {
        v.as_str()
            .map(|s| s.to_string())
            .or_else(|| v.get("Utf8").and_then(|v2| v2.as_str().map(|s| s.to_string())))
    })
}

/// Extract an i64 column value from a row.
pub fn get_i64_value(row: &[KalamCellValue], idx: usize) -> Option<i64> {
    row.get(idx).and_then(json_to_i64)
}

// =============================================================================
// Assertion Helpers
// =============================================================================

/// Assert that a SQL query response was successful.
///
/// # Panics
/// Panics if the response status is not Success.
pub fn assert_query_success(response: &QueryResponse, context: &str) {
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "{}: SQL failed: {:?}",
        context,
        response.error
    );
}

/// Assert that a SQL query response succeeded (using kalam-client's success() method).
pub fn assert_success(resp: &QueryResponse, context: &str) {
    assert!(resp.success(), "{}: expected success, got error: {:?}", context, resp.error);
}

/// Assert that a SQL query response has results.
pub fn assert_query_has_results(response: &QueryResponse, context: &str) {
    assert_query_success(response, context);
    assert!(
        !response.results.is_empty(),
        "{}: Query succeeded but returned no results",
        context
    );
}

/// Assert that a SQL query response has the expected row count.
pub fn assert_row_count(response: &QueryResponse, expected: usize, context: &str) {
    assert_query_success(response, context);
    let actual = response.row_count();
    assert_eq!(actual, expected, "{}: Expected {} rows, got {}", context, expected, actual);
}

/// Assert that a SQL query response has at least the minimum row count.
pub fn assert_min_row_count(resp: &QueryResponse, min: usize, context: &str) {
    assert_success(resp, context);
    let actual = resp.row_count();
    assert!(actual >= min, "{}: expected at least {} rows, got {}", context, min, actual);
}

/// Assert that a SQL response failed with an expected error message substring.
pub fn assert_error_contains(resp: &QueryResponse, expected: &str, context: &str) {
    assert_eq!(resp.status, ResponseStatus::Error, "{}: expected error, got success", context);
    let msg = resp.error.as_ref().map(|e| e.message.as_str()).unwrap_or("");
    assert!(
        msg.to_lowercase().contains(&expected.to_lowercase()),
        "{}: expected error containing '{}', got '{}'",
        context,
        expected,
        msg
    );
}

/// Assert no duplicates by primary key in query results.
pub fn assert_no_duplicates(resp: &QueryResponse, pk_column: &str) -> Result<()> {
    assert_success(resp, "query for duplicate check");

    let pk_idx = get_column_index(resp, pk_column)
        .ok_or_else(|| anyhow::anyhow!("Column {} not found", pk_column))?;

    let mut seen = HashSet::new();
    for row in get_rows(resp) {
        if let Some(pk) = row.get(pk_idx) {
            let pk_str = pk.to_string();
            if !seen.insert(pk_str.clone()) {
                return Err(anyhow::anyhow!("Duplicate primary key: {}", pk_str));
            }
        }
    }
    Ok(())
}

// =============================================================================
// User/Auth Helpers
// =============================================================================

/// Create a user if they don't exist and return their user_id.
///
/// Returns Ok(user_id) whether user was created or already existed.
pub async fn ensure_user_exists(
    server: &HttpTestServer,
    user_id: &str,
    password: &str,
    role: &Role,
) -> Result<String> {
    let lookup_sql =
        format!("SELECT user_id FROM system.users WHERE user_id = '{}' LIMIT 1", user_id);
    let create_sql =
        format!("CREATE USER '{}' WITH PASSWORD '{}' ROLE '{}'", user_id, password, role);

    for attempt in 0..10 {
        if let Ok(resp) = server.execute_sql(&lookup_sql).await {
            if resp.status == ResponseStatus::Success {
                if let Some(rows) = resp.rows_as_maps().first() {
                    if let Some(user_id_val) = rows.get("user_id") {
                        let user_id_str = user_id_val
                            .as_str()
                            .map(ToString::to_string)
                            .or_else(|| {
                                user_id_val
                                    .get("Utf8")
                                    .and_then(|v| v.as_str())
                                    .map(ToString::to_string)
                            })
                            .unwrap_or_default();

                        if !user_id_str.is_empty() {
                            server.cache_user_id(user_id, &user_id_str);
                            server.cache_user_password(user_id, password);
                            return Ok(user_id_str);
                        }
                    }
                }
            }
        }

        let _ = server.execute_sql(&create_sql).await;

        if attempt < 9 {
            let backoff = 100u64.saturating_mul((attempt + 1) as u64);
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
    }

    let resp = server.execute_sql(&lookup_sql).await?;
    let resolved_user_id = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("user_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Failed to get user_id for {}", user_id))?
        .to_string();

    server.cache_user_id(user_id, &resolved_user_id);
    server.cache_user_password(user_id, password);
    Ok(resolved_user_id)
}

/// Create multiple test users at once.
pub async fn create_test_users(server: &HttpTestServer, users: &[(&str, &Role)]) -> Result<()> {
    for (username, role) in users {
        ensure_user_exists(server, username, "test123", role).await?;
    }
    Ok(())
}

/// Create a user and return a link client configured for that user.
pub async fn create_user_and_client(
    server: &HttpTestServer,
    username: &str,
    role: &Role,
) -> Result<kalam_client::KalamLinkClient> {
    let user_id = ensure_user_exists(server, username, "test123", role).await?;
    server.cache_user_password(username, "test123");
    Ok(server.link_client_with_id(&user_id, username, role))
}

// =============================================================================
// Storage & Validation Helpers
// =============================================================================

/// Find files recursively by filename.
pub fn find_files_recursive(root: &Path, filename: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                results.extend(find_files_recursive(&path, filename));
            } else if path.file_name().map(|n| n == filename).unwrap_or(false) {
                results.push(path);
            }
        }
    }
    results
}

/// Assert that manifest.json exists for a table.
pub fn assert_manifest_exists(storage_root: &Path, ns: &str, table: &str) -> Result<()> {
    let manifests = find_files_recursive(storage_root, "manifest.json");
    let matching: Vec<_> = manifests
        .iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains(ns) && s.contains(table)
        })
        .collect();

    if matching.is_empty() {
        return Err(anyhow::anyhow!(
            "No manifest.json found for {}.{} under {}",
            ns,
            table,
            storage_root.display()
        ));
    }

    Ok(())
}

/// Assert that parquet files exist and are non-empty for a table.
pub fn assert_parquet_exists_and_nonempty(
    storage_root: &Path,
    ns: &str,
    table: &str,
) -> Result<()> {
    let parquets = find_files_recursive(storage_root, ".parquet");
    let matching: Vec<_> = parquets
        .iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains(ns) && s.contains(table)
        })
        .collect();

    if matching.is_empty() {
        return Err(anyhow::anyhow!(
            "No parquet files found for {}.{} under {}",
            ns,
            table,
            storage_root.display()
        ));
    }

    // Check all files are non-empty
    for file in matching {
        let metadata = std::fs::metadata(file)?;
        if metadata.len() == 0 {
            return Err(anyhow::anyhow!("Parquet file is empty: {}", file.display()));
        }
    }

    Ok(())
}

// =============================================================================
// Subscription Helpers
// =============================================================================

/// Wait for an ACK message from a subscription.
pub async fn wait_for_ack(
    subscription: &mut SubscriptionManager,
    timeout_duration: Duration,
) -> Result<(String, usize)> {
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        match timeout(Duration::from_millis(100), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Ack {
                subscription_id,
                total_rows,
                ..
            }))) => {
                return Ok((subscription_id, total_rows as usize));
            },
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!("Subscription error: {:?}", e)),
            Ok(None) => return Err(anyhow::anyhow!("Subscription stream ended")),
            Err(_) => continue,
        }
    }
    Err(anyhow::anyhow!("Timed out waiting for ACK"))
}

/// Drain initial data batches from a subscription.
pub async fn drain_initial_data(
    subscription: &mut SubscriptionManager,
    timeout_duration: Duration,
) -> Result<usize> {
    let deadline = Instant::now() + timeout_duration;
    let mut total_rows = 0;

    while Instant::now() < deadline {
        match timeout(Duration::from_millis(100), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Ack { .. }))) => continue,
            Ok(Some(Ok(ChangeEvent::InitialDataBatch {
                rows,
                batch_control,
                ..
            }))) => {
                total_rows += rows.len();
                if !batch_control.has_more {
                    break;
                }
            },
            Ok(Some(Ok(_))) => break,
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!("Subscription error: {:?}", e)),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    Ok(total_rows)
}

// =============================================================================
// Parallel Testing Helpers
// =============================================================================

/// Run a function for multiple users in parallel.
pub async fn run_parallel_users<F, Fut>(
    user_count: usize,
    user_prefix: &str,
    f: F,
) -> Vec<Result<()>>
where
    F: Fn(String, usize) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let mut handles = vec![];

    for i in 0..user_count {
        let f = f.clone();
        let user_id = format!("{}_{}", user_prefix, i);
        let handle = tokio::spawn(async move { f(user_id, i).await });
        handles.push(handle);
    }

    let mut results = vec![];
    for handle in handles {
        match handle.await {
            Ok(result) => results.push(result),
            Err(e) => results.push(Err(anyhow::anyhow!("Task failed: {}", e))),
        }
    }

    results
}

/// Generate unique namespace name for test isolation.
pub fn unique_namespace(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}_{}", prefix, nanos)
}

/// Generate unique table name for test isolation.
pub fn unique_table(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}_{}", prefix, nanos)
}
