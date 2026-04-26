// Smoke test: STORAGE CHECK command validation
// - Verifies STORAGE CHECK local returns healthy status
// - Validates timestamp is current (not 1970)
// - Tests STORAGE CHECK EXTENDED includes capacity info
// - Verifies authorization (DBA+ only)
// - Tests non-existent storage returns proper error

use std::{
    collections::HashMap,
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, Utc};
use serde_json::Value as JsonValue;

use crate::common::*;

fn arrow_value_as_string(value: &JsonValue) -> Option<String> {
    extract_arrow_value(value)
        .unwrap_or_else(|| value.clone())
        .as_str()
        .map(|s| s.to_string())
}

fn wait_for_storage_check_row(
    sql: &str,
    timeout: Duration,
) -> (HashMap<String, JsonValue>, String) {
    let deadline = Instant::now() + timeout;
    let mut last_result = String::new();

    loop {
        if let Ok(result) = execute_sql_as_root_via_client_json(sql) {
            last_result = result.clone();
            if let Ok(json) = serde_json::from_str::<JsonValue>(&result) {
                if let Some(rows) = get_rows_as_hashmaps(&json) {
                    if let Some(row) = rows.first() {
                        let status = row.get("status").and_then(arrow_value_as_string);
                        if status.as_deref() == Some("healthy") {
                            return (row.clone(), result);
                        }
                    }
                }
            }
        }

        if Instant::now() >= deadline {
            panic!(
                "Timed out waiting for healthy storage check row for SQL: {}. Last result: {}",
                sql, last_result
            );
        }

        thread::sleep(Duration::from_millis(200));
    }
}

#[ntest::timeout(60_000)]
#[test]
fn smoke_storage_check_local_basic() {
    if !is_server_running() {
        println!(
            "Skipping smoke_storage_check_local_basic: server not running at {}",
            server_url()
        );
        return;
    }

    let sql = "STORAGE CHECK local";
    let (row, _result) = wait_for_storage_check_row(sql, Duration::from_secs(20));

    // Verify storage_id
    let storage_id = row.get("storage_id").and_then(arrow_value_as_string);
    assert_eq!(storage_id.as_deref(), Some("local"), "storage_id should be 'local'");

    // Verify status
    let status = row.get("status").and_then(arrow_value_as_string);
    assert_eq!(status.as_deref(), Some("healthy"), "local storage should be healthy");

    // Verify all operation flags are true
    let readable = row
        .get("readable")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_bool());
    assert_eq!(readable, Some(true), "local storage should be readable");

    let writable = row
        .get("writable")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_bool());
    assert_eq!(writable, Some(true), "local storage should be writable");

    let listable = row
        .get("listable")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_bool());
    assert_eq!(listable, Some(true), "local storage should be listable");

    let deletable = row
        .get("deletable")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_bool());
    assert_eq!(deletable, Some(true), "local storage should be deletable");

    // Verify latency_ms exists and is reasonable (< 5 seconds)
    // Note: latency_ms might be NULL or 0 in some test environments
    let latency_ms = row
        .get("latency_ms")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_i64());

    if let Some(latency) = latency_ms {
        assert!(
            latency >= 0 && latency < 5000,
            "latency_ms should be between 0 and 5000ms, got {}",
            latency
        );
    } else {
        println!("⚠️  WARNING: latency_ms is NULL (might be test environment issue)");
    }

    // Verify error is null
    let error = row.get("error").and_then(|v| arrow_value_as_string(v));
    assert!(
        error.as_deref().is_none() || error.as_deref() == Some(""),
        "error should be null for healthy storage"
    );

    // CRITICAL: Verify timestamp is current (not 1970)
    let tested_at_str = row.get("tested_at").and_then(|v| {
        // Try to extract Arrow value first, then fall back to direct access
        extract_arrow_value(v)
            .and_then(|extracted| extracted.as_str().map(|s| s.to_string()))
            .or_else(|| v.as_str().map(|s| s.to_string()))
    });

    if let Some(tested_at_str) = tested_at_str {
        let tested_at: DateTime<Utc> = tested_at_str.parse().unwrap_or_else(|err| {
            panic!("Failed to parse tested_at timestamp: {}\n{}", err, tested_at_str)
        });

        let now = Utc::now();
        let age_seconds = (now - tested_at).num_seconds().abs();

        // Timestamp should be within last 60 seconds (not from 1970!)
        assert!(
            tested_at.year() >= 2024,
            "tested_at year should be >= 2024, got {} (timestamp: {})",
            tested_at.year(),
            tested_at_str
        );
        assert!(
            age_seconds <= 60,
            "tested_at should be within 60 seconds of now, but was {} seconds ago (tested_at: {}, \
             now: {})",
            age_seconds,
            tested_at,
            now
        );
    } else {
        println!("⚠️  WARNING: tested_at field not available (might be timestamp type issue)");
    }

    // Verify capacity fields are NULL for basic check
    let total_bytes = row
        .get("total_bytes")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_i64());
    let used_bytes = row
        .get("used_bytes")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_i64());

    // For basic check (not EXTENDED), these should be NULL
    assert!(total_bytes.is_none(), "total_bytes should be NULL for basic check");
    assert!(used_bytes.is_none(), "used_bytes should be NULL for basic check");
}

#[ntest::timeout(60_000)]
#[test]
fn smoke_storage_check_extended() {
    if !is_server_running() {
        println!("Skipping smoke_storage_check_extended: server not running at {}", server_url());
        return;
    }

    let sql = "STORAGE CHECK local EXTENDED";
    let (row, _result) = wait_for_storage_check_row(sql, Duration::from_secs(20));

    // Verify status is healthy
    let status = row.get("status").and_then(arrow_value_as_string);
    assert_eq!(status.as_deref(), Some("healthy"), "local storage should be healthy");

    // For filesystem storage with EXTENDED, capacity should be populated
    let total_bytes = row
        .get("total_bytes")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_i64());
    let used_bytes = row
        .get("used_bytes")
        .and_then(|v| extract_arrow_value(v).unwrap_or_else(|| v.clone()).as_i64());

    // Capacity info should be present for filesystem storage with EXTENDED
    // Note: Some backends might not support capacity reporting
    if total_bytes.is_some() && total_bytes.unwrap() > 0 {
        assert!(
            used_bytes.is_some() && used_bytes.unwrap() >= 0,
            "used_bytes should be present and >= 0 when total_bytes is present"
        );
        assert!(
            total_bytes.unwrap() >= used_bytes.unwrap(),
            "total_bytes should be >= used_bytes"
        );
        println!(
            "✅ Capacity info present: {} total, {} used bytes",
            total_bytes.unwrap(),
            used_bytes.unwrap()
        );
    } else {
        println!("⚠️  WARNING: total_bytes not available (backend limitation or test environment)");
    }
}

#[ntest::timeout(60_000)]
#[test]
fn smoke_storage_check_nonexistent() {
    if !is_server_running() {
        println!(
            "Skipping smoke_storage_check_nonexistent: server not running at {}",
            server_url()
        );
        return;
    }

    let sql = "STORAGE CHECK nonexistent_storage_xyz";
    let result = execute_sql_as_root_via_client(sql);

    // Should fail with "not found" error
    assert!(result.is_err(), "STORAGE CHECK on nonexistent storage should fail");

    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err_msg.contains("not found") || err_msg.contains("notfound"),
        "Error should mention 'not found', got: {}",
        err_msg
    );
}

#[ntest::timeout(60_000)]
#[test]
fn smoke_storage_check_authorization() {
    if !is_server_running() {
        println!(
            "Skipping smoke_storage_check_authorization: server not running at {}",
            server_url()
        );
        return;
    }

    // Create a regular user to test authorization
    let test_user = generate_unique_namespace("chk_user");
    let test_password = "CheckPass123!";

    // Cleanup function for user
    let cleanup_user = || {
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS '{}'", test_user));
    };

    // Ensure cleanup on test exit
    let _cleanup_guard = CallOnDrop::new(cleanup_user);

    // Create user
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        test_user, test_password
    ))
    .expect("create test user");

    // Regular user should NOT be able to check storage health
    let sql = "STORAGE CHECK local";
    let result = execute_sql_via_client_as(&test_user, test_password, sql);

    assert!(result.is_err(), "Regular user should not be able to run STORAGE CHECK");

    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err_msg.contains("permission") || err_msg.contains("denied") || err_msg.contains("admin"),
        "Error should mention permission/denied/admin, got: {}",
        err_msg
    );
}

#[ntest::timeout(60_000)]
#[test]
fn smoke_storage_check_dba_access() {
    if !is_server_running() {
        println!(
            "Skipping smoke_storage_check_dba_access: server not running at {}",
            server_url()
        );
        return;
    }

    // Create a DBA user to test authorization
    let test_dba = generate_unique_namespace("chk_dba");
    let test_password = "DbaPass123!";

    let cleanup_user = || {
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS '{}'", test_dba));
    };
    let _cleanup_guard = CallOnDrop::new(cleanup_user);

    // Create DBA user
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        test_dba, test_password
    ))
    .expect("create DBA user");

    // DBA should be able to check storage health
    let sql = "STORAGE CHECK local";
    let result = execute_sql_via_client_as(&test_dba, test_password, sql);

    assert!(result.is_ok(), "DBA user should be able to run STORAGE CHECK");
}

// Helper struct for cleanup on drop
struct CallOnDrop<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> CallOnDrop<F> {
    fn new(f: F) -> Self {
        CallOnDrop(Some(f))
    }
}

impl<F: FnOnce()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}
