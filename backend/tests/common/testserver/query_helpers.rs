//! Consolidated test helpers for query execution.
//!
//! This module provides utilities for working with query responses in tests.
//! It uses the built-in helpers from `kalam_client::models::QueryResponse` where
//! possible, and adds test-specific utilities for common patterns.
//!
//! # Core Principle
//! - Use `QueryResponse` built-in methods: `rows_as_maps()`, `first_row_as_map()`, `get_i64()`,
//!   `get_string()`
//! - Add test-specific helpers here for common assertions and patterns
//! - Keep all query helpers in this single file

use kalam_client::{
    models::{QueryResponse, ResponseStatus},
    KalamCellValue,
};
use serde_json::Value as JsonValue;

/// Get a count value from a COUNT(*) query response safely.
///
/// Tries multiple column names: "count", "COUNT(*)", "total"
/// Returns the provided default if no count is found.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT COUNT(*) as total FROM users").await;
/// let count = get_count_value(&response, 0);
/// assert_eq!(count, 10);
/// ```
pub fn get_count_value(response: &QueryResponse, default: i64) -> i64 {
    response
        .first_row_as_map()
        .and_then(|row| {
            // Try multiple column names that COUNT queries might use
            row.get("count")
                .or_else(|| row.get("COUNT(*)"))
                .or_else(|| row.get("total"))
                .and_then(|v| match v.inner() {
                    JsonValue::Number(n) => n.as_i64(),
                    JsonValue::String(s) => s.parse::<i64>().ok(),
                    _ => None,
                })
        })
        .unwrap_or(default)
}

/// Assert that a query response succeeded.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT 1").await;
/// assert_query_success(&response, "SELECT 1 should succeed");
/// ```
pub fn assert_query_success(response: &QueryResponse, context: &str) {
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "{}: SQL failed: {:?}",
        context,
        response.error
    );
}

/// Assert that a query response succeeded and has at least one result.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT * FROM users").await;
/// assert_query_has_results(&response, "SELECT should return results");
/// ```
pub fn assert_query_has_results(response: &QueryResponse, context: &str) {
    assert_query_success(response, context);
    assert!(
        !response.results.is_empty(),
        "{}: Query succeeded but returned no results",
        context
    );
}

/// Assert that a query response succeeded and returned the expected row count.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT * FROM users").await;
/// assert_row_count(&response, 10, "Should have 10 users");
/// ```
pub fn assert_row_count(response: &QueryResponse, expected: usize, context: &str) {
    assert_query_success(response, context);
    let actual = response.row_count();
    assert_eq!(actual, expected, "{}: Expected {} rows, got {}", context, expected, actual);
}

/// Get a value from the first row safely, with a default.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT name FROM users LIMIT 1").await;
/// let name = get_value_or_default(&response, "name", "unknown".into());
/// ```
pub fn get_value_or_default(
    response: &QueryResponse,
    column_name: &str,
    default: KalamCellValue,
) -> KalamCellValue {
    response.get_value(column_name).unwrap_or(default)
}

/// Get an i64 value from the first row safely, with a default.
///
/// Handles both numeric and string-encoded integers.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT id FROM users LIMIT 1").await;
/// let id = get_i64_or_default(&response, "id", 0);
/// ```
pub fn get_i64_or_default(response: &QueryResponse, column_name: &str, default: i64) -> i64 {
    response.get_i64(column_name).unwrap_or(default)
}

/// Get a string value from the first row safely, with a default.
///
/// # Example
/// ```ignore
/// let response = server.execute_sql("SELECT name FROM users LIMIT 1").await;
/// let name = get_string_or_default(&response, "name", "unknown");
/// ```
pub fn get_string_or_default(response: &QueryResponse, column_name: &str, default: &str) -> String {
    response.get_string(column_name).unwrap_or_else(|| default.to_string())
}
