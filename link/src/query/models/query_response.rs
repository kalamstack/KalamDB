use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::models::KalamCellValue;
use super::error_detail::ErrorDetail;
use super::query_result::QueryResult;
use super::response_status::ResponseStatus;

/// Contains query results, execution metadata, and optional error information.
/// Matches the server's SqlResponse structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct QueryResponse {
    /// Query execution status ("success" or "error")
    pub status: ResponseStatus,

    /// Array of result sets, one per executed statement
    #[serde(default)]
    pub results: Vec<QueryResult>,

    /// Query execution time in milliseconds (with fractional precision)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub took: Option<f64>,

    /// Error details if status is "error"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

impl QueryResponse {
    /// Returns true if the query executed successfully.
    pub fn success(&self) -> bool {
        self.status == ResponseStatus::Success
    }

    /// Returns the first result's rows, if any (as arrays).
    pub fn rows(&self) -> Vec<Vec<KalamCellValue>> {
        self.results.first().and_then(|r| r.rows.as_ref()).cloned().unwrap_or_default()
    }

    /// Returns the first result's rows as HashMaps (column name → value).
    pub fn rows_as_maps(&self) -> Vec<HashMap<String, KalamCellValue>> {
        let Some(result) = self.results.first() else {
            return Vec::new();
        };
        let row_count = result.rows.as_ref().map(|r| r.len()).unwrap_or(0);
        (0..row_count).filter_map(|i| result.row_as_map(i)).collect()
    }

    /// Returns the first row as a HashMap, if any.
    pub fn first_row_as_map(&self) -> Option<HashMap<String, KalamCellValue>> {
        self.results.first().and_then(|r| r.row_as_map(0))
    }

    /// Returns the first result's row count.
    pub fn row_count(&self) -> usize {
        self.results.first().map(|r| r.row_count).unwrap_or(0)
    }

    /// Get column index by name from schema.
    pub fn column_index(&self, column_name: &str) -> Option<usize> {
        self.results
            .first()
            .and_then(|r| r.schema.iter().position(|f| f.name == column_name))
    }

    /// Get a value from the first row by column name.
    pub fn get_value(&self, column_name: &str) -> Option<KalamCellValue> {
        self.first_row_as_map().and_then(|row| row.get(column_name).cloned())
    }

    /// Get an i64 value from the first row by column name.
    ///
    /// Handles both numeric values and string-encoded integers (for Int64 precision).
    /// The backend serializes Int64 as strings to preserve precision in JSON.
    pub fn get_i64(&self, column_name: &str) -> Option<i64> {
        self.get_value(column_name).and_then(|v| match v.inner() {
            serde_json::Value::Number(n) => n.as_i64(),
            serde_json::Value::String(s) => s.parse::<i64>().ok(),
            _ => None,
        })
    }

    /// Get a string value from the first row by column name.
    pub fn get_string(&self, column_name: &str) -> Option<String> {
        self.get_value(column_name).and_then(|v| v.as_str().map(|s| s.to_string()))
    }
}
