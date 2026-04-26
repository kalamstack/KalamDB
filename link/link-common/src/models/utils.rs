use super::kalam_cell_value::KalamCellValue;

/// Parse an i64 from a [`KalamCellValue`] that might be a Number or a String.
///
/// The backend serializes Int64 as strings to preserve precision in JSON.
/// This utility handles both formats for convenience.
///
/// # Example
///
/// ```rust
/// use kalam_client::{models::KalamCellValue, parse_i64};
///
/// let num_value = KalamCellValue::int(42);
/// let str_value = KalamCellValue::text("42");
///
/// assert_eq!(parse_i64(&num_value), 42);
/// assert_eq!(parse_i64(&str_value), 42);
/// ```
pub fn parse_i64(value: &KalamCellValue) -> i64 {
    match value.inner() {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}
