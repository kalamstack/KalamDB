//! SQL response model
//!
//! This module defines the structure for SQL execution responses from the `/v1/api/sql` endpoint.

use std::fmt;

use kalamdb_commons::{
    models::{datatypes::KalamDataType, KalamCellValue},
    schemas::SchemaField,
};
use serde::{Deserialize, Serialize, Serializer};

/// Custom serializer to limit took field to 3 decimal places
fn serialize_took<S>(took: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let rounded = (took * 1000.0).round() / 1000.0;
    serializer.serialize_f64(rounded)
}

/// Error code enum for type-safe error handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Invalid parameter
    InvalidParameter,
    /// Batch parse error
    BatchParseError,
    /// Empty SQL statement
    EmptySql,
    /// Parameters with batch
    ParamsWithBatch,
    /// SQL execution error
    SqlExecutionError,
    /// Forward failed
    ForwardFailed,
    /// NOT_LEADER error (follower node)
    NotLeader,
    /// Invalid SQL syntax
    InvalidSql,
    /// Table not found
    TableNotFound,
    /// Permission denied
    PermissionDenied,
    /// Cluster unavailable
    ClusterUnavailable,
    /// Leader node not available
    LeaderNotAvailable,
    /// Internal error
    InternalError,
    /// Invalid input data
    InvalidInput,
    /// File too large
    FileTooLarge,
    /// Too many files in request
    TooManyFiles,
    /// Missing required file
    MissingFile,
    /// Extra file not referenced in SQL
    ExtraFile,
    /// File not found
    FileNotFound,
    /// Invalid MIME type
    InvalidMimeType,
}

impl ErrorCode {
    /// Check if this error code indicates a NOT_LEADER error
    #[inline]
    pub fn is_not_leader(&self) -> bool {
        matches!(self, ErrorCode::NotLeader)
    }

    /// Get the string representation of the error code
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::RateLimitExceeded => "RATE_LIMIT_EXCEEDED",
            ErrorCode::InvalidParameter => "INVALID_PARAMETER",
            ErrorCode::BatchParseError => "BATCH_PARSE_ERROR",
            ErrorCode::EmptySql => "EMPTY_SQL",
            ErrorCode::ParamsWithBatch => "PARAMS_WITH_BATCH",
            ErrorCode::SqlExecutionError => "SQL_EXECUTION_ERROR",
            ErrorCode::ForwardFailed => "FORWARD_FAILED",
            ErrorCode::NotLeader => "NOT_LEADER",
            ErrorCode::InvalidSql => "INVALID_SQL",
            ErrorCode::TableNotFound => "TABLE_NOT_FOUND",
            ErrorCode::PermissionDenied => "PERMISSION_DENIED",
            ErrorCode::ClusterUnavailable => "CLUSTER_UNAVAILABLE",
            ErrorCode::LeaderNotAvailable => "LEADER_NOT_AVAILABLE",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::InvalidInput => "INVALID_INPUT",
            ErrorCode::FileTooLarge => "FILE_TOO_LARGE",
            ErrorCode::TooManyFiles => "TOO_MANY_FILES",
            ErrorCode::MissingFile => "MISSING_FILE",
            ErrorCode::ExtraFile => "EXTRA_FILE",
            ErrorCode::FileNotFound => "FILE_NOT_FOUND",
            ErrorCode::InvalidMimeType => "INVALID_MIME_TYPE",
        }
    }

    #[inline]
    fn public_message(&self) -> Option<&'static str> {
        match self {
            ErrorCode::BatchParseError => Some("Failed to parse SQL batch"),
            ErrorCode::SqlExecutionError => Some("SQL statement failed"),
            ErrorCode::InvalidSql => Some("SQL statement is invalid or not allowed"),
            ErrorCode::TableNotFound => Some("Requested table is not available"),
            ErrorCode::InternalError => Some("SQL request failed"),
            _ => None,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for ErrorCode {
    fn from(s: &str) -> Self {
        match s {
            "RATE_LIMIT_EXCEEDED" => ErrorCode::RateLimitExceeded,
            "INVALID_PARAMETER" => ErrorCode::InvalidParameter,
            "BATCH_PARSE_ERROR" => ErrorCode::BatchParseError,
            "EMPTY_SQL" => ErrorCode::EmptySql,
            "PARAMS_WITH_BATCH" => ErrorCode::ParamsWithBatch,
            "SQL_EXECUTION_ERROR" => ErrorCode::SqlExecutionError,
            "FORWARD_FAILED" => ErrorCode::ForwardFailed,
            "NOT_LEADER" => ErrorCode::NotLeader,
            "INVALID_SQL" => ErrorCode::InvalidSql,
            "TABLE_NOT_FOUND" => ErrorCode::TableNotFound,
            "PERMISSION_DENIED" => ErrorCode::PermissionDenied,
            "CLUSTER_UNAVAILABLE" => ErrorCode::ClusterUnavailable,
            "LEADER_NOT_AVAILABLE" => ErrorCode::LeaderNotAvailable,
            "INVALID_INPUT" => ErrorCode::InvalidInput,
            "FILE_TOO_LARGE" => ErrorCode::FileTooLarge,
            "TOO_MANY_FILES" => ErrorCode::TooManyFiles,
            "MISSING_FILE" => ErrorCode::MissingFile,
            "EXTRA_FILE" => ErrorCode::ExtraFile,
            "FILE_NOT_FOUND" => ErrorCode::FileNotFound,
            "INVALID_MIME_TYPE" => ErrorCode::InvalidMimeType,
            _ => ErrorCode::InternalError,
        }
    }
}

/// Execution status enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ResponseStatus {
    Success,
    Error,
}

impl std::fmt::Display for ResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseStatus::Success => write!(f, "success"),
            ResponseStatus::Error => write!(f, "error"),
        }
    }
}

/// Response from SQL execution via REST API
///
/// Contains execution status, results, timing information, and any errors that occurred.
///
/// # Example Success Response
/// ```json
/// {
///   "status": "success",
///   "results": [
///     {
///       "schema": [
///         {"name": "id", "data_type": "BigInt", "index": 0},
///         {"name": "name", "data_type": "Text", "index": 1}
///       ],
///       "rows": [
///         ["1", "Alice"],
///         ["2", "Bob"]
///       ],
///       "row_count": 2
///     }
///   ],
///   "took": 15.0,
///   "error": null
/// }
/// ```
///
/// # Example Error Response
/// ```json
/// {
///   "status": "error",
///   "results": [],
///   "took": 5.0,
///   "error": {
///     "code": "INVALID_SQL",
///     "message": "Syntax error near 'SELCT'"
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlResponse {
    /// Overall execution status: "success" or "error"
    pub status: ResponseStatus,

    /// Array of result sets, one per executed statement
    pub results: Vec<QueryResult>,

    /// Total execution time in milliseconds (with fractional precision, limited to 3 decimal
    /// places)
    #[serde(serialize_with = "serialize_took")]
    pub took: f64,

    /// Error details if status is "error", otherwise null
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

/// Individual query result within a SQL response
///
/// Each executed SQL statement produces one QueryResult.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Schema describing the columns in the result set
    /// Each field contains: name, data_type (KalamDataType), and index
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema: Vec<SchemaField>,

    /// The result rows as arrays of values (ordered by schema index)
    /// Example: [["123", "Alice", 1699000000000000], ["456", "Bob", 1699000001000000]]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<KalamCellValue>>>,

    /// Number of rows affected (for INSERT/UPDATE/DELETE) or returned (for SELECT)
    pub row_count: usize,

    /// Optional message for non-query statements (e.g., "Table created successfully")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Effective user identifier this statement executed as.
    pub as_user: String,
}

/// Error details for failed SQL execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// Error code enum (type-safe)
    pub code: ErrorCode,

    /// Human-readable error message
    pub message: String,

    /// Optional detailed context (e.g., line number, column position)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl SqlResponse {
    /// Create a successful response with results
    pub fn success(results: Vec<QueryResult>, took: f64) -> Self {
        Self {
            status: ResponseStatus::Success,
            results,
            took,
            error: None,
        }
    }

    /// Create an error response
    pub fn error(code: ErrorCode, message: &str, took: f64) -> Self {
        Self {
            status: ResponseStatus::Error,
            results: Vec::new(),
            took,
            error: Some(ErrorDetail {
                code,
                message: message.to_string(),
                details: None,
            }),
        }
    }

    /// Create an error response and optionally redact sensitive SQL details.
    pub fn error_for_privilege(
        code: ErrorCode,
        message: &str,
        took: f64,
        include_sensitive_details: bool,
    ) -> Self {
        if !include_sensitive_details {
            if let Some(public_message) = code.public_message() {
                return Self::error(code, public_message, took);
            }
        }

        Self::error(code, message, took)
    }

    /// Create an error response with additional details
    pub fn error_with_details(code: ErrorCode, message: &str, details: &str, took: f64) -> Self {
        Self {
            status: ResponseStatus::Error,
            results: Vec::new(),
            took,
            error: Some(ErrorDetail {
                code,
                message: message.to_string(),
                details: Some(details.to_string()),
            }),
        }
    }

    /// Create an error response with optional redaction of sensitive SQL details.
    pub fn error_with_details_for_privilege(
        code: ErrorCode,
        message: &str,
        details: &str,
        took: f64,
        include_sensitive_details: bool,
    ) -> Self {
        if !include_sensitive_details {
            if let Some(public_message) = code.public_message() {
                return Self::error(code, public_message, took);
            }
        }

        Self::error_with_details(code, message, details, took)
    }
}

impl QueryResult {
    /// Create a result for a SELECT query with rows and schema
    pub fn with_rows_and_schema(rows: Vec<Vec<KalamCellValue>>, schema: Vec<SchemaField>) -> Self {
        let row_count = rows.len();
        Self {
            schema,
            rows: Some(rows),
            row_count,
            message: None,
            as_user: "unknown".to_string(),
        }
    }

    /// Create a result for a DML statement (INSERT/UPDATE/DELETE)
    pub fn with_affected_rows(row_count: usize, message: Option<String>) -> Self {
        Self {
            schema: Vec::new(),
            rows: None,
            row_count,
            message,
            as_user: "unknown".to_string(),
        }
    }

    /// Create a result for a DDL statement (CREATE/ALTER/DROP)
    pub fn with_message(message: String) -> Self {
        Self {
            schema: Vec::new(),
            rows: None,
            row_count: 0,
            message: Some(message),
            as_user: "unknown".to_string(),
        }
    }

    /// Create a result for a SUBSCRIBE TO statement
    ///
    /// Returns subscription metadata as a single row result
    pub fn subscription(subscription_data: serde_json::Value) -> Self {
        let schema = vec![
            SchemaField::new("status", KalamDataType::Text, 0),
            SchemaField::new("ws_url", KalamDataType::Text, 1),
            SchemaField::new("subscription", KalamDataType::Json, 2),
            SchemaField::new("message", KalamDataType::Text, 3),
        ];

        // Extract values in schema order
        let row = if let serde_json::Value::Object(map) = subscription_data {
            vec![
                KalamCellValue::from(map.get("status").cloned().unwrap_or(serde_json::Value::Null)),
                KalamCellValue::from(map.get("ws_url").cloned().unwrap_or(serde_json::Value::Null)),
                KalamCellValue::from(
                    map.get("subscription").cloned().unwrap_or(serde_json::Value::Null),
                ),
                KalamCellValue::from(
                    map.get("message").cloned().unwrap_or(serde_json::Value::Null),
                ),
            ]
        } else {
            vec![KalamCellValue::null(); 4]
        };

        Self {
            schema,
            rows: Some(vec![row]),
            row_count: 1,
            message: None,
            as_user: "unknown".to_string(),
        }
    }

    /// Get column names from the schema
    pub fn column_names(&self) -> Vec<String> {
        self.schema.iter().map(|f| f.name.clone()).collect()
    }

    /// Set the effective execution username for this result.
    pub fn with_as_user(mut self, as_user: String) -> Self {
        self.as_user = as_user;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_response_serialization() {
        let row1 = vec![KalamCellValue::text("1"), KalamCellValue::text("Alice")];

        let schema = vec![
            SchemaField::new("id", KalamDataType::BigInt, 0),
            SchemaField::new("name", KalamDataType::Text, 1),
        ];
        let result = QueryResult::with_rows_and_schema(vec![row1], schema);

        let response = SqlResponse::success(vec![result], 15.0);

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("success"));
        assert!(json.contains("Alice"));
        assert!(json.contains("15"));
        // Verify schema is present
        assert!(json.contains("\"schema\""));
        assert!(json.contains("\"data_type\":\"BigInt\""));
        assert!(json.contains("\"data_type\":\"Text\""));
    }

    #[test]
    fn test_schema_field_serialization() {
        let schema = vec![
            SchemaField::new("id", KalamDataType::BigInt, 0),
            SchemaField::new("name", KalamDataType::Text, 1),
            SchemaField::new("created_at", KalamDataType::Timestamp, 2),
        ];
        let result = QueryResult::with_rows_and_schema(vec![], schema);

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"name\":\"id\""));
        assert!(json.contains("\"index\":0"));
        assert!(json.contains("\"data_type\":\"BigInt\""));
        assert!(json.contains("\"data_type\":\"Timestamp\""));
    }

    #[test]
    fn test_column_names() {
        let schema = vec![
            SchemaField::new("id", KalamDataType::BigInt, 0),
            SchemaField::new("name", KalamDataType::Text, 1),
        ];
        let result = QueryResult::with_rows_and_schema(vec![], schema);

        let names = result.column_names();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn test_array_rows_format() {
        let rows = vec![
            vec![
                KalamCellValue::text("123"),
                KalamCellValue::text("Alice"),
                KalamCellValue::int(1699000000000000i64),
            ],
            vec![
                KalamCellValue::text("456"),
                KalamCellValue::text("Bob"),
                KalamCellValue::int(1699000001000000i64),
            ],
        ];
        let schema = vec![
            SchemaField::new("id", KalamDataType::BigInt, 0),
            SchemaField::new("name", KalamDataType::Text, 1),
            SchemaField::new("created_at", KalamDataType::Timestamp, 2),
        ];
        let result = QueryResult::with_rows_and_schema(rows, schema);

        assert_eq!(result.row_count, 2);
        let json = serde_json::to_string(&result).unwrap();
        // Rows should be arrays, not objects
        assert!(json.contains("[[\"123\",\"Alice\""));
    }

    #[test]
    fn test_error_response_serialization() {
        let response = SqlResponse::error(ErrorCode::InvalidSql, "Syntax error", 5.0);

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("error"));
        assert!(json.contains("INVALID_SQL"));
        assert!(json.contains("Syntax error"));
    }

    #[test]
    fn test_non_admin_sql_errors_are_redacted() {
        let response = SqlResponse::error_with_details_for_privilege(
            ErrorCode::SqlExecutionError,
            "Statement 1 failed: table 'secret.payroll' not found",
            "SELECT * FROM secret.payroll",
            5.0,
            false,
        );

        let error = response.error.expect("error response should include an error payload");
        assert_eq!(error.code, ErrorCode::SqlExecutionError);
        assert_eq!(error.message, "SQL statement failed");
        assert!(error.details.is_none());
    }

    #[test]
    fn test_admin_sql_errors_preserve_full_details() {
        let response = SqlResponse::error_with_details_for_privilege(
            ErrorCode::SqlExecutionError,
            "Statement 1 failed: table 'secret.payroll' not found",
            "SELECT * FROM secret.payroll",
            5.0,
            true,
        );

        let error = response.error.expect("error response should include an error payload");
        assert_eq!(error.code, ErrorCode::SqlExecutionError);
        assert_eq!(error.message, "Statement 1 failed: table 'secret.payroll' not found");
        assert_eq!(error.details.as_deref(), Some("SELECT * FROM secret.payroll"));
    }

    #[test]
    fn test_query_result_with_message() {
        let result = QueryResult::with_message("Table created successfully".to_string());

        assert_eq!(result.row_count, 0);
        assert!(result.rows.is_none());
        assert!(result.schema.is_empty());
        assert_eq!(result.message, Some("Table created successfully".to_string()));
        assert_eq!(result.as_user, "unknown".to_string());
    }

    #[test]
    fn test_query_result_with_affected_rows() {
        let result = QueryResult::with_affected_rows(5, Some("5 rows inserted".to_string()));

        assert_eq!(result.row_count, 5);
        assert!(result.rows.is_none());
        assert!(result.schema.is_empty());
        assert_eq!(result.message, Some("5 rows inserted".to_string()));
        assert_eq!(result.as_user, "unknown".to_string());
    }

    #[test]
    fn test_query_result_with_as_user() {
        let result = QueryResult::with_message("ok".to_string()).with_as_user("alice".to_string());

        assert_eq!(result.as_user, "alice".to_string());
    }
}
