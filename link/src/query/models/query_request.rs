use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Request payload for SQL query execution.
///
/// # Examples
///
/// ```rust
/// use kalam_link::QueryRequest;
/// use serde_json::json;
///
/// // Simple query without parameters
/// let request = QueryRequest {
///     sql: "SELECT * FROM users".to_string(),
///     params: None,
///     namespace_id: None,
/// };
///
/// // Parametrized query
/// let request = QueryRequest {
///     sql: "SELECT * FROM users WHERE id = $1".to_string(),
///     params: Some(vec![json!(42)]),
///     namespace_id: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// SQL query string (may contain $1, $2... placeholders)
    pub sql: String,

    /// Optional parameter values for placeholders
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<JsonValue>>,

    /// Optional namespace ID for unqualified table names.
    /// When set, queries like `SELECT * FROM users` resolve to `namespace_id.users`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
}
