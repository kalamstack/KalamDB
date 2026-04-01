//! SQL request model
//!
//! This module defines the structure for SQL query requests sent to the `/v1/api/sql` endpoint.

use kalamdb_commons::NamespaceId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Request payload for SQL execution via REST API
///
/// # Example (without parameters)
/// ```json
/// {
///   "sql": "SELECT * FROM messages WHERE user_id = CURRENT_USER()"
/// }
/// ```
///
/// # Example (with parameters)
/// ```json
/// {
///   "sql": "SELECT * FROM users WHERE id = $1 AND status = $2",
///   "params": [42, "active"]
/// }
/// ```
///
/// # Example (with namespace)
/// ```json
/// {
///   "sql": "SELECT * FROM messages",
///   "namespace_id": "chat"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The SQL statement(s) to execute. Multiple statements can be separated by semicolons.
    pub sql: String,

    /// Optional query parameters for parameterized queries ($1, $2, ...).
    /// Parameters are validated (max 50 params, 512KB each) before execution.
    /// Supported types: null, bool, integers, floats, strings, timestamps.
    #[serde(default)]
    pub params: Option<Vec<JsonValue>>,

    /// Optional namespace ID for unqualified table names.
    /// When set, queries like `SELECT * FROM users` resolve to `namespace_id.users`.
    /// Set via `USE namespace` command in CLI clients.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<NamespaceId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request_serialization() {
        let request = QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            params: None,
            namespace_id: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("SELECT * FROM users"));

        let deserialized: QueryRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sql, "SELECT * FROM users");
        assert!(deserialized.params.is_none());
    }

    #[test]
    fn test_query_request_with_multiple_statements() {
        let request = QueryRequest {
            sql: "INSERT INTO users VALUES (1, 'Alice'); SELECT * FROM users;".to_string(),
            params: None,
            namespace_id: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: QueryRequest = serde_json::from_str(&json).unwrap();
        assert!(deserialized.sql.contains("INSERT"));
        assert!(deserialized.sql.contains("SELECT"));
    }

    #[test]
    fn test_query_request_with_parameters() {
        let request = QueryRequest {
            sql: "SELECT * FROM users WHERE id = $1 AND status = $2".to_string(),
            params: Some(vec![serde_json::json!(42), serde_json::json!("active")]),
            namespace_id: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("$1"));
        assert!(json.contains("params"));

        let deserialized: QueryRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.params.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_query_request_params_optional() {
        // Test that params field is optional (serde default)
        let json = r#"{"sql": "SELECT * FROM users"}"#;
        let deserialized: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.sql, "SELECT * FROM users");
        assert!(deserialized.params.is_none());
    }

    #[test]
    fn test_query_request_with_namespace() {
        let json = r#"{"sql": "SELECT * FROM messages", "namespace_id": "chat"}"#;
        let deserialized: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.sql, "SELECT * FROM messages");
        assert_eq!(deserialized.namespace_id, Some(NamespaceId::new("chat")));
    }
}
