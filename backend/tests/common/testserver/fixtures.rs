//! Test fixtures for KalamDB integration tests.
//!
//! This module provides reusable fixtures for creating test data:
//! - Namespaces with various configurations
//! - User tables with different schemas and flush policies
//! - Shared tables for multi-user scenarios
//! - Stream tables for event processing
//! - Sample data generators for testing queries
//!
//! # Usage
//!
//! ```no_run
//! use integration::common::fixtures;
//!
//! #[actix_web::test]
//! async fn test_example() {
//!     let server = TestServer::new_shared().await;
//!
//!     // Create namespace
//!     fixtures::create_namespace(&server, "app").await;
//!
//!     // Create user table with sample data
//!     fixtures::create_messages_table(&server, "app").await;
//!     fixtures::insert_sample_messages(&server, "app", "user123", 10).await;
//!
//!     // Run your test...
//! }
//! ```

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use anyhow::Result;
use kalam_client::models::{QueryResponse, QueryResult, ResponseStatus};
use kalamdb_commons::models::NamespaceId;
use serde_json::json;
use tokio::time::sleep;

use super::TestServer;

static UNIQUE_NS_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_namespace(prefix: &str) -> String {
    let id = UNIQUE_NS_COUNTER.fetch_add(1, Ordering::Relaxed);
    let module_tag = module_path!().replace("::", "_");
    format!("{}_{}_{}", prefix, module_tag, id)
}

/// Execute SQL with a specific user context.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `sql` - SQL query to execute
/// * `user_id` - User ID for execution context
///
/// # Returns
///
/// * `Result<QueryResponse>` - Response from the SQL execution
///
/// # Example
///
/// ```no_run
/// fixtures::execute_sql(&server, "CREATE TABLE test.messages (...)", "user1")
///     .await
///     .unwrap();
/// ```
pub async fn execute_sql(server: &TestServer, sql: &str, user_id: &str) -> Result<QueryResponse> {
    Ok(server.execute_sql_as_user(sql, user_id).await)
}

/// Create a namespace with default options.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Name of the namespace to create
///
/// # Example
///
/// ```no_run
/// fixtures::create_namespace(&server, "app").await;
/// ```
pub async fn create_namespace(server: &TestServer, namespace: &str) -> QueryResponse {
    let sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    // Perform namespace creation as 'root' admin user for RBAC enforcement
    let resp = server.execute_sql_as_user(&sql, "root").await;
    if resp.status != ResponseStatus::Success {
        panic!("CREATE NAMESPACE failed: ns={}, error={:?}", namespace, resp.error);
    }
    // Namespaces are registered asynchronously, so poll until the system catalog observes it.
    let namespaces_provider = server.app_context.system_tables().namespaces();
    let namespace_id = NamespaceId::new(namespace);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match namespaces_provider.get_namespace(&namespace_id) {
            Ok(Some(_)) => break,
            Ok(None) => {},
            Err(err) => {
                panic!("Failed to verify namespace '{}': {:?}", namespace, err);
            },
        }
        if Instant::now() >= deadline {
            panic!("Namespace '{}' was not visible within 2s after creation", namespace);
        }
        sleep(Duration::from_millis(25)).await;
    }
    resp
}

/// Create a namespace with specific options.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Name of the namespace
/// * `options` - JSON options for the namespace
pub async fn create_namespace_with_options(
    server: &TestServer,
    namespace: &str,
    options: &str,
) -> QueryResponse {
    let sql = format!("CREATE NAMESPACE {} WITH OPTIONS {}", namespace, options);
    server.execute_sql(&sql).await
}

/// Drop a namespace (with CASCADE to drop all tables).
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Name of the namespace to drop
pub async fn drop_namespace(server: &TestServer, namespace: &str) -> QueryResponse {
    let sql = format!("DROP NAMESPACE {} CASCADE", namespace);
    server.execute_sql(&sql).await
}

/// Create a simple user table for messages.
///
/// Schema:
/// - id: INT (auto-increment)
/// - user_id: VARCHAR (for filtering)
/// - content: VARCHAR
/// - created_at: TIMESTAMP
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
///
/// # Example
///
/// ```no_run
/// fixtures::create_messages_table(&server, "app", Some("user123")).await;
/// ```
pub async fn create_messages_table(
    server: &TestServer,
    namespace: &str,
    _user_id: Option<&str>,
) -> QueryResponse {
    let sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {}.messages (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            user_id VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:100'
        )"#,
        namespace
    );
    // Use system user since only System/Dba roles can create tables
    let resp = server.execute_sql_as_user(&sql, "system").await;
    if resp.status != ResponseStatus::Success {
        // Treat already-exists as success for idempotent tests
        let already_exists = resp
            .error
            .as_ref()
            .map(|e| e.message.to_lowercase().contains("already exists"))
            .unwrap_or(false);
        if already_exists {
            return QueryResponse {
                status: ResponseStatus::Success,
                results: vec![QueryResult {
                    schema: vec![],
                    rows: None,
                    named_rows: None,
                    row_count: 0,
                    message: Some("Table already existed".to_string()),
                }],
                took: Some(0.0),
                error: None,
            };
        } else {
            eprintln!(
                "CREATE MESSAGES TABLE failed: status={}, error={:?}",
                resp.status, resp.error
            );
        }
    }
    resp
}

/// Create a user table with custom flush policy.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `table_name` - Table name
/// * `flush_rows` - Number of rows before flush
pub async fn create_user_table_with_flush(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
    flush_rows: u32,
) -> QueryResponse {
    let sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {}.{} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            user_id VARCHAR NOT NULL,
            data VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:{}'
        )"#,
        namespace, table_name, flush_rows
    );
    server.execute_sql_as_user(&sql, "system").await
}

/// Create a shared table (accessible to all users).
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `table_name` - Table name
pub async fn create_shared_table(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
) -> QueryResponse {
    let columns = if table_name == "config" {
        r#"
            name TEXT PRIMARY KEY NOT NULL,
            value TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        "#
    } else {
        r#"
            conversation_id TEXT PRIMARY KEY NOT NULL,
            title TEXT,
            status TEXT,
            participant_count BIGINT,
            created_at TIMESTAMP
        "#
    };

    let sql = format!(
        "CREATE TABLE {}.{} ({}) WITH (TYPE = 'SHARED', FLUSH_POLICY = 'rows:50')",
        namespace, table_name, columns
    );
    server.execute_sql(&sql).await
}

/// Create a stream table for event processing.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `table_name` - Table name
/// * `ttl_seconds` - TTL in seconds for automatic eviction
pub async fn create_stream_table(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
    ttl_seconds: u32,
) -> QueryResponse {
    let sql = format!(
        r#"CREATE TABLE IF NOT EXISTS {}.{} (
            event_id TEXT NOT NULL,
            event_type TEXT,
            payload TEXT,
            timestamp TIMESTAMP
        ) WITH (
            TYPE = 'STREAM',
            TTL_SECONDS = {}
        )"#,
        namespace, table_name, ttl_seconds
    );
    let resp = server.execute_sql(&sql).await;
    if resp.status != ResponseStatus::Success {
        let already_exists = resp
            .error
            .as_ref()
            .map(|e| e.message.to_lowercase().contains("already exists"))
            .unwrap_or(false);
        if already_exists {
            return QueryResponse {
                status: ResponseStatus::Success,
                results: vec![QueryResult {
                    schema: vec![],
                    rows: None,
                    named_rows: None,
                    row_count: 0,
                    message: Some("Table already existed".to_string()),
                }],
                took: Some(0.0),
                error: None,
            };
        }
    }
    resp
}

/// Drop a table.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `table_name` - Table name
pub async fn drop_table(server: &TestServer, namespace: &str, table_name: &str) -> QueryResponse {
    let lookup_sql = format!(
        "SELECT table_type FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table_name
    );
    let lookup_response = server.execute_sql(&lookup_sql).await;

    let table_type = if lookup_response.status == ResponseStatus::Success {
        lookup_response
            .results
            .first()
            .and_then(|result| super::QueryResultTestExt::row_as_map(result, 0))
            .and_then(|row| row.get("table_type").cloned())
            .and_then(|value| value.as_str().map(|s| s.to_string()))
    } else {
        None
    };

    let drop_sql = match table_type.as_deref() {
        Some("user") => format!("DROP USER TABLE {}.{}", namespace, table_name),
        Some("shared") => format!("DROP SHARED TABLE {}.{}", namespace, table_name),
        Some("stream") => format!("DROP STREAM TABLE {}.{}", namespace, table_name),
        _ => format!("DROP TABLE {}.{}", namespace, table_name),
    };

    server.execute_sql(&drop_sql).await
}

/// Insert sample messages into a messages table.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `user_id` - User ID for the messages
/// * `count` - Number of messages to insert
///
/// # Returns
///
/// Vector of QueryResponse for each INSERT operation
pub async fn insert_sample_messages(
    server: &TestServer,
    namespace: &str,
    user_id: &str,
    count: usize,
) -> Vec<QueryResponse> {
    let mut responses = Vec::new();

    for i in 0..count {
        let sql = format!(
            r#"INSERT INTO {}.messages (user_id, content) VALUES ('{}', 'Message {}')"#,
            namespace, user_id, i
        );
        responses.push(server.execute_sql_as_user(&sql, user_id).await);
    }

    responses
}

/// Insert a single message.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `user_id` - User ID
/// * `content` - Message content
pub async fn insert_message(
    server: &TestServer,
    namespace: &str,
    user_id: &str,
    content: &str,
) -> QueryResponse {
    let sql = format!(
        r#"INSERT INTO {}.messages (user_id, content) VALUES ('{}', '{}')"#,
        namespace, user_id, content
    );
    let resp = server.execute_sql_as_user(&sql, user_id).await;
    if resp.status != ResponseStatus::Success {
        eprintln!("INSERT failed: status={}, error={:?}", resp.status, resp.error);
    }
    resp
}

/// Update a message by ID.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `id` - Message ID
/// * `new_content` - New content
pub async fn update_message(
    server: &TestServer,
    namespace: &str,
    id: i64,
    new_content: &str,
) -> QueryResponse {
    let sql = format!(
        r#"UPDATE {}.messages SET content = '{}' WHERE id = {}"#,
        namespace, new_content, id
    );
    server.execute_sql(&sql).await
}

/// Delete a message by ID (soft delete).
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `id` - Message ID
pub async fn delete_message(server: &TestServer, namespace: &str, id: i64) -> QueryResponse {
    let sql = format!(r#"DELETE FROM {}.messages WHERE id = {}"#, namespace, id);
    server.execute_sql(&sql).await
}

/// Query all messages for a user.
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
/// * `user_id` - User ID
pub async fn query_user_messages(
    server: &TestServer,
    namespace: &str,
    user_id: &str,
) -> QueryResponse {
    let sql = format!(
        r#"SELECT * FROM {}.messages WHERE user_id = '{}' ORDER BY created_at DESC"#,
        namespace, user_id
    );
    let resp = server.execute_sql_as_user(&sql, user_id).await;
    if resp.status != ResponseStatus::Success {
        eprintln!(
            "QUERY USER MESSAGES failed: ns={}, user={}, error={:?}",
            namespace, user_id, resp.error
        );
    }
    resp
}

/// Generate sample user data.
///
/// Returns a vector of (user_id, username, email) tuples.
///
/// # Arguments
///
/// * `count` - Number of users to generate
pub fn generate_user_data(count: usize) -> Vec<(String, String, String)> {
    (0..count)
        .map(|i| (format!("user{}", i), format!("testuser{}", i), format!("user{}@example.com", i)))
        .collect()
}

/// Generate sample event data.
///
/// Returns a vector of (event_type, payload) tuples.
///
/// # Arguments
///
/// * `count` - Number of events to generate
pub fn generate_stream_events(count: usize) -> Vec<(String, String)> {
    let event_types = ["login", "logout", "purchase", "view", "click"];

    (0..count)
        .map(|i| {
            let event_type = event_types[i % event_types.len()].to_string();
            let payload = json!({
                "user_id": format!("user{}", i % 10),
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "data": format!("payload_{}", i)
            })
            .to_string();

            (event_type, payload)
        })
        .collect()
}

/// Create a complete test environment with namespace and tables.
///
/// Creates:
/// - Namespace
/// - User messages table
/// - Shared configuration table
/// - Stream events table
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `namespace` - Namespace name
///
/// # Returns
///
/// Result indicating success or failure
pub async fn setup_complete_environment(server: &TestServer, namespace: &str) -> Result<()> {
    // Create namespace
    let resp = create_namespace(server, namespace).await;
    if resp.status != ResponseStatus::Success {
        anyhow::bail!("Failed to create namespace: {:?}", resp.error);
    }

    // Create user table
    let resp = create_messages_table(server, namespace, Some("user123")).await;
    if resp.status != ResponseStatus::Success {
        anyhow::bail!("Failed to create messages table: {:?}", resp.error);
    }

    // Create shared table
    let resp = create_shared_table(server, namespace, "config").await;
    if resp.status != ResponseStatus::Success {
        anyhow::bail!("Failed to create shared table: {:?}", resp.error);
    }

    // Create stream table
    let resp = create_stream_table(server, namespace, "events", 3600).await;
    if resp.status != ResponseStatus::Success {
        anyhow::bail!("Failed to create stream table: {:?}", resp.error);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn test_create_namespace() {
        let server = TestServer::new_shared().await;
        let response = create_namespace(&server, "test_ns").await;
        assert_eq!(response.status, ResponseStatus::Success);
        assert!(server.namespace_exists("test_ns").await);
    }

    #[actix_web::test]
    async fn test_create_messages_table() {
        let server = TestServer::new_shared().await;
        create_namespace(&server, "app").await;

        let response = create_messages_table(&server, "app", Some("user123")).await;
        if response.status != ResponseStatus::Success {
            // In shared TestServer runs, provider may already be registered; accept idempotent
            // already-exists
            let msg = response.error.as_ref().map(|e| e.message.clone()).unwrap_or_default();
            assert!(
                msg.contains("already exists"),
                "CREATE TABLE failed unexpectedly: {:?}",
                response.error
            );
        }
        assert!(server.table_exists("app", "messages").await);
    }

    #[actix_web::test]
    async fn test_insert_sample_messages() {
        let server = TestServer::new_shared().await;
        create_namespace(&server, "app").await;
        create_messages_table(&server, "app", Some("user123")).await;

        let responses = insert_sample_messages(&server, "app", "user123", 5).await;
        assert_eq!(responses.len(), 5);

        for (i, response) in responses.iter().enumerate() {
            if response.status != ResponseStatus::Success {
                eprintln!("Response {}: status={}, error={:?}", i, response.status, response.error);
            }
            assert_eq!(
                response.status,
                ResponseStatus::Success,
                "Insert {} failed: {:?}",
                i,
                response.error
            );
        }
    }

    #[actix_web::test]
    async fn test_generate_user_data() {
        let users = generate_user_data(5);
        assert_eq!(users.len(), 5);
        assert_eq!(users[0].0, "user0");
        assert_eq!(users[0].1, "testuser0");
        assert_eq!(users[0].2, "user0@example.com");
    }

    #[actix_web::test]
    async fn test_setup_complete_environment() {
        let server = TestServer::new_shared().await;
        // Use a unique namespace to avoid collision with parallel cleanup tests
        let ns = unique_namespace("unique_setup_env");
        let result = setup_complete_environment(&server, ns.as_str()).await;
        if let Err(e) = &result {
            eprintln!("Setup failed with error: {}", e);
        }
        assert!(result.is_ok(), "Setup failed: {:?}", result.err());

        // Verify all components exist
        assert!(server.namespace_exists(ns.as_str()).await);
        assert!(server.table_exists(ns.as_str(), "messages").await);
        assert!(server.table_exists(ns.as_str(), "config").await);
        assert!(server.table_exists(ns.as_str(), "events").await);

        // Manual cleanup
        let _ = drop_namespace(&server, ns.as_str()).await;
    }
}
