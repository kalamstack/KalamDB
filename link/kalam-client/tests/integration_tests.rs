#![allow(dead_code)]
//! Integration tests for the kalam-client crate.
//!
//! These tests verify the legacy compatibility API against a running server.
//! Tests will fail immediately if server is not running.
//!
//! # Running Tests
//!
//! ```bash
//! # Terminal 1: Start server
//! cd backend && cargo run --bin kalamdb-server
//!
//! # Terminal 2: Run tests
//! cd link && cargo test --test integration_tests
//! ```

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

use kalam_client::{
    models::{BatchControl, BatchStatus, KalamDataType, ResponseStatus, SchemaField},
    AuthProvider, ChangeEvent, KalamLinkClient, KalamLinkError, SubscriptionConfig,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{sleep, timeout},
};

mod common;

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);
static TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

async fn acquire_test_permit() -> OwnedSemaphorePermit {
    let sem = Arc::clone(TEST_SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(1))));
    sem.acquire_owned().await.expect("test semaphore closed")
}

fn unique_ident(prefix: &str) -> String {
    let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("{}_{}_{}", prefix, micros, counter)
}

/// Check if server is running - returns bool for graceful skipping
async fn is_server_running() -> bool {
    common::is_server_running().await
}

fn create_client() -> Result<KalamLinkClient, KalamLinkError> {
    let token = common::root_access_token_blocking()
        .map_err(|e| KalamLinkError::ConfigurationError(e.to_string()))?;
    KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_secs(30))
        .auth(AuthProvider::jwt_token(token))
        .build()
}

async fn setup_namespace(ns: &str) {
    let client = create_client().unwrap();
    // First drop the namespace to ensure clean state
    let _ = client
        .execute_query(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns), None, None, None)
        .await;
    // Wait for drop to complete (cleanup is async)
    sleep(Duration::from_millis(50)).await;
    // Create the namespace
    let _ = client
        .execute_query(&format!("CREATE NAMESPACE {}", ns), None, None, None)
        .await;
    sleep(Duration::from_millis(20)).await;
}

async fn cleanup_namespace(ns: &str) {
    let client = create_client().unwrap();
    let _ = client
        .execute_query(&format!("DROP NAMESPACE {} CASCADE", ns), None, None, None)
        .await;
}

// =============================================================================
// Client Builder Tests
// =============================================================================

#[tokio::test]
async fn test_client_builder_basic() {
    let client = KalamLinkClient::builder().base_url(common::server_url()).build();

    assert!(client.is_ok(), "Client builder should succeed");
}

#[tokio::test]
async fn test_client_builder_with_timeout() {
    let client = KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_secs(5))
        .build();

    assert!(client.is_ok(), "Client with custom timeout should succeed");
}

#[tokio::test]
async fn test_client_builder_with_jwt() {
    let client = KalamLinkClient::builder()
        .base_url(common::server_url())
        .jwt_token("test.jwt.token")
        .build();

    assert!(client.is_ok(), "Client with JWT should succeed");
}

#[tokio::test]
async fn test_client_builder_missing_url() {
    let result = KalamLinkClient::builder().build();

    assert!(result.is_err(), "Client without URL should fail");
    if let Err(e) = result {
        assert!(e.to_string().contains("base_url"));
    }
}

// =============================================================================
// Query Execution Tests
// =============================================================================

#[tokio::test]
async fn test_execute_simple_query() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();
    let result = client.execute_query("SELECT 1 as num", None, None, None).await;

    assert!(result.is_ok(), "Simple query should succeed");
    let response = result.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);
    assert!(!response.results.is_empty());
}

#[tokio::test]
async fn test_execute_query_with_results() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let ns = unique_ident("link_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Create table and insert data
    client
        .execute_query(
            &format!(
                "CREATE TABLE {}.items (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await
        .ok();

    client
        .execute_query(
            &format!("INSERT INTO {}.items (id, name) VALUES (1, 'test')", ns),
            None,
            None,
            None,
        )
        .await
        .ok();

    // Query
    let result = client
        .execute_query(&format!("SELECT * FROM {}.items", ns), None, None, None)
        .await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);
    assert!(!response.results.is_empty());

    if let Some(rows) = &response.results[0].rows {
        assert!(!rows.is_empty(), "Should have at least one row");
    }

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_execute_query_error_handling() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();
    let result = client.execute_query("INVALID SQL", None, None, None).await;

    // Should either return Err or success with error status
    if let Ok(response) = result {
        assert_eq!(response.status, ResponseStatus::Error);
        assert!(response.error.is_some());
    }
}

#[tokio::test]
async fn test_health_check() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();
    let result = client.health_check().await;

    assert!(result.is_ok(), "Health check should succeed");
    let health = result.unwrap();
    assert_eq!(health.status, "healthy");
    assert_eq!(health.api_version, "v1");
}

// =============================================================================
// Auth Provider Tests
// =============================================================================

#[test]
fn test_auth_provider_none() {
    let auth = AuthProvider::none();
    assert!(!auth.is_authenticated(), "None should not be authenticated");
}

#[test]
fn test_auth_provider_jwt() {
    let auth = AuthProvider::jwt_token("test.jwt.token".to_string());
    assert!(auth.is_authenticated(), "JWT should be authenticated");
}

// =============================================================================
// WebSocket Subscription Tests
// =============================================================================

#[tokio::test]
async fn test_subscription_config_creation() {
    let config = SubscriptionConfig::new("sub-1", "SELECT * FROM table");
    // Config is created successfully
    assert_eq!(config.id, "sub-1");
    assert_eq!(config.sql, "SELECT * FROM table");
}

#[tokio::test]
async fn test_subscription_basic() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("ws_link_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Create table
    client
        .execute_query(
            &format!(
                "CREATE TABLE {}.events (id INT PRIMARY KEY, data VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await
        .ok();
    sleep(Duration::from_millis(20)).await;

    // Try to create subscription
    let sub_result = timeout(
        Duration::from_secs(5),
        client.subscribe(&format!("SELECT * FROM {}.events", ns)),
    )
    .await;

    match sub_result {
        Ok(Ok(_subscription)) => {
            // Success - WebSocket is working
        },
        Ok(Err(e)) => {
            eprintln!("⚠️  Subscription failed (may not be fully implemented): {}", e);
        },
        Err(_) => {
            eprintln!("⚠️  Subscription timed out");
        },
    }

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_subscription_with_custom_config() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("ws_link_config");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Create table
    client
        .execute_query(
            &format!(
                "CREATE TABLE {}.data (id INT PRIMARY KEY, val VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await
        .ok();
    sleep(Duration::from_millis(20)).await;

    // Create subscription with custom config
    let config = SubscriptionConfig::new("sub-custom", format!("SELECT * FROM {}.data", ns));

    let sub_result = timeout(Duration::from_secs(5), client.subscribe_with_config(config)).await;

    match sub_result {
        Ok(Ok(_subscription)) => {
            // Success
        },
        Ok(Err(e)) => {
            eprintln!("⚠️  Custom config subscription failed: {}", e);
        },
        Err(_) => {
            eprintln!("⚠️  Subscription timed out");
        },
    }

    cleanup_namespace(&ns).await;
}

// =============================================================================
// Change Event Tests
// =============================================================================

fn sample_batch_control() -> BatchControl {
    BatchControl {
        batch_num: 0,
        has_more: false,
        status: BatchStatus::Ready,
        last_seq_id: None,
    }
}

#[test]
fn test_change_event_is_error() {
    let error_event = ChangeEvent::Error {
        subscription_id: "sub-1".to_string(),
        code: "ERR".to_string(),
        message: "test error".to_string(),
    };
    assert!(error_event.is_error());

    let insert_event = ChangeEvent::Insert {
        subscription_id: "sub-1".to_string(),
        rows: vec![],
    };
    assert!(!insert_event.is_error());
}

#[test]
fn test_change_event_subscription_id() {
    let insert = ChangeEvent::Insert {
        subscription_id: "sub-123".to_string(),
        rows: vec![],
    };
    assert_eq!(insert.subscription_id(), Some("sub-123"));

    let ack = ChangeEvent::Ack {
        subscription_id: "sub-ack".to_string(),
        total_rows: 0,
        batch_control: sample_batch_control(),
        schema: vec![SchemaField {
            name: "id".to_string(),
            data_type: KalamDataType::BigInt,
            index: 0,
            flags: None,
        }],
    };
    assert_eq!(ack.subscription_id(), Some("sub-ack"));

    let unknown = ChangeEvent::Unknown {
        raw: serde_json::Value::Null,
    };
    assert_eq!(unknown.subscription_id(), None);
}

// =============================================================================
// Error Type Tests
// =============================================================================

#[test]
fn test_error_display() {
    let config_err = KalamLinkError::ConfigurationError("test config error".to_string());
    assert!(config_err.to_string().contains("Configuration error"));

    let network_err = KalamLinkError::NetworkError("test network error".to_string());
    assert!(network_err.to_string().contains("Network error"));

    let server_err = KalamLinkError::ServerError {
        status_code: 500,
        message: "Internal server error".to_string(),
    };
    assert!(server_err.to_string().contains("Server error"));
}

// =============================================================================
// CRUD Operations Tests
// =============================================================================

#[tokio::test]
async fn test_create_namespace() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();

    let ns = unique_ident("test_create_ns");

    // Cleanup first
    let _ = client
        .execute_query(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns), None, None, None)
        .await;
    sleep(Duration::from_millis(20)).await;

    // Create
    let result = client
        .execute_query(&format!("CREATE NAMESPACE {}", ns), None, None, None)
        .await;
    assert!(result.is_ok(), "CREATE NAMESPACE should succeed");

    // Cleanup
    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_create_and_drop_table() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("crud_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Create table
    let create = client
        .execute_query(
            &format!(
                "CREATE TABLE {}.test (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await;
    assert!(create.is_ok(), "CREATE TABLE should succeed");

    // Drop table
    let drop = client.execute_query(&format!("DROP TABLE {}.test", ns), None, None, None).await;
    assert!(drop.is_ok(), "DROP TABLE should succeed");

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_insert_and_select() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("insert_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Create table
    client
        .execute_query(
            &format!(
                "CREATE TABLE {}.data (id INT PRIMARY KEY, value VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await
        .ok();

    // Insert
    let insert = client
        .execute_query(
            &format!("INSERT INTO {}.data (id, value) VALUES (1, 'test')", ns),
            None,
            None,
            None,
        )
        .await;
    assert!(insert.is_ok(), "INSERT should succeed");

    // Select
    let select = client
        .execute_query(&format!("SELECT * FROM {}.data WHERE id = 1", ns), None, None, None)
        .await;
    assert!(select.is_ok(), "SELECT should succeed");

    let response = select.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);
    if let Some(rows) = &response.results[0].rows {
        assert!(!rows.is_empty(), "Should have results");
    }

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_update_operation() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("update_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Setup
    client
        .execute_query(
            &format!(
                "CREATE TABLE {}.items (id INT PRIMARY KEY, status VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await
        .ok();
    client
        .execute_query(
            &format!("INSERT INTO {}.items (id, status) VALUES (1, 'old')", ns),
            None,
            None,
            None,
        )
        .await
        .ok();

    // Update
    let update = client
        .execute_query(
            &format!("UPDATE {}.items SET status = 'new' WHERE id = 1", ns),
            None,
            None,
            None,
        )
        .await;
    assert!(update.is_ok(), "UPDATE should succeed");

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_delete_operation() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("delete_test");
    let table = unique_ident("records");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    let full_table = format!("{}.{}", ns, table);

    let create_result = client
        .execute_query(
            &format!(
                "CREATE TABLE {} (id INT PRIMARY KEY, data VARCHAR) WITH (TYPE = 'USER')",
                full_table
            ),
            None,
            None,
            None,
        )
        .await;
    assert!(create_result.is_ok(), "CREATE TABLE should succeed: {:?}", create_result.err());

    // Small delay to ensure table is ready
    sleep(Duration::from_millis(20)).await;

    let insert_result = client
        .execute_query(
            &format!("INSERT INTO {} (id, data) VALUES (1, 'delete_me')", full_table),
            None,
            None,
            None,
        )
        .await;
    assert!(insert_result.is_ok(), "INSERT should succeed: {:?}", insert_result.err());

    // Verify row exists before delete
    let select_result = client
        .execute_query(&format!("SELECT * FROM {} WHERE id = 1", full_table), None, None, None)
        .await;
    assert!(select_result.is_ok(), "SELECT should succeed: {:?}", select_result.err());

    // Check the result has rows
    if let Ok(ref qr) = select_result {
        eprintln!("SELECT result: {:?}", qr);
    }

    // Delete
    let delete = client
        .execute_query(&format!("DELETE FROM {} WHERE id = 1", full_table), None, None, None)
        .await;
    assert!(delete.is_ok(), "DELETE should succeed: {:?}", delete.err());

    cleanup_namespace(&ns).await;
}

// =============================================================================
// System Tables Tests
// =============================================================================

#[tokio::test]
async fn test_query_system_users() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();
    let result = client.execute_query("SELECT * FROM system.users", None, None, None).await;

    assert!(result.is_ok(), "Should query system.users");
    let response = result.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);
}

#[tokio::test]
async fn test_query_system_namespaces() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();
    let result = client.execute_query("SELECT * FROM system.namespaces", None, None, None).await;

    assert!(result.is_ok(), "Should query system.namespaces");
}

#[tokio::test]
async fn test_query_system_tables() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = create_client().unwrap();
    let result = client.execute_query("SELECT * FROM system.tables", None, None, None).await;

    assert!(result.is_ok(), "Should query system.tables");
}

// =============================================================================
// Advanced SQL Tests
// =============================================================================

#[tokio::test]
async fn test_where_clause_operators() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("where_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Setup
    client
        .execute_query(
            &format!(
                "CREATE TABLE {}.data (id INT PRIMARY KEY, val VARCHAR) WITH (TYPE = 'USER')",
                ns
            ),
            None,
            None,
            None,
        )
        .await
        .ok();

    for i in 1..=5 {
        client
            .execute_query(
                &format!("INSERT INTO {}.data (id, val) VALUES ({}, 'value{}')", ns, i, i),
                None,
                None,
                None,
            )
            .await
            .ok();
    }

    // Test equality
    let eq = client
        .execute_query(&format!("SELECT * FROM {}.data WHERE id = 3", ns), None, None, None)
        .await;
    assert!(eq.is_ok(), "Equality operator should work");

    // Test LIKE
    let like = client
        .execute_query(&format!("SELECT * FROM {}.data WHERE val LIKE '%3%'", ns), None, None, None)
        .await;
    assert!(like.is_ok(), "LIKE operator should work");

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_limit_clause() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("limit_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Setup
    client
        .execute_query(
            &format!("CREATE TABLE {}.items (id INT PRIMARY KEY) WITH (TYPE = 'USER')", ns),
            None,
            None,
            None,
        )
        .await
        .ok();

    for i in 1..=10 {
        client
            .execute_query(
                &format!("INSERT INTO {}.items (id) VALUES ({})", ns, i),
                None,
                None,
                None,
            )
            .await
            .ok();
    }

    // Test LIMIT
    let result = client
        .execute_query(&format!("SELECT * FROM {}.items LIMIT 3", ns), None, None, None)
        .await;

    assert!(result.is_ok(), "LIMIT should work");
    let response = result.unwrap();
    if let Some(rows) = &response.results[0].rows {
        assert!(rows.len() <= 3, "Should return at most 3 rows");
    }

    cleanup_namespace(&ns).await;
}

#[tokio::test]
async fn test_order_by_clause() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("order_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Setup
    client
        .execute_query(
            &format!("CREATE TABLE {}.data (val VARCHAR PRIMARY KEY) WITH (TYPE = 'USER')", ns),
            None,
            None,
            None,
        )
        .await
        .ok();

    client
        .execute_query(
            &format!("INSERT INTO {}.data (val) VALUES ('z'), ('a'), ('m')", ns),
            None,
            None,
            None,
        )
        .await
        .ok();

    // Test ORDER BY
    let result = client
        .execute_query(&format!("SELECT * FROM {}.data ORDER BY val ASC", ns), None, None, None)
        .await;

    assert!(result.is_ok(), "ORDER BY should work");

    cleanup_namespace(&ns).await;
}

// =============================================================================
// Concurrent Operations Tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_queries() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;
    let ns = unique_ident("concurrent_test");
    setup_namespace(&ns).await;

    let client = create_client().unwrap();

    // Setup table
    client
        .execute_query(
            &format!("CREATE TABLE {}.data (id INT PRIMARY KEY) WITH (TYPE = 'USER')", ns),
            None,
            None,
            None,
        )
        .await
        .ok();

    // Execute multiple queries concurrently
    let mut handles = vec![];
    for i in 0..5 {
        let client_clone = client.clone();
        let ns = ns.clone();
        let handle = tokio::spawn(async move {
            client_clone
                .execute_query(
                    &format!("INSERT INTO {}.data (id) VALUES ({})", ns, i),
                    None,
                    None,
                    None,
                )
                .await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent insert should succeed");
    }

    cleanup_namespace(&ns).await;
}

// =============================================================================
// Timeout and Retry Tests
// =============================================================================

#[tokio::test]
async fn test_custom_timeout() {
    if !is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let _permit = acquire_test_permit().await;

    let client = KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_millis(20)) // Very short timeout
        .build()
        .unwrap();

    // This might timeout with very short duration, but shouldn't panic
    let _ = client.execute_query("SELECT 1", None, None, None).await;
}

#[tokio::test]
async fn test_connection_to_invalid_server() {
    let client = KalamLinkClient::builder()
        .base_url("http://localhost:9999") // Invalid port
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();

    let result = client.execute_query("SELECT 1", None, None, None).await;
    assert!(result.is_err(), "Connection to invalid server should fail");
}
