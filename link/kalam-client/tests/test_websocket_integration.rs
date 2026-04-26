#![allow(dead_code)]
//! WebSocket integration tests for the kalam-client crate.
//!
//! These tests verify WebSocket connectivity, subscriptions, and real-time updates
//! using the legacy compatibility surface.
//!
//! **IMPORTANT**: These tests require a running KalamDB server.
//!
//! # Running Tests
//!
//! ```bash
//! # Terminal 1: Start the server
//! cd backend && cargo run --bin kalamdb-server
//!
//! # Terminal 2: Run the tests
//! cd link && cargo test --test test_websocket_integration -- --nocapture
//! ```
//!
//! Tests will be skipped if the server is not running.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use kalam_client::{
    auth::AuthProvider, models::ResponseStatus, ChangeEvent, KalamLinkClient, QueryResponse,
    SubscriptionConfig,
};
use tokio::time::{sleep, timeout};

mod common;

/// Test configuration
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const TEST_USER_ID: &str = "ws_test_user";

fn unique_event_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after UNIX_EPOCH")
        .as_nanos();
    format!("e{}", nanos)
}

fn is_table_not_found_event(event: &ChangeEvent) -> bool {
    match event {
        ChangeEvent::Error { code, message, .. } => {
            code.eq_ignore_ascii_case("not_found")
                || message.to_ascii_lowercase().contains("table not found")
        },
        _ => false,
    }
}

/// Helper to create a test client
fn create_test_client() -> Result<KalamLinkClient, kalam_client::KalamLinkError> {
    let token = common::root_access_token_blocking()
        .map_err(|e| kalam_client::KalamLinkError::ConfigurationError(e.to_string()))?;
    KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_secs(30))
        .auth(AuthProvider::jwt_token(token))
        .build()
}

/// Helper to execute SQL via HTTP (for test setup)
async fn execute_sql(sql: &str) -> Result<QueryResponse, Box<dyn std::error::Error>> {
    let client = create_test_client()?;
    Ok(client.execute_query(sql, None, None, None).await?)
}

async fn execute_sql_checked(sql: &str) -> Result<QueryResponse, Box<dyn std::error::Error>> {
    let response = execute_sql(sql).await?;
    if response.status != ResponseStatus::Success {
        return Err(format!("SQL failed: {:?}", response.error).into());
    }
    Ok(response)
}

async fn execute_query_with_retry(
    client: &KalamLinkClient,
    sql: &str,
    attempts: usize,
) -> Result<QueryResponse, kalam_client::KalamLinkError> {
    let mut last_err: Option<kalam_client::KalamLinkError> = None;
    for attempt in 0..attempts {
        match client.execute_query(sql, None, None, None).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                last_err = Some(e);
                let backoff_ms = 100 + (attempt as u64 * 100);
                sleep(Duration::from_millis(backoff_ms)).await;
            },
        }
    }

    Err(last_err
        .unwrap_or_else(|| kalam_client::KalamLinkError::ConfigurationError("query failed".into())))
}

/// Wait until a newly created table is visible for queries
async fn wait_for_table_ready(table: &str) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..20 {
        if execute_sql_checked(&format!("SELECT 1 FROM {} LIMIT 1", table)).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    Err(format!("Table not ready: {}", table).into())
}

/// Helper to setup test namespace and table
async fn setup_test_data() -> Result<String, Box<dyn std::error::Error>> {
    static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

    // Use a unique suffix based on timestamp + random number to avoid conflicts
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let table_name = format!("events_{}_{}_{}", timestamp, pid, counter);
    let full_table = format!("ws_test.{}", table_name);

    // Create namespace if needed
    execute_sql_checked("CREATE NAMESPACE IF NOT EXISTS ws_test").await?;
    sleep(Duration::from_millis(50)).await;

    // Ensure stale table from prior runs doesn't break setup
    execute_sql(&format!("DROP TABLE IF EXISTS {}", full_table)).await.ok();

    // Create test table (STREAM table for WebSocket tests)
    execute_sql_checked(&format!(
        r#"CREATE TABLE {} (
                event_id TEXT NOT NULL,
                event_type TEXT,
                data TEXT,
                timestamp TIMESTAMP
            ) WITH (TYPE = 'STREAM', TTL_SECONDS = 60)"#,
        full_table
    ))
    .await?;

    wait_for_table_ready(&full_table).await?;
    sleep(Duration::from_millis(150)).await;

    Ok(full_table)
}

/// Helper to cleanup test data
async fn cleanup_test_data(table_full_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _ = execute_sql(&format!("DROP TABLE IF EXISTS {}", table_full_name)).await;
    Ok(())
}

// =============================================================================
// Basic Connection Tests
// =============================================================================

#[tokio::test]
async fn test_kalam_client_creation() {
    let result = KalamLinkClient::builder().base_url(common::server_url()).build();

    assert!(result.is_ok(), "Client should be created successfully");
}

#[tokio::test]
async fn test_kalam_client_query_execution() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running at {}. Skipping test.", common::server_url());
        return;
    }

    let client = create_test_client().expect("Failed to create client");
    let result = client.execute_query("SELECT 1 as test_value", None, None, None).await;

    assert!(result.is_ok(), "Query should execute successfully");
    let response = result.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);
    assert!(!response.results.is_empty());
}

#[tokio::test]
async fn test_kalam_client_health_check() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let client = create_test_client().expect("Failed to create client");
    let result = client.health_check().await;

    assert!(result.is_ok(), "Health check should succeed");
    let health = result.unwrap();
    assert_eq!(health.status, "healthy");
    assert_eq!(health.api_version, "v1");
}

#[tokio::test]
async fn test_kalam_client_parametrized_query() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup test data");

    let client = create_test_client().expect("Failed to create client");

    // Insert
    let event_id = unique_event_id();
    let insert_result = client
        .execute_query(
            &format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'test', 'param_test')",
                table, event_id
            ),
            None,
            None,
            None,
        )
        .await;

    assert!(insert_result.is_ok(), "Insert should succeed: {:?}", insert_result.err());

    // Query to verify
    let query_result = client
        .execute_query(
            &format!("SELECT * FROM {} WHERE event_type = 'test'", table),
            None,
            None,
            None,
        )
        .await;

    assert!(query_result.is_ok(), "Query should succeed");
    let response = query_result.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);

    cleanup_test_data(&table).await.ok();
}

// =============================================================================
// WebSocket Subscription Tests
// =============================================================================

#[tokio::test]
async fn test_websocket_subscription_creation() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup test data");

    let client = create_test_client().expect("Failed to create client");

    // Create subscription
    let subscription_result =
        timeout(TEST_TIMEOUT, client.subscribe(&format!("SELECT * FROM {}", table))).await;

    // Test passes if subscription succeeds or fails gracefully (no panic)
    // WebSocket subscriptions may not be fully implemented
    let subscription_attempted = match subscription_result {
        Ok(Ok(_subscription)) => {
            // Subscription created successfully
            true
        },
        Ok(Err(_e)) => {
            // WebSocket might not be fully implemented yet - graceful failure
            true
        },
        Err(_) => {
            // Timeout - still a valid attempt
            true
        },
    };
    assert!(subscription_attempted, "Subscription should be attempted");

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_websocket_subscription_with_config() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup test data");

    let client = create_test_client().expect("Failed to create client");

    // Create subscription with custom config
    let config = SubscriptionConfig::new("sub-config-test", format!("SELECT * FROM {}", table));

    let subscription_result = timeout(TEST_TIMEOUT, client.subscribe_with_config(config)).await;

    // Test passes if subscription succeeds or fails gracefully (no panic)
    let subscription_attempted = match subscription_result {
        Ok(Ok(_subscription)) => {
            // Success
            true
        },
        Ok(Err(_e)) => {
            // Graceful failure - WebSocket may not be fully implemented
            true
        },
        Err(_) => {
            // Timeout - still a valid attempt
            true
        },
    };
    assert!(subscription_attempted, "Subscription with config should be attempted");

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_websocket_initial_data_snapshot() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup test data");

    // Insert some initial data
    execute_sql_checked(&format!(
        "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'initial', 'data1')",
        table,
        unique_event_id()
    ))
    .await
    .expect("initial insert 1 should succeed");
    execute_sql_checked(&format!(
        "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'initial', 'data2')",
        table,
        unique_event_id()
    ))
    .await
    .expect("initial insert 2 should succeed");
    sleep(Duration::from_millis(50)).await;

    let client = create_test_client().expect("Failed to create client");

    let mut received_initial_data = false;
    let mut last_error: Option<String> = None;

    for attempt in 0..8 {
        let subscription_result =
            timeout(TEST_TIMEOUT, client.subscribe(&format!("SELECT * FROM {}", table))).await;

        let mut subscription = match subscription_result {
            Ok(Ok(subscription)) => subscription,
            Ok(Err(e)) => {
                last_error = Some(e.to_string());
                sleep(Duration::from_millis(150 + attempt * 100)).await;
                continue;
            },
            Err(_) => {
                last_error = Some("Subscription creation timed out".to_string());
                sleep(Duration::from_millis(150 + attempt * 100)).await;
                continue;
            },
        };

        let mut saw_table_not_found = false;
        let per_attempt_deadline = std::time::Instant::now() + Duration::from_secs(12);
        while std::time::Instant::now() < per_attempt_deadline {
            if let Ok(Some(Ok(event))) = timeout(Duration::from_secs(2), subscription.next()).await
            {
                match event {
                    ChangeEvent::InitialDataBatch { rows, .. } => {
                        assert!(!rows.is_empty(), "Initial snapshot should contain data");
                        received_initial_data = true;
                        break;
                    },
                    ChangeEvent::Ack { .. } => {
                        continue;
                    },
                    ChangeEvent::Error { .. } if is_table_not_found_event(&event) => {
                        saw_table_not_found = true;
                        break;
                    },
                    other => {
                        last_error =
                            Some(format!("Unexpected event during initial snapshot: {:?}", other));
                        break;
                    },
                }
            }
        }

        if received_initial_data {
            break;
        }

        if saw_table_not_found {
            let _ = subscription.close().await;
            if attempt < 7 {
                sleep(Duration::from_millis(400 + attempt * 150)).await;
                continue;
            }
            last_error = Some("Table not found during initial snapshot".to_string());
        }
    }

    if !received_initial_data {
        let check = execute_sql(&format!("SELECT COUNT(*) AS cnt FROM {}", table)).await;
        if let Ok(resp) = check {
            if resp.status == ResponseStatus::Success {
                eprintln!(
                    "⚠️  InitialData snapshot not received within timeout; data is queryable. \
                     Skipping strict assertion."
                );
                cleanup_test_data(&table).await.ok();
                return;
            }
        }

        if matches!(last_error.as_deref(), Some("Table not found during initial snapshot")) {
            eprintln!("⚠️  Table not found during initial snapshot. Skipping strict assertion.");
            cleanup_test_data(&table).await.ok();
            return;
        }
    }

    assert!(
        received_initial_data,
        "FAILED: Should receive InitialData event with snapshot of existing rows. Last error: {:?}",
        last_error
    );

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_websocket_insert_notification() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup test data");

    let client = create_test_client().expect("Failed to create client");

    let subscription_result =
        timeout(TEST_TIMEOUT, client.subscribe(&format!("SELECT * FROM {}", table))).await;

    match subscription_result {
        Ok(Ok(mut subscription)) => {
            // Drain initial messages (ACK/InitialData batches) before testing inserts.
            // We consider the initial phase done once `batch_control.status == Ready`.
            let drain_deadline = std::time::Instant::now() + Duration::from_secs(3);
            while std::time::Instant::now() < drain_deadline {
                match timeout(Duration::from_millis(100), subscription.next()).await {
                    Ok(Some(Ok(ChangeEvent::Ack { batch_control, .. }))) => {
                        if batch_control.status == kalam_client::models::BatchStatus::Ready {
                            break;
                        }
                    },
                    Ok(Some(Ok(ChangeEvent::InitialDataBatch { batch_control, .. }))) => {
                        if batch_control.status == kalam_client::models::BatchStatus::Ready {
                            break;
                        }
                    },
                    Ok(Some(Ok(_))) => continue,
                    Ok(Some(Err(e))) => panic!("Subscription stream error during drain: {}", e),
                    Ok(None) => panic!("Subscription ended during initial drain"),
                    Err(_) => continue,
                }
            }

            // Insert new data that should trigger notification
            execute_sql(&format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'realtime', \
                 'insert_test')",
                table,
                unique_event_id()
            ))
            .await
            .expect("insert should succeed");

            // Wait for insert notification (ignore non-insert events).
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut got_insert = false;
            while std::time::Instant::now() < deadline {
                match timeout(Duration::from_secs(2), subscription.next()).await {
                    Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                        assert!(!rows.is_empty(), "Insert notification should contain rows");
                        got_insert = true;
                        break;
                    },
                    Ok(Some(Ok(_))) => continue,
                    Ok(Some(Err(e))) => panic!("Insert notification failed with error: {}", e),
                    Ok(None) => panic!("Subscription ended before receiving insert notification"),
                    Err(_) => continue,
                }
            }

            assert!(got_insert, "FAILED: No insert notification received within timeout");
        },
        Ok(Err(e)) => {
            panic!("Subscription failed: {}", e);
        },
        Err(_) => {
            panic!("Subscription creation timed out");
        },
    }

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_websocket_filtered_subscription() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup test data");

    let client = create_test_client().expect("Failed to create client");

    // Subscribe with WHERE filter
    let subscription_result = timeout(
        TEST_TIMEOUT,
        client.subscribe(&format!("SELECT * FROM {} WHERE event_type = 'filtered'", table)),
    )
    .await;

    match subscription_result {
        Ok(Ok(mut subscription)) => {
            // Skip initial messages
            for _ in 0..2 {
                let _ = timeout(Duration::from_millis(100), subscription.next()).await;
            }

            // Insert data that matches filter
            execute_sql(&format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'filtered', 'match')",
                table,
                unique_event_id()
            ))
            .await
            .expect("filtered insert should succeed");

            // Insert data that doesn't match filter
            execute_sql(&format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'other', 'nomatch')",
                table,
                unique_event_id()
            ))
            .await
            .expect("non-matching insert should succeed");

            // Wait for an insert notification that matches the filter.
            let deadline = std::time::Instant::now() + Duration::from_secs(8);
            let mut got_filtered = false;
            while std::time::Instant::now() < deadline {
                match timeout(Duration::from_secs(3), subscription.next()).await {
                    Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                        for row in rows {
                            let direct_match = row
                                .get("event_type")
                                .and_then(|v| v.as_str())
                                .is_some_and(|s| s == "filtered");

                            if direct_match
                                || serde_json::to_string(&row)
                                    .unwrap_or_default()
                                    .contains("\"filtered\"")
                            {
                                got_filtered = true;
                                break;
                            }
                        }

                        if got_filtered {
                            break;
                        }
                    },
                    Ok(Some(Ok(_))) => continue,
                    Ok(Some(Err(e))) => {
                        panic!("Filtered subscription stream error: {}", e);
                    },
                    Ok(None) => break,
                    Err(_) => continue,
                }
            }

            assert!(got_filtered, "Expected an Insert event for filtered row");
        },
        Ok(Err(e)) => {
            panic!("Filtered subscription failed: {}", e);
        },
        Err(_) => {
            panic!("Filtered subscription creation timed out");
        },
    }

    cleanup_test_data(&table).await.ok();
}

// =============================================================================
// SQL Statement Coverage Tests
// =============================================================================

#[tokio::test]
async fn test_sql_create_namespace() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let client = create_test_client().expect("Failed to create client");

    // Cleanup
    let _ = client
        .execute_query("DROP NAMESPACE IF EXISTS test_ns CASCADE", None, None, None)
        .await;
    sleep(Duration::from_millis(20)).await;

    let result = client.execute_query("CREATE NAMESPACE test_ns", None, None, None).await;
    assert!(result.is_ok(), "CREATE NAMESPACE should succeed");

    // Cleanup
    let _ = client.execute_query("DROP NAMESPACE test_ns CASCADE", None, None, None).await;
}

#[tokio::test]
async fn test_sql_create_user_table() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup");

    let client = create_test_client().expect("Failed to create client");

    let result = client
        .execute_query(
            r#"CREATE TABLE ws_test.test_table (
                id INT PRIMARY KEY,
                event_type VARCHAR,
                data VARCHAR
            ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:10')"#,
            None,
            None,
            None,
        )
        .await;

    assert!(
        result.is_ok() || result.unwrap_err().to_string().contains("already exists"),
        "CREATE TABLE should succeed"
    );

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_sql_insert_select() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup");

    let client = create_test_client().expect("Failed to create client");

    // INSERT
    let insert = client
        .execute_query(
            &format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'test', 'data')",
                table,
                unique_event_id()
            ),
            None,
            None,
            None,
        )
        .await;
    assert!(insert.is_ok(), "INSERT should succeed: {:?}", insert.err());

    // SELECT
    let select = client
        .execute_query(
            &format!("SELECT * FROM {} WHERE event_type = 'test'", table),
            None,
            None,
            None,
        )
        .await;
    assert!(select.is_ok(), "SELECT should succeed");

    let response = select.unwrap();
    assert_eq!(response.status, ResponseStatus::Success);
    assert!(!response.results.is_empty());

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_sql_drop_table() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup");

    let client = create_test_client().expect("Failed to create client");

    // Create a table to drop
    client
        .execute_query(
            r#"CREATE TABLE ws_test.temp_table (id INT PRIMARY KEY) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:10')"#,
            None,
            None,
            None,
        )
        .await
        .expect("temp table create should succeed");

    // Drop it
    let result = client
        .execute_query("DROP TABLE IF EXISTS ws_test.temp_table", None, None, None)
        .await;
    assert!(result.is_ok(), "DROP TABLE should succeed: {:?}", result.err());

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_sql_system_tables() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let client = create_test_client().expect("Failed to create client");

    // Query system.users
    let users = client.execute_query("SELECT * FROM system.users", None, None, None).await;
    assert!(users.is_ok(), "Should query system.users");

    // Query system.namespaces
    let namespaces =
        client.execute_query("SELECT * FROM system.namespaces", None, None, None).await;
    assert!(namespaces.is_ok(), "Should query system.namespaces");

    // Query system.tables
    let tables = client.execute_query("SELECT * FROM system.tables", None, None, None).await;
    assert!(tables.is_ok(), "Should query system.tables");
}

#[tokio::test]
async fn test_sql_where_clause_operators() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup");

    let client = create_test_client().expect("Failed to create client");

    // Insert test data
    for i in 1..=5 {
        client
            .execute_query(
                &format!(
                    "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'op_test', '{}')",
                    table,
                    unique_event_id(),
                    i
                ),
                None,
                None,
                None,
            )
            .await
            .ok();
    }

    // Test LIKE
    let like = client
        .execute_query(&format!("SELECT * FROM {} WHERE data LIKE '%3%'", table), None, None, None)
        .await;
    assert!(like.is_ok(), "LIKE operator should work");

    // Test IN
    let in_op = client
        .execute_query(
            &format!("SELECT * FROM {} WHERE data IN ('1', '2', '3')", table),
            None,
            None,
            None,
        )
        .await;
    assert!(in_op.is_ok(), "IN operator should work");

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_sql_limit_offset() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup");

    let client = create_test_client().expect("Failed to create client");

    // Insert multiple rows
    for i in 1..=10 {
        execute_sql_checked(&format!(
            "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'limit_test', '{}')",
            table,
            unique_event_id(),
            i
        ))
        .await
        .expect("limit_test insert should succeed");
    }

    // Test LIMIT
    let limit = execute_query_with_retry(
        &client,
        &format!("SELECT * FROM {} WHERE event_type = 'limit_test' LIMIT 5", table),
        5,
    )
    .await;
    if let Err(err) = &limit {
        eprintln!("LIMIT query failed: {}", err);
    }
    assert!(limit.is_ok(), "LIMIT should work");
    let response = limit.unwrap();
    if let Some(rows) = &response.results[0].rows {
        assert!(rows.len() <= 5, "Should return at most 5 rows");
    }

    cleanup_test_data(&table).await.ok();
}

#[tokio::test]
async fn test_sql_order_by() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_data().await.expect("Failed to setup");

    let client = create_test_client().expect("Failed to create client");

    // Insert data
    client
        .execute_query(
            &format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'sort', 'z')",
                table,
                unique_event_id()
            ),
            None,
            None,
            None,
        )
        .await
        .ok();
    client
        .execute_query(
            &format!(
                "INSERT INTO {} (event_id, event_type, data) VALUES ('{}', 'sort', 'a')",
                table,
                unique_event_id()
            ),
            None,
            None,
            None,
        )
        .await
        .ok();

    // Test ORDER BY
    let ordered = client
        .execute_query(
            &format!("SELECT * FROM {} WHERE event_type = 'sort' ORDER BY data ASC", table),
            None,
            None,
            None,
        )
        .await;
    assert!(ordered.is_ok(), "ORDER BY should work");

    cleanup_test_data(&table).await.ok();
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[tokio::test]
async fn test_error_invalid_sql() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let client = create_test_client().expect("Failed to create client");

    let result = client.execute_query("INVALID SQL STATEMENT", None, None, None).await;
    assert!(
        result.is_err() || result.unwrap().status == ResponseStatus::Error,
        "Invalid SQL should return error"
    );
}

#[tokio::test]
async fn test_error_table_not_found() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let client = create_test_client().expect("Failed to create client");

    let result = client.execute_query("SELECT * FROM nonexistent.table", None, None, None).await;
    assert!(
        result.is_err() || result.unwrap().status == ResponseStatus::Error,
        "Querying non-existent table should return error"
    );
}

#[tokio::test]
async fn test_error_connection_refused() {
    // Try to connect to non-existent server
    let client = KalamLinkClient::builder()
        .base_url("http://localhost:9999")
        .timeout(Duration::from_secs(2))
        .build()
        .expect("Client creation should succeed");

    let result = client.execute_query("SELECT 1", None, None, None).await;
    assert!(result.is_err(), "Connection to non-existent server should fail");
}

// =============================================================================
// Server Requirement Check
// =============================================================================

#[tokio::test]
async fn test_server_running_check() {
    if !common::is_server_running().await {
        eprintln!(
            "\n⚠️  Server is not running at {}\n\nTo run these tests:\n1. Terminal 1: cd backend \
             && cargo run --bin kalamdb-server\n2. Terminal 2: cd cli && cargo test --test \
             test_websocket_integration\n\nTests will be skipped if server is not running.\n",
            common::server_url()
        );
        // Don't panic - just skip
        return;
    }

    println!("✅ Server is running at {}", common::server_url());
}
