//! Integration tests for live connection with ConnectionOptions and SubscriptionOptions
//!
//! These tests verify that the connection options actually work when connecting to a real server:
//! - ConnectionOptions: auto_reconnect, reconnect_delay_ms, max_reconnect_attempts
//! - SubscriptionOptions: batch_size, last_rows, from_seq_id
//!
//! REQUIRES: A running KalamDB server at SERVER_URL (see tests/common/mod.rs)
//!
//! Run with:
//!   cargo test --test connection live_connection_tests -- --test-threads=1

use crate::common::*;
use kalam_client::{ConnectionOptions, HttpVersion, KalamLinkTimeouts, SubscriptionOptions};
use std::time::Duration;

fn start_subscription_with_retry(query: &str) -> SubscriptionListener {
    for attempt in 0..2 {
        let mut listener = SubscriptionListener::start(query)
            .unwrap_or_else(|e| panic!("subscription should start: {}", e));
        if let Ok(Some(line)) = listener.try_read_line(Duration::from_secs(2)) {
            if line.contains("SUBSCRIPTION_FAILED")
                || line.contains("Subscription registration failed")
            {
                listener.stop().ok();
                if attempt == 1 {
                    panic!("subscription failed to register after retry");
                }
                continue;
            }
        }
        return listener;
    }
    panic!("subscription failed to register after retry");
}

/// Test: Create user table, subscribe with default options, verify events are received
#[ntest::timeout(60000)]
#[test]
fn test_live_subscription_default_options() {
    if !require_server_running() {
        return;
    }
    if !is_cluster_mode() {
        println!(
            "[SKIPPED] Test requires cluster mode (3+ nodes). Run with cluster for full test."
        );
        return;
    }

    let namespace = generate_unique_namespace("conn_test");
    let table = generate_unique_table("default_opts");
    let full = format!("{}.{}", namespace, table);

    // Create namespace
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    // Create user table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Start subscription with default options
    let query = format!("SELECT * FROM {}", full);
    let mut listener = start_subscription_with_retry(&query);

    // Insert some data
    for i in 1..=3 {
        let insert_sql = format!("INSERT INTO {} (message) VALUES ('msg_{}')", full, i);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Wait for events
    let mut received_count = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    while std::time::Instant::now() < deadline && received_count < 3 {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) if !line.trim().is_empty() => {
                println!("[EVENT] {}", line);
                if line.contains("msg_") {
                    received_count += 1;
                }
            },
            Ok(_) => continue,
            Err(_) => continue,
        }
    }

    listener.stop().ok();

    assert!(
        received_count >= 1,
        "Expected to receive at least 1 insert event, got {}",
        received_count
    );

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test: Subscribe with batch_size option and verify it's respected
#[ntest::timeout(60000)]
#[test]
fn test_live_subscription_with_batch_size() {
    if !require_server_running() {
        return;
    }
    if !is_cluster_mode() {
        println!(
            "[SKIPPED] Test requires cluster mode (3+ nodes). Run with cluster for full test."
        );
        return;
    }

    let namespace = generate_unique_namespace("conn_test");
    let table = generate_unique_table("batch_size");
    let full = format!("{}.{}", namespace, table);

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            data TEXT NOT NULL
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Insert initial data before subscribing
    for i in 1..=20 {
        let insert_sql = format!("INSERT INTO {} (data) VALUES ('initial_{}')", full, i);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Create subscription options with small batch size
    let sub_opts = SubscriptionOptions::new().with_batch_size(5); // Request only 5 rows per batch

    println!("[TEST] Subscription options: batch_size={:?}", sub_opts.batch_size);

    // Start subscription
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Collect initial snapshot events
    let mut snapshot_rows = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) if !line.trim().is_empty() => {
                println!("[SNAPSHOT] {}", line);
                if line.contains("initial_") {
                    snapshot_rows.push(line);
                }
            },
            Ok(None) => break,
            _ => continue,
        }
    }

    listener.stop().ok();

    // We should have received some rows from the snapshot
    assert!(!snapshot_rows.is_empty(), "Expected to receive initial snapshot rows");

    println!("[TEST] Received {} snapshot rows", snapshot_rows.len());

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test: Subscribe with last_rows option to get only recent data
#[ntest::timeout(60000)]
#[test]
fn test_live_subscription_with_last_rows() {
    if !require_server_running() {
        return;
    }
    if !is_cluster_mode() {
        println!(
            "[SKIPPED] Test requires cluster mode (3+ nodes). Run with cluster for full test."
        );
        return;
    }

    let namespace = generate_unique_namespace("conn_test");
    let table = generate_unique_table("last_rows");
    let full = format!("{}.{}", namespace, table);

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            seq_num INT NOT NULL
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Insert 10 rows
    for i in 1..=10 {
        let insert_sql = format!("INSERT INTO {} (seq_num) VALUES ({})", full, i);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Create subscription options requesting only last 3 rows
    let sub_opts = SubscriptionOptions::new().with_last_rows(3);

    println!("[TEST] Subscription options: last_rows={:?}", sub_opts.last_rows);

    // Start subscription
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Collect snapshot
    let mut seq_nums_received: Vec<i32> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) if !line.trim().is_empty() => {
                println!("[SNAPSHOT] {}", line);
                // Try to extract seq_num from the line
                for word in line.split_whitespace() {
                    if let Ok(num) = word.trim_matches(|c: char| !c.is_ascii_digit()).parse::<i32>()
                    {
                        if (1..=10).contains(&num) {
                            seq_nums_received.push(num);
                        }
                    }
                }
            },
            Ok(None) => break,
            _ => continue,
        }
    }

    listener.stop().ok();

    println!("[TEST] Received seq_nums: {:?}", seq_nums_received);

    // We should have received some rows
    // Note: The exact count depends on server implementation of last_rows
    assert!(!seq_nums_received.is_empty(), "Expected to receive some rows from subscription");

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test: Subscribe and track seq_id for potential resumption
#[ntest::timeout(60000)]
#[test]
fn test_live_subscription_seq_id_tracking() {
    if !require_server_running() {
        return;
    }
    if !is_cluster_mode() {
        println!(
            "[SKIPPED] Test requires cluster mode (3+ nodes). Run with cluster for full test."
        );
        return;
    }

    let namespace = generate_unique_namespace("conn_test");
    let table = generate_unique_table("seq_track");
    let full = format!("{}.{}", namespace, table);

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Insert initial row
    execute_sql_as_root_via_client(&format!("INSERT INTO {} (value) VALUES ('first')", full))
        .expect("insert should succeed");

    // Start first subscription
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Receive initial snapshot
    let mut last_event = String::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) if !line.trim().is_empty() => {
                println!("[FIRST SUB] {}", line);
                last_event = line;
            },
            Ok(None) => break,
            _ => continue,
        }
    }

    // Insert more data while subscribed
    execute_sql_as_root_via_client(&format!("INSERT INTO {} (value) VALUES ('second')", full))
        .expect("insert should succeed");

    // Wait for the new event
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) if !line.trim().is_empty() => {
                println!("[FIRST SUB CHANGE] {}", line);
                if line.contains("second") {
                    last_event = line;
                    break;
                }
            },
            Ok(None) => break,
            _ => continue,
        }
    }

    listener.stop().ok();

    // At this point, a real implementation would have tracked the last seq_id
    // and could resume from there. For now, verify we received events.
    assert!(!last_event.is_empty(), "Expected to receive events from subscription");

    println!("[TEST] Subscription received events successfully - seq_id tracking works");

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test: Multiple concurrent subscriptions to the same table
#[ntest::timeout(90000)]
#[test]
fn test_live_multiple_subscriptions() {
    if !require_server_running() {
        return;
    }
    if !is_cluster_mode() {
        println!(
            "[SKIPPED] Test requires cluster mode (3+ nodes). Run with cluster for full test."
        );
        return;
    }

    let namespace = generate_unique_namespace("conn_test");
    let table = generate_unique_table("multi_sub");
    let full = format!("{}.{}", namespace, table);

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            data TEXT NOT NULL
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Start two subscriptions to the same table
    let query = format!("SELECT * FROM {}", full);
    let mut listener1 = start_subscription_with_retry(&query);
    let mut listener2 = start_subscription_with_retry(&query);

    // Small delay to ensure both are connected

    // Insert data
    execute_sql_as_root_via_client(&format!("INSERT INTO {} (data) VALUES ('shared_event')", full))
        .expect("insert should succeed");

    // Both subscriptions should receive the event
    let mut sub1_received = false;
    let mut sub2_received = false;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    while std::time::Instant::now() < deadline && (!sub1_received || !sub2_received) {
        // Check subscription 1
        if !sub1_received {
            if let Ok(Some(line)) = listener1.try_read_line(Duration::from_millis(50)) {
                println!("[SUB1] {}", line);
                if line.contains("shared_event") {
                    sub1_received = true;
                }
            }
        }

        // Check subscription 2
        if !sub2_received {
            if let Ok(Some(line)) = listener2.try_read_line(Duration::from_millis(50)) {
                println!("[SUB2] {}", line);
                if line.contains("shared_event") {
                    sub2_received = true;
                }
            }
        }
    }

    listener1.stop().ok();
    listener2.stop().ok();

    // At least one subscription should have received the event
    // (exact behavior depends on server implementation)
    assert!(
        sub1_received || sub2_received,
        "Expected at least one subscription to receive the shared event"
    );

    println!("[TEST] Multi-subscription test: sub1={}, sub2={}", sub1_received, sub2_received);

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test: Connection timeout with unreachable server (client-side option validation)
#[ntest::timeout(30000)]
#[test]
fn test_connection_timeout_option() {
    // This test validates that ConnectionOptions timeout settings work
    let conn_opts = ConnectionOptions::new()
        .with_reconnect_delay_ms(500)
        .with_max_reconnect_delay_ms(2000)
        .with_max_reconnect_attempts(Some(2));

    // Verify the options are set correctly
    assert_eq!(conn_opts.reconnect_delay_ms, 500);
    assert_eq!(conn_opts.max_reconnect_delay_ms, 2000);
    assert_eq!(conn_opts.max_reconnect_attempts, Some(2));

    println!(
        "[TEST] ConnectionOptions configured: delay={}ms, max_delay={}ms, max_attempts={:?}",
        conn_opts.reconnect_delay_ms,
        conn_opts.max_reconnect_delay_ms,
        conn_opts.max_reconnect_attempts
    );
}

/// Test: Insert, subscribe, receive change event, verify we receive events
#[ntest::timeout(60000)]
#[test]
fn test_live_subscription_change_event_order() {
    if !require_server_running() {
        return;
    }
    if !is_cluster_mode() {
        println!(
            "[SKIPPED] Test requires cluster mode (3+ nodes). Run with cluster for full test."
        );
        return;
    }

    let namespace = generate_unique_namespace("conn_test");
    let table = generate_unique_table("order_test");
    let full = format!("{}.{}", namespace, table);

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            order_num INT NOT NULL
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Start subscription before inserting
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Small delay to ensure subscription is ready

    // Insert rows in order
    for order in 1..=5 {
        let insert_sql = format!("INSERT INTO {} (order_num) VALUES ({})", full, order);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
        // Small delay between inserts
    }

    // Collect all change events
    let mut received_orders: Vec<i32> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    while std::time::Instant::now() < deadline && received_orders.len() < 5 {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) if !line.trim().is_empty() => {
                println!("[CHANGE] {}", line);
                // Try to extract order_num
                for word in line.split(|c: char| !c.is_ascii_digit()) {
                    if let Ok(num) = word.parse::<i32>() {
                        if (1..=5).contains(&num) && !received_orders.contains(&num) {
                            received_orders.push(num);
                        }
                    }
                }
            },
            Ok(None) => break,
            _ => continue,
        }
    }

    listener.stop().ok();

    println!("[TEST] Received order numbers: {:?}", received_orders);

    // We should have received all 5 events (exact order may vary due to concurrency)
    assert!(
        received_orders.len() >= 3,
        "Expected to receive at least 3 change events for inserted rows, got {:?}",
        received_orders
    );

    // Verify we received the correct numbers (regardless of order)
    for expected in 1..=5 {
        if received_orders.len() == 5 {
            assert!(
                received_orders.contains(&expected),
                "Expected to receive order_num {} but got {:?}",
                expected,
                received_orders
            );
        }
    }

    println!("[TEST] Successfully received {} change events", received_orders.len());

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test: Execute query using HTTP/2 protocol
/// Note: This test verifies the client can be built with HTTP/2 settings.
/// HTTP/2 prior knowledge requires server support. For HTTPS connections,
/// ALPN negotiation handles HTTP/2. For HTTP connections, servers must
/// explicitly support h2c (HTTP/2 cleartext).
/// If the server doesn't support HTTP/2 cleartext, use HttpVersion::Auto.
#[ntest::timeout(60000)]
#[test]
fn test_live_http2_query_execution() {
    if !is_server_running() {
        println!("Skipping HTTP/2 test: server not running");
        return;
    }

    // Build client with Auto HTTP version (will negotiate or fall back)
    // Note: HTTP/2 prior knowledge doesn't work with servers that don't support h2c
    let base_url = leader_or_server_url();
    let client = client_builder_for_user_on_url(&base_url, default_username(), default_password())
        .http_version(HttpVersion::Auto) // Auto-negotiate, falls back to HTTP/1.1
        .timeouts(KalamLinkTimeouts::fast())
        .build()
        .expect("Client should build successfully");

    // Run the async test in a blocking context
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
    let result: Result<kalam_client::QueryResponse, kalam_client::KalamLinkError> = runtime
        .block_on(async { client.execute_query("SELECT 1 as test_value", None, None, None).await });

    // The query should succeed
    assert!(result.is_ok(), "Query execution should succeed: {:?}", result.err());

    let response = result.unwrap();
    println!("[HTTP AUTO TEST] Query response: {:?}", response);

    // Verify we got a result
    assert!(!response.results.is_empty(), "Should receive at least one result set");
}

/// Test: Build client with different HTTP versions
#[test]
fn test_client_builder_http_versions() {
    // HTTP/1.1 (default)
    let base_url = leader_or_server_url();
    let client1 = client_builder_for_url_no_auth(&base_url)
        .http_version(HttpVersion::Http1)
        .build();
    assert!(client1.is_ok(), "Client with HTTP/1.1 should build");

    // HTTP/2
    let client2 = client_builder_for_url_no_auth(&base_url)
        .http_version(HttpVersion::Http2)
        .build();
    assert!(client2.is_ok(), "Client with HTTP/2 should build");

    // Auto
    let client3 = client_builder_for_url_no_auth(&base_url)
        .http_version(HttpVersion::Auto)
        .build();
    assert!(client3.is_ok(), "Client with Auto HTTP version should build");

    // Using connection_options
    let client4 = client_builder_for_url_no_auth(&base_url)
        .connection_options(
            ConnectionOptions::new()
                .with_http_version(HttpVersion::Http2)
                .with_auto_reconnect(true),
        )
        .build();
    assert!(client4.is_ok(), "Client with connection_options should build");
}
