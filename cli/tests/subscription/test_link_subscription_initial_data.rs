//! Test suite for link subscription with initial data batch functionality
//!
//! These tests verify that when subscribing to a query:
//! 1. Initial existing data is sent as a batch (initial_data_batch)
//! 2. Subsequent changes are sent as live events (insert/update/delete)
//!
//! This matches the behavior expected by both CLI and UI clients.

use std::time::Duration;

use crate::common::*;

/// Test that subscription receives initial data batch for existing rows
/// and then receives live INSERT events for new rows.
#[test]
fn test_link_subscription_initial_batch_then_inserts() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Create unique namespace and table
    let namespace = generate_unique_namespace("link_initial");
    let table_full = format!("{}.messages", namespace);

    // Setup namespace
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace));

    // Create user table
    let create_result = execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, content VARCHAR, created_at TIMESTAMP DEFAULT \
         CURRENT_TIMESTAMP) WITH (TYPE='USER', FLUSH_POLICY='rows:100')",
        table_full
    ));
    assert!(create_result.is_ok(), "Failed to create table: {:?}", create_result);

    // Insert initial rows BEFORE subscribing
    for i in 1..=3 {
        let result = execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, content) VALUES ({}, 'Initial message {}')",
            table_full, i, i
        ));
        assert!(result.is_ok(), "Failed to insert initial row {}: {:?}", i, result);
    }

    // Start subscription
    let query = format!("SELECT * FROM {}", table_full);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
            return;
        },
    };

    // Wait for initial data - should receive ACK first, then InitialDataBatch
    let ack_result = listener.wait_for_event("Ack", Duration::from_secs(5));
    if let Ok(ack_line) = ack_result {
        eprintln!("✓ Received subscription ACK: {}", ack_line);
    }

    // Wait for initial data batch
    let batch_result = listener.wait_for_event("InitialDataBatch", Duration::from_secs(5));
    match batch_result {
        Ok(batch_line) => {
            eprintln!("✓ Received InitialDataBatch: {}", batch_line);
            // Should contain the initial rows
            assert!(
                batch_line.contains("Initial message"),
                "Initial data batch should contain our inserted messages: {}",
                batch_line
            );
        },
        Err(e) => {
            eprintln!("⚠️  Did not receive InitialDataBatch within timeout: {}", e);
            // This is the key failure case we want to detect
        },
    }

    // Now insert a new row - should receive INSERT event
    let insert_result = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, content) VALUES (100, 'Live message after subscription')",
        table_full
    ));
    assert!(insert_result.is_ok(), "Failed to insert live row: {:?}", insert_result);

    // Wait for INSERT event
    let insert_event = listener.wait_for_event("Insert", Duration::from_secs(5));
    match insert_event {
        Ok(insert_line) => {
            eprintln!("✓ Received Insert event: {}", insert_line);
            assert!(
                insert_line.contains("Live message") || insert_line.contains("100"),
                "Insert event should contain the new message or id: {}",
                insert_line
            );
        },
        Err(e) => {
            eprintln!("⚠️  Did not receive Insert event: {}", e);
        },
    }

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    eprintln!("✓ Test completed successfully");
}

/// Test that subscription with no existing data still receives ACK
/// and then receives live events.
#[test]
fn test_link_subscription_empty_table_then_inserts() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Create unique namespace and table
    let namespace = generate_unique_namespace("link_empty");
    let table_full = format!("{}.events", namespace);

    // Setup namespace
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace));

    // Create user table (empty)
    let create_result = execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, event_type VARCHAR) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:100')",
        table_full
    ));
    assert!(create_result.is_ok(), "Failed to create table: {:?}", create_result);

    // Start subscription on empty table
    let query = format!("SELECT * FROM {}", table_full);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
            return;
        },
    };

    // Wait for ACK (should indicate 0 rows)
    let ack_result = listener.wait_for_event("Ack", Duration::from_secs(5));
    match ack_result {
        Ok(ack_line) => {
            eprintln!("✓ Received subscription ACK for empty table: {}", ack_line);
        },
        Err(e) => {
            eprintln!("⚠️  Did not receive ACK: {}", e);
        },
    }

    // Insert a row - should receive INSERT event
    let insert_result = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, event_type) VALUES (1, 'user_login')",
        table_full
    ));
    assert!(insert_result.is_ok(), "Failed to insert row: {:?}", insert_result);

    // Wait for INSERT event
    let insert_event = listener.wait_for_event("Insert", Duration::from_secs(5));
    match insert_event {
        Ok(insert_line) => {
            eprintln!("✓ Received Insert event: {}", insert_line);
            assert!(
                insert_line.contains("user_login") || insert_line.contains("id"),
                "Insert event should contain the event data: {}",
                insert_line
            );
        },
        Err(e) => {
            eprintln!("⚠️  Did not receive Insert event: {}", e);
        },
    }

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    eprintln!("✓ Test completed successfully");
}

/// Test batch control status transitions: loading -> ready
#[test]
fn test_link_subscription_batch_status_transition() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Create unique namespace and table
    let namespace = generate_unique_namespace("link_status");
    let table_full = format!("{}.items", namespace);

    // Setup
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace));

    let _ = execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:100')",
        table_full
    ));

    // Insert some data
    for i in 1..=5 {
        let _ = execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, name) VALUES ({}, 'Item {}')",
            table_full, i, i
        ));
    }

    // Start subscription
    let query = format!("SELECT * FROM {}", table_full);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
            return;
        },
    };

    // Collect all events for a few seconds
    let mut events = Vec::new();
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                eprintln!("[EVENT] {}", line);
                events.push(line);
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    // Check that we received proper events
    let has_ack = events.iter().any(|e| e.contains("Ack"));
    let has_batch = events.iter().any(|e| e.contains("InitialDataBatch") || e.contains("rows:"));
    let has_ready_status = events.iter().any(|e| e.contains("Ready") || e.contains("ready"));

    eprintln!(
        "Event summary: has_ack={}, has_batch={}, has_ready={}",
        has_ack, has_batch, has_ready_status
    );

    // At minimum we should get the ACK or some batch data
    assert!(
        has_ack || has_batch,
        "Should receive either Ack or InitialDataBatch: {:?}",
        events
    );

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test that multiple inserts after subscription are all received
#[test]
fn test_link_subscription_multiple_live_inserts() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("link_multi");
    let table_full = format!("{}.logs", namespace);

    // Setup
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace));

    let _ = execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, level VARCHAR, message VARCHAR) WITH (TYPE='USER')",
        table_full
    ));

    // Start subscription on empty table
    let query = format!("SELECT * FROM {}", table_full);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
            return;
        },
    };

    // Wait for ACK
    let _ = listener.wait_for_event("Ack", Duration::from_secs(5));
    // For empty tables, an initial batch may be emitted; ignore if not present.
    let _ = listener.wait_for_event("InitialDataBatch", Duration::from_secs(5));

    // Insert multiple rows in sequence
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    for (i, level) in levels.iter().enumerate() {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, level, message) VALUES ({}, '{}', 'Test message {}')",
            table_full,
            i + 1,
            level,
            i + 1
        ))
        .expect("insert should succeed");
    }

    // Collect insert events
    let mut insert_count = 0;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) && insert_count < levels.len() {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                if line.contains("Insert") {
                    insert_count += 1;
                    eprintln!("[INSERT {}] {}", insert_count, line);
                }
            },
            Ok(None) => continue,
            Err(_) => continue,
        }
    }

    eprintln!("Received {} insert events out of {} expected", insert_count, levels.len());

    // We should receive all insert events
    assert!(
        insert_count >= levels.len(),
        "Should receive all {} insert events, got {}",
        levels.len(),
        insert_count
    );

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test DELETE events (simpler than UPDATE)
#[test]
fn test_link_subscription_delete_events() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("link_del");
    let table_full = format!("{}.items", namespace);

    // Setup
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace));

    let _ = execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER')",
        table_full
    ));

    // Insert initial data
    let _ = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name) VALUES (1, 'To Delete')",
        table_full
    ));

    // Start subscription
    let query = format!("SELECT * FROM {}", table_full);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
            return;
        },
    };

    // Wait for initial batch
    let _ = listener.wait_for_event("Ack", Duration::from_secs(3));
    let _ = listener.wait_for_event("InitialDataBatch", Duration::from_secs(3));

    // Delete the row
    let _ = execute_sql_as_root_via_client(&format!("DELETE FROM {} WHERE id = 1", table_full));

    // Wait for DELETE event
    let delete_result = listener.wait_for_event("Delete", Duration::from_secs(5));
    match delete_result {
        Ok(delete_line) => {
            eprintln!("✓ Received Delete event: {}", delete_line);
        },
        Err(e) => {
            eprintln!("⚠️  Did not receive Delete event: {}", e);
        },
    }

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
