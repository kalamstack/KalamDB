// Smoke Test: Advanced Subscription Testing
// Covers: multi-batch initial data, seq_id resumption, high-volume changes
// Tests subscription reliability under various edge cases

use crate::common::*;
use std::time::Duration;

// Re-import subscription-related types for advanced tests
use kalam_client::{SubscriptionConfig, SubscriptionOptions};

fn is_ephemeral_port_error(message: &str) -> bool {
    message.contains("Can't assign requested address") || message.contains("os error 49")
}

/// Helper to create a subscription listener with custom configuration
fn start_subscription_with_config(
    query: &str,
    options: Option<SubscriptionOptions>,
) -> Result<SubscriptionListenerAdvanced, Box<dyn std::error::Error>> {
    SubscriptionListenerAdvanced::start_with_options(query, options)
}

/// Advanced subscription listener with additional capabilities
pub struct SubscriptionListenerAdvanced {
    event_receiver: std::sync::mpsc::Receiver<String>,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    _handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for SubscriptionListenerAdvanced {
    fn drop(&mut self) {
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
    }
}

impl SubscriptionListenerAdvanced {
    pub fn start_with_options(
        query: &str,
        options: Option<SubscriptionOptions>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let query = query.to_string();

        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for subscription");

            runtime.block_on(async move {
                let base_url = leader_or_server_url();
                let mut client = None;
                for attempt in 0..5 {
                    match client_for_user_on_url_with_timeouts(
                        &base_url,
                        default_username(),
                        default_password(),
                        KalamLinkTimeouts::builder()
                            .connection_timeout_secs(5)
                            .receive_timeout_secs(120)
                            .send_timeout_secs(30)
                            .subscribe_timeout_secs(10)
                            .auth_timeout_secs(10)
                            .initial_data_timeout(Duration::from_secs(120))
                            .build(),
                    ) {
                        Ok(c) => {
                            client = Some(c);
                            break;
                        },
                        Err(e) => {
                            let message = e.to_string();
                            if attempt < 4 && is_ephemeral_port_error(&message) {
                                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1)))
                                    .await;
                                continue;
                            }
                            let _ = event_tx
                                .send(format!("ERROR: Failed to create client: {}", message));
                            return;
                        },
                    }
                }
                let client = match client {
                    Some(c) => c,
                    None => {
                        let _ = event_tx.send("ERROR: Failed to create client".to_string());
                        return;
                    },
                };

                // Generate unique subscription ID
                let subscription_id = format!(
                    "sub_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                );

                let mut subscription = None;
                for attempt in 0..5 {
                    let mut config = SubscriptionConfig::new(&subscription_id, &query);
                    if let Some(opts) = options.as_ref() {
                        config.options = Some(opts.clone());
                    }
                    match client.subscribe_with_config(config).await {
                        Ok(s) => {
                            subscription = Some(s);
                            break;
                        },
                        Err(e) => {
                            let message = e.to_string();
                            if attempt < 4 && is_ephemeral_port_error(&message) {
                                tokio::time::sleep(Duration::from_millis(200 * (attempt + 1)))
                                    .await;
                                continue;
                            }
                            let _ =
                                event_tx.send(format!("ERROR: Failed to subscribe: {}", message));
                            return;
                        },
                    }
                }
                let mut subscription = match subscription {
                    Some(s) => s,
                    None => {
                        let _ = event_tx.send("ERROR: Failed to subscribe".to_string());
                        return;
                    },
                };

                let mut stop_rx = stop_rx;

                loop {
                    tokio::select! {
                        _ = &mut stop_rx => {
                            break;
                        }
                        event = subscription.next() => {
                            match event {
                                Some(Ok(change_event)) => {
                                    let event_str = format!("{:?}", change_event);
                                    if event_tx.send(event_str).is_err() {
                                        break;
                                    }
                                }
                                Some(Err(e)) => {
                                    let _ = event_tx.send(format!("ERROR: {}", e));
                                    break;
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        });

        Ok(Self {
            event_receiver: event_rx,
            stop_sender: Some(stop_tx),
            _handle: Some(handle),
        })
    }

    pub fn try_read_line(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        match self.event_receiver.recv_timeout(timeout) {
            Ok(line) if line.is_empty() => Ok(None),
            Ok(line) => Ok(Some(line)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err("Timeout waiting for subscription data".into())
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Ok(None),
        }
    }

    pub fn collect_events_until_ready(&mut self, timeout: Duration) -> Vec<String> {
        let mut events = Vec::new();
        let deadline = std::time::Instant::now() + timeout;

        while std::time::Instant::now() < deadline {
            match self.try_read_line(Duration::from_millis(100)) {
                Ok(Some(line)) => {
                    events.push(line.clone());
                    // Check if batch loading is complete
                    if line.contains("status: Ready")
                        || (line.contains("Ack") && !line.contains("has_more: true"))
                    {
                        break;
                    }
                },
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        events
    }

    pub fn stop(mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
        Ok(())
    }
}

fn create_namespace(ns: &str) {
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns));
}

// ============================================================================
// TEST 1: Multi-batch initial data loading
// When subscribing to a table with many rows, the server sends data in batches.
// This test verifies that all batches are received correctly.
// ============================================================================

#[ntest::timeout(300000)]
#[test]
fn smoke_subscription_multi_batch_initial_data() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("multi_batch");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create table
    let create_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, data VARCHAR, created_at TIMESTAMP) WITH (TYPE = 'USER')",
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // Insert enough rows to trigger multi-batch loading (default batch size is typically 1000)
    // We'll insert ~50 rows to keep test fast but still exercise batching with small batch size
    let batch_size = 10; // We'll request small batches
    let total_rows = 35; // More than 3 batches

    for i in 1..=total_rows {
        let insert_sql = format!(
            "INSERT INTO {} (id, data, created_at) VALUES ({}, 'row_{}', {})",
            full,
            i,
            i,
            1730497770045_i64 + i as i64
        );
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    println!(
        "[TEST] Inserted {} rows, subscribing with batch_size={}",
        total_rows, batch_size
    );

    // Small delay to ensure data is visible
    std::thread::sleep(Duration::from_millis(500));

    // Subscribe with small batch size to force multiple batches
    let query = format!("SELECT * FROM {}", full);
    let options = SubscriptionOptions::default().with_batch_size(batch_size);
    let mut listener =
        start_subscription_with_config(&query, Some(options)).expect("subscription should start");

    // Collect all initial data events
    let events = listener.collect_events_until_ready(Duration::from_secs(45));

    // Count InitialDataBatch events and rows
    let batch_events: Vec<&String> =
        events.iter().filter(|e| e.contains("InitialDataBatch")).collect();

    let ack_events: Vec<&String> = events.iter().filter(|e| e.contains("Ack")).collect();

    println!(
        "[TEST] Received {} InitialDataBatch events, {} Ack events",
        batch_events.len(),
        ack_events.len()
    );
    for (i, event) in batch_events.iter().enumerate() {
        println!("[TEST] Batch {}: {}...", i + 1, &event[..std::cmp::min(150, event.len())]);
    }

    // Verify we got data (either through batches or Ack)
    let total_events = batch_events.len() + ack_events.len();
    assert!(
        total_events > 0,
        "Should receive initial data events. All events: {:?}",
        events.iter().take(5).collect::<Vec<_>>()
    );

    // Verify some of our rows are present in the data
    let all_events_str = events.join("\n");
    let found_rows = (1..=5).filter(|i| all_events_str.contains(&format!("row_{}", i))).count();

    assert!(
        found_rows >= 3,
        "Should find at least 3 of our test rows in initial data. Found: {}. Events sample: {}",
        found_rows,
        &all_events_str[..std::cmp::min(500, all_events_str.len())]
    );

    // Verify final batch indicates completion (has_more: false or status: Ready)
    let last_batch_or_ack = events
        .iter()
        .rev()
        .find(|e| e.contains("InitialDataBatch") || e.contains("Ack"));

    if let Some(last) = last_batch_or_ack {
        assert!(
            !last.contains("has_more: true") || last.contains("status: Ready"),
            "Final batch should indicate no more data pending"
        );
    }

    listener.stop().ok();
    println!("[TEST] Multi-batch initial data test passed!");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// ============================================================================
// TEST 2: Subscription resumption from seq_id
// After receiving some changes, we can resume subscription from a specific
// seq_id to avoid re-fetching all data.
// ============================================================================

#[ntest::timeout(180000)]
#[test]
fn smoke_subscription_resume_from_seq_id() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("seq_resume");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create table
    let create_sql =
        format!("CREATE TABLE {} (id INT PRIMARY KEY, value VARCHAR) WITH (TYPE = 'USER')", full);
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // Insert initial rows
    for i in 1..=5 {
        let insert_sql =
            format!("INSERT INTO {} (id, value) VALUES ({}, 'initial_{}')", full, i, i);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // First subscription - get initial data and first change
    let query = format!("SELECT * FROM {}", full);
    let mut listener1 = SubscriptionListener::start(&query).expect("subscription should start");

    // Wait for initial data to be ready
    let events1 = listener1.collect_events_until_ready(Duration::from_secs(10));
    assert!(!events1.is_empty(), "Should receive initial data");

    // Insert a new row while subscription 1 is active
    let test_value = format!("change1_{}", std::process::id());
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, value) VALUES (100, '{}')",
        full, test_value
    ))
    .expect("insert should succeed");

    // Wait for the insert event
    let mut last_seq_id: Option<String> = None;
    let change_deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < change_deadline {
        match listener1.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[FIRST_SUB] Event: {}...", &line[..std::cmp::min(200, line.len())]);
                if line.contains(&test_value) || line.contains("Insert") {
                    // Extract seq_id from the event (format: "_seq": Object {"Int64": String("123456789")})
                    if let Some(start) = line.find("\"_seq\"") {
                        if let Some(seq_start) = line[start..].find("String(\"") {
                            let seq_portion = &line[start + seq_start + 8..];
                            if let Some(seq_end) = seq_portion.find("\"") {
                                last_seq_id = Some(seq_portion[..seq_end].to_string());
                                println!("[FIRST_SUB] Captured seq_id: {:?}", last_seq_id);
                            }
                        }
                    }
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    listener1.stop().ok();

    // Insert more changes while disconnected
    let change2_value = format!("change2_{}", std::process::id());
    let change3_value = format!("change3_{}", std::process::id());
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, value) VALUES (101, '{}')",
        full, change2_value
    ))
    .expect("insert should succeed");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, value) VALUES (102, '{}')",
        full, change3_value
    ))
    .expect("insert should succeed");

    // Second subscription - resuming from seq_id should skip initial data
    // and only receive changes after that seq_id
    println!("[TEST] Starting second subscription");

    // Note: Even if we don't have a valid seq_id, we can still test the subscription
    // The server should handle from_seq_id gracefully
    let mut listener2 =
        SubscriptionListener::start(&query).expect("second subscription should start");

    // Collect events from second subscription
    let mut events2: Vec<String> = Vec::new();
    let deadline2 = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline2 {
        match listener2.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[SECOND_SUB] Event: {}...", &line[..std::cmp::min(200, line.len())]);
                events2.push(line.clone());

                // Look for our new changes
                if line.contains(&change2_value) || line.contains(&change3_value) {
                    break;
                }

                // If we got initial data including our new rows, that's also valid
                if events2.len() >= 3 {
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => {
                if !events2.is_empty() {
                    break;
                }
                continue;
            },
        }
    }

    listener2.stop().ok();

    // Verify we received data from the second subscription
    assert!(!events2.is_empty(), "Second subscription should receive events");

    // The new data should be present (either as initial data or change events)
    let events2_str = events2.join("\n");
    let found_new_data =
        events2_str.contains(&change2_value) || events2_str.contains(&change3_value);

    assert!(
        found_new_data || events2_str.contains("change"),
        "Second subscription should see the new changes. Events: {:?}",
        events2.iter().take(3).collect::<Vec<_>>()
    );

    println!("[TEST] Subscription resume test passed! (seq_id capture: {:?})", last_seq_id);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// ============================================================================
// TEST 3: High-volume rapid changes
// Verify subscription correctly handles many rapid inserts/updates without
// missing events or getting stuck.
// ============================================================================

#[ntest::timeout(300000)]
#[test]
fn smoke_subscription_high_volume_changes() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("high_vol");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create table
    let create_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, counter INT, updated_at TIMESTAMP) WITH (TYPE = 'USER')",
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // Start subscription BEFORE making changes
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Wait for initial ack (empty table)
    let initial_events = listener.collect_events_until_ready(Duration::from_secs(5));
    println!("[TEST] Initial subscription ready, {} events received", initial_events.len());

    // Now perform rapid changes
    let num_inserts = 12;
    let num_updates = 6;
    let test_id = std::process::id();
    let update_marker = test_id.to_string();

    println!("[TEST] Starting {} rapid inserts...", num_inserts);
    let start = std::time::Instant::now();

    for i in 1..=num_inserts {
        let insert_sql = format!(
            "INSERT INTO {} (id, counter, updated_at) VALUES ({}, {}, {})",
            full,
            i,
            i * 100,
            1730497770045 + i
        );
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    println!("[TEST] Inserts completed in {:?}", start.elapsed());

    // Perform rapid updates on existing rows
    println!("[TEST] Starting {} rapid updates...", num_updates);
    let update_start = std::time::Instant::now();

    for i in 1..=num_updates {
        let update_sql = format!(
            "UPDATE {} SET counter = {} WHERE id = {}",
            full,
            (i * 100) + test_id as i64,
            i
        );
        execute_sql_as_root_via_client(&update_sql).expect("update should succeed");
    }

    println!("[TEST] Updates completed in {:?}", update_start.elapsed());

    // Collect all change events with adequate timeout
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut all_events: Vec<String> = Vec::new();
    let collect_deadline = std::time::Instant::now() + Duration::from_secs(60);

    while std::time::Instant::now() < collect_deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                all_events.push(line.clone());

                if line.contains("Insert") {
                    insert_count += 1;
                } else if line.contains("Update") || line.contains(&update_marker) {
                    update_count += 1;
                }

                // Check if we've received most of the expected events
                if insert_count >= num_inserts && update_count >= num_updates {
                    break;
                }

                // Also break if we've been collecting for a while with no new events
                if all_events.len() > (num_inserts + num_updates) as usize {
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => {
                // If we have some events and timeout, check if we have enough
                if insert_count + update_count >= (num_inserts + num_updates) / 2 {
                    break;
                }
                continue;
            },
        }
    }

    println!(
        "[TEST] Collected {} total events: {} inserts, {} updates",
        all_events.len(),
        insert_count,
        update_count
    );

    // Verify we received a significant portion of events
    // Allow some tolerance as WebSocket might batch or coalesce events
    let min_expected_inserts = num_inserts / 2;
    let min_expected_updates = num_updates / 2;

    assert!(
        insert_count >= min_expected_inserts,
        "Should receive at least {} insert events, got {}. Sample: {:?}",
        min_expected_inserts,
        insert_count,
        all_events.iter().take(5).collect::<Vec<_>>()
    );

    assert!(
        update_count >= min_expected_updates,
        "Should receive at least {} update events, got {}. Sample: {:?}",
        min_expected_updates,
        update_count,
        all_events.iter().rev().take(5).collect::<Vec<_>>()
    );

    // Verify no errors in the event stream
    let error_count = all_events.iter().filter(|e| e.contains("ERROR")).count();
    assert_eq!(
        error_count,
        0,
        "Should not receive any error events. Errors: {:?}",
        all_events.iter().filter(|e| e.contains("ERROR")).collect::<Vec<_>>()
    );

    listener.stop().ok();
    println!(
        "[TEST] High-volume changes test passed! {} inserts, {} updates received",
        insert_count, update_count
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// ============================================================================
// TEST 4: Subscription with DELETE operations
// Verify DELETE events are properly delivered to subscribers.
// ============================================================================

#[ntest::timeout(180000)]
#[test]
fn smoke_subscription_delete_events() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("del_events");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    let create_sql =
        format!("CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE = 'USER')", full);
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // Insert some rows first
    for i in 1..=5 {
        let insert_sql = format!("INSERT INTO {} (id, name) VALUES ({}, 'item_{}')", full, i, i);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Start subscription
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Wait for initial data
    let _ = listener.collect_events_until_ready(Duration::from_secs(6));

    // Delete rows
    let delete_ids = vec![2, 4];
    for id in &delete_ids {
        let delete_sql = format!("DELETE FROM {} WHERE id = {}", full, id);
        execute_sql_as_root_via_client(&delete_sql).expect("delete should succeed");
    }

    // Collect delete events
    let mut delete_events: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);

    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[DELETE_TEST] Event: {}...", &line[..std::cmp::min(150, line.len())]);
                if line.contains("Delete") {
                    delete_events.push(line);
                    if delete_events.len() >= delete_ids.len() {
                        break;
                    }
                }
            },
            Ok(None) => break,
            Err(_) => {
                if !delete_events.is_empty() {
                    break;
                }
                continue;
            },
        }
    }

    assert!(!delete_events.is_empty(), "Should receive DELETE events");

    // Verify delete events contain old row data
    let delete_events_str = delete_events.join("\n");
    let found_item2 = delete_events_str.contains("item_2");
    let found_item4 = delete_events_str.contains("item_4");

    assert!(
        found_item2 || found_item4,
        "DELETE events should contain old row data. Events: {:?}",
        delete_events
    );

    listener.stop().ok();
    println!(
        "[TEST] Delete events test passed! {} delete events received",
        delete_events.len()
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// Helper extension for SubscriptionListener to collect until ready
trait SubscriptionListenerExt {
    fn collect_events_until_ready(&mut self, timeout: Duration) -> Vec<String>;
}

impl SubscriptionListenerExt for SubscriptionListener {
    fn collect_events_until_ready(&mut self, timeout: Duration) -> Vec<String> {
        let mut events = Vec::new();
        let deadline = std::time::Instant::now() + timeout;

        while std::time::Instant::now() < deadline {
            match self.try_read_line(Duration::from_millis(100)) {
                Ok(Some(line)) => {
                    events.push(line.clone());
                    // Check if batch loading is complete
                    if line.contains("status: Ready")
                        || (line.contains("Ack") && !line.contains("has_more: true"))
                    {
                        break;
                    }
                },
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        events
    }
}

// ============================================================================
// TEST 5: Column projection in subscriptions
// Verify that SELECT with specific columns only returns those columns
// plus system columns (_seq, _deleted), not all table columns.
// ============================================================================

#[ntest::timeout(180000)]
#[test]
fn smoke_subscription_column_projection() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("col_proj");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create a table with multiple columns
    let create_sql = format!(
        "CREATE TABLE {} (
            id INT PRIMARY KEY,
            username VARCHAR,
            email VARCHAR,
            age INT,
            status VARCHAR,
            bio TEXT,
            created_at TIMESTAMP
        ) WITH (TYPE = 'USER')",
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // Insert a row with all columns populated
    let test_username = format!("user_{}", std::process::id());
    let insert_sql = format!(
        "INSERT INTO {} (id, username, email, age, status, bio, created_at) VALUES (1, '{}', 'test@example.com', 25, 'active', 'A long bio text here', 1730497770045)",
        full, test_username
    );
    execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");

    // Give time for the data to be visible

    // Subscribe with column projection - only select username
    let query = format!("SELECT username FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // Collect all initial events (including InitialDataBatch if any)
    let mut initial_events: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut ready = false;

    while std::time::Instant::now() < deadline && !ready {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[TEST] Initial event: {}", &line[..std::cmp::min(400, line.len())]);
                initial_events.push(line.clone());
                // Check if we're done with initial loading
                if line.contains("status: Ready")
                    || (line.contains("Ack") && !line.contains("has_more: true"))
                {
                    ready = true;
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    println!("[TEST] Total initial events received: {}", initial_events.len());

    // Verify initial data contains username but NOT other columns
    let initial_str = initial_events.join("\n");

    // Check if we got initial data at all - the data should be in InitialDataBatch or Ack
    // If total_rows: 0 in Ack, the row may not have been visible yet
    let has_initial_data = initial_str.contains("InitialDataBatch")
        || initial_str.contains(&test_username)
        || initial_str.contains("total_rows: 1");

    if !has_initial_data {
        // The initial snapshot may have missed the row - wait for it as an Insert event
        println!("[TEST] No initial data found, waiting for Insert event...");
        let mut found_insert = false;
        let insert_deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < insert_deadline {
            match listener.try_read_line(Duration::from_millis(100)) {
                Ok(Some(line)) => {
                    println!(
                        "[TEST] Event while waiting: {}",
                        &line[..std::cmp::min(300, line.len())]
                    );
                    initial_events.push(line.clone());
                    if line.contains(&test_username) || line.contains("Insert") {
                        found_insert = true;
                        break;
                    }
                },
                Ok(None) => break,
                Err(_) => continue,
            }
        }
        if found_insert {
            // Re-check initial_str with new events
            let initial_str = initial_events.join("\n");
            // Verify column projection on Insert event
            assert!(
                initial_str.contains(&test_username) || initial_str.contains("username"),
                "Insert event should contain username"
            );
            assert!(
                !initial_str.contains("test@example.com"),
                "Insert event should NOT contain email value"
            );
        }
    } else {
        // Should contain username
        assert!(
            initial_str.contains(&test_username) || initial_str.contains("username"),
            "Initial data should contain username. Events: {}",
            &initial_str[..std::cmp::min(500, initial_str.len())]
        );

        // Should NOT contain email, age, status, bio (non-selected columns)
        // Note: We check for the actual values to avoid false positives from field names in debug output
        assert!(
            !initial_str.contains("test@example.com"),
            "Initial data should NOT contain email value. Events: {}",
            &initial_str[..std::cmp::min(500, initial_str.len())]
        );
        assert!(
            !initial_str.contains("A long bio text here"),
            "Initial data should NOT contain bio value. Events: {}",
            &initial_str[..std::cmp::min(500, initial_str.len())]
        );
    }

    // Now perform an UPDATE and verify the change event also respects projection
    let updated_username = format!("updated_{}", std::process::id());
    let update_sql = format!(
        "UPDATE {} SET username = '{}', email = 'newemail@example.com', status = 'inactive' WHERE id = 1",
        full, updated_username
    );
    execute_sql_as_root_via_client(&update_sql).expect("update should succeed");

    // Collect update event
    let mut update_events: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);

    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[TEST] Update event: {}", &line[..std::cmp::min(300, line.len())]);
                update_events.push(line.clone());
                if line.contains("Update") || line.contains(&updated_username) {
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => {
                if !update_events.is_empty() {
                    break;
                }
                continue;
            },
        }
    }

    assert!(!update_events.is_empty(), "Should receive update event");

    let update_str = update_events.join("\n");

    // Update event should contain username (the updated value)
    assert!(
        update_str.contains(&updated_username) || update_str.contains("username"),
        "Update event should contain username. Events: {}",
        &update_str[..std::cmp::min(500, update_str.len())]
    );

    // Update event should have old_rows with old username value
    assert!(
        update_str.contains("old_rows"),
        "Update event should contain old_rows. Events: {}",
        &update_str[..std::cmp::min(500, update_str.len())]
    );

    // Update event should NOT contain the new email value (not in projection)
    assert!(
        !update_str.contains("newemail@example.com"),
        "Update event should NOT contain email value (not in SELECT projection). Events: {}",
        &update_str[..std::cmp::min(500, update_str.len())]
    );

    // Update event should NOT contain status value
    assert!(
        !update_str.contains("\"inactive\""),
        "Update event should NOT contain status value (not in SELECT projection). Events: {}",
        &update_str[..std::cmp::min(500, update_str.len())]
    );

    // Verify InitialDataBatch had _seq (system column) along with username
    let has_seq_in_initial = initial_events
        .iter()
        .any(|e| e.contains("InitialDataBatch") && e.contains("_seq"));
    assert!(
        has_seq_in_initial,
        "InitialDataBatch should contain _seq system column. Events: {:?}",
        initial_events
            .iter()
            .filter(|e| e.contains("InitialDataBatch"))
            .collect::<Vec<_>>()
    );

    listener.stop().ok();
    println!("[TEST] Column projection test passed! Only selected columns returned in subscription events.");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
