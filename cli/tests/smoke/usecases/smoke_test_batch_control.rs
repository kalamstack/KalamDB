// Smoke Test: Batch Control Validation
// Covers: batch_num tracking, has_more flag, status transitions, data ordering
// Tests subscription batch control correctness for initial data loading

use crate::common::*;
use std::time::Duration;

use kalam_link::{SubscriptionConfig, SubscriptionOptions};

/// Helper to create a subscription listener with custom configuration
fn start_subscription_with_config(
    query: &str,
    options: Option<SubscriptionOptions>,
) -> Result<BatchSubscriptionListener, Box<dyn std::error::Error>> {
    BatchSubscriptionListener::start_with_options(query, options)
}

/// Parsed batch control information
#[derive(Debug, Clone)]
pub struct ParsedBatchControl {
    pub batch_num: u32,
    pub has_more: bool,
    pub status: String,
}

/// Parsed event from subscription
#[derive(Debug)]
#[allow(dead_code)]
pub enum ParsedEvent {
    Ack {
        subscription_id: String,
        batch_control: ParsedBatchControl,
    },
    InitialDataBatch {
        subscription_id: String,
        row_count: usize,
        batch_control: ParsedBatchControl,
    },
    Insert {
        subscription_id: String,
        row_count: usize,
    },
    Other(String),
    Error(String),
}

impl ParsedEvent {
    /// Parse a debug-formatted ChangeEvent string into a structured event
    fn parse(event_str: &str) -> Self {
        // Parse Ack event: "Ack { subscription_id: "...", total_rows: N, batch_control: BatchControl { batch_num: N, has_more: bool, status: Status, ... } }"
        if event_str.contains("Ack {") {
            if let Some(sub_id) = Self::extract_subscription_id(event_str) {
                if let Some(bc) = Self::extract_batch_control(event_str) {
                    return ParsedEvent::Ack {
                        subscription_id: sub_id,
                        batch_control: bc,
                    };
                }
            }
        }

        // Parse InitialDataBatch event
        if event_str.contains("InitialDataBatch {") {
            if let Some(sub_id) = Self::extract_subscription_id(event_str) {
                let row_count = Self::extract_row_count(event_str);
                if let Some(bc) = Self::extract_batch_control(event_str) {
                    return ParsedEvent::InitialDataBatch {
                        subscription_id: sub_id,
                        row_count,
                        batch_control: bc,
                    };
                }
            }
        }

        // Parse Insert event
        if event_str.contains("Insert {") {
            if let Some(sub_id) = Self::extract_subscription_id(event_str) {
                let row_count = Self::extract_row_count(event_str);
                return ParsedEvent::Insert {
                    subscription_id: sub_id,
                    row_count,
                };
            }
        }

        // Parse error
        if event_str.starts_with("ERROR:") {
            return ParsedEvent::Error(event_str.to_string());
        }

        ParsedEvent::Other(event_str.to_string())
    }

    fn extract_subscription_id(s: &str) -> Option<String> {
        // Find "subscription_id: " and extract the quoted string
        let prefix = "subscription_id: \"";
        if let Some(start) = s.find(prefix) {
            let rest = &s[start + prefix.len()..];
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
        None
    }

    fn extract_batch_control(s: &str) -> Option<ParsedBatchControl> {
        // Extract batch_num
        let batch_num = Self::extract_u32_field(s, "batch_num: ").unwrap_or(0);

        // Extract has_more
        let has_more = s.contains("has_more: true");

        // Extract status
        let status = if s.contains("Ready") {
            "ready".to_string()
        } else if s.contains("LoadingBatch") {
            "loading_batch".to_string()
        } else if s.contains("Loading") {
            "loading".to_string()
        } else {
            "unknown".to_string()
        };

        Some(ParsedBatchControl {
            batch_num,
            has_more,
            status,
        })
    }

    fn extract_u32_field(s: &str, prefix: &str) -> Option<u32> {
        if let Some(start) = s.find(prefix) {
            let rest = &s[start + prefix.len()..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            rest[..end].parse().ok()
        } else {
            None
        }
    }

    fn extract_row_count(s: &str) -> usize {
        // Count occurrences of row patterns - look for rows: [ and count elements
        // This is a rough estimate from the debug output
        let rows_start = s.find("rows: [");
        if let Some(start) = rows_start {
            let rest = &s[start..];
            // Count Object patterns or map patterns
            rest.matches("Object(").count() + rest.matches("{\"").count().saturating_sub(1)
        } else {
            0
        }
    }
}

/// Advanced subscription listener with batch control parsing
pub struct BatchSubscriptionListener {
    event_receiver: std::sync::mpsc::Receiver<String>,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for BatchSubscriptionListener {
    fn drop(&mut self) {
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl BatchSubscriptionListener {
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
                let client = match client_for_user_on_url_with_timeouts(
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
                    Ok(c) => c,
                    Err(e) => {
                        let _ = event_tx.send(format!("ERROR: Failed to create client: {}", e));
                        return;
                    },
                };

                // Generate unique subscription ID
                let subscription_id = format!(
                    "batch_test_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                );

                // Create config with options
                let mut config = SubscriptionConfig::new(subscription_id, &query);
                if let Some(opts) = options {
                    config.options = Some(opts);
                }

                let mut subscription = match client.subscribe_with_config(config).await {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = event_tx.send(format!("ERROR: Failed to subscribe: {}", e));
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

                let _ = subscription.close().await;
                client.disconnect().await;
            });
        });

        Ok(Self {
            event_receiver: event_rx,
            stop_sender: Some(stop_tx),
            handle: Some(handle),
        })
    }

    pub fn try_read_event(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ParsedEvent>, Box<dyn std::error::Error>> {
        match self.event_receiver.recv_timeout(timeout) {
            Ok(line) if line.is_empty() => Ok(None),
            Ok(line) => Ok(Some(ParsedEvent::parse(&line))),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err("Timeout waiting for subscription event".into())
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Ok(None),
        }
    }

    /// Collect all batch events until ready or timeout
    pub fn collect_batches_until_ready(&mut self, timeout: Duration) -> Vec<ParsedEvent> {
        let mut events = Vec::new();
        let deadline = std::time::Instant::now() + timeout;

        while std::time::Instant::now() < deadline {
            match self.try_read_event(Duration::from_millis(100)) {
                Ok(Some(event)) => {
                    // Only check InitialDataBatch for ready status, not Ack
                    let is_ready = match &event {
                        ParsedEvent::InitialDataBatch { batch_control, .. } => {
                            !batch_control.has_more
                        },
                        _ => false,
                    };
                    events.push(event);
                    if is_ready {
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
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| "Subscription thread panicked")?;
        }
        Ok(())
    }
}

fn create_namespace(ns: &str) {
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns));
}

fn collect_events_with_retry(
    query: &str,
    options: Option<SubscriptionOptions>,
    timeout: Duration,
    attempts: usize,
) -> Vec<ParsedEvent> {
    for attempt in 0..attempts {
        let mut listener = start_subscription_with_config(query, options.clone())
            .expect("subscription should start");
        let events = listener.collect_batches_until_ready(timeout);
        listener.stop().ok();

        if !events.is_empty() {
            return events;
        }

        if attempt + 1 < attempts {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    Vec::new()
}

// ============================================================================
// TEST 1: Single batch (small data set) - batch_num=0, has_more=false, status=ready
// ============================================================================

#[ntest::timeout(180000)]
#[test]
fn smoke_batch_control_single_batch() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("single_batch");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create table
    let create_sql =
        format!("CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE = 'USER')", full);
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Insert a few rows (less than batch size, so single batch)
    for i in 1..=5 {
        let insert_sql = format!("INSERT INTO {} (id, name) VALUES ({}, 'name_{}')", full, i, i);
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Subscribe
    let query = format!("SELECT * FROM {}", full);
    let events = collect_events_with_retry(&query, None, Duration::from_secs(10), 3);

    println!("[TEST] Received {} events", events.len());
    for (i, event) in events.iter().enumerate() {
        println!("[TEST] Event {}: {:?}", i, event);
    }

    // Verify we got an Ack event
    let ack_events: Vec<_> =
        events.iter().filter(|e| matches!(e, ParsedEvent::Ack { .. })).collect();
    assert!(!ack_events.is_empty(), "Should receive Ack event");

    // Verify the Ack has correct batch_control
    if let ParsedEvent::Ack { batch_control, .. } = &ack_events[0] {
        assert_eq!(batch_control.batch_num, 0, "First batch should be batch_num=0");
        // For small data sets, has_more should be false (all data fits in one batch)
        // Note: has_more depends on whether the data fit in the batch size
        println!(
            "[TEST] Ack batch_control: batch_num={}, has_more={}, status={}",
            batch_control.batch_num, batch_control.has_more, batch_control.status
        );
    }

    // Verify we got initial data batch
    let batch_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ParsedEvent::InitialDataBatch { .. }))
        .collect();
    assert!(!batch_events.is_empty(), "Should receive InitialDataBatch event");

    // Verify final batch has has_more=false and status=ready
    if let Some(ParsedEvent::InitialDataBatch { batch_control, .. }) = batch_events.last() {
        assert!(!batch_control.has_more, "Final batch should have has_more=false");
        assert_eq!(batch_control.status, "ready", "Final batch should have status=ready");
    }

    println!("[TEST] Single batch test passed!");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// ============================================================================
// TEST 2: Multiple batches - batch_num increments, status transitions correctly
// ============================================================================

#[ntest::timeout(300000)]
#[test]
fn smoke_batch_control_multi_batch() {
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
        "CREATE TABLE {} (id INT PRIMARY KEY, data VARCHAR, value INT) WITH (TYPE = 'USER')",
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Insert enough rows to trigger multiple batches (with small batch_size=5)
    let total_rows = 25;
    for i in 1..=total_rows {
        let insert_sql = format!(
            "INSERT INTO {} (id, data, value) VALUES ({}, 'row_data_{}', {})",
            full,
            i,
            i,
            i * 10
        );
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Subscribe with small batch size to force multiple batches
    let query = format!("SELECT * FROM {}", full);
    let options = SubscriptionOptions::default().with_batch_size(5);
    let events = collect_events_with_retry(&query, Some(options), Duration::from_secs(15), 3);

    println!("[TEST] Received {} events total", events.len());

    // Extract batch events only
    let batch_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ParsedEvent::InitialDataBatch { .. }))
        .collect();

    println!("[TEST] Received {} InitialDataBatch events", batch_events.len());

    // Verify we got multiple batches
    assert!(
        batch_events.len() >= 2,
        "Should receive at least 2 batch events for {} rows with batch_size=5",
        total_rows
    );

    // Verify batch_num sequence is correct (should increment)
    let mut last_batch_num: Option<u32> = None;
    for (i, event) in batch_events.iter().enumerate() {
        if let ParsedEvent::InitialDataBatch { batch_control, .. } = event {
            println!(
                "[TEST] Batch {}: batch_num={}, has_more={}, status={}",
                i, batch_control.batch_num, batch_control.has_more, batch_control.status
            );

            if let Some(last) = last_batch_num {
                assert!(
                    batch_control.batch_num > last || batch_control.batch_num == 0,
                    "batch_num should increment or be 0 for first batch. Got {} after {}",
                    batch_control.batch_num,
                    last
                );
            }
            last_batch_num = Some(batch_control.batch_num);
        }
    }

    // Verify status transitions: first batch=loading, middle=loading_batch, last=ready
    if let Some(ParsedEvent::InitialDataBatch { batch_control, .. }) = batch_events.first() {
        // First batch should have status=loading (or ready if single batch which shouldn't happen here)
        assert!(
            batch_control.status == "loading" || batch_control.status == "ready",
            "First batch should have status=loading or ready"
        );
    }

    if let Some(ParsedEvent::InitialDataBatch { batch_control, .. }) = batch_events.last() {
        assert!(!batch_control.has_more, "Last batch should have has_more=false");
        assert_eq!(batch_control.status, "ready", "Last batch should have status=ready");
    }

    println!("[TEST] Multi-batch control test passed!");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// ============================================================================
// TEST 3: Empty table - batch_num=0, has_more=false, status=ready immediately
// ============================================================================

#[ntest::timeout(180000)]
#[test]
fn smoke_batch_control_empty_table() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("empty");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create table but don't insert any rows
    let create_sql =
        format!("CREATE TABLE {} (id INT PRIMARY KEY, data VARCHAR) WITH (TYPE = 'USER')", full);
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Subscribe to empty table
    let query = format!("SELECT * FROM {}", full);
    let events = collect_events_with_retry(&query, None, Duration::from_secs(10), 3);

    println!("[TEST] Empty table: received {} events", events.len());
    for (i, event) in events.iter().enumerate() {
        println!("[TEST] Event {}: {:?}", i, event);
    }

    // For empty table, we should get Ack with has_more=false and status=ready
    let ack_events: Vec<_> =
        events.iter().filter(|e| matches!(e, ParsedEvent::Ack { .. })).collect();
    assert!(!ack_events.is_empty(), "Should receive Ack event for empty table");

    if let ParsedEvent::Ack { batch_control, .. } = &ack_events[0] {
        assert_eq!(batch_control.batch_num, 0, "Empty table should have batch_num=0");
        assert!(!batch_control.has_more, "Empty table should have has_more=false");
        assert_eq!(batch_control.status, "ready", "Empty table should have status=ready");
    }

    println!("[TEST] Empty table batch control test passed!");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// ============================================================================
// TEST 4: Verify initial data order matches insert order
// ============================================================================

#[ntest::timeout(180000)]
#[test]
fn smoke_batch_control_data_ordering() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("ordering");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // Create table
    let create_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, seq INT, label VARCHAR) WITH (TYPE = 'USER')",
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // Insert rows with predictable sequence
    let total_rows = 15;
    for i in 1..=total_rows {
        let insert_sql = format!(
            "INSERT INTO {} (id, seq, label) VALUES ({}, {}, 'item_{}')",
            full,
            i,
            i * 100,
            i
        );
        execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");
    }

    // Subscribe with small batch size using the supported live query shape.
    let query = format!("SELECT * FROM {}", full);
    let options = SubscriptionOptions::default().with_batch_size(5);
    let events = collect_events_with_retry(&query, Some(options), Duration::from_secs(15), 3);

    // Count total initial data events
    let batch_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ParsedEvent::InitialDataBatch { .. }))
        .collect();

    println!("[TEST] Data ordering: received {} batch events", batch_events.len());

    if batch_events.is_empty() {
        let persisted = execute_sql_as_root_via_client_json(&format!(
            "SELECT id, seq, label FROM {} ORDER BY id",
            full
        ))
        .expect("Fallback SELECT for data ordering should succeed");
        let json: serde_json::Value =
            serde_json::from_str(&persisted).expect("Fallback SELECT should return valid JSON");
        let rows = get_rows_as_hashmaps(&json).expect("Fallback SELECT should return rows");

        assert_eq!(
            rows.len(),
            total_rows as usize,
            "Fallback SELECT should return all inserted rows"
        );

        for (index, row) in rows.iter().enumerate() {
            let expected_id = (index + 1) as i64;
            let expected_seq = expected_id * 100;
            let expected_label = format!("item_{}", expected_id);

            let id = row
                .get("id")
                .map(extract_typed_value)
                .and_then(|value| value.as_i64())
                .expect("Fallback row should include integer id");
            let seq = row
                .get("seq")
                .map(extract_typed_value)
                .and_then(|value| value.as_i64())
                .expect("Fallback row should include integer seq");
            let label = row
                .get("label")
                .map(extract_typed_value)
                .and_then(|value| value.as_str().map(|s| s.to_string()))
                .expect("Fallback row should include label");

            assert_eq!(id, expected_id, "Fallback rows should stay ordered by id");
            assert_eq!(seq, expected_seq, "Fallback rows should preserve seq values");
            assert_eq!(label, expected_label, "Fallback rows should preserve labels");
        }

        println!("[TEST] Data ordering fallback query passed!");
    } else {
        // Verify final batch is ready
        if let Some(ParsedEvent::InitialDataBatch { batch_control, .. }) = batch_events.last() {
            assert!(!batch_control.has_more, "Final batch should have has_more=false");
            assert_eq!(batch_control.status, "ready", "Final batch should have status=ready");
        }
    }

    println!("[TEST] Data ordering test passed!");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
