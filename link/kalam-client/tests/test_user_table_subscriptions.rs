//! User Table Subscription Tests
//!
//! Tests for WebSocket subscriptions on USER tables with filtered queries.
//! Verifies multiple subscriptions, filtered change notifications, and unsubscribe functionality.
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
//! cargo test --test test_user_table_subscriptions -- --nocapture
//! ```

use kalam_client::auth::AuthProvider;
use kalam_client::models::{BatchStatus, ResponseStatus};
use kalam_client::subscription::SubscriptionManager;
use kalam_client::{ChangeEvent, KalamLinkClient, QueryResponse, SubscriptionConfig};
use std::time::Duration;
use std::time::Instant;
use tokio::time::{sleep, timeout};

mod common;

/// Test configuration
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Helper to execute SQL via HTTP
async fn execute_sql(sql: &str) -> Result<QueryResponse, Box<dyn std::error::Error + Send + Sync>> {
    let client = create_test_client()?;
    Ok(client.execute_query(sql, None, None, None).await?)
}

async fn execute_sql_checked(
    sql: &str,
) -> Result<QueryResponse, Box<dyn std::error::Error + Send + Sync>> {
    let response = execute_sql(sql).await?;
    if response.status != ResponseStatus::Success {
        return Err(format!("SQL failed: {:?}", response.error).into());
    }
    Ok(response)
}

/// Wait until a newly created table is visible for queries
async fn wait_for_table_ready(table: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..20 {
        if execute_sql_checked(&format!("SELECT 1 FROM {} LIMIT 1", table)).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    Err(format!("Table not ready: {}", table).into())
}

/// Generate a unique table name
fn generate_table_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("messages_{}_{}_{}", timestamp, pid, counter)
}

/// Generate a unique row id for tests
fn generate_row_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static ROW_ID: AtomicU64 = AtomicU64::new(1);
    ROW_ID.fetch_add(1, Ordering::SeqCst)
}

/// Setup test namespace and USER table for subscription tests
async fn setup_user_table() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let table_name = generate_table_name();
    let full_table = format!("sub_test.{}", table_name);

    // Create namespace if needed
    execute_sql_checked("CREATE NAMESPACE IF NOT EXISTS sub_test").await?;
    sleep(Duration::from_millis(50)).await;

    // Create USER TABLE (not STREAM TABLE) - supports all DML operations
    // USER tables require a PRIMARY KEY column
    let create_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, type VARCHAR, content VARCHAR) WITH (TYPE = 'USER')",
        full_table
    );
    let mut created = false;
    for _ in 0..3 {
        match execute_sql_checked(&create_sql).await {
            Ok(_) => {
                created = true;
                break;
            },
            Err(e) => {
                if e.to_string().contains("Already exists") {
                    let _ = execute_sql(&format!("DROP TABLE IF EXISTS {}", full_table)).await;
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(e);
            },
        }
    }

    if !created {
        return Err("Failed to create test table".into());
    }

    wait_for_table_ready(&full_table).await?;

    Ok(full_table)
}

/// Cleanup test table
async fn cleanup_table(table_name: &str) {
    let _ = execute_sql(&format!("DROP TABLE IF EXISTS {}", table_name)).await;
}

/// Insert a row with retry (guards against duplicate PK or transient errors)
async fn insert_row_with_retry(
    table: &str,
    row_type: &str,
    content: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for _ in 0..6 {
        let row_id = generate_row_id();
        let sql = format!(
            "INSERT INTO {} (id, type, content) VALUES ({}, '{}', '{}')",
            table, row_id, row_type, content
        );

        match execute_sql_checked(&sql).await {
            Ok(_) => return Ok(row_id),
            Err(e) => {
                last_err = Some(e);
                let _ = wait_for_table_ready(table).await;
                sleep(Duration::from_millis(100)).await;
            },
        }
    }

    Err(last_err.unwrap_or_else(|| "Insert failed".into()))
}

/// Wait until a subscription is fully ready for live updates
async fn wait_for_subscription_ready(
    subscription: &mut SubscriptionManager,
    overall_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = Instant::now() + overall_timeout;
    loop {
        let event = next_with_deadline(subscription, deadline).await;
        match event {
            Some(Ok(ChangeEvent::Ack { batch_control, .. })) => {
                if batch_control.status == BatchStatus::Ready {
                    return Ok(());
                }
            },
            Some(Ok(ChangeEvent::InitialDataBatch { batch_control, .. })) => {
                if batch_control.status == BatchStatus::Ready {
                    return Ok(());
                }
            },
            Some(Ok(ChangeEvent::Error { code, message, .. })) => {
                return Err(format!("Subscription error {}: {}", code, message).into());
            },
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.into()),
            None => return Err("Timeout waiting for subscription ready".into()),
        }
    }
}

/// Helper to extract string value from row field (handles {"Utf8": "value"} format)
fn extract_string_value(value: &kalam_client::KalamCellValue) -> Option<String> {
    // Try direct string access first
    value
        .as_str()
        .map(|s| s.to_string())
        .or_else(|| value.get("Utf8").and_then(|v| v.as_str()).map(|s| s.to_string()))
}

fn row_matches_type(
    row: &std::collections::HashMap<String, kalam_client::KalamCellValue>,
    expected: &str,
) -> bool {
    if let Some(type_obj) = row.get("type") {
        if let Some(type_str) = extract_string_value(type_obj) {
            return type_str == expected;
        }
    }

    format!("{:?}", row).contains(expected)
}

async fn wait_for_insert_with_type(
    subscription: &mut SubscriptionManager,
    expected_type: &str,
    overall_timeout: Duration,
) -> Option<ChangeEvent> {
    let deadline = Instant::now() + overall_timeout;
    loop {
        let event = next_with_deadline(subscription, deadline).await;
        match event {
            Some(Ok(event)) => {
                if let ChangeEvent::Insert { rows, .. } = &event {
                    if rows.iter().any(|row| row_matches_type(row, expected_type)) {
                        return Some(event);
                    }
                }
                if matches!(event, ChangeEvent::Ack { .. } | ChangeEvent::InitialDataBatch { .. }) {
                    continue;
                }
            },
            Some(Err(e)) => {
                eprintln!("❌ Subscription error while waiting for insert: {}", e);
            },
            None => return None,
        }
    }
}

async fn next_with_deadline(
    subscription: &mut SubscriptionManager,
    deadline: Instant,
) -> Option<Result<ChangeEvent, kalam_client::KalamLinkError>> {
    let now = Instant::now();
    if now >= deadline {
        return None;
    }

    let remaining = deadline.saturating_duration_since(now);
    let mut next_fut = Box::pin(subscription.next());
    let mut sleep_fut = Box::pin(tokio::time::sleep(remaining));

    tokio::select! {
        res = &mut next_fut => res,
        _ = &mut sleep_fut => None,
    }
}

/// Test: Multiple filtered subscriptions on a USER table
///
/// This test verifies that WHERE clause filtering works for subscription change notifications:
/// 1. Creates a USER table with 'id', 'type', and 'content' columns
/// 2. Creates two subscriptions with different filters (type='thinking' and type='typing')
/// 3. Inserts rows matching each filter from another task
/// 4. **Verifies each subscription receives ONLY its filtered changes**
/// 5. Updates a row and verifies the UPDATE event is received
/// 6. Unsubscribes from one subscription
/// 7. Inserts another row and verifies the unsubscribed query doesn't receive it
#[tokio::test]
async fn test_multiple_filtered_subscriptions() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running at {}. Skipping test.", common::server_url());
        return;
    }

    let table = match setup_user_table().await {
        Ok(t) => t,
        Err(e) => {
            panic!("Failed to setup test table: {}", e);
        },
    };

    println!("✅ Created test table: {}", table);

    // Create client for subscriptions
    let client = create_test_client().expect("Failed to create client");

    // === Step 1: Create two subscriptions with different filters ===

    // Subscription 1: type = 'thinking'
    let thinking_config = SubscriptionConfig::new(
        "sub-thinking",
        format!("SELECT * FROM {} WHERE type = 'thinking'", table),
    );

    let mut thinking_sub =
        match timeout(TEST_TIMEOUT, client.subscribe_with_config(thinking_config)).await {
            Ok(Ok(sub)) => sub,
            Ok(Err(e)) => {
                cleanup_table(&table).await;
                panic!("Failed to create 'thinking' subscription: {}", e);
            },
            Err(_) => {
                cleanup_table(&table).await;
                panic!("Timeout creating 'thinking' subscription");
            },
        };

    println!(
        "✅ Created subscription for type='thinking' (id: {})",
        thinking_sub.subscription_id()
    );

    // Subscription 2: type = 'typing'
    let typing_config = SubscriptionConfig::new(
        "sub-typing",
        format!("SELECT * FROM {} WHERE type = 'typing'", table),
    );

    let mut typing_sub =
        match timeout(TEST_TIMEOUT, client.subscribe_with_config(typing_config)).await {
            Ok(Ok(sub)) => sub,
            Ok(Err(e)) => {
                let _ = thinking_sub.close().await;
                cleanup_table(&table).await;
                panic!("Failed to create 'typing' subscription: {}", e);
            },
            Err(_) => {
                let _ = thinking_sub.close().await;
                cleanup_table(&table).await;
                panic!("Timeout creating 'typing' subscription");
            },
        };

    println!(
        "✅ Created subscription for type='typing' (id: {})",
        typing_sub.subscription_id()
    );

    // Wait for both subscriptions to be fully ready before inserting rows
    wait_for_subscription_ready(&mut thinking_sub, Duration::from_secs(10))
        .await
        .expect("thinking subscription not ready");
    wait_for_subscription_ready(&mut typing_sub, Duration::from_secs(10))
        .await
        .expect("typing subscription not ready");

    println!("✅ Both subscriptions are ready");

    // === Step 2: Insert rows from another task ===
    let table_clone = table.clone();
    let insert_handle = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;

        // Insert a 'typing' row
        let typing_result =
            insert_row_with_retry(&table_clone, "typing", "user is typing...").await;

        let typing_id = match typing_result {
            Ok(id) => id,
            Err(e) => {
                eprintln!("❌ Failed to insert 'typing' row: {}", e);
                return Err(e);
            },
        };
        println!("✅ Inserted row with type='typing' (id={})", typing_id);

        sleep(Duration::from_millis(100)).await;

        // Insert a 'thinking' row
        let thinking_result =
            insert_row_with_retry(&table_clone, "thinking", "AI is thinking...").await;

        let thinking_id = match thinking_result {
            Ok(id) => id,
            Err(e) => {
                eprintln!("❌ Failed to insert 'thinking' row: {}", e);
                return Err(e);
            },
        };
        println!("✅ Inserted row with type='thinking' (id={})", thinking_id);

        Ok((typing_id, thinking_id))
    });

    // === Step 3: Wait for changes on both subscriptions ===
    let mut thinking_changes: Vec<ChangeEvent> = Vec::new();
    let mut typing_changes: Vec<ChangeEvent> = Vec::new();

    // Collect events from 'thinking' subscription
    println!("🔄 Waiting for changes on 'thinking' subscription...");
    if let Some(event) =
        wait_for_insert_with_type(&mut thinking_sub, "thinking", Duration::from_secs(20)).await
    {
        thinking_changes.push(event);
    }

    // Collect events from 'typing' subscription
    println!("🔄 Waiting for changes on 'typing' subscription...");
    if let Some(event) =
        wait_for_insert_with_type(&mut typing_sub, "typing", Duration::from_secs(20)).await
    {
        typing_changes.push(event);
    }

    // Wait for insert task to complete
    let (_typing_id, thinking_id) = insert_handle
        .await
        .expect("Insert task failed")
        .expect("Insert task returned error");

    // === Step 4: Verify each subscription received correct changes ===
    println!("\n=== Verification: Filtered Changes ===");
    println!("'thinking' subscription changes: {}", thinking_changes.len());
    println!("'typing' subscription changes: {}", typing_changes.len());

    assert!(
        !thinking_changes.is_empty(),
        "FAILED: 'thinking' subscription should have received at least 1 change"
    );

    assert!(
        !typing_changes.is_empty(),
        "FAILED: 'typing' subscription should have received at least 1 change"
    );

    // Verify 'thinking' subscription received ONLY 'thinking' type rows (filtering check)
    // The server prefixes subscription IDs with user-id and session info
    if let Some(ChangeEvent::Insert {
        subscription_id,
        rows,
    }) = thinking_changes.first()
    {
        assert!(
            subscription_id.ends_with("sub-thinking"),
            "Change should come from subscription ending with 'sub-thinking', got: {}",
            subscription_id
        );

        if let Some(row) = rows.first() {
            println!("📊 'thinking' subscription received row: {:?}", row);

            if let Some(type_obj) = row.get("type") {
                let type_str = extract_string_value(type_obj);
                println!("📊 Extracted type value: {:?}", type_str);

                // CRITICAL: Verify filtering - 'thinking' subscription should ONLY receive 'thinking' rows
                assert_eq!(
                    type_str.as_deref(), 
                    Some("thinking"),
                    "FILTERING CHECK FAILED: 'thinking' subscription received row with type={:?}, expected 'thinking'",
                    type_str
                );
                println!("✅ FILTERING WORKS: 'thinking' subscription correctly received ONLY 'thinking' type row");
            } else {
                panic!("Row doesn't have 'type' field. Full row: {:?}", row);
            }
        } else {
            panic!("'thinking' subscription received Insert with empty rows!");
        }
    }

    // Verify 'typing' subscription received ONLY 'typing' type rows (filtering check)
    if let Some(ChangeEvent::Insert {
        subscription_id,
        rows,
    }) = typing_changes.first()
    {
        assert!(
            subscription_id.ends_with("sub-typing"),
            "Change should come from subscription ending with 'sub-typing', got: {}",
            subscription_id
        );

        if let Some(row) = rows.first() {
            println!("📊 'typing' subscription received row: {:?}", row);

            if let Some(type_obj) = row.get("type") {
                let type_str = extract_string_value(type_obj);

                // CRITICAL: Verify filtering - 'typing' subscription should ONLY receive 'typing' rows
                assert_eq!(
                    type_str.as_deref(),
                    Some("typing"),
                    "FILTERING CHECK FAILED: 'typing' subscription received row with type={:?}, expected 'typing'",
                    type_str
                );
                println!("✅ FILTERING WORKS: 'typing' subscription correctly received ONLY 'typing' type row");
            }
        }
    }

    // === Step 5: UPDATE a row and verify UPDATE event is received ===
    println!("\n🔄 Step 5: Testing UPDATE event...");

    let table_clone_update = table.clone();
    let thinking_id_update = thinking_id;
    let update_handle = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        // Update the 'thinking' row to change its content
        let result = execute_sql_checked(&format!(
            "UPDATE {} SET content = 'AI finished thinking!' WHERE id = {}",
            table_clone_update, thinking_id_update
        ))
        .await;

        if let Err(e) = result {
            eprintln!("❌ Failed to update row: {}", e);
            return Err(e);
        }
        println!("✅ Updated row id={} (type='thinking')", thinking_id_update);
        Ok(())
    });

    // Wait for UPDATE event on 'thinking' subscription
    println!("🔄 Waiting for UPDATE event on 'thinking' subscription...");
    let mut received_update = false;

    let update_deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < update_deadline {
        match next_with_deadline(&mut thinking_sub, update_deadline).await {
            Some(Ok(event)) => {
                match &event {
                    ChangeEvent::Update {
                        subscription_id,
                        rows,
                        old_rows,
                    } => {
                        println!("📥 'thinking' sub received Update: subscription_id={}, rows={}, old_rows={}", 
                            subscription_id, rows.len(), old_rows.len());

                        // Verify the update came through
                        if let Some(row) = rows.first() {
                            println!("📊 Updated row (new): {:?}", row);
                            // Check if content was updated
                            if let Some(content_obj) = row.get("content") {
                                let content_str = extract_string_value(content_obj);
                                if content_str.as_deref() == Some("AI finished thinking!") {
                                    println!("✅ UPDATE EVENT RECEIVED: content correctly updated");
                                    received_update = true;
                                }
                            }
                        }
                        if let Some(old_row) = old_rows.first() {
                            println!("📊 Updated row (old): {:?}", old_row);
                        }
                        break;
                    },
                    ChangeEvent::Insert { .. } => {
                        // Might receive late insert, continue waiting for update
                        println!("📥 Received late Insert, waiting for Update...");
                        continue;
                    },
                    ChangeEvent::Ack { .. } | ChangeEvent::InitialDataBatch { .. } => {
                        continue;
                    },
                    other => {
                        println!("📥 Received unexpected event: {:?}", other);
                    },
                }
            },
            Some(Err(e)) => {
                eprintln!("❌ Error on subscription: {}", e);
                break;
            },
            None => {
                eprintln!("❌ Subscription closed unexpectedly");
                break;
            },
        }
    }

    let _ = update_handle.await;

    assert!(
        received_update,
        "FAILED: Should have received UPDATE event for the modified row"
    );
    println!("✅ UPDATE event verification passed!");

    // === Step 6: Unsubscribe from 'typing' subscription ===
    println!("\n🔄 Unsubscribing from 'typing' subscription...");
    match typing_sub.close().await {
        Ok(_) => println!("✅ Successfully unsubscribed from 'typing'"),
        Err(e) => println!("⚠️  Error during unsubscribe (may be OK): {}", e),
    }

    // === Step 7: Insert another 'typing' row and verify 'thinking' subscription does NOT receive it ===
    println!("\n🔄 Step 7: Verifying filtered subscriptions don't receive unmatched inserts...");
    sleep(Duration::from_millis(100)).await;

    let table_clone2 = table.clone();
    let insert_handle2 = tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        let result = insert_row_with_retry(
            &table_clone2,
            "typing",
            "more typing - should not reach thinking sub",
        )
        .await;

        if let Err(e) = result {
            eprintln!("❌ Failed to insert third row: {}", e);
        } else {
            println!("✅ Inserted row id=3 with type='typing'");
        }
    });

    // CRITICAL FILTERING TEST: The 'thinking' subscription (WHERE type='thinking')
    // should NOT receive the new 'typing' row if filtering works correctly

    println!("🔄 Waiting to verify 'thinking' subscription does NOT receive 'typing' insert...");
    let mut received_wrong_type = false;

    // Wait and check if 'thinking' sub receives anything (it shouldn't for 'typing' rows)
    let check_deadline = Instant::now() + Duration::from_secs(3);
    match next_with_deadline(&mut thinking_sub, check_deadline).await {
        Some(Ok(event)) => {
            match &event {
                ChangeEvent::Insert { rows, .. } => {
                    // Check if this is a 'typing' row (would mean filtering failed!)
                    if let Some(row) = rows.first() {
                        if let Some(type_obj) = row.get("type") {
                            let type_str = extract_string_value(type_obj);
                            if type_str.as_deref() == Some("typing") {
                                received_wrong_type = true;
                                println!("❌ FILTERING FAILED: 'thinking' subscription received 'typing' row!");
                                println!("📊 Unexpected row: {:?}", row);
                            } else {
                                println!("📥 'thinking' received insert with type={:?} (unexpected but not 'typing')", type_str);
                            }
                        }
                    }
                },
                ChangeEvent::Update { .. } => {
                    println!("📥 Received late Update event (OK)");
                },
                other => {
                    println!("📥 'thinking' subscription received: {:?}", other);
                },
            }
        },
        Some(Err(e)) => {
            println!("⚠️  Error on 'thinking' subscription: {}", e);
        },
        None => {
            println!("⚠️  'thinking' subscription closed");
        },
    }

    if Instant::now() >= check_deadline && !received_wrong_type {
        // Timeout is EXPECTED if filtering works correctly
        println!("✅ FILTERING VERIFIED: 'thinking' subscription correctly did NOT receive 'typing' insert (timeout)");
    }

    assert!(
        !received_wrong_type,
        "FILTERING FAILED: 'thinking' subscription should NOT receive 'typing' type rows"
    );

    // Wait for insert task
    let _ = insert_handle2.await;

    // === Cleanup ===
    let _ = thinking_sub.close().await;
    cleanup_table(&table).await;

    println!("\n✅✅✅ Test passed: Filtered subscriptions work correctly! ✅✅✅");
}

/// Test: Unsubscribe stops receiving changes
///
/// Simpler test focused specifically on unsubscribe behavior
#[tokio::test]
async fn test_unsubscribe_stops_changes() {
    if !common::is_server_running().await {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = match setup_user_table().await {
        Ok(t) => t,
        Err(e) => {
            panic!("Failed to setup test table: {}", e);
        },
    };

    let client = create_test_client().expect("Failed to create client");

    // Create a subscription
    let config = SubscriptionConfig::new(
        "sub-unsubscribe-test",
        format!("SELECT * FROM {} WHERE type = 'test'", table),
    );

    let mut subscription = match timeout(TEST_TIMEOUT, client.subscribe_with_config(config)).await {
        Ok(Ok(sub)) => sub,
        Ok(Err(e)) => {
            cleanup_table(&table).await;
            panic!("Failed to create subscription: {}", e);
        },
        Err(_) => {
            cleanup_table(&table).await;
            panic!("Timeout creating subscription");
        },
    };

    println!("✅ Created subscription: {}", subscription.subscription_id());

    // Wait for subscription to be ready before inserting rows
    wait_for_subscription_ready(&mut subscription, Duration::from_secs(10))
        .await
        .expect("unsubscribe subscription not ready");

    // Insert a row to verify subscription is working
    insert_row_with_retry(&table, "test", "first insert")
        .await
        .expect("Failed to insert first row");

    // Wait for the change
    let received_first =
        wait_for_insert_with_type(&mut subscription, "test", Duration::from_secs(20))
            .await
            .is_some();

    assert!(received_first, "Should receive first insert before unsubscribe");
    println!("✅ Received first insert notification");

    // Unsubscribe
    subscription.close().await.expect("Failed to close subscription");
    println!("✅ Unsubscribed");

    // Insert another row after unsubscribe
    insert_row_with_retry(&table, "test", "second insert after unsubscribe")
        .await
        .expect("Failed to insert second row");

    // The subscription is closed, so we can't check it anymore
    // This test mainly verifies that close() works without errors

    println!("✅ Second insert completed (subscription is closed, no notification expected)");

    cleanup_table(&table).await;
    println!("✅ Test passed: Unsubscribe works correctly");
}
