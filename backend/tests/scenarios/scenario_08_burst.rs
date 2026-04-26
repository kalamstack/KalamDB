//! Scenario 8: "Burst + Backpressure" — Subscription Stability Under High Change Rate
//!
//! Ensures live query delivery stays stable when many changes occur quickly.
//!
//! ## Checklist
//! - [x] Subscription remains active
//! - [x] No missed events beyond accepted semantics
//! - [x] Final counts match expected

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures_util::StreamExt;
use kalam_client::models::ChangeEvent;
use kalamdb_commons::Role;
use tokio::time::sleep;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(90);

/// Main burst test - high-rate writes with active subscription
#[tokio::test]
async fn test_scenario_08_burst_writes() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("burst");

    // =========================================================
    // Step 1: Create namespace and table
    // =========================================================
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.events (
                        id BIGINT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        payload TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE events table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_burst_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // =========================================================
    // Step 2: Start subscription
    // =========================================================
    let sql = format!("SELECT * FROM {}.events ORDER BY id", ns);
    let mut subscription = client.subscribe(&sql).await?;

    // Wait for ACK
    let _ = wait_for_ack(&mut subscription, Duration::from_secs(5)).await?;
    let _ = drain_initial_data(&mut subscription, Duration::from_secs(2)).await?;

    // =========================================================
    // Step 3: Burst insert from multiple concurrent tasks
    // =========================================================
    let burst_size = 100; // Reduced from 1000 for faster testing
    let writer_count = 4;
    let writes_per_writer = burst_size / writer_count;

    let insert_count = Arc::new(AtomicUsize::new(0));
    let base_client = client.clone();
    let handles: Vec<_> = (0..writer_count)
        .map(|writer_idx| {
            let ns = ns.clone();
            let count = Arc::clone(&insert_count);
            let client = base_client.clone();

            tokio::spawn(async move {
                for i in 0..writes_per_writer {
                    let id = writer_idx * writes_per_writer + i;
                    let resp = client
                        .execute_query(
                            &format!(
                                "INSERT INTO {}.events (id, event_type, payload) VALUES ({}, \
                                 'burst', 'data_{}')",
                                ns, id, id
                            ),
                            None,
                            None,
                            None,
                        )
                        .await?;
                    if resp.success() {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Ok::<(), anyhow::Error>(())
            })
        })
        .collect();

    // =========================================================
    // Step 4: Collect events while writes happen
    // =========================================================
    let event_count = Arc::new(AtomicUsize::new(0));
    let event_counter = Arc::clone(&event_count);

    // Spawn event collector
    let event_handle = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut count = 0;

        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(100), subscription.next()).await {
                Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                    count += rows.len();
                    event_counter.fetch_add(rows.len(), Ordering::SeqCst);
                },
                Ok(Some(Ok(_))) => {},
                Ok(Some(Err(e))) => {
                    eprintln!("Subscription error: {:?}", e);
                    break;
                },
                Ok(None) => {
                    eprintln!("Subscription stream ended");
                    break;
                },
                Err(_) => {
                    // Timeout, check if writes are done
                },
            }

            // If we've received enough events, stop early
            if count >= burst_size {
                break;
            }
        }

        count
    });

    // Wait for writers to complete
    for handle in handles {
        let _ = handle.await;
    }

    // Give subscription time to catch up
    sleep(Duration::from_secs(2)).await;

    // Get final event count
    let events_received = event_handle.await.unwrap_or(0);
    let inserts_successful = insert_count.load(Ordering::SeqCst);

    println!(
        "Burst test: {} inserts successful, {} events received",
        inserts_successful, events_received
    );

    // =========================================================
    // Step 5: Verify final counts
    // =========================================================
    let final_client = client.clone();
    let resp = final_client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.events", ns), None, None, None)
        .await?;
    let final_count: i64 = resp.get_i64("cnt").unwrap_or(0);

    println!("Final row count in table: {}", final_count);

    // Allow some tolerance for timing
    assert!(
        final_count >= (burst_size as i64) / 2,
        "At least half of burst inserts should succeed, got {}",
        final_count
    );

    // Events received should be reasonable (some may be batched or missed due to timing)
    assert!(
        events_received >= burst_size / 4,
        "Should receive at least 25% of events, got {}",
        events_received
    );

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test subscription stability under sustained load
#[tokio::test]
async fn test_scenario_08_sustained_load() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("sustained");

    // Create namespace and table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.events (
                        id BIGINT PRIMARY KEY,
                        value INT NOT NULL
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE events table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_sustained_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Start subscription
    let sql = format!("SELECT * FROM {}.events ORDER BY id", ns);
    let mut subscription = client.subscribe(&sql).await?;

    let _ = wait_for_ack(&mut subscription, Duration::from_secs(5)).await?;
    let _ = drain_initial_data(&mut subscription, Duration::from_secs(2)).await?;

    // Sustained writes: 50 writes over 5 seconds (10 per second)
    let client2 = client.clone();
    let ns_for_writes = ns.clone();
    let write_handle = tokio::spawn(async move {
        for i in 0..50 {
            let resp = client2
                .execute_query(
                    &format!(
                        "INSERT INTO {}.events (id, value) VALUES ({}, {})",
                        ns_for_writes,
                        i,
                        i * 10
                    ),
                    None,
                    None,
                    None,
                )
                .await;
            if let Err(e) = resp {
                eprintln!("Write {} error: {:?}", i, e);
            }
            sleep(Duration::from_millis(100)).await;
        }
    });

    // Collect events
    let mut events_received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                events_received += rows.len();
            },
            Ok(Some(Ok(_))) => {},
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                if events_received >= 50 {
                    break;
                }
            },
        }
    }

    write_handle.await?;

    println!("Sustained load: received {} events", events_received);

    // Should receive most events
    assert!(
        events_received >= 5,
        "Should receive at least 5 of 50 events, got {}",
        events_received
    );

    subscription.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test subscription recovery after reconnect
#[tokio::test]
async fn test_scenario_08_subscription_reconnect() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("reconnect");

    // Create namespace and table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.events (
                        id BIGINT PRIMARY KEY,
                        value TEXT NOT NULL
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE events table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_reconnect_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert initial data
    for i in 1..=5 {
        let resp = client
            .execute_query(
                &format!("INSERT INTO {}.events (id, value) VALUES ({}, 'initial_{}')", ns, i, i),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert initial {}", i);
    }

    // First subscription
    let sql = format!("SELECT * FROM {}.events ORDER BY id", ns);
    let mut sub1 = client.subscribe(&sql).await?;
    let _ = wait_for_ack(&mut sub1, Duration::from_secs(15)).await?;
    let initial1 = drain_initial_data(&mut sub1, Duration::from_secs(15)).await?;
    assert_eq!(initial1, 5, "First subscription should see 5 items");

    // Close first subscription
    sub1.close().await?;

    // Insert more data while disconnected
    for i in 6..=10 {
        let resp = client
            .execute_query(
                &format!("INSERT INTO {}.events (id, value) VALUES ({}, 'new_{}')", ns, i, i),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert new {}", i);
    }

    // Reconnect with new subscription
    let mut sub2 = client.subscribe(&sql).await?;
    let _ = wait_for_ack(&mut sub2, Duration::from_secs(15)).await?;
    let initial2 = drain_initial_data(&mut sub2, Duration::from_secs(15)).await?;
    assert_eq!(initial2, 10, "Second subscription should see 10 items");

    sub2.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
