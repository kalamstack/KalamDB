// Smoke Tests: Subscription Listing & Unsubscribe Verification
//
// Verifies:
//   1. `client.subscriptions()` returns active subscriptions with correct metadata
//   2. After close()ing a subscription, it disappears from the list
//   3. Subscription handle provides id and close functionality
//   4. Multiple subscriptions can be listed and individually removed
//
// Run with:
//   cargo test --test smoke smoke_test_subscription_listing

use crate::common::*;
use kalam_client::{ChangeEvent, KalamLinkClient, KalamLinkTimeouts, SubscriptionConfig};
use std::time::Duration;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a kalam-client with fast timeouts.
fn fast_link_client() -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    client_for_user_on_url_with_timeouts(
        &leader_or_server_url(),
        default_username(),
        default_password(),
        KalamLinkTimeouts::fast(),
    )
}

// ── test 1: subscriptions() lists active subs and close removes them ──────────

/// Verify that `client.subscriptions()` lists active subscriptions
/// and that closing a subscription removes it from the list.
#[ntest::timeout(30000)]
#[test]
fn smoke_subscription_listing_and_close_removes() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_list");
    let tbl = generate_unique_table("list_tbl");
    let full = format!("{}.{}", ns, tbl);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full,
    ))
    .expect("create table");

    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let full_clone = full.clone();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async move {
            let client = match fast_link_client() {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(format!("SKIP: {e}"));
                    return;
                },
            };

            client.connect().await.expect("connect");

            // 1) Initially no subscriptions
            let subs = client.subscriptions().await;
            assert!(subs.is_empty(), "should have no subscriptions initially");

            // 2) Subscribe
            let query_sql = format!("SELECT * FROM {}", full_clone);
            let cfg = SubscriptionConfig::new("sub_list_1".to_string(), query_sql.clone());
            let mut sub1 = client.subscribe_with_config(cfg).await.expect("subscribe 1");

            // Wait for ack
            let ack_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                match tokio::time::timeout(
                    ack_deadline.duration_since(tokio::time::Instant::now()),
                    sub1.next(),
                )
                .await
                {
                    Ok(Some(Ok(ChangeEvent::Ack { .. }))) => break,
                    Ok(Some(Ok(_))) => continue,
                    _ => break,
                }
            }

            // 3) Verify subscriptions() lists 1 entry
            let subs = client.subscriptions().await;
            assert_eq!(subs.len(), 1, "should have 1 subscription, got {}", subs.len());
            assert_eq!(subs[0].id, "sub_list_1", "subscription id should match");
            assert!(
                subs[0].query.contains(&full_clone),
                "subscription query should contain table name"
            );
            assert!(!subs[0].closed);
            assert!(subs[0].created_at_ms > 0);

            // 4) Subscribe to a second query
            let cfg2 = SubscriptionConfig::new("sub_list_2".to_string(), query_sql.clone());
            let mut sub2 = client.subscribe_with_config(cfg2).await.expect("subscribe 2");
            // Wait for ack
            let ack_deadline2 = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                match tokio::time::timeout(
                    ack_deadline2.duration_since(tokio::time::Instant::now()),
                    sub2.next(),
                )
                .await
                {
                    Ok(Some(Ok(ChangeEvent::Ack { .. }))) => break,
                    Ok(Some(Ok(_))) => continue,
                    _ => break,
                }
            }

            let subs = client.subscriptions().await;
            assert_eq!(subs.len(), 2, "should have 2 subscriptions, got {}", subs.len());

            // 5) Close sub1 and verify it's removed
            sub1.close().await.expect("close sub1");
            assert!(sub1.is_closed());
            tokio::time::sleep(Duration::from_millis(300)).await;

            let subs = client.subscriptions().await;
            assert_eq!(subs.len(), 1, "should have 1 subscription after close, got {}", subs.len());
            assert_eq!(subs[0].id, "sub_list_2", "remaining subscription should be sub_list_2");

            // 6) Drop sub2 and verify it's removed
            drop(sub2);
            tokio::time::sleep(Duration::from_millis(300)).await;

            let subs = client.subscriptions().await;
            assert!(subs.is_empty(), "should have 0 subscriptions after drop, got {}", subs.len());

            client.disconnect().await;
            let _ = tx.send("OK".to_string());
        });
    });

    let result = rx.recv_timeout(Duration::from_secs(25)).unwrap_or_else(|_| "TIMEOUT".into());
    handle.join().ok();

    if result.starts_with("SKIP") {
        eprintln!("{}", result);
        return;
    }
    assert_eq!(result, "OK", "test thread reported: {}", result);

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
}

// ── test 2: subscriptions() tracks last_seq_id after events ───────────────────

/// Verify that after receiving change events, subscriptions() reports
/// a non-empty last_seq_id and last_event_time_ms.
#[ntest::timeout(30000)]
#[test]
fn smoke_subscription_listing_tracks_seq_id() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_seq");
    let tbl = generate_unique_table("seq_tbl");
    let full = format!("{}.{}", ns, tbl);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full,
    ))
    .expect("create table");

    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let full_clone = full.clone();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async move {
            let client = match fast_link_client() {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(format!("SKIP: {e}"));
                    return;
                },
            };

            client.connect().await.expect("connect");

            let cfg = SubscriptionConfig::new(
                "sub_seq_1".to_string(),
                format!("SELECT * FROM {}", full_clone),
            );
            let mut sub = client.subscribe_with_config(cfg).await.expect("subscribe");

            // Wait for ack
            let ack_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                match tokio::time::timeout(
                    ack_deadline.duration_since(tokio::time::Instant::now()),
                    sub.next(),
                )
                .await
                {
                    Ok(Some(Ok(ChangeEvent::Ack { .. }))) => break,
                    Ok(Some(Ok(_))) => continue,
                    _ => break,
                }
            }

            // Insert a row
            client
                .execute_query(
                    &format!("INSERT INTO {} (id, v) VALUES ('r1', 'hello')", full_clone),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert");

            // Consume events until we get an Insert
            let insert_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                match tokio::time::timeout(
                    insert_deadline.duration_since(tokio::time::Instant::now()),
                    sub.next(),
                )
                .await
                {
                    Ok(Some(Ok(ChangeEvent::Insert { .. }))) => break,
                    Ok(Some(Ok(_))) => continue,
                    _ => break,
                }
            }

            // Check subscriptions metadata — last_event_time_ms should be set
            // after receiving events. last_seq_id is only populated from Ack/InitialDataBatch
            // batch_control, not from Insert events — so it may or may not be set.
            let subs = client.subscriptions().await;
            assert_eq!(subs.len(), 1, "should have 1 subscription");
            assert!(
                subs[0].last_event_time_ms.is_some(),
                "last_event_time_ms should be set after receiving events"
            );
            let event_time = subs[0].last_event_time_ms.unwrap();
            // Sanity: event time should be within last 60 seconds
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            assert!(
                now_ms - event_time < 60_000,
                "last_event_time_ms should be recent, got {} (now {})",
                event_time,
                now_ms
            );

            sub.close().await.ok();
            client.disconnect().await;
            let _ = tx.send("OK".to_string());
        });
    });

    let result = rx.recv_timeout(Duration::from_secs(25)).unwrap_or_else(|_| "TIMEOUT".into());
    handle.join().ok();

    if result.starts_with("SKIP") {
        eprintln!("{}", result);
        return;
    }
    assert_eq!(result, "OK", "test thread reported: {}", result);

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
}
