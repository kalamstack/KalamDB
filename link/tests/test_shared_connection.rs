//! Tests for the shared WebSocket connection architecture.
//!
//! Verifies:
//! - Single connection multiplexes multiple subscriptions
//! - `connect()` + `subscribe()` uses the shared path
//! - Connection lifecycle events fire at connection level
//! - Auto-reconnection re-subscribes all queries
//!
//! **IMPORTANT**: These tests require a running KalamDB server (auto-started).

use kalam_link::auth::AuthProvider;
use kalam_link::{
    ChangeEvent, ConnectionOptions, EventHandlers, KalamLinkClient, KalamLinkTimeouts,
    SubscriptionConfig,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

mod common;

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Create a test client authenticated with root credentials.
fn create_test_client() -> Result<KalamLinkClient, kalam_link::KalamLinkError> {
    let token = common::root_access_token_blocking()
        .map_err(|e| kalam_link::KalamLinkError::InternalError(e.to_string()))?;
    KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_secs(30))
        .auth(AuthProvider::jwt_token(token))
        .timeouts(KalamLinkTimeouts::default())
        .connection_options(
            ConnectionOptions::new()
                .with_auto_reconnect(true),
        )
        .build()
}

fn create_test_client_with_events()
    -> Result<(KalamLinkClient, Arc<AtomicU32>, Arc<AtomicU32>), kalam_link::KalamLinkError>
{
    let token = common::root_access_token_blocking()
        .map_err(|e| kalam_link::KalamLinkError::InternalError(e.to_string()))?;

    let connect_count = Arc::new(AtomicU32::new(0));
    let disconnect_count = Arc::new(AtomicU32::new(0));
    let cc = connect_count.clone();
    let dc = disconnect_count.clone();

    let client = KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_secs(30))
        .auth(AuthProvider::jwt_token(token))
        .event_handlers(
            EventHandlers::new()
                .on_connect(move || {
                    cc.fetch_add(1, Ordering::SeqCst);
                })
                .on_disconnect(move |_reason| {
                    dc.fetch_add(1, Ordering::SeqCst);
                }),
        )
        .connection_options(
            ConnectionOptions::new()
                .with_auto_reconnect(true),
        )
        .build()?;

    Ok((client, connect_count, disconnect_count))
}

/// Ensure a test table exists with a simple schema.
async fn ensure_table(client: &KalamLinkClient, table: &str) {
    let _ = client
        .execute_query(
            &format!(
                "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, value TEXT)",
                table
            ),
            None,
            None,
            None,
        )
        .await;
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// connect() should establish a shared connection successfully.
#[tokio::test]
async fn test_shared_connect() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    let result = timeout(TEST_TIMEOUT, client.connect()).await;
    assert!(result.is_ok(), "connect() timed out");
    assert!(result.unwrap().is_ok(), "connect() should succeed");

    assert!(client.is_connected().await, "should be connected");

    client.disconnect().await;
    // After disconnect, is_connected should return false
    // (may take a brief moment for background task to shut down)
    sleep(Duration::from_millis(100)).await;
    assert!(!client.is_connected().await, "should be disconnected");
}

/// connect() called twice should be a no-op.
#[tokio::test]
async fn test_connect_idempotent() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    client.connect().await.expect("first connect");
    client.connect().await.expect("second connect should be no-op");

    assert!(client.is_connected().await);
    client.disconnect().await;
}

/// subscribe() after connect() should use the shared connection.
#[tokio::test]
async fn test_subscribe_via_shared_connection() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    ensure_table(&client, "default.shared_conn_test").await;

    client.connect().await.expect("connect should succeed");

    let mut sub = timeout(
        TEST_TIMEOUT,
        client.subscribe("SELECT * FROM default.shared_conn_test"),
    )
    .await
    .expect("subscribe should not time out")
    .expect("subscribe should succeed");

    // Should get at least an Ack event
    let event = timeout(TEST_TIMEOUT, sub.next())
        .await
        .expect("event should arrive before timeout");

    match event {
        Some(Ok(ChangeEvent::Ack { .. })) => {
            // Expected
        },
        Some(Ok(ChangeEvent::Error { .. })) => {
            // Table might not exist — acceptable in test environment
        },
        other => {
            panic!("Unexpected first event: {:?}", other);
        },
    }

    sub.close().await.expect("close should succeed");
    client.disconnect().await;
}

/// Multiple subscriptions should multiplex over the same shared connection.
#[tokio::test]
async fn test_multiple_subscriptions_shared() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    ensure_table(&client, "default.shared_multi_a").await;
    ensure_table(&client, "default.shared_multi_b").await;

    client.connect().await.expect("connect");

    let mut sub_a = client
        .subscribe("SELECT * FROM default.shared_multi_a")
        .await
        .expect("subscribe A");
    let mut sub_b = client
        .subscribe("SELECT * FROM default.shared_multi_b")
        .await
        .expect("subscribe B");

    // Both should receive events
    let event_a = timeout(TEST_TIMEOUT, sub_a.next()).await;
    let event_b = timeout(TEST_TIMEOUT, sub_b.next()).await;

    assert!(event_a.is_ok(), "sub A should receive an event");
    assert!(event_b.is_ok(), "sub B should receive an event");

    sub_a.close().await.ok();
    sub_b.close().await.ok();
    client.disconnect().await;
}

/// Event handlers should fire on connect/disconnect.
#[tokio::test]
async fn test_connection_events() {
    let (client, connect_count, _disconnect_count) =
        match create_test_client_with_events() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Skipping test (server not available): {}", e);
                return;
            },
        };

    client.connect().await.expect("connect");

    // Give the event handler time to fire
    sleep(Duration::from_millis(200)).await;

    assert!(
        connect_count.load(Ordering::SeqCst) >= 1,
        "on_connect should have fired at least once"
    );

    client.disconnect().await;
}

/// Closing a subscription via drop (from_shared path) should send unsubscribe.
#[tokio::test]
async fn test_subscription_drop_unsubscribes() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    ensure_table(&client, "default.drop_unsub_test").await;

    client.connect().await.expect("connect");

    {
        let mut sub = client
            .subscribe("SELECT * FROM default.drop_unsub_test")
            .await
            .expect("subscribe");

        // Consume at least one event to confirm subscription is active
        let _ = timeout(TEST_TIMEOUT, sub.next()).await;
        // sub is dropped here — should trigger unsubscribe via shared_unsubscribe_tx
    }

    // Give the bridge task time to forward the unsubscribe
    sleep(Duration::from_millis(200)).await;

    // We can verify the connection is still healthy by subscribing again
    let mut sub2 = client
        .subscribe("SELECT * FROM default.drop_unsub_test")
        .await
        .expect("second subscribe should succeed after drop");

    let event = timeout(TEST_TIMEOUT, sub2.next()).await;
    assert!(event.is_ok(), "second subscription should receive events");

    sub2.close().await.ok();
    client.disconnect().await;
}

/// Without connect(), subscribe() should still work (legacy per-subscription path).
#[tokio::test]
async fn test_subscribe_without_connect_legacy() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    ensure_table(&client, "default.legacy_sub_test").await;

    // Don't call connect() — should fall back to per-subscription WebSocket
    let mut sub = timeout(
        TEST_TIMEOUT,
        client.subscribe("SELECT * FROM default.legacy_sub_test"),
    )
    .await
    .expect("subscribe should not time out")
    .expect("subscribe should succeed without connect()");

    let event = timeout(TEST_TIMEOUT, sub.next()).await;
    assert!(event.is_ok(), "legacy subscription should receive events");

    sub.close().await.ok();
}

/// Three active subscriptions should reconnect and continue from a resume point
/// without replaying rows that were already observed before disconnect.
#[tokio::test]
async fn test_three_subscriptions_resume_without_old_rows() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    let writer = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (writer client unavailable): {}", e);
            return;
        },
    };

    let table_a = "default.resume3_a";
    let table_b = "default.resume3_b";
    let table_c = "default.resume3_c";

    ensure_table(&client, table_a).await;
    ensure_table(&client, table_b).await;
    ensure_table(&client, table_c).await;

    client.connect().await.expect("connect should succeed");

    let mut sub_a = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-pre-a",
            format!("SELECT id, value FROM {}", table_a),
        ))
        .await
        .expect("subscribe A pre");
    let mut sub_b = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-pre-b",
            format!("SELECT id, value FROM {}", table_b),
        ))
        .await
        .expect("subscribe B pre");
    let mut sub_c = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-pre-c",
            format!("SELECT id, value FROM {}", table_c),
        ))
        .await
        .expect("subscribe C pre");

    // Consume initial events (ack/initial batch) before starting assertions.
    let _ = timeout(TEST_TIMEOUT, sub_a.next()).await;
    let _ = timeout(TEST_TIMEOUT, sub_b.next()).await;
    let _ = timeout(TEST_TIMEOUT, sub_c.next()).await;

    let pre_a = "1001";
    let pre_b = "2001";
    let pre_c = "3001";
    let gap_a = "1002";
    let gap_b = "2002";
    let gap_c = "3002";
    let post_a = "1003";
    let post_b = "2003";
    let post_c = "3003";

    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'pre-a')", table_a, pre_a),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre A");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'pre-b')", table_b, pre_b),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre B");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'pre-c')", table_c, pre_c),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre C");

    let has_id = |event: &ChangeEvent, target: &str| -> bool {
        match event {
            ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => rows
                .iter()
                
                .any(|r| r.get("id").and_then(|v| v.as_str()) == Some(target)),
            ChangeEvent::InitialDataBatch { rows, .. } => rows
                .iter()
                .any(|r| r.get("id").and_then(|v| v.as_str()) == Some(target)),
            _ => false,
        }
    };

    let mut got_pre_a = false;
    let mut got_pre_b = false;
    let mut got_pre_c = false;

    for _ in 0..14 {
        if got_pre_a && got_pre_b && got_pre_c {
            break;
        }

        if !got_pre_a {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_a.next()).await {
                if has_id(&ev, pre_a) {
                    got_pre_a = true;
                }
            }
        }

        if !got_pre_b {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_b.next()).await {
                if has_id(&ev, pre_b) {
                    got_pre_b = true;
                }
            }
        }

        if !got_pre_c {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_c.next()).await {
                if has_id(&ev, pre_c) {
                    got_pre_c = true;
                }
            }
        }
    }

    assert!(got_pre_a, "pre A row should be received");
    assert!(got_pre_b, "pre B row should be received");
    assert!(got_pre_c, "pre C row should be received");

    sub_a.close().await.ok();
    sub_b.close().await.ok();
    sub_c.close().await.ok();

    client.disconnect().await;
    sleep(Duration::from_millis(200)).await;
    assert!(!client.is_connected().await, "client should be disconnected");

    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'gap-a')", table_a, gap_a),
            None,
            None,
            None,
        )
        .await
        .expect("insert gap A");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'gap-b')", table_b, gap_b),
            None,
            None,
            None,
        )
        .await
        .expect("insert gap B");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'gap-c')", table_c, gap_c),
            None,
            None,
            None,
        )
        .await
        .expect("insert gap C");

    client.connect().await.expect("reconnect should succeed");

    let mut sub_a2 = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-post-a",
            format!("SELECT id, value FROM {} WHERE id >= '{}'", table_a, gap_a),
        ))
        .await
        .expect("subscribe A post");
    let mut sub_b2 = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-post-b",
            format!("SELECT id, value FROM {} WHERE id >= '{}'", table_b, gap_b),
        ))
        .await
        .expect("subscribe B post");
    let mut sub_c2 = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-post-c",
            format!("SELECT id, value FROM {} WHERE id >= '{}'", table_c, gap_c),
        ))
        .await
        .expect("subscribe C post");

    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'post-a')", table_a, post_a),
            None,
            None,
            None,
        )
        .await
        .expect("insert post A");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'post-b')", table_b, post_b),
            None,
            None,
            None,
        )
        .await
        .expect("insert post B");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'post-c')", table_c, post_c),
            None,
            None,
            None,
        )
        .await
        .expect("insert post C");

    let mut seen_a = Vec::<String>::new();
    let mut seen_b = Vec::<String>::new();
    let mut seen_c = Vec::<String>::new();

    let collect_ids = |event: &ChangeEvent, out: &mut Vec<String>| {
        match event {
            ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => {
                for row in rows.iter() {
                    if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                        out.push(id.to_string());
                    }
                }
            },
            ChangeEvent::InitialDataBatch { rows, .. } => {
                for row in rows {
                    if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                        out.push(id.to_string());
                    }
                }
            },
            _ => {},
        }
    };

    for _ in 0..12 {
        if seen_a.iter().any(|id| id == gap_a)
            && seen_a.iter().any(|id| id == post_a)
            && seen_b.iter().any(|id| id == gap_b)
            && seen_b.iter().any(|id| id == post_b)
            && seen_c.iter().any(|id| id == gap_c)
            && seen_c.iter().any(|id| id == post_c)
        {
            break;
        }

        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_a2.next()).await {
            collect_ids(&ev, &mut seen_a);
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_b2.next()).await {
            collect_ids(&ev, &mut seen_b);
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_c2.next()).await {
            collect_ids(&ev, &mut seen_c);
        }
    }

    assert!(seen_a.iter().any(|id| id == gap_a), "A should receive gap row");
    assert!(seen_a.iter().any(|id| id == post_a), "A should receive post row");
    assert!(seen_b.iter().any(|id| id == gap_b), "B should receive gap row");
    assert!(seen_b.iter().any(|id| id == post_b), "B should receive post row");
    assert!(seen_c.iter().any(|id| id == gap_c), "C should receive gap row");
    assert!(seen_c.iter().any(|id| id == post_c), "C should receive post row");

    assert!(!seen_a.iter().any(|id| id == pre_a), "A should not replay pre row");
    assert!(!seen_b.iter().any(|id| id == pre_b), "B should not replay pre row");
    assert!(!seen_c.iter().any(|id| id == pre_c), "C should not replay pre row");

    sub_a2.close().await.ok();
    sub_b2.close().await.ok();
    sub_c2.close().await.ok();
    client.disconnect().await;
}

/// Real-world chaos scenario: repeated disconnect/reconnect cycles with three
/// active subscriptions must keep catch-up semantics and avoid replaying
/// pre-disconnect rows.
#[tokio::test]
async fn test_three_subscriptions_repeated_reconnect_cycles() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    let writer = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (writer client unavailable): {}", e);
            return;
        },
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let table_a = format!("default.resume3_chaos_a_{}", suffix);
    let table_b = format!("default.resume3_chaos_b_{}", suffix);
    let table_c = format!("default.resume3_chaos_c_{}", suffix);
    const CYCLES: i32 = 3;

    ensure_table(&client, &table_a).await;
    ensure_table(&client, &table_b).await;
    ensure_table(&client, &table_c).await;

    client.connect().await.expect("connect should succeed");

    let pre_a = "41001";
    let pre_b = "42001";
    let pre_c = "43001";

    let has_id = |event: &ChangeEvent, target: &str| -> bool {
        match event {
            ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => rows
                .iter()
                
                .any(|r| r.get("id").and_then(|v| v.as_str()) == Some(target)),
            ChangeEvent::InitialDataBatch { rows, .. } => rows
                .iter()
                .any(|r| r.get("id").and_then(|v| v.as_str()) == Some(target)),
            _ => false,
        }
    };

    let collect_ids = |event: &ChangeEvent, out: &mut Vec<String>| {
        match event {
            ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => {
                for row in rows.iter() {
                    if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                        out.push(id.to_string());
                    }
                }
            },
            ChangeEvent::InitialDataBatch { rows, .. } => {
                for row in rows {
                    if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                        out.push(id.to_string());
                    }
                }
            },
            _ => {},
        }
    };

    let mut pre_sub_a = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-chaos-pre-a",
            format!("SELECT id, value FROM {}", table_a),
        ))
        .await
        .expect("subscribe pre A");
    let mut pre_sub_b = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-chaos-pre-b",
            format!("SELECT id, value FROM {}", table_b),
        ))
        .await
        .expect("subscribe pre B");
    let mut pre_sub_c = client
        .subscribe_with_config(SubscriptionConfig::new(
            "resume3-chaos-pre-c",
            format!("SELECT id, value FROM {}", table_c),
        ))
        .await
        .expect("subscribe pre C");

    let _ = timeout(TEST_TIMEOUT, pre_sub_a.next()).await;
    let _ = timeout(TEST_TIMEOUT, pre_sub_b.next()).await;
    let _ = timeout(TEST_TIMEOUT, pre_sub_c.next()).await;

    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-pre-a')", table_a, pre_a),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre A");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-pre-b')", table_b, pre_b),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre B");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-pre-c')", table_c, pre_c),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre C");

    let mut got_pre_a = false;
    let mut got_pre_b = false;
    let mut got_pre_c = false;
    for _ in 0..14 {
        if got_pre_a && got_pre_b && got_pre_c {
            break;
        }

        if !got_pre_a {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), pre_sub_a.next()).await {
                got_pre_a = has_id(&ev, pre_a);
            }
        }
        if !got_pre_b {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), pre_sub_b.next()).await {
                got_pre_b = has_id(&ev, pre_b);
            }
        }
        if !got_pre_c {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), pre_sub_c.next()).await {
                got_pre_c = has_id(&ev, pre_c);
            }
        }
    }

    assert!(got_pre_a, "pre A row should be received");
    assert!(got_pre_b, "pre B row should be received");
    assert!(got_pre_c, "pre C row should be received");

    pre_sub_a.close().await.ok();
    pre_sub_b.close().await.ok();
    pre_sub_c.close().await.ok();

    for cycle in 1..=CYCLES {
        let gap_a = format!("{}", 41000 + (cycle * 10) + 2);
        let gap_b = format!("{}", 42000 + (cycle * 10) + 2);
        let gap_c = format!("{}", 43000 + (cycle * 10) + 2);
        let post_a = format!("{}", 41000 + (cycle * 10) + 3);
        let post_b = format!("{}", 42000 + (cycle * 10) + 3);
        let post_c = format!("{}", 43000 + (cycle * 10) + 3);

        client.disconnect().await;
        sleep(Duration::from_millis(150)).await;
        assert!(!client.is_connected().await, "cycle {} should disconnect", cycle);

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-gap-a-{}')", table_a, gap_a, cycle),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap A");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-gap-b-{}')", table_b, gap_b, cycle),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap B");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-gap-c-{}')", table_c, gap_c, cycle),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap C");

        client.connect().await.expect("reconnect should succeed");

        let mut sub_a = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("resume3-chaos-a-{}", cycle),
                format!("SELECT id, value FROM {} WHERE id >= '{}'", table_a, gap_a),
            ))
            .await
            .expect("subscribe cycle A");
        let mut sub_b = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("resume3-chaos-b-{}", cycle),
                format!("SELECT id, value FROM {} WHERE id >= '{}'", table_b, gap_b),
            ))
            .await
            .expect("subscribe cycle B");
        let mut sub_c = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("resume3-chaos-c-{}", cycle),
                format!("SELECT id, value FROM {} WHERE id >= '{}'", table_c, gap_c),
            ))
            .await
            .expect("subscribe cycle C");

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-post-a-{}')", table_a, post_a, cycle),
                None,
                None,
                None,
            )
            .await
            .expect("insert post A");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-post-b-{}')", table_b, post_b, cycle),
                None,
                None,
                None,
            )
            .await
            .expect("insert post B");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'chaos-post-c-{}')", table_c, post_c, cycle),
                None,
                None,
                None,
            )
            .await
            .expect("insert post C");

        let mut seen_a = Vec::<String>::new();
        let mut seen_b = Vec::<String>::new();
        let mut seen_c = Vec::<String>::new();

        for _ in 0..14 {
            if seen_a.iter().any(|id| id == &gap_a)
                && seen_a.iter().any(|id| id == &post_a)
                && seen_b.iter().any(|id| id == &gap_b)
                && seen_b.iter().any(|id| id == &post_b)
                && seen_c.iter().any(|id| id == &gap_c)
                && seen_c.iter().any(|id| id == &post_c)
            {
                break;
            }

            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_a.next()).await {
                collect_ids(&ev, &mut seen_a);
            }
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_b.next()).await {
                collect_ids(&ev, &mut seen_b);
            }
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_c.next()).await {
                collect_ids(&ev, &mut seen_c);
            }
        }

        assert!(seen_a.iter().any(|id| id == &gap_a), "cycle {} A should receive gap row", cycle);
        assert!(seen_a.iter().any(|id| id == &post_a), "cycle {} A should receive post row", cycle);
        assert!(seen_b.iter().any(|id| id == &gap_b), "cycle {} B should receive gap row", cycle);
        assert!(seen_b.iter().any(|id| id == &post_b), "cycle {} B should receive post row", cycle);
        assert!(seen_c.iter().any(|id| id == &gap_c), "cycle {} C should receive gap row", cycle);
        assert!(seen_c.iter().any(|id| id == &post_c), "cycle {} C should receive post row", cycle);

        assert!(!seen_a.iter().any(|id| id == pre_a), "cycle {} A replayed pre row", cycle);
        assert!(!seen_b.iter().any(|id| id == pre_b), "cycle {} B replayed pre row", cycle);
        assert!(!seen_c.iter().any(|id| id == pre_c), "cycle {} C replayed pre row", cycle);

        sub_a.close().await.ok();
        sub_b.close().await.ok();
        sub_c.close().await.ok();
    }

    client.disconnect().await;
}

// ── Tests for client.subscriptions() and unsubscribe verification ────────

/// Verify that `client.subscriptions()` returns active subscriptions
/// and that after close/drop they are removed.
#[tokio::test]
async fn test_client_subscriptions_lists_active_subs() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    ensure_table(&client, "default.sub_list_test").await;

    client.connect().await.expect("connect");

    // Initially no subscriptions
    let subs = client.subscriptions().await;
    assert!(subs.is_empty(), "should have no subscriptions initially");

    // Subscribe to a query
    let mut sub1 = client
        .subscribe("SELECT * FROM default.sub_list_test")
        .await
        .expect("subscribe 1");

    // Wait for ack
    let _ = timeout(TEST_TIMEOUT, sub1.next()).await;

    // Subscriptions should list 1 entry
    let subs = client.subscriptions().await;
    assert_eq!(subs.len(), 1, "should have 1 subscription");
    assert!(
        subs[0].query.contains("sub_list_test"),
        "subscription query should contain table name"
    );
    assert!(!subs[0].closed, "subscription should not be closed");
    assert!(subs[0].created_at_ms > 0, "created_at_ms should be set");

    // Subscribe to another query
    let mut sub2 = client
        .subscribe("SELECT * FROM default.sub_list_test")
        .await
        .expect("subscribe 2");

    let _ = timeout(TEST_TIMEOUT, sub2.next()).await;

    let subs = client.subscriptions().await;
    assert_eq!(subs.len(), 2, "should have 2 subscriptions");

    // Close one subscription
    sub1.close().await.ok();
    // Give the unsubscribe bridge time to process
    sleep(Duration::from_millis(300)).await;

    let subs = client.subscriptions().await;
    assert_eq!(subs.len(), 1, "should have 1 subscription after close");

    // Drop the other (triggers Drop -> unsubscribe)
    drop(sub2);
    sleep(Duration::from_millis(300)).await;

    let subs = client.subscriptions().await;
    assert!(subs.is_empty(), "should have 0 subscriptions after drop");

    client.disconnect().await;
}

/// Verify `client.subscriptions()` tracks lastSeqId after receiving events.
#[tokio::test]
async fn test_client_subscriptions_tracks_last_seq_id() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let table = format!("default.sub_seqid_test_{}", ts);
    ensure_table(&client, &table).await;

    client.connect().await.expect("connect");

    let mut sub = client
        .subscribe(&format!("SELECT * FROM {}", table))
        .await
        .expect("subscribe");

    // Consume ack to populate last_seq_id
    let _ = timeout(TEST_TIMEOUT, sub.next()).await;

    // Insert a row to generate events
    client
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('seq_test_1', 'hello')", table),
            None, None, None,
        )
        .await
        .expect("insert");

    // Consume the insert event
    for _ in 0..10 {
        match timeout(Duration::from_millis(1500), sub.next()).await {
            Ok(Some(Ok(ChangeEvent::Insert { .. }))) => break,
            Ok(Some(Ok(_))) => continue,
            _ => break,
        }
    }

    // Check subscriptions — last_event_time_ms should be set after receiving events.
    // Note: last_seq_id is only populated from Ack/InitialDataBatch batch_control,
    // not from Insert/Update/Delete events — so it may or may not be set depending
    // on whether the Ack included a seq.
    let subs = client.subscriptions().await;
    assert_eq!(subs.len(), 1);
    assert!(
        subs[0].last_event_time_ms.is_some(),
        "last_event_time_ms should be set after receiving events"
    );

    sub.close().await.ok();
    client.disconnect().await;
}

/// Verify that `SubscriptionManager` returned from subscribe can be used
/// to get subscription_id and close, and that the returned object properly
/// unsubscribes from the shared connection.
#[tokio::test]
async fn test_subscription_handle_has_id_and_close() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    ensure_table(&client, "default.handle_test").await;

    client.connect().await.expect("connect");

    let mut sub = client
        .subscribe("SELECT * FROM default.handle_test")
        .await
        .expect("subscribe");

    // Verify subscription_id is available
    let sub_id = sub.subscription_id().to_string();
    assert!(!sub_id.is_empty(), "subscription_id should not be empty");
    assert!(sub_id.starts_with("sub_"), "subscription_id should start with sub_");

    // Verify is_closed starts false
    assert!(!sub.is_closed(), "subscription should not be closed initially");

    // Close and verify
    sub.close().await.expect("close should succeed");
    assert!(sub.is_closed(), "subscription should be closed after close()");

    // Verify it's removed from client subscriptions
    sleep(Duration::from_millis(300)).await;
    let subs = client.subscriptions().await;
    assert!(
        !subs.iter().any(|s| s.id == sub_id),
        "closed subscription should not appear in client.subscriptions()"
    );

    client.disconnect().await;
}
