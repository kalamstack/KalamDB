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
use kalam_link::seq_tracking::{extract_max_seq, row_seq};
use kalam_link::{
    ChangeEvent, ConnectionOptions, EventHandlers, KalamCellValue, KalamLinkClient,
    KalamLinkTimeouts, LiveRowsConfig, LiveRowsEvent, SeqId, SubscriptionConfig,
    SubscriptionOptions,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

mod common;

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Create a test client authenticated with root credentials.
fn create_test_client() -> Result<KalamLinkClient, kalam_link::KalamLinkError> {
    create_test_client_for_base_url(common::server_url())
}

fn create_test_client_for_base_url(
    base_url: &str,
) -> Result<KalamLinkClient, kalam_link::KalamLinkError> {
    let token = common::root_access_token_blocking()
        .map_err(|e| kalam_link::KalamLinkError::ConfigurationError(e.to_string()))?;
    KalamLinkClient::builder()
        .base_url(base_url)
        .timeout(Duration::from_secs(30))
        .auth(AuthProvider::jwt_token(token))
        .timeouts(KalamLinkTimeouts::default())
        .connection_options(ConnectionOptions::new().with_auto_reconnect(true))
        .build()
}

fn create_test_client_with_events(
) -> Result<(KalamLinkClient, Arc<AtomicU32>, Arc<AtomicU32>), kalam_link::KalamLinkError> {
    create_test_client_with_events_for_base_url(common::server_url())
}

fn create_test_client_with_events_for_base_url(
    base_url: &str,
) -> Result<(KalamLinkClient, Arc<AtomicU32>, Arc<AtomicU32>), kalam_link::KalamLinkError> {
    let token = common::root_access_token_blocking()
        .map_err(|e| kalam_link::KalamLinkError::ConfigurationError(e.to_string()))?;

    let connect_count = Arc::new(AtomicU32::new(0));
    let disconnect_count = Arc::new(AtomicU32::new(0));
    let cc = connect_count.clone();
    let dc = disconnect_count.clone();

    let client = KalamLinkClient::builder()
        .base_url(base_url)
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
        .connection_options(ConnectionOptions::new().with_auto_reconnect(true))
        .build()?;

    Ok((client, connect_count, disconnect_count))
}

/// Ensure a test table exists with a simple schema.
async fn ensure_table(client: &KalamLinkClient, table: &str) {
    let _ = client
        .execute_query(
            &format!("CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, value TEXT)", table),
            None,
            None,
            None,
        )
        .await;
}

async fn query_max_seq(client: &KalamLinkClient, table: &str) -> SeqId {
    let result = client
        .execute_query(&format!("SELECT MAX(_seq) AS max_seq FROM {}", table), None, None, None)
        .await
        .expect("max seq query should succeed");

    let max_seq = result
        .get_i64("max_seq")
        .unwrap_or_else(|| panic!("max seq query should return a value for {}", table));

    SeqId::from_i64(max_seq)
}

fn change_event_rows(event: &ChangeEvent) -> Option<&[HashMap<String, KalamCellValue>]> {
    match event {
        ChangeEvent::Insert { rows, .. }
        | ChangeEvent::Update { rows, .. }
        | ChangeEvent::InitialDataBatch { rows, .. } => Some(rows.as_slice()),
        _ => None,
    }
}

fn row_id(row: &HashMap<String, KalamCellValue>) -> Option<&str> {
    row.get("id").and_then(|value| value.as_str())
}

fn event_last_seq(event: &ChangeEvent) -> Option<SeqId> {
    match event {
        ChangeEvent::Ack { batch_control, .. }
        | ChangeEvent::InitialDataBatch { batch_control, .. } => batch_control.last_seq_id,
        ChangeEvent::Insert { rows, .. } | ChangeEvent::Update { rows, .. } => {
            extract_max_seq(rows)
        },
        ChangeEvent::Delete { old_rows, .. } => extract_max_seq(old_rows),
        _ => None,
    }
}

fn assert_event_rows_strictly_after(event: &ChangeEvent, from: SeqId, context: &str) {
    let Some(rows) = change_event_rows(event) else {
        return;
    };

    for row in rows {
        if let Some(seq) = row_seq(row) {
            assert!(
                seq > from,
                "{}: received stale row with _seq={} at/before from={}; id={:?}; row={:?}",
                context,
                seq,
                from,
                row_id(row),
                row
            );
        }
    }
}

fn collect_ids_and_track_seq(
    event: &ChangeEvent,
    ids: &mut Vec<String>,
    max_seq: &mut Option<SeqId>,
    strict_from: Option<SeqId>,
    context: &str,
) {
    if let Some(from) = strict_from {
        assert_event_rows_strictly_after(event, from, context);
    }

    if let Some(seq) = event_last_seq(event) {
        *max_seq = Some(max_seq.map_or(seq, |prev| prev.max(seq)));
    }

    let Some(rows) = change_event_rows(event) else {
        return;
    };

    for row in rows {
        if let Some(id) = row_id(row) {
            ids.push(id.to_string());
        }
    }
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

    let mut sub = timeout(TEST_TIMEOUT, client.subscribe("SELECT * FROM default.shared_conn_test"))
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

/// Invalid subscriptions on the shared connection should fail the subscribe call
/// instead of returning a handle that later emits an error.
#[tokio::test]
async fn test_shared_subscription_failure_rejects_early() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    client.connect().await.expect("connect");

    let result = timeout(
        TEST_TIMEOUT,
        client.subscribe("SELECT * FROM nonexistent.shared_conn_missing_table"),
    )
    .await
    .expect("subscribe should not time out");

    let err = match result {
        Ok(_) => panic!("invalid subscription should fail before returning a handle"),
        Err(err) => err,
    };
    let err_text = err.to_string().to_lowercase();
    assert!(
        err_text.contains("subscription failed")
            || err_text.contains("not found")
            || err_text.contains("does not exist"),
        "unexpected error text: {}",
        err
    );

    let subscriptions = client.subscriptions().await;
    assert!(subscriptions.is_empty(), "failed subscription should not remain registered");

    client.disconnect().await;
}

/// High-level live rows subscriptions should materialize snapshots over the shared connection.
#[tokio::test]
async fn test_shared_live_rows_subscription_materializes_snapshots() {
    let client = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let table = format!("default.shared_live_rows_test_{}", suffix);
    ensure_table(&client, &table).await;
    let _ = client.execute_query(&format!("DELETE FROM {}", table), None, None, None).await;

    client.connect().await.expect("connect");

    let config = SubscriptionConfig::new(
        format!("shared-live-rows-test-{}", suffix),
        format!("SELECT * FROM {}", table),
    );
    let mut sub = client
        .live_query_rows_with_config(
            config,
            LiveRowsConfig {
                limit: Some(10),
                key_columns: None,
            },
        )
        .await
        .expect("live rows subscribe should succeed");

    let initial = timeout(TEST_TIMEOUT, sub.next())
        .await
        .expect("initial snapshot should arrive")
        .expect("subscription should stay open")
        .expect("initial snapshot should not error");
    match initial {
        LiveRowsEvent::Rows { rows, .. } => {
            assert!(rows.is_empty(), "new table should start empty")
        },
        other => panic!("unexpected initial live rows event: {:?}", other),
    }

    client
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('row-1', 'hello')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert should succeed");

    let inserted = timeout(TEST_TIMEOUT, async {
        loop {
            let event = sub
                .next()
                .await
                .expect("subscription should stay open")
                .expect("inserted snapshot should not error");

            match &event {
                LiveRowsEvent::Rows { rows, .. } if !rows.is_empty() => break event,
                _ => continue,
            }
        }
    })
    .await
    .expect("inserted snapshot should arrive");
    match inserted {
        LiveRowsEvent::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("id").and_then(|value| value.as_text()), Some("row-1"));
            assert_eq!(rows[0].get("value").and_then(|value| value.as_text()), Some("hello"));
        },
        other => panic!("unexpected inserted live rows event: {:?}", other),
    }

    sub.close().await.expect("close should succeed");
    client.disconnect().await;
}

/// Event handlers should fire on connect/disconnect.
#[tokio::test]
async fn test_connection_events() {
    let (client, connect_count, _disconnect_count) = match create_test_client_with_events() {
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

/// Without connect(), subscribe() should return an error.
#[tokio::test]
async fn test_subscribe_without_connect_returns_error() {
    let token = match common::root_access_token_blocking() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Skipping test (server not available): {}", e);
            return;
        },
    };

    // Build a client with lazy-connect disabled so that subscribe() without
    // an explicit connect() returns an error instead of auto-connecting.
    let client = KalamLinkClient::builder()
        .base_url(common::server_url())
        .timeout(Duration::from_secs(30))
        .auth(AuthProvider::jwt_token(token))
        .connection_options(ConnectionOptions::new().with_ws_lazy_connect(false))
        .build()
        .expect("build client");

    ensure_table(&client, "default.legacy_sub_test").await;

    // Don't call connect() — should fail because no shared connection and lazy-connect is off
    let result = timeout(TEST_TIMEOUT, client.subscribe("SELECT * FROM default.legacy_sub_test"))
        .await
        .expect("subscribe should not time out");

    assert!(result.is_err(), "subscribe without connect() should fail");
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

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let table_a = format!("default.resume3_a_{}", suffix);
    let table_b = format!("default.resume3_b_{}", suffix);
    let table_c = format!("default.resume3_c_{}", suffix);

    ensure_table(&client, &table_a).await;
    ensure_table(&client, &table_b).await;
    ensure_table(&client, &table_c).await;

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

    let mut got_pre_a = false;
    let mut got_pre_b = false;
    let mut got_pre_c = false;
    let mut baseline_ids_a = Vec::<String>::new();
    let mut baseline_ids_b = Vec::<String>::new();
    let mut baseline_ids_c = Vec::<String>::new();
    let mut max_seq_a = None;
    let mut max_seq_b = None;
    let mut max_seq_c = None;

    for _ in 0..14 {
        if got_pre_a && got_pre_b && got_pre_c {
            break;
        }

        if !got_pre_a {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_a.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut baseline_ids_a,
                    &mut max_seq_a,
                    None,
                    "resume3 baseline A",
                );
                if baseline_ids_a.iter().any(|id| id == pre_a) {
                    got_pre_a = true;
                }
            }
        }

        if !got_pre_b {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_b.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut baseline_ids_b,
                    &mut max_seq_b,
                    None,
                    "resume3 baseline B",
                );
                if baseline_ids_b.iter().any(|id| id == pre_b) {
                    got_pre_b = true;
                }
            }
        }

        if !got_pre_c {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_c.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut baseline_ids_c,
                    &mut max_seq_c,
                    None,
                    "resume3 baseline C",
                );
                if baseline_ids_c.iter().any(|id| id == pre_c) {
                    got_pre_c = true;
                }
            }
        }
    }

    assert!(got_pre_a, "pre A row should be received");
    assert!(got_pre_b, "pre B row should be received");
    assert!(got_pre_c, "pre C row should be received");
    let from_a = query_max_seq(&writer, &table_a).await;
    let from_b = query_max_seq(&writer, &table_b).await;
    let from_c = query_max_seq(&writer, &table_c).await;

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

    let mut config_a =
        SubscriptionConfig::new("resume3-post-a", format!("SELECT id, value FROM {}", table_a));
    config_a.options = Some(SubscriptionOptions::new().with_from(from_a));
    let mut sub_a2 = client.subscribe_with_config(config_a).await.expect("subscribe A post");
    let mut config_b =
        SubscriptionConfig::new("resume3-post-b", format!("SELECT id, value FROM {}", table_b));
    config_b.options = Some(SubscriptionOptions::new().with_from(from_b));
    let mut sub_b2 = client.subscribe_with_config(config_b).await.expect("subscribe B post");
    let mut config_c =
        SubscriptionConfig::new("resume3-post-c", format!("SELECT id, value FROM {}", table_c));
    config_c.options = Some(SubscriptionOptions::new().with_from(from_c));
    let mut sub_c2 = client.subscribe_with_config(config_c).await.expect("subscribe C post");

    let mut seen_a = Vec::<String>::new();
    let mut seen_b = Vec::<String>::new();
    let mut seen_c = Vec::<String>::new();
    let mut resumed_max_seq_a = Some(from_a);
    let mut resumed_max_seq_b = Some(from_b);
    let mut resumed_max_seq_c = Some(from_c);

    for _ in 0..12 {
        if seen_a.iter().any(|id| id == gap_a)
            && seen_b.iter().any(|id| id == gap_b)
            && seen_c.iter().any(|id| id == gap_c)
        {
            break;
        }

        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_a2.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut seen_a,
                &mut resumed_max_seq_a,
                Some(from_a),
                "resume3 resumed A",
            );
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_b2.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut seen_b,
                &mut resumed_max_seq_b,
                Some(from_b),
                "resume3 resumed B",
            );
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_c2.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut seen_c,
                &mut resumed_max_seq_c,
                Some(from_c),
                "resume3 resumed C",
            );
        }
    }

    assert!(seen_a.iter().any(|id| id == gap_a), "A should receive gap row");
    assert!(seen_b.iter().any(|id| id == gap_b), "B should receive gap row");
    assert!(seen_c.iter().any(|id| id == gap_c), "C should receive gap row");

    assert!(!seen_a.iter().any(|id| id == pre_a), "A should not replay pre row");
    assert!(!seen_b.iter().any(|id| id == pre_b), "B should not replay pre row");
    assert!(!seen_c.iter().any(|id| id == pre_c), "C should not replay pre row");

    sub_a2.close().await.ok();
    sub_b2.close().await.ok();
    sub_c2.close().await.ok();
    client.disconnect().await;
}

#[tokio::test]
async fn test_fresh_subscribe_with_from_fails_on_any_stale_seq_row() {
    let observer = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (observer client unavailable): {}", e);
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
    let table = format!("default.resume_from_fresh_{}", suffix);

    ensure_table(&observer, &table).await;

    let baseline_a = "51001";
    let baseline_b = "51002";
    let fresh_id = "51003";

    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'baseline-a')", table, baseline_a),
            None,
            None,
            None,
        )
        .await
        .expect("insert baseline A");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'baseline-b')", table, baseline_b),
            None,
            None,
            None,
        )
        .await
        .expect("insert baseline B");

    let max_seq = query_max_seq(&observer, &table).await;

    let resumed = create_test_client().expect("create resumed client");
    resumed.connect().await.expect("resumed connect");

    let mut config =
        SubscriptionConfig::new("resume-from-fresh", format!("SELECT id, value FROM {}", table));
    config.options = Some(SubscriptionOptions::new().with_from(max_seq));

    let mut resumed_sub = resumed.subscribe_with_config(config).await.expect("subscribe with from");

    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('{}', 'fresh')", table, fresh_id),
            None,
            None,
            None,
        )
        .await
        .expect("insert fresh row");

    let mut replayed_ids = Vec::<String>::new();
    let mut saw_fresh = false;
    let mut resumed_max_seq = Some(max_seq);

    for _ in 0..14 {
        match timeout(Duration::from_millis(1200), resumed_sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut replayed_ids,
                    &mut resumed_max_seq,
                    Some(max_seq),
                    "fresh subscribe(from)",
                );
                if replayed_ids.iter().any(|id| id == fresh_id) {
                    saw_fresh = true;
                }
                replayed_ids.retain(|id| id != fresh_id);

                if saw_fresh {
                    break;
                }
            },
            Ok(Some(Err(e))) => panic!("resumed subscription error: {}", e),
            Ok(None) => panic!("resumed subscription ended unexpectedly"),
            Err(_) => {},
        }
    }

    assert!(saw_fresh, "fresh row should arrive after subscribe(from)");
    assert!(
        !replayed_ids.iter().any(|id| id == baseline_a || id == baseline_b),
        "baseline rows should not be replayed after subscribe(from); saw {:?}",
        replayed_ids
    );

    resumed_sub.close().await.ok();
    resumed.disconnect().await;
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
    let mut pre_ids_a = Vec::<String>::new();
    let mut pre_ids_b = Vec::<String>::new();
    let mut pre_ids_c = Vec::<String>::new();
    let mut last_seq_a = None;
    let mut last_seq_b = None;
    let mut last_seq_c = None;
    for _ in 0..14 {
        if got_pre_a && got_pre_b && got_pre_c {
            break;
        }

        if !got_pre_a {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), pre_sub_a.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut pre_ids_a,
                    &mut last_seq_a,
                    None,
                    "chaos pre A",
                );
                got_pre_a = pre_ids_a.iter().any(|id| id == pre_a);
            }
        }
        if !got_pre_b {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), pre_sub_b.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut pre_ids_b,
                    &mut last_seq_b,
                    None,
                    "chaos pre B",
                );
                got_pre_b = pre_ids_b.iter().any(|id| id == pre_b);
            }
        }
        if !got_pre_c {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), pre_sub_c.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut pre_ids_c,
                    &mut last_seq_c,
                    None,
                    "chaos pre C",
                );
                got_pre_c = pre_ids_c.iter().any(|id| id == pre_c);
            }
        }
    }

    assert!(got_pre_a, "pre A row should be received");
    assert!(got_pre_b, "pre B row should be received");
    assert!(got_pre_c, "pre C row should be received");
    let mut last_seq_a = query_max_seq(&writer, &table_a).await;
    let mut last_seq_b = query_max_seq(&writer, &table_b).await;
    let mut last_seq_c = query_max_seq(&writer, &table_c).await;

    pre_sub_a.close().await.ok();
    pre_sub_b.close().await.ok();
    pre_sub_c.close().await.ok();

    for cycle in 1..=CYCLES {
        let gap_a = format!("{}", 41000 + (cycle * 10) + 2);
        let gap_b = format!("{}", 42000 + (cycle * 10) + 2);
        let gap_c = format!("{}", 43000 + (cycle * 10) + 2);
        let live_a = format!("{}", 41000 + (cycle * 10) + 3);
        let live_b = format!("{}", 42000 + (cycle * 10) + 3);
        let live_c = format!("{}", 43000 + (cycle * 10) + 3);
        client.disconnect().await;
        sleep(Duration::from_millis(150)).await;
        assert!(!client.is_connected().await, "cycle {} should disconnect", cycle);

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'chaos-gap-a-{}')",
                    table_a, gap_a, cycle
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap A");
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'chaos-gap-b-{}')",
                    table_b, gap_b, cycle
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap B");
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'chaos-gap-c-{}')",
                    table_c, gap_c, cycle
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap C");

        client.connect().await.expect("reconnect should succeed");

        let mut config_a = SubscriptionConfig::new(
            format!("resume3-chaos-a-{}", cycle),
            format!("SELECT id, value FROM {}", table_a),
        );
        config_a.options = Some(SubscriptionOptions::new().with_from(last_seq_a));
        let mut sub_a = client.subscribe_with_config(config_a).await.expect("subscribe cycle A");
        let mut config_b = SubscriptionConfig::new(
            format!("resume3-chaos-b-{}", cycle),
            format!("SELECT id, value FROM {}", table_b),
        );
        config_b.options = Some(SubscriptionOptions::new().with_from(last_seq_b));
        let mut sub_b = client.subscribe_with_config(config_b).await.expect("subscribe cycle B");
        let mut config_c = SubscriptionConfig::new(
            format!("resume3-chaos-c-{}", cycle),
            format!("SELECT id, value FROM {}", table_c),
        );
        config_c.options = Some(SubscriptionOptions::new().with_from(last_seq_c));
        let mut sub_c = client.subscribe_with_config(config_c).await.expect("subscribe cycle C");

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'chaos-live-a-{}')",
                    table_a, live_a, cycle
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live A");
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'chaos-live-b-{}')",
                    table_b, live_b, cycle
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live B");
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'chaos-live-c-{}')",
                    table_c, live_c, cycle
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live C");

        let mut seen_a = Vec::<String>::new();
        let mut seen_b = Vec::<String>::new();
        let mut seen_c = Vec::<String>::new();
        let mut cycle_max_seq_a = Some(last_seq_a);
        let mut cycle_max_seq_b = Some(last_seq_b);
        let mut cycle_max_seq_c = Some(last_seq_c);

        for _ in 0..14 {
            if seen_a.iter().any(|id| id == &gap_a)
                && seen_a.iter().any(|id| id == &live_a)
                && seen_b.iter().any(|id| id == &gap_b)
                && seen_b.iter().any(|id| id == &live_b)
                && seen_c.iter().any(|id| id == &gap_c)
                && seen_c.iter().any(|id| id == &live_c)
            {
                break;
            }

            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_a.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut seen_a,
                    &mut cycle_max_seq_a,
                    Some(last_seq_a),
                    &format!("chaos cycle {} A", cycle),
                );
            }
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_b.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut seen_b,
                    &mut cycle_max_seq_b,
                    Some(last_seq_b),
                    &format!("chaos cycle {} B", cycle),
                );
            }
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_c.next()).await {
                collect_ids_and_track_seq(
                    &ev,
                    &mut seen_c,
                    &mut cycle_max_seq_c,
                    Some(last_seq_c),
                    &format!("chaos cycle {} C", cycle),
                );
            }
        }

        assert!(seen_a.iter().any(|id| id == &gap_a), "cycle {} A should receive gap row", cycle);
        assert!(
            seen_a.iter().any(|id| id == &live_a),
            "cycle {} A should receive live row",
            cycle
        );
        assert!(seen_b.iter().any(|id| id == &gap_b), "cycle {} B should receive gap row", cycle);
        assert!(
            seen_b.iter().any(|id| id == &live_b),
            "cycle {} B should receive live row",
            cycle
        );
        assert!(seen_c.iter().any(|id| id == &gap_c), "cycle {} C should receive gap row", cycle);
        assert!(
            seen_c.iter().any(|id| id == &live_c),
            "cycle {} C should receive live row",
            cycle
        );

        assert!(!seen_a.iter().any(|id| id == pre_a), "cycle {} A replayed pre row", cycle);
        assert!(!seen_b.iter().any(|id| id == pre_b), "cycle {} B replayed pre row", cycle);
        assert!(!seen_c.iter().any(|id| id == pre_c), "cycle {} C replayed pre row", cycle);

        last_seq_a = query_max_seq(&writer, &table_a).await;
        last_seq_b = query_max_seq(&writer, &table_b).await;
        last_seq_c = query_max_seq(&writer, &table_c).await;

        sub_a.close().await.ok();
        sub_b.close().await.ok();
        sub_c.close().await.ok();
    }

    client.disconnect().await;
}

#[tokio::test]
async fn test_multiple_subscriptions_with_distinct_from_values_fail_fast_on_stale_rows() {
    let observer = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (observer client unavailable): {}", e);
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
    let table_a = format!("default.resume_multi_from_a_{}", suffix);
    let table_b = format!("default.resume_multi_from_b_{}", suffix);
    let table_c = format!("default.resume_multi_from_c_{}", suffix);

    ensure_table(&observer, &table_a).await;
    ensure_table(&observer, &table_b).await;
    ensure_table(&observer, &table_c).await;

    observer.connect().await.expect("observer connect");

    let baseline_a = "61001";
    let baseline_b = "62001";
    let baseline_c = "63001";
    let fresh_a = "61002";
    let fresh_b = "62002";
    let fresh_c = "63002";

    for (table, id, value) in [
        (&table_a, baseline_a, "baseline-a"),
        (&table_b, baseline_b, "baseline-b"),
        (&table_c, baseline_c, "baseline-c"),
    ] {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', '{}')", table, id, value),
                None,
                None,
                None,
            )
            .await
            .expect("insert baseline row");
    }

    let mut baseline_sub_a = observer
        .subscribe_with_config(SubscriptionConfig::new(
            "multi-from-baseline-a",
            format!("SELECT id, value FROM {}", table_a),
        ))
        .await
        .expect("subscribe baseline A");
    let mut baseline_sub_b = observer
        .subscribe_with_config(SubscriptionConfig::new(
            "multi-from-baseline-b",
            format!("SELECT id, value FROM {}", table_b),
        ))
        .await
        .expect("subscribe baseline B");
    let mut baseline_sub_c = observer
        .subscribe_with_config(SubscriptionConfig::new(
            "multi-from-baseline-c",
            format!("SELECT id, value FROM {}", table_c),
        ))
        .await
        .expect("subscribe baseline C");

    let mut ids_a = Vec::<String>::new();
    let mut ids_b = Vec::<String>::new();
    let mut ids_c = Vec::<String>::new();
    let mut from_a = None;
    let mut from_b = None;
    let mut from_c = None;

    for _ in 0..12 {
        if ids_a.iter().any(|id| id == baseline_a)
            && ids_b.iter().any(|id| id == baseline_b)
            && ids_c.iter().any(|id| id == baseline_c)
        {
            break;
        }

        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), baseline_sub_a.next()).await
        {
            collect_ids_and_track_seq(&ev, &mut ids_a, &mut from_a, None, "multi baseline A");
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), baseline_sub_b.next()).await
        {
            collect_ids_and_track_seq(&ev, &mut ids_b, &mut from_b, None, "multi baseline B");
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), baseline_sub_c.next()).await
        {
            collect_ids_and_track_seq(&ev, &mut ids_c, &mut from_c, None, "multi baseline C");
        }
    }

    assert!(ids_a.iter().any(|id| id == baseline_a));
    assert!(ids_b.iter().any(|id| id == baseline_b));
    assert!(ids_c.iter().any(|id| id == baseline_c));
    let from_a = from_a.expect("baseline A max seq should be captured");
    let from_b = from_b.expect("baseline B max seq should be captured");
    let from_c = from_c.expect("baseline C max seq should be captured");

    baseline_sub_a.close().await.ok();
    baseline_sub_b.close().await.ok();
    baseline_sub_c.close().await.ok();
    observer.disconnect().await;
    sleep(Duration::from_millis(150)).await;

    for (table, id, value) in [
        (&table_a, fresh_a, "fresh-a"),
        (&table_b, fresh_b, "fresh-b"),
        (&table_c, fresh_c, "fresh-c"),
    ] {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', '{}')", table, id, value),
                None,
                None,
                None,
            )
            .await
            .expect("insert fresh row");
    }

    let resumed = create_test_client().expect("create resumed client");
    resumed.connect().await.expect("resumed connect");

    let mut config_a = SubscriptionConfig::new(
        "multi-from-resumed-a",
        format!("SELECT id, value FROM {}", table_a),
    );
    config_a.options = Some(SubscriptionOptions::new().with_from(from_a));
    let mut config_b = SubscriptionConfig::new(
        "multi-from-resumed-b",
        format!("SELECT id, value FROM {}", table_b),
    );
    config_b.options = Some(SubscriptionOptions::new().with_from(from_b));
    let mut config_c = SubscriptionConfig::new(
        "multi-from-resumed-c",
        format!("SELECT id, value FROM {}", table_c),
    );
    config_c.options = Some(SubscriptionOptions::new().with_from(from_c));

    let mut resumed_a = resumed.subscribe_with_config(config_a).await.expect("subscribe resumed A");
    let mut resumed_b = resumed.subscribe_with_config(config_b).await.expect("subscribe resumed B");
    let mut resumed_c = resumed.subscribe_with_config(config_c).await.expect("subscribe resumed C");

    let mut resumed_ids_a = Vec::<String>::new();
    let mut resumed_ids_b = Vec::<String>::new();
    let mut resumed_ids_c = Vec::<String>::new();
    let mut resumed_seq_a = Some(from_a);
    let mut resumed_seq_b = Some(from_b);
    let mut resumed_seq_c = Some(from_c);

    for _ in 0..14 {
        if resumed_ids_a.iter().any(|id| id == fresh_a)
            && resumed_ids_b.iter().any(|id| id == fresh_b)
            && resumed_ids_c.iter().any(|id| id == fresh_c)
        {
            break;
        }

        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), resumed_a.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut resumed_ids_a,
                &mut resumed_seq_a,
                Some(from_a),
                "multi resumed A",
            );
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), resumed_b.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut resumed_ids_b,
                &mut resumed_seq_b,
                Some(from_b),
                "multi resumed B",
            );
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), resumed_c.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut resumed_ids_c,
                &mut resumed_seq_c,
                Some(from_c),
                "multi resumed C",
            );
        }
    }

    assert!(resumed_ids_a.iter().any(|id| id == fresh_a), "A should receive fresh row");
    assert!(resumed_ids_b.iter().any(|id| id == fresh_b), "B should receive fresh row");
    assert!(resumed_ids_c.iter().any(|id| id == fresh_c), "C should receive fresh row");
    assert!(
        !resumed_ids_a.iter().any(|id| id == baseline_a)
            && !resumed_ids_b.iter().any(|id| id == baseline_b)
            && !resumed_ids_c.iter().any(|id| id == baseline_c),
        "baseline rows must not replay after subscribe(from)"
    );

    resumed_a.close().await.ok();
    resumed_b.close().await.ok();
    resumed_c.close().await.ok();
    resumed.disconnect().await;
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

    let mut sub = client.subscribe(&format!("SELECT * FROM {}", table)).await.expect("subscribe");

    // Consume ack to populate last_seq_id
    let _ = timeout(TEST_TIMEOUT, sub.next()).await;

    // Insert a row to generate events
    client
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('seq_test_1', 'hello')", table),
            None,
            None,
            None,
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

    let mut sub = client.subscribe("SELECT * FROM default.handle_test").await.expect("subscribe");

    // Verify subscription_id is available
    let sub_id = sub.subscription_id().to_string();
    assert!(!sub_id.is_empty(), "subscription_id should not be empty");
    assert!(sub_id.starts_with("sub_"), "subscription_id should start with sub_");

    // Verify is_closed starts false
    assert!(!sub.is_closed(), "subscription should not be closed initially");

    // Close and verify
    sub.close().await.expect("close should succeed");
    assert!(sub.is_closed(), "subscription should be closed after close()");

    // Verify it's removed from active subscriptions (may appear as closed
    // in seq_id_cache).
    sleep(Duration::from_millis(300)).await;
    let subs = client.subscriptions().await;
    assert!(
        !subs.iter().any(|s| s.id == sub_id && !s.closed),
        "closed subscription should not appear as active in client.subscriptions()"
    );

    client.disconnect().await;
}

/// After closing a subscription and re-subscribing with the same ID, the
/// new subscription should resume from the last received seq_id (not from
/// the beginning). This tests the `seq_id_cache` mechanism that preserves
/// `last_seq_id` across close/resubscribe cycles.
#[tokio::test]
async fn test_close_resubscribe_resumes_from_last_seq_id() {
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
    let table = format!("default.resume_close_resub_{}", suffix);
    ensure_table(&client, &table).await;

    // Insert baseline row before subscribing.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('pre-1', 'baseline')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert baseline row");

    // ── First subscription: observe baseline + one live insert ──────────
    client.connect().await.expect("connect");

    let sub_id = "resume-close-test";
    let config = SubscriptionConfig::new(sub_id, format!("SELECT id, value FROM {}", table));
    let mut sub = client.subscribe_with_config(config).await.expect("first subscribe");

    // Drain until we see the baseline row in initial data.
    let mut first_ids = Vec::<String>::new();
    let mut first_max_seq: Option<SeqId> = None;
    for _ in 0..6 {
        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut first_ids,
                    &mut first_max_seq,
                    None,
                    "close-resub first",
                );
                if first_ids.iter().any(|id| id == "pre-1") {
                    break;
                }
            },
            _ => {},
        }
    }
    assert!(first_ids.iter().any(|id| id == "pre-1"), "should observe baseline row");
    assert!(first_max_seq.is_some(), "should have received a seq_id");

    // Insert a second row while subscribed.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('live-1', 'during-first')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert live row");

    // Drain until we see the live insert.
    for _ in 0..8 {
        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut first_ids,
                    &mut first_max_seq,
                    None,
                    "close-resub live",
                );
                if first_ids.iter().any(|id| id == "live-1") {
                    break;
                }
            },
            _ => {},
        }
    }
    assert!(first_ids.iter().any(|id| id == "live-1"), "should observe live-1");
    let latest_seq = first_max_seq.unwrap();

    // ── Close first subscription ────────────────────────────────────────
    sub.close().await.expect("close first sub");
    sleep(Duration::from_millis(200)).await;

    // The closed subscription's last_seq_id should be available via the
    // seq_id_cache (shown in subscriptions() as a closed entry).
    let subs_snapshot = client.subscriptions().await;
    let cached = subs_snapshot.iter().find(|s| s.id == sub_id);
    assert!(
        cached.is_some(),
        "seq_id_cache should still expose the closed subscription's seq_id"
    );
    assert!(cached.unwrap().last_seq_id.is_some(), "cached last_seq_id should be set");
    assert!(cached.unwrap().closed, "entry should be marked as closed");

    // Insert a row while disconnected from the subscription.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('gap-1', 'between-subs')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert gap row");

    // ── Re-subscribe with the same ID (no explicit `from`) ──────────────
    // The shared connection should inherit the last_seq_id from the cache
    // and resume from there, NOT from the beginning.
    let config2 = SubscriptionConfig::new(sub_id, format!("SELECT id, value FROM {}", table));
    let mut sub2 = client.subscribe_with_config(config2).await.expect("second subscribe");

    let mut second_ids = Vec::<String>::new();
    let mut second_max_seq: Option<SeqId> = Some(latest_seq);

    for _ in 0..12 {
        match timeout(Duration::from_millis(2000), sub2.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut second_ids,
                    &mut second_max_seq,
                    Some(latest_seq),
                    "close-resub second",
                );
                if second_ids.iter().any(|id| id == "gap-1") {
                    break;
                }
            },
            Ok(Some(Err(e))) => panic!("second sub error: {}", e),
            Ok(None) => panic!("second sub ended unexpectedly"),
            Err(_) => {},
        }
    }

    // The gap row should be received (it was inserted after the last_seq_id
    // from the first subscription).
    assert!(
        second_ids.iter().any(|id| id == "gap-1"),
        "gap row should be received on re-subscribe"
    );

    // Baseline and first live row should NOT be replayed.
    assert!(
        !second_ids.iter().any(|id| id == "pre-1"),
        "baseline row should NOT be replayed on re-subscribe; saw {:?}",
        second_ids
    );
    assert!(
        !second_ids.iter().any(|id| id == "live-1"),
        "live-1 should NOT be replayed on re-subscribe; saw {:?}",
        second_ids
    );

    sub2.close().await.ok();
    client.disconnect().await;
}

/// Verifies the seq_id_cache also works when the original subscription was
/// opened with an explicit `from` value. On re-subscribe, the new
/// subscription should use `max(original_from, cached_last_seq_id)`.
#[tokio::test]
async fn test_close_resubscribe_with_explicit_from_uses_max() {
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
    let table = format!("default.resume_from_max_{}", suffix);
    ensure_table(&client, &table).await;

    // Insert two baseline rows.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('base-a', 'a')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert base-a");
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('base-b', 'b')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert base-b");

    let from_seq = query_max_seq(&writer, &table).await;

    // Insert another row AFTER the from_seq checkpoint.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('after-from', 'c')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert after-from");

    // ── First subscription with explicit from ───────────────────────────
    client.connect().await.expect("connect");

    let sub_id = "resume-from-max-test";
    let mut config = SubscriptionConfig::new(sub_id, format!("SELECT id, value FROM {}", table));
    config.options = Some(SubscriptionOptions::new().with_from(from_seq));

    let mut sub = client.subscribe_with_config(config).await.expect("first subscribe with from");

    let mut first_ids = Vec::<String>::new();
    let mut first_max_seq: Option<SeqId> = Some(from_seq);

    for _ in 0..10 {
        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut first_ids,
                    &mut first_max_seq,
                    Some(from_seq),
                    "from-max first",
                );
                if first_ids.iter().any(|id| id == "after-from") {
                    break;
                }
            },
            _ => {},
        }
    }

    assert!(first_ids.iter().any(|id| id == "after-from"), "should observe after-from row");
    assert!(
        !first_ids.iter().any(|id| id == "base-a" || id == "base-b"),
        "baseline rows should not appear with from filter"
    );

    let cached_seq = first_max_seq.unwrap();
    assert!(cached_seq > from_seq, "cached_seq should be > from_seq");

    // ── Close and re-subscribe WITHOUT explicit from ────────────────────
    sub.close().await.expect("close");
    sleep(Duration::from_millis(200)).await;

    // Insert a gap row.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('gap-x', 'gap')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert gap-x");

    // Re-subscribe with same ID but no explicit from — should resume from
    // cached_seq (the higher value), not from_seq (the original explicit).
    let config2 = SubscriptionConfig::new(sub_id, format!("SELECT id, value FROM {}", table));
    let mut sub2 = client.subscribe_with_config(config2).await.expect("second subscribe (no from)");

    let mut second_ids = Vec::<String>::new();
    let mut second_max_seq: Option<SeqId> = Some(cached_seq);

    for _ in 0..10 {
        match timeout(Duration::from_millis(2000), sub2.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut second_ids,
                    &mut second_max_seq,
                    Some(cached_seq),
                    "from-max second",
                );
                if second_ids.iter().any(|id| id == "gap-x") {
                    break;
                }
            },
            _ => {},
        }
    }

    assert!(second_ids.iter().any(|id| id == "gap-x"), "gap row should be received");
    assert!(
        !second_ids.iter().any(|id| id == "after-from"),
        "after-from should NOT be replayed (was seen in first subscription)"
    );
    assert!(
        !second_ids.iter().any(|id| id == "base-a" || id == "base-b"),
        "baseline rows should NOT appear"
    );

    sub2.close().await.ok();
    client.disconnect().await;
}

/// Verify that disconnect() + connect() + re-subscribe with explicit from
/// resumes correctly.
#[tokio::test]
async fn test_disconnect_reconnect_resubscribe_resumes_seq_id() {
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
    let table = format!("default.disc_reconn_resub_{}", suffix);
    ensure_table(&client, &table).await;

    // Insert baseline row.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('row-1', 'first')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert row-1");

    client.connect().await.expect("connect");

    let sub_id = "disc-reconn-test";
    let config = SubscriptionConfig::new(sub_id, format!("SELECT id, value FROM {}", table));
    let mut sub = client.subscribe_with_config(config).await.expect("first subscribe");

    let mut seen_ids = Vec::<String>::new();
    let mut max_seq: Option<SeqId> = None;

    for _ in 0..8 {
        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut seen_ids,
                    &mut max_seq,
                    None,
                    "disc-reconn first",
                );
                if seen_ids.iter().any(|id| id == "row-1") {
                    break;
                }
            },
            _ => {},
        }
    }
    assert!(seen_ids.iter().any(|id| id == "row-1"), "should see row-1");
    let last_seq = max_seq.unwrap();

    // Close subscription and disconnect (simulates what happens when a
    // Flutter app detects disconnect and tears down).
    sub.close().await.expect("close sub");
    client.disconnect().await;
    sleep(Duration::from_millis(300)).await;

    // Insert rows while disconnected.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('row-2', 'while-away')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert row-2");

    // Reconnect and re-subscribe with the same ID + explicit from.
    client.connect().await.expect("reconnect");

    let mut config2 = SubscriptionConfig::new(sub_id, format!("SELECT id, value FROM {}", table));
    config2.options = Some(SubscriptionOptions::new().with_from(last_seq));

    let mut sub2 = client.subscribe_with_config(config2).await.expect("re-subscribe with from");

    let mut second_ids = Vec::<String>::new();
    let mut second_max_seq: Option<SeqId> = Some(last_seq);

    for _ in 0..10 {
        match timeout(Duration::from_millis(2000), sub2.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut second_ids,
                    &mut second_max_seq,
                    Some(last_seq),
                    "disc-reconn second",
                );
                if second_ids.iter().any(|id| id == "row-2") {
                    break;
                }
            },
            _ => {},
        }
    }

    assert!(
        second_ids.iter().any(|id| id == "row-2"),
        "gap row should be received after reconnect"
    );
    assert!(!second_ids.iter().any(|id| id == "row-1"), "old row should NOT be replayed");

    sub2.close().await.ok();
    client.disconnect().await;
}
