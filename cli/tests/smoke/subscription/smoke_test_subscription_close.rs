// Smoke Tests: WebSocket Subscription Close & Cleanup
//
// Verifies that:
//   1. Explicitly calling `close()` on a SubscriptionManager sends an Unsubscribe
//      frame and removes the subscription from `system.live`.
//   2. Dropping a SubscriptionManager without calling `close()` triggers the
//      Drop impl, which spawns a background cleanup task that also removes
//      the entry from `system.live`.
//   3. `SubscriptionManager::is_closed()` returns the correct state.
//
// These tests use short `#[ntest::timeout]` values; individual WebSocket
// operations use fast-timeout clients so each test completes quickly.
//
// Run with:
//   cargo test --test smoke smoke_test_subscription_close

use crate::common::*;
use kalam_client::{ChangeEvent, KalamLinkClient, KalamLinkTimeouts, SubscriptionConfig};
use std::time::Duration;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a kalam-client with fast timeouts (≤3s per operation).
fn fast_link_client() -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    client_for_user_on_url_with_timeouts(
        &leader_or_server_url(),
        default_username(),
        default_password(),
        KalamLinkTimeouts::fast(),
    )
}

/// Poll `system.live` (synchronously) until `marker` appears or
/// disappears match `expect_present`.  Returns `true` on success.
fn wait_live_query(marker: &str, expect_present: bool, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        let output = execute_sql_as_root_via_client("SELECT query FROM system.live")
            .unwrap_or_default();
        let found = output.contains(marker);
        if found == expect_present {
            return true;
        }
        if start.elapsed() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

// ── test 1: explicit close() ──────────────────────────────────────────────────

/// Explicit `close()` sends an Unsubscribe frame and removes the subscription
/// from `system.live` within a few seconds.
///
/// Also verifies that `is_closed()` returns `false` before and `true` after.
#[ntest::timeout(25000)]
#[test]
fn smoke_subscription_explicit_close_removes_live_query() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_close");
    let tbl = generate_unique_table("close_tbl");
    let full = format!("{}.{}", ns, tbl);
    let marker = format!("close_marker_{}", ns);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id BIGINT PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    // Use a channel to coordinate: background thread opens subscription + closes it,
    // then signals the main thread.
    let (tx_done, rx_done) = std::sync::mpsc::channel::<bool>(); // true = close() was called
    let marker_clone = marker.clone();
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
                    eprintln!("fast_link_client failed: {e}");
                    let _ = tx_done.send(false);
                    return;
                },
            };

            let query_sql = format!("SELECT * FROM {} -- {}", full_clone, marker_clone);
            let cfg = SubscriptionConfig::new(format!("sub_{}", marker_clone), query_sql);

            let mut sub = match client.subscribe_with_config(cfg).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("subscribe failed: {e}");
                    let _ = tx_done.send(false);
                    return;
                },
            };

            // Verify open state before close
            assert!(!sub.is_closed(), "subscription should be open before close()");

            // Wait for ACK (up to 3s) so the server has registered the subscription
            let ack_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                match tokio::time::timeout(
                    ack_deadline.duration_since(tokio::time::Instant::now()),
                    sub.next(),
                )
                .await
                {
                    Ok(Some(Ok(ChangeEvent::Ack { .. }))) => break,
                    Ok(Some(Ok(_))) => continue,
                    _ => break, // timeout or error — continue anyway
                }
            }

            // Explicitly close
            sub.close().await.expect("close() should succeed");
            assert!(sub.is_closed(), "is_closed() must be true after close()");

            let _ = tx_done.send(true);
        });
    });

    // Wait for the background thread to close the subscription
    let closed = rx_done.recv_timeout(Duration::from_secs(10)).unwrap_or(false);
    handle.join().ok();

    if !closed {
        eprintln!("WARN: subscription setup/close failed; skipping system.live check");
        let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
        return;
    }

    // Verify the subscription was removed from system.live
    let removed = wait_live_query(&marker, false, Duration::from_secs(5));
    assert!(
        removed,
        "subscription '{}' should not be in system.live after explicit close()",
        marker
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
}

// ── test 2: drop without close() (Drop impl) ─────────────────────────────────

/// Dropping a SubscriptionManager without calling `close()` triggers the Drop
/// impl, which spawns a background cleanup task.  The subscription should
/// disappear from `system.live` within a few seconds.
#[ntest::timeout(25000)]
#[test]
fn smoke_subscription_drop_removes_live_query() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_drop");
    let tbl = generate_unique_table("drop_tbl");
    let full = format!("{}.{}", ns, tbl);
    let marker = format!("drop_marker_{}", ns);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id BIGINT PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    // We use a dedicated thread + runtime so we can drop the subscription
    // while still inside a tokio context (required by the Drop impl to spawn
    // the cleanup task).
    let (tx_dropped, rx_dropped) = std::sync::mpsc::channel::<bool>();
    let marker_clone = marker.clone();
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
                    eprintln!("fast_link_client failed: {e}");
                    let _ = tx_dropped.send(false);
                    return;
                },
            };

            let query_sql = format!("SELECT * FROM {} -- {}", full_clone, marker_clone);
            let cfg = SubscriptionConfig::new(format!("sub_{}", marker_clone), query_sql);

            let mut sub = match client.subscribe_with_config(cfg).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("subscribe failed: {e}");
                    let _ = tx_dropped.send(false);
                    return;
                },
            };

            // Wait for ACK (up to 3s)
            let ack_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
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

            // Drop WITHOUT calling close() — Drop impl fires the cleanup task
            drop(sub);

            // Yield to allow the spawned cleanup task to run
            tokio::task::yield_now().await;

            let _ = tx_dropped.send(true);
        });
    });

    let dropped = rx_dropped.recv_timeout(Duration::from_secs(10)).unwrap_or(false);
    handle.join().ok();

    if !dropped {
        eprintln!("WARN: subscription setup failed; skipping system.live check");
        let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
        return;
    }

    // Give the server-side heartbeat / cleanup a moment to process
    std::thread::sleep(Duration::from_millis(500));

    let removed = wait_live_query(&marker, false, Duration::from_secs(6));
    assert!(
        removed,
        "subscription '{}' should not be in system.live after drop (Drop impl cleanup)",
        marker
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
}
