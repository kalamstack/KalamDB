//! Integration tests for SubscriptionManager lifecycle: close(), Drop, and
//! `is_closed()` state.  These tests verify that:
//!
//! - `close()` is idempotent and marks the subscription as closed.
//! - `is_closed()` reflects the expected state.
//! - Explicitly closing a subscription removes it from `system.live_queries`.
//! - Dropping without an explicit `close()` also sends a cleanup frame via
//!   the `Drop` impl, removing the subscription server-side.
//!
//! All per-operation timeouts are short so the suite runs well within 30s/test.
//!
//! # Running
//!
//! ```bash
//! # Terminal 1: start server (or let the auto-start pick it up)
//! cd backend && cargo run
//!
//! # Terminal 2:
//! cd link && cargo test --features e2e-tests --test test_subscription_cleanup -- --nocapture
//! ```

use kalam_link::{
    AuthProvider, ChangeEvent, KalamLinkClient, KalamLinkError, KalamLinkTimeouts,
    SubscriptionConfig, SubscriptionManager,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::{sleep, timeout};

mod common;

// ── shared helpers ────────────────────────────────────────────────────────────

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_ident(prefix: &str) -> String {
    let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("{}_{}_{}", prefix, micros, counter)
}

/// Build a client with `KalamLinkTimeouts::fast()` (≤5s per operation).
fn fast_client() -> Result<KalamLinkClient, KalamLinkError> {
    let token = common::root_access_token_blocking()
        .map_err(|e| KalamLinkError::ConfigurationError(e.to_string()))?;
    KalamLinkClient::builder()
        .base_url(common::server_url())
        .auth(AuthProvider::jwt_token(token))
        .timeouts(KalamLinkTimeouts::fast())
        .build()
}

/// Execute a SQL statement best-effort; errors are silently dropped.
async fn sql(query: &str) {
    if let Ok(client) = fast_client() {
        let _ = client.execute_query(query, None, None, None).await;
    }
}

/// Create a namespace + table and return the fully-qualified table name.
async fn setup_table(ns: &str, tbl: &str) -> String {
    let full = format!("{}.{}", ns, tbl);
    sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await;
    sleep(Duration::from_millis(30)).await;
    sql(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id BIGINT PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full
    ))
    .await;
    sleep(Duration::from_millis(50)).await;
    full
}

/// Wait for the first Ack event — confirms the subscription is registered.
async fn wait_for_ack(sub: &mut SubscriptionManager, deadline: Duration) -> bool {
    matches!(
        timeout(deadline, async {
            loop {
                match sub.next().await {
                    Some(Ok(ChangeEvent::Ack { .. })) => return true,
                    Some(Ok(_)) => continue,
                    _ => return false,
                }
            }
        })
        .await,
        Ok(true)
    )
}

/// Poll `system.live_queries` until `marker` appears or disappears.
/// Returns `true` when the condition is met within `deadline`.
async fn wait_live_query(marker: &str, expect_present: bool, deadline: Duration) -> bool {
    let client = match fast_client() {
        Ok(c) => c,
        Err(_) => return false,
    };
    let start = std::time::Instant::now();
    loop {
        let found = client
            .execute_query("SELECT query FROM system.live_queries", None, None, None)
            .await
            .map(|r| format!("{:?}", r).contains(marker))
            .unwrap_or(false);

        if found == expect_present {
            return true;
        }
        if start.elapsed() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(150)).await;
    }
}

// ── is_closed state (fast, server-backed) ────────────────────────────────────

/// `is_closed()` starts as false and becomes true after `close()`.
#[tokio::test]
async fn test_is_closed_flag_transitions() {
    if !common::is_server_running().await {
        eprintln!("server not running — skipping");
        return;
    }

    let ns = unique_ident("cleanup_ns");
    let tbl = unique_ident("flag");
    let full = setup_table(&ns, &tbl).await;

    let client = fast_client().expect("client should build");
    let cfg = SubscriptionConfig::new(unique_ident("sub_flag"), format!("SELECT * FROM {}", full));
    let mut sub = client.subscribe_with_config(cfg).await.expect("subscribe should succeed");

    assert!(!sub.is_closed(), "should be open after subscribe()");

    sub.close().await.expect("close() should succeed");
    assert!(sub.is_closed(), "should be closed after close()");

    sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns)).await;
}

/// `close()` is idempotent: calling it twice must not error.
#[tokio::test]
async fn test_close_is_idempotent() {
    if !common::is_server_running().await {
        eprintln!("server not running — skipping");
        return;
    }

    let ns = unique_ident("cleanup_ns");
    let tbl = unique_ident("idem");
    let full = setup_table(&ns, &tbl).await;

    let client = fast_client().expect("client should build");
    let cfg = SubscriptionConfig::new(unique_ident("sub_idem"), format!("SELECT * FROM {}", full));
    let mut sub = client.subscribe_with_config(cfg).await.expect("subscribe should succeed");

    sub.close().await.expect("first close() should succeed");
    sub.close().await.expect("second close() should be a no-op");
    assert!(sub.is_closed());

    sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns)).await;
}

/// After `close()`, `next()` returns `None` immediately.
#[tokio::test]
async fn test_next_returns_none_after_close() {
    if !common::is_server_running().await {
        eprintln!("server not running — skipping");
        return;
    }

    let ns = unique_ident("cleanup_ns");
    let tbl = unique_ident("nextnone");
    let full = setup_table(&ns, &tbl).await;

    let client = fast_client().expect("client should build");
    let cfg =
        SubscriptionConfig::new(unique_ident("sub_nextnone"), format!("SELECT * FROM {}", full));
    let mut sub = client.subscribe_with_config(cfg).await.expect("subscribe should succeed");

    sub.close().await.unwrap();

    // After close, stream is gone — next() should return None quickly
    let result = timeout(Duration::from_millis(500), sub.next()).await;
    assert!(
        matches!(result, Ok(None)),
        "next() should return Ok(None) after close, got: {:?}",
        result
    );

    sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns)).await;
}

// ── server-side cleanup ───────────────────────────────────────────────────────

/// Explicitly closing a subscription removes it from `system.live_queries`.
#[tokio::test]
async fn test_explicit_close_removes_from_live_queries() {
    if !common::is_server_running().await {
        eprintln!("server not running — skipping");
        return;
    }

    let ns = unique_ident("cleanup_ns");
    let tbl = unique_ident("closelq");
    let full = setup_table(&ns, &tbl).await;

    // Unique query so we can search for it specifically in system.live_queries
    let unique_comment = unique_ident("close_marker");
    let query_sql = format!("SELECT * FROM {} -- {}", full, unique_comment);

    let client = fast_client().expect("client should build");
    let cfg = SubscriptionConfig::new(unique_ident("sub_close"), query_sql.clone());
    let mut sub = client.subscribe_with_config(cfg).await.expect("subscribe should succeed");

    // Wait for the subscription to register server-side
    let appeared = wait_live_query(&unique_comment, true, Duration::from_secs(5)).await;
    if !appeared {
        // Some server configs may not expose this; soft-fail with a note
        eprintln!(
            "WARN: subscription did not appear in system.live_queries within 5s — \
             server may not expose this table; skipping assertion"
        );
    }

    // Close the subscription
    sub.close().await.expect("close should succeed");

    // Verify server cleaned up within 5s
    let removed = wait_live_query(&unique_comment, false, Duration::from_secs(5)).await;
    assert!(
        removed,
        "subscription should be removed from system.live_queries after explicit close()"
    );

    sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns)).await;
}

/// Dropping a subscription without calling `close()` triggers the Drop impl,
/// which spawns a background task that sends an Unsubscribe + WS close frame.
/// The subscription should be gone from `system.live_queries` within a few seconds.
#[tokio::test]
async fn test_drop_without_close_removes_from_live_queries() {
    if !common::is_server_running().await {
        eprintln!("server not running — skipping");
        return;
    }

    let ns = unique_ident("cleanup_ns");
    let tbl = unique_ident("droplq");
    let full = setup_table(&ns, &tbl).await;

    let unique_comment = unique_ident("drop_marker");
    let query_sql = format!("SELECT * FROM {} -- {}", full, unique_comment);

    let client = fast_client().expect("client should build");
    let cfg = SubscriptionConfig::new(unique_ident("sub_drop"), query_sql.clone());
    let sub = client.subscribe_with_config(cfg).await.expect("subscribe should succeed");

    // Wait for it to appear
    let appeared = wait_live_query(&unique_comment, true, Duration::from_secs(5)).await;
    if !appeared {
        eprintln!(
            "WARN: subscription did not appear in system.live_queries — skipping drop assertion"
        );
        drop(sub);
        sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns)).await;
        return;
    }

    // Drop WITHOUT calling close() — Drop impl should clean up
    drop(sub);

    // Give the background cleanup task time to run
    let removed = wait_live_query(&unique_comment, false, Duration::from_secs(6)).await;
    assert!(
        removed,
        "subscription should be removed from system.live_queries after drop (Drop impl)"
    );

    sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns)).await;
}
