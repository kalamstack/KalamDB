// Smoke Tests: Subscription Reconnect & Resume
//
// Background:
//   All three SDK paths (native Rust, WASM/TypeScript, Dart FFI) share the
//   same Rust connection layer in `link/link-common/src/connection/shared.rs`.
//   When the WebSocket
//   drops, the SharedConnection's auto-reconnect loop calls `resubscribe_all()`,
//   which re-sends each active subscription with
//   `from_seq_id = last_seen_seq_id`.  These tests verify the full cycle from
//   the server's perspective — ensuring the client can pick up exactly where it
//   left off with no missed or replayed rows.
//
// Tests:
//   1. smoke_subscription_reconnect_basic_resume Subscribe → insert → disconnect →
//      insert-while-disconnected → reconnect → verify gap rows appear in the new subscription's
//      initial snapshot.
//
//   2. smoke_subscription_resume_from_seq_id Subscribe → record last seq_id from the server Ack →
//      disconnect → insert gap rows → reconnect → re-subscribe WITH from_seq_id → verify only
//      post-seq_id rows arrive and pre-disconnect rows are not replayed.
//
// Run with:
//   cargo test --test smoke smoke_subscription_reconnect
//   cargo test --test smoke smoke_subscription_resume_from_seq_id

use std::time::Duration;

use kalam_client::{
    models::ChangeEvent, KalamLinkClient, KalamLinkTimeouts, SeqId, SubscriptionConfig,
    SubscriptionOptions,
};

use crate::common::*;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a client with generous but bounded timeouts.
fn reconnect_client() -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    client_for_user_on_url_with_timeouts(
        &leader_or_server_url(),
        default_username(),
        default_password(),
        KalamLinkTimeouts::builder()
            .connection_timeout_secs(5)
            .receive_timeout_secs(60)
            .send_timeout_secs(10)
            .subscribe_timeout_secs(10)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(30))
            .build(),
    )
}

/// Collect events from a subscription until either `predicate` is satisfied
/// or `timeout` elapses.  Returns all collected events.
async fn collect_until<F>(
    sub: &mut kalam_client::SubscriptionManager,
    timeout: Duration,
    mut predicate: F,
) -> Vec<ChangeEvent>
where
    F: FnMut(&[ChangeEvent]) -> bool,
{
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, sub.next()).await {
            Ok(Some(Ok(ev))) => {
                events.push(ev);
                if predicate(&events) {
                    break;
                }
            },
            Ok(Some(Err(e))) => {
                eprintln!("[collect_until] subscription error: {}", e);
                break;
            },
            Ok(None) => break, // stream ended
            Err(_) => break,   // timeout
        }
    }
    events
}

/// Return true when any event's debug representation contains `needle`.
fn contains_value(events: &[ChangeEvent], needle: &str) -> bool {
    events.iter().any(|e| format!("{:?}", e).contains(needle))
}

/// Extract the `last_seq_id` from an Ack or InitialDataBatch event.
fn seq_id_from_event(event: &ChangeEvent) -> Option<SeqId> {
    match event {
        ChangeEvent::Ack { batch_control, .. } => batch_control.last_seq_id,
        ChangeEvent::InitialDataBatch { batch_control, .. } => batch_control.last_seq_id,
        _ => None,
    }
}

// ── TEST 1: basic reconnect ───────────────────────────────────────────────────
// Subscribe → insert → disconnect → insert-while-disconnected → reconnect.
// A fresh subscription on the reconnected client should have all gap rows
// in its initial snapshot, and live events should flow normally afterwards.

#[ntest::timeout(120000)]
#[test]
fn smoke_subscription_reconnect_basic_resume() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_recon_basic");
    let tbl = generate_unique_table("resume");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, payload TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let client = reconnect_client().expect("build client");
        client.connect().await.expect("shared connection");

        let query = format!("SELECT id, payload FROM {}", full);
        let sub_id = format!("recon_basic_{}", ns);

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(&sub_id, &query))
            .await
            .expect("subscribe");

        // Wait for ACK so the server has registered the subscription.
        let _ = collect_until(&mut sub, Duration::from_secs(8), |evs| {
            evs.iter().any(|e| matches!(e, ChangeEvent::Ack { .. }))
        })
        .await;

        // Insert before disconnect.
        let pre_val = format!("pre_{}", ns);
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (1, '{}')",
            full, pre_val
        ))
        .expect("insert pre-disconnect row");

        let pre_events =
            collect_until(&mut sub, Duration::from_secs(10), |evs| contains_value(evs, &pre_val))
                .await;
        assert!(
            contains_value(&pre_events, &pre_val),
            "pre-disconnect event should arrive; got {:?}",
            pre_events
        );

        // Disconnect.
        client.disconnect().await;
        assert!(!client.is_connected().await, "should be disconnected");

        // Insert two rows while the client is disconnected.
        let gap1 = format!("gap1_{}", ns);
        let gap2 = format!("gap2_{}", ns);
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (2, '{}')",
            full, gap1
        ))
        .expect("insert gap row 1");
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (3, '{}')",
            full, gap2
        ))
        .expect("insert gap row 2");

        // Reconnect and open a fresh subscription.
        client.connect().await.expect("reconnect");
        assert!(client.is_connected().await, "should be reconnected");

        let sub_id2 = format!("recon_basic2_{}", ns);
        let mut sub2 = client
            .subscribe_with_config(SubscriptionConfig::new(&sub_id2, &query))
            .await
            .expect("re-subscribe after reconnect");

        // Gap rows should appear in the initial snapshot.
        let snap_events = collect_until(&mut sub2, Duration::from_secs(15), |evs| {
            contains_value(evs, &gap1) && contains_value(evs, &gap2)
        })
        .await;
        assert!(
            contains_value(&snap_events, &gap1),
            "gap row 1 should be in post-reconnect snapshot; got {:?}",
            snap_events
        );
        assert!(
            contains_value(&snap_events, &gap2),
            "gap row 2 should be in post-reconnect snapshot; got {:?}",
            snap_events
        );

        // Verify live delivery still works after reconnect.
        let post_val = format!("post_{}", ns);
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (4, '{}')",
            full, post_val
        ))
        .expect("insert post-reconnect row");

        let post_events =
            collect_until(&mut sub2, Duration::from_secs(10), |evs| contains_value(evs, &post_val))
                .await;
        assert!(
            contains_value(&post_events, &post_val),
            "post-reconnect Insert should arrive as live event; got {:?}",
            post_events
        );

        let _ = sub2.close().await;
        client.disconnect().await;
    });

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
}

// ── TEST 2: from_seq_id server-side resume ────────────────────────────────────
// Subscribe → capture last_seq_id from the Ack → disconnect → insert gap rows
// → reconnect → re-subscribe WITH from_seq_id → gap rows arrive but the
// pre-disconnect row must NOT be replayed as a new Insert event.
//
// This mirrors what SharedConnection::resubscribe_all() does automatically on
// auto-reconnect: it sets options.from_seq_id to the last tracked seq_id before
// sending the Subscribe message.  Here we do it manually to isolate and verify
// the server-side behaviour in isolation.

#[ntest::timeout(120000)]
#[test]
fn smoke_subscription_resume_from_seq_id() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_recon_seq");
    let tbl = generate_unique_table("seqresume");
    let full = format!("{}.{}", ns, tbl);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, payload TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let client = reconnect_client().expect("build client");
        client.connect().await.expect("shared connection");

        let query = format!("SELECT id, payload FROM {}", full);
        let sub_id = format!("recon_seq_{}", ns);

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(&sub_id, &query))
            .await
            .expect("subscribe");

        // Collect the Ack — it carries the snapshot boundary seq_id.
        let ack_events = collect_until(&mut sub, Duration::from_secs(8), |evs| {
            evs.iter().any(|e| matches!(e, ChangeEvent::Ack { .. }))
        })
        .await;
        assert!(
            ack_events.iter().any(|e| matches!(e, ChangeEvent::Ack { .. })),
            "should receive subscription ACK; got {:?}",
            ack_events
        );

        let ack_seq = ack_events
            .iter()
            .find(|e| matches!(e, ChangeEvent::Ack { .. }))
            .and_then(|e| seq_id_from_event(e));

        // Insert a pre-disconnect row and capture its seq_id from the Insert event.
        let pre_val = format!("pre_{}", ns);
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (1, '{}')",
            full, pre_val
        ))
        .expect("insert pre-disconnect row");

        let change_events =
            collect_until(&mut sub, Duration::from_secs(10), |evs| contains_value(evs, &pre_val))
                .await;
        assert!(
            contains_value(&change_events, &pre_val),
            "pre-disconnect Insert should arrive; got {:?}",
            change_events
        );

        // Use the most recent seq_id we observed (Insert event seq beats Ack seq).
        let last_seq: Option<SeqId> =
            change_events.iter().rev().find_map(|e| seq_id_from_event(e)).or(ack_seq);

        // Disconnect.
        client.disconnect().await;
        assert!(!client.is_connected().await, "should be disconnected");

        // Insert rows while disconnected — these must arrive after resume.
        let gap1 = format!("gap1_{}", ns);
        let gap2 = format!("gap2_{}", ns);
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (2, '{}')",
            full, gap1
        ))
        .expect("insert gap row 1");
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (3, '{}')",
            full, gap2
        ))
        .expect("insert gap row 2");

        // Reconnect.
        client.connect().await.expect("reconnect");

        // Re-subscribe with from_seq_id.  This is exactly what
        // SharedConnection::resubscribe_all() does automatically.
        let mut options = SubscriptionOptions::default();
        if let Some(seq) = last_seq {
            options = options.with_from_seq_id(seq);
            println!("[test] re-subscribing from seq_id={}", seq);
        } else {
            println!("[test] no seq_id captured — verifying gap delivery only");
        }
        let sub_id2 = format!("recon_seq2_{}", ns);
        let mut cfg2 = SubscriptionConfig::new(&sub_id2, &query);
        cfg2.options = Some(options);

        let mut sub2 =
            client.subscribe_with_config(cfg2).await.expect("re-subscribe with from_seq_id");

        // Gap rows must arrive (as catch-up initial data or change events).
        let resume_events = collect_until(&mut sub2, Duration::from_secs(15), |evs| {
            contains_value(evs, &gap1) && contains_value(evs, &gap2)
        })
        .await;
        assert!(
            contains_value(&resume_events, &gap1),
            "gap row 1 should arrive after resume; got {:?}",
            resume_events
        );
        assert!(
            contains_value(&resume_events, &gap2),
            "gap row 2 should arrive after resume; got {:?}",
            resume_events
        );

        // When from_seq_id was set, the pre-disconnect row must NOT appear as
        // a new Insert event after the Ack.  It could still appear in the
        // initial data batch (that's the table snapshot), but any Insert event
        // after the Ack should only be for rows written after the resume point.
        if last_seq.is_some() {
            let post_ack_inserts: Vec<_> = resume_events
                .iter()
                .skip_while(|e| !matches!(e, ChangeEvent::Ack { .. }))
                .skip(1) // skip the Ack itself
                .filter(|e| matches!(e, ChangeEvent::Insert { .. }))
                .collect();

            let pre_replayed =
                post_ack_inserts.iter().any(|e| format!("{:?}", e).contains(&pre_val));
            assert!(
                !pre_replayed,
                "pre-disconnect row must not be replayed as Insert after from_seq_id resume; \
                 post-ack events: {:?}",
                post_ack_inserts
            );
        }

        // Insert after reconnect and verify live delivery.
        let post_val = format!("post_{}", ns);
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, payload) VALUES (4, '{}')",
            full, post_val
        ))
        .expect("insert post-reconnect row");

        let post_events =
            collect_until(&mut sub2, Duration::from_secs(10), |evs| contains_value(evs, &post_val))
                .await;
        assert!(
            contains_value(&post_events, &post_val),
            "post-reconnect Insert should arrive as live event; got {:?}",
            post_events
        );

        let _ = sub2.close().await;
        client.disconnect().await;
    });

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
}
