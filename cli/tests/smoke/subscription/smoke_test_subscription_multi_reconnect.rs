// Smoke Tests: Multi-Reconnect with Parallel Subscriptions
//
// Validates that clients with multiple active subscriptions (mimicking a
// React app rendering live queries on two tables) continue receiving
// live changes across many rapid connect/disconnect cycles.
//
// Scenario mirrors the chat-with-ai App.tsx pattern:
//   - Subscribe to TWO queries simultaneously (messages + events)
//   - Repeat N disconnect/reconnect cycles
//   - After every reconnect, insert a row and verify live delivery on BOTH subs
//
// Run with:
//   cargo test --test smoke smoke_subscription_multi_reconnect

use crate::common::*;
use kalam_link::{models::ChangeEvent, KalamLinkClient, KalamLinkTimeouts, SubscriptionConfig};
use std::time::Duration;

// ── helpers ──────────────────────────────────────────────────────────────────

fn build_reconnect_client() -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
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

async fn collect_until<F>(
    sub: &mut kalam_link::SubscriptionManager,
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
            Ok(None) => break,
            Err(_) => break,
        }
    }
    events
}

fn contains_value(events: &[ChangeEvent], needle: &str) -> bool {
    events.iter().any(|e| format!("{:?}", e).contains(needle))
}

// ── TEST ──────────────────────────────────────────────────────────────────────
// Simulates a browser that refreshes multiple times.  Each cycle:
//   1. Connect + subscribe to TWO queries
//   2. Insert a row into each table
//   3. Assert live events arrive on BOTH subscriptions
//   4. Disconnect (simulates tab close / page refresh)
//
// After N cycles the test must have received live events every single time.

const RECONNECT_CYCLES: usize = 5;

#[ntest::timeout(180000)]
#[test]
fn smoke_subscription_multi_reconnect_parallel() {
    if !require_server_running() {
        return;
    }

    let ns = generate_unique_namespace("sub_multi_recon");
    let tbl_a = generate_unique_table("msgs");
    let tbl_b = generate_unique_table("evts");
    let full_a = format!("{}.{}", ns, tbl_a);
    let full_b = format!("{}.{}", ns, tbl_b);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, payload TEXT) WITH (TYPE='USER')",
        full_a
    ))
    .expect("create table A");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, payload TEXT) WITH (TYPE='USER')",
        full_b
    ))
    .expect("create table B");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let mut row_id = 1;

        for cycle in 0..RECONNECT_CYCLES {
            eprintln!("[cycle {}/{}] connecting...", cycle + 1, RECONNECT_CYCLES);

            let client = build_reconnect_client().expect("build client");
            client.connect().await.expect("connect");

            let query_a = format!("SELECT id, payload FROM {}", full_a);
            let query_b = format!("SELECT id, payload FROM {}", full_b);
            let sub_id_a = format!("multi_a_{}_{}", ns, cycle);
            let sub_id_b = format!("multi_b_{}_{}", ns, cycle);

            // Subscribe to BOTH queries (mirrors App.tsx with messages + agent_events).
            let mut sub_a = client
                .subscribe_with_config(SubscriptionConfig::new(&sub_id_a, &query_a))
                .await
                .expect("subscribe A");
            let mut sub_b = client
                .subscribe_with_config(SubscriptionConfig::new(&sub_id_b, &query_b))
                .await
                .expect("subscribe B");

            // Wait for both ACKs so the server has registered the subscriptions.
            let _ = collect_until(&mut sub_a, Duration::from_secs(10), |evs| {
                evs.iter().any(|e| matches!(e, ChangeEvent::Ack { .. }))
            })
            .await;
            let _ = collect_until(&mut sub_b, Duration::from_secs(10), |evs| {
                evs.iter().any(|e| matches!(e, ChangeEvent::Ack { .. }))
            })
            .await;

            // Drain any initial data batches.
            let _ = collect_until(&mut sub_a, Duration::from_millis(500), |_| false).await;
            let _ = collect_until(&mut sub_b, Duration::from_millis(500), |_| false).await;

            // Insert a row into EACH table.
            let val_a = format!("cycleA_{}_{}", cycle, ns);
            let val_b = format!("cycleB_{}_{}", cycle, ns);
            execute_sql_as_root_via_client(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, '{}')",
                full_a, row_id, val_a
            ))
            .expect("insert into A");
            execute_sql_as_root_via_client(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, '{}')",
                full_b, row_id, val_b
            ))
            .expect("insert into B");

            // Verify live events arrive on BOTH subscriptions.
            let events_a = collect_until(&mut sub_a, Duration::from_secs(15), |evs| {
                contains_value(evs, &val_a)
            })
            .await;
            assert!(
                contains_value(&events_a, &val_a),
                "[cycle {}] table A live event missing for '{}'; got {:?}",
                cycle + 1,
                val_a,
                events_a
            );

            let events_b = collect_until(&mut sub_b, Duration::from_secs(15), |evs| {
                contains_value(evs, &val_b)
            })
            .await;
            assert!(
                contains_value(&events_b, &val_b),
                "[cycle {}] table B live event missing for '{}'; got {:?}",
                cycle + 1,
                val_b,
                events_b
            );

            eprintln!(
                "[cycle {}/{}] both live events received — disconnecting",
                cycle + 1,
                RECONNECT_CYCLES
            );

            // Close subscriptions then disconnect (simulates page close).
            let _ = sub_a.close().await;
            let _ = sub_b.close().await;
            client.disconnect().await;

            row_id += 1;

            // Brief pause between cycles to let the server clean up.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        eprintln!(
            "All {} reconnect cycles passed — subscriptions delivered live events every time",
            RECONNECT_CYCLES
        );
    });

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
}
