use std::{sync::atomic::Ordering, time::Duration};

use kalam_client::SubscriptionConfig;
use tokio::time::timeout;

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

/// After a complex outage+recovery sequence, verify that connect and disconnect
/// event counters are consistent: each reconnect fires exactly one on_connect,
/// each forcible drop fires exactly one on_disconnect, and the final state after
/// shutdown is deterministic.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_event_counter_integrity_through_multiple_outages() {
    let result = timeout(Duration::from_secs(60), async {
        let writer = match create_test_client() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Skipping test (writer client unavailable): {}", e);
                return;
            },
        };

        let proxy = TcpDisconnectProxy::start(upstream_server_url()).await;
        let (client, connect_count, disconnect_count) =
            match create_test_client_with_events_for_base_url(proxy.base_url()) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Skipping test (proxy client unavailable): {}", e);
                    proxy.shutdown().await;
                    return;
                },
            };

        let suffix = unique_suffix();
        let table = format!("default.event_counter_{}", suffix);
        ensure_table(&writer, &table).await;

        // ── Phase 1: Initial connect ────────────────────────────────────
        client.connect().await.expect("initial connect");

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("event-counter-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        wait_for_reconnect(&client, &connect_count, 1, "event counter initial connect").await;

        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            1,
            "exactly one connect event should fire after initial connect"
        );
        assert_eq!(
            disconnect_count.load(Ordering::SeqCst),
            0,
            "no disconnect events should fire before any outage"
        );

        // ── Phase 2: First outage + recovery ────────────────────────────
        let expected_disconnects = disconnect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_down().await;
        wait_for_disconnect_count(
            &disconnect_count,
            expected_disconnects,
            "event counter first outage",
        )
        .await;
        assert_eq!(
            disconnect_count.load(Ordering::SeqCst),
            1,
            "exactly one disconnect event after first outage"
        );

        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_up();
        wait_for_reconnect(
            &client,
            &connect_count,
            expected_connects,
            "event counter first recovery",
        )
        .await;
        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            2,
            "exactly two connect events after first recovery"
        );

        // ── Phase 3: Second outage + recovery ───────────────────────────
        let expected_disconnects = disconnect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_down().await;
        wait_for_disconnect_count(
            &disconnect_count,
            expected_disconnects,
            "event counter second outage",
        )
        .await;
        assert_eq!(
            disconnect_count.load(Ordering::SeqCst),
            2,
            "exactly two disconnect events after second outage"
        );

        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_up();
        wait_for_reconnect(
            &client,
            &connect_count,
            expected_connects,
            "event counter second recovery",
        )
        .await;
        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            3,
            "exactly three connect events after second recovery"
        );

        // ── Phase 4: Third outage + recovery (verify no drift) ──────────
        let expected_disconnects = disconnect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_down().await;
        wait_for_disconnect_count(
            &disconnect_count,
            expected_disconnects,
            "event counter third outage",
        )
        .await;
        assert_eq!(
            disconnect_count.load(Ordering::SeqCst),
            3,
            "exactly three disconnect events after third outage"
        );

        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_up();
        wait_for_reconnect(
            &client,
            &connect_count,
            expected_connects,
            "event counter third recovery",
        )
        .await;
        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            4,
            "exactly four connect events after third recovery"
        );

        // Final state: all counters balanced (connects = disconnects + 1 for
        // the still-active connection).
        let final_connects = connect_count.load(Ordering::SeqCst);
        let final_disconnects = disconnect_count.load(Ordering::SeqCst);
        assert_eq!(
            final_connects,
            final_disconnects + 1,
            "connects ({}) should equal disconnects ({}) + 1 (the active connection)",
            final_connects,
            final_disconnects
        );

        // Verify that live data still flows.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('counter-verify', 'v')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert counter-verify row");

        let mut ids = Vec::<String>::new();
        let mut seq = None;
        for _ in 0..12 {
            if ids.iter().any(|id| id == "counter-verify") {
                break;
            }
            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(&ev, &mut ids, &mut seq, None, "counter verify live");
                },
                _ => {},
            }
        }
        assert!(
            ids.iter().any(|id| id == "counter-verify"),
            "live data should still flow after all the outage cycles"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "event counter integrity test timed out");
}
