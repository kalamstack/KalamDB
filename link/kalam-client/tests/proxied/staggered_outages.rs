use std::{sync::atomic::Ordering, time::Duration};

use kalam_client::SubscriptionConfig;
use tokio::time::{sleep, timeout};

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

/// Repeated outages with different downtime windows should preserve
/// forward-only resume semantics on a single shared subscription.
#[tokio::test]
async fn test_shared_connection_recovers_across_staggered_outages() {
    let result = timeout(Duration::from_secs(45), async {
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
        let table = format!("default.staggered_outages_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("initial connect through proxy");

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("staggered-outages-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe through proxy");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('baseline', 'ready')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert baseline row");

        let mut baseline_ids = Vec::<String>::new();
        let mut checkpoint = None;
        for _ in 0..12 {
            if baseline_ids.iter().any(|id| id == "baseline") {
                break;
            }

            match timeout(Duration::from_millis(1200), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut baseline_ids,
                        &mut checkpoint,
                        None,
                        "staggered baseline",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription error before outages: {}", e),
                Ok(None) => panic!("subscription ended unexpectedly before outages"),
                Err(_) => {},
            }
        }

        assert!(
            baseline_ids.iter().any(|id| id == "baseline"),
            "baseline row should be observed before outage cycles"
        );

        let scenarios = [
            ("fast", Duration::from_millis(0), Duration::from_millis(150)),
            ("medium", Duration::from_millis(200), Duration::from_millis(700)),
            ("slow", Duration::from_millis(900), Duration::from_millis(1500)),
        ];

        for (index, (label, pre_drop_delay, outage_duration)) in scenarios.iter().enumerate() {
            if !pre_drop_delay.is_zero() {
                sleep(*pre_drop_delay).await;
            }

            let cycle_from = checkpoint.expect("checkpoint should be known before outage");
            let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
            let disconnects_before = disconnect_count.load(Ordering::SeqCst);

            proxy.pause();
            proxy.drop_active_connections().await;

            for _ in 0..40 {
                if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }

            assert!(
                disconnect_count.load(Ordering::SeqCst) > disconnects_before,
                "outage cycle {} should trigger a disconnect",
                label
            );

            let gap_id = format!("gap-{}-{}", index, label);
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('{}', 'during-outage')",
                        table, gap_id
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert gap row during outage");

            sleep(*outage_duration).await;
            proxy.resume();

            let reconnect_context = format!("staggered {} outage", label);
            wait_for_reconnect(&client, &connect_count, expected_connects, &reconnect_context)
                .await;

            let live_id = format!("live-{}-{}", index, label);
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('{}', 'after-reconnect')",
                        table, live_id
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert live row after reconnect");

            let mut resumed_ids = Vec::<String>::new();
            let mut resumed_seq = Some(cycle_from);
            for _ in 0..20 {
                if resumed_ids.iter().any(|id| id == &gap_id)
                    && resumed_ids.iter().any(|id| id == &live_id)
                {
                    break;
                }

                match timeout(Duration::from_millis(1500), sub.next()).await {
                    Ok(Some(Ok(ev))) => {
                        collect_ids_and_track_seq(
                            &ev,
                            &mut resumed_ids,
                            &mut resumed_seq,
                            Some(cycle_from),
                            "staggered outage resumed",
                        );
                    },
                    Ok(Some(Err(e))) => panic!("subscription error after {} outage: {}", label, e),
                    Ok(None) => panic!("subscription ended unexpectedly after {} outage", label),
                    Err(_) => {},
                }
            }

            assert!(
                resumed_ids.iter().any(|id| id == &gap_id),
                "subscription should receive {} after reconnect",
                gap_id
            );
            assert!(
                resumed_ids.iter().any(|id| id == &live_id),
                "subscription should receive {} after reconnect",
                live_id
            );
            assert!(
                !resumed_ids.iter().any(|id| id == "baseline"),
                "baseline row must not be replayed during {} outage cycle",
                label
            );

            for _ in 0..8 {
                match timeout(Duration::from_millis(120), sub.next()).await {
                    Ok(Some(Ok(ev))) => {
                        collect_ids_and_track_seq(
                            &ev,
                            &mut resumed_ids,
                            &mut resumed_seq,
                            Some(cycle_from),
                            "staggered outage local tail",
                        );
                    },
                    _ => break,
                }
            }

            checkpoint = resumed_seq;
        }

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "staggered outage test timed out");
}
