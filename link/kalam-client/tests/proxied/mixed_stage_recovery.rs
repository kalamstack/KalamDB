use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// One subscription is already in steady-state live mode while a second
/// subscription is still loading a large initial snapshot when the network
/// drops. After reconnect the live subscription must resume from its durable
/// checkpoint while the loading subscription must still complete its snapshot.
#[tokio::test]
#[ntest::timeout(35000)]
async fn test_shared_connection_recovers_subscriptions_in_different_stages() {
    let result = timeout(Duration::from_secs(120), async {
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
        let live_table = format!("default.mixed_live_{}", suffix);
        let seed_table = format!("default.mixed_seed_{}", suffix);
        ensure_table(&writer, &live_table).await;
        ensure_table(&writer, &seed_table).await;

        for index in 0..40 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('seed-{}', 'seed-value-{}')",
                        seed_table, index, index
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert seed row");
        }

        client.connect().await.expect("connect through proxy");

        let mut live_sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("mixed-live-{}", suffix),
                format!("SELECT id, value FROM {}", live_table),
            ))
            .await
            .expect("subscribe live table");

        let _ = timeout(TEST_TIMEOUT, live_sub.next()).await;

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('live-before', 'steady-state')",
                    live_table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live-before row");

        let mut live_pre_ids = Vec::<String>::new();
        let mut live_checkpoint = None;
        for _ in 0..12 {
            if live_pre_ids.iter().any(|id| id == "live-before") {
                break;
            }

            match timeout(Duration::from_millis(1200), live_sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut live_pre_ids,
                        &mut live_checkpoint,
                        None,
                        "mixed-stage live pre",
                    );
                },
                Ok(Some(Err(e))) => panic!("live subscription errored before outage: {}", e),
                Ok(None) => panic!("live subscription ended unexpectedly before outage"),
                Err(_) => {},
            }
        }

        assert!(
            live_pre_ids.iter().any(|id| id == "live-before"),
            "live subscription should observe steady-state row before outage"
        );
        let live_from = query_max_seq(&writer, &live_table).await;

        let mut seed_sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("mixed-seed-{}", suffix),
                format!("SELECT id, value FROM {}", seed_table),
            ))
            .await
            .expect("subscribe seed table");

        let mut seed_seen = HashSet::<String>::new();
        for _ in 0..3 {
            match timeout(Duration::from_millis(400), seed_sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "mixed-stage seed pre",
                    );
                    seed_seen.extend(ids);
                },
                _ => break,
            }
        }

        let disconnects_before = disconnect_count.load(Ordering::SeqCst);
        proxy.simulate_server_down().await;

        for _ in 0..40 {
            if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            disconnect_count.load(Ordering::SeqCst) > disconnects_before,
            "mixed-stage outage should trigger a disconnect"
        );

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('live-gap', 'during-outage')",
                    live_table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live gap row");
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('seed-gap', 'during-outage')",
                    seed_table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert seed gap row");

        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_up();
        wait_for_reconnect(
            &client,
            &connect_count,
            expected_connects,
            "mixed-stage outage",
        )
        .await;

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('live-after', 'after-reconnect')",
                    live_table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live-after row");
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('seed-after', 'after-reconnect')",
                    seed_table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert seed-after row");

        let mut live_resumed_ids = Vec::<String>::new();
        let mut live_resumed_seq = Some(live_from);
        for _ in 0..40 {
            if live_resumed_ids.iter().any(|id| id == "live-gap")
                && live_resumed_ids.iter().any(|id| id == "live-after")
            {
                break;
            }

            match timeout(Duration::from_millis(2000), live_sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut live_resumed_ids,
                        &mut live_resumed_seq,
                        Some(live_from),
                        "mixed-stage live resumed",
                    );
                },
                Ok(Some(Err(e))) => panic!("live subscription errored after reconnect: {}", e),
                Ok(None) => panic!("live subscription ended unexpectedly after reconnect"),
                Err(_) => {},
            }
        }

        assert!(
            live_resumed_ids.iter().any(|id| id == "live-gap"),
            "live subscription should receive gap row"
        );
        assert!(
            live_resumed_ids.iter().any(|id| id == "live-after"),
            "live subscription should receive post-reconnect row"
        );
        assert!(
            !live_resumed_ids.iter().any(|id| id == "live-before"),
            "live subscription must not replay checkpointed row"
        );

        for _ in 0..80 {
            if seed_seen.contains("seed-gap")
                && seed_seen.contains("seed-after")
                && (0..40).all(|index| seed_seen.contains(&format!("seed-{}", index)))
            {
                break;
            }

            match timeout(Duration::from_millis(2000), seed_sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "mixed-stage seed resumed",
                    );
                    seed_seen.extend(ids);
                },
                Ok(Some(Err(e))) => panic!("seed subscription errored after reconnect: {}", e),
                Ok(None) => panic!("seed subscription ended unexpectedly after reconnect"),
                Err(_) => {},
            }
        }

        for index in 0..40 {
            assert!(
                seed_seen.contains(&format!("seed-{}", index)),
                "seed snapshot row {} should be delivered",
                index
            );
        }
        assert!(seed_seen.contains("seed-gap"), "seed subscription should receive outage row");
        assert!(
            seed_seen.contains("seed-after"),
            "seed subscription should receive post-reconnect row"
        );

        live_sub.close().await.ok();
        seed_sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "mixed-stage recovery test timed out");
}
