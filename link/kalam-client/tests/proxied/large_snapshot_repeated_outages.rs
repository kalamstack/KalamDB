use std::{collections::HashSet, sync::atomic::Ordering, time::Duration};

use kalam_client::{SubscriptionConfig, SubscriptionOptions};
use tokio::time::{sleep, timeout};

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

/// A large initial snapshot should still complete when the connection drops
/// more than once before the client reaches steady-state live delivery.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_large_initial_snapshot_survives_repeated_outages() {
    let result = timeout(Duration::from_secs(25), async {
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
        let table = format!("default.large_snapshot_drop_{}", suffix);
        ensure_table(&writer, &table).await;

        for index in 0..30 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('seed-{}', 'bulk-seed-{}')",
                        table, index, index
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert large seed row");
        }

        client.connect().await.expect("connect through proxy");

        let mut config = SubscriptionConfig::new(
            format!("large-snapshot-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        );
        config.options = Some(SubscriptionOptions::new().with_batch_size(3));

        let mut sub = client
            .subscribe_with_config(config)
            .await
            .expect("subscribe large snapshot table");

        let mut seen_ids = HashSet::<String>::new();
        for _ in 0..4 {
            match timeout(Duration::from_millis(400), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "large snapshot pre first outage",
                    );
                    seen_ids.extend(ids);
                },
                _ => break,
            }
        }

        let first_disconnects = disconnect_count.load(Ordering::SeqCst);
        proxy.simulate_server_down().await;

        for _ in 0..40 {
            if disconnect_count.load(Ordering::SeqCst) > first_disconnects {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            disconnect_count.load(Ordering::SeqCst) > first_disconnects,
            "first outage should trigger a disconnect"
        );

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('gap-one', 'during-first-outage')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert first outage row");

        proxy.simulate_server_up();
        wait_for_reconnect(&client, &connect_count, 2, "large snapshot first outage").await;

        for _ in 0..6 {
            match timeout(Duration::from_millis(350), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "large snapshot between outages",
                    );
                    seen_ids.extend(ids);
                },
                _ => break,
            }
        }

        let second_disconnects = disconnect_count.load(Ordering::SeqCst);
        proxy.pause();
        proxy.drop_active_connections().await;

        for _ in 0..40 {
            if disconnect_count.load(Ordering::SeqCst) > second_disconnects {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            disconnect_count.load(Ordering::SeqCst) > second_disconnects,
            "second outage should trigger a disconnect"
        );

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('gap-two', 'during-second-outage')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert second outage row");

        sleep(Duration::from_millis(900)).await;
        proxy.resume();
        wait_for_reconnect(&client, &connect_count, 3, "large snapshot second outage").await;

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('after-final-reconnect', 'live-after')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert post-reconnect row");

        for _ in 0..120 {
            if seen_ids.contains("gap-one")
                && seen_ids.contains("gap-two")
                && seen_ids.contains("after-final-reconnect")
                && (0..30).all(|index| seen_ids.contains(&format!("seed-{}", index)))
            {
                break;
            }

            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "large snapshot resumed",
                    );
                    seen_ids.extend(ids);
                },
                Ok(Some(Err(e))) => panic!("large snapshot subscription errored: {}", e),
                Ok(None) => panic!("large snapshot subscription ended unexpectedly"),
                Err(_) => {},
            }
        }

        for index in 0..30 {
            assert!(
                seen_ids.contains(&format!("seed-{}", index)),
                "seed row {} should be delivered across repeated outages",
                index
            );
        }
        assert!(seen_ids.contains("gap-one"), "first outage row should be delivered");
        assert!(seen_ids.contains("gap-two"), "second outage row should be delivered");
        assert!(
            seen_ids.contains("after-final-reconnect"),
            "post-reconnect live row should be delivered"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "large snapshot repeated outages test timed out");
}
