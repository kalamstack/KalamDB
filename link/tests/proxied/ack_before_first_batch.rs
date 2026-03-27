use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_link::{models::BatchStatus, ChangeEvent, SubscriptionConfig, SubscriptionOptions};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Disconnect after the server acknowledges the subscription but before the
/// client finishes receiving the initial data batches. The subscription should
/// still replay the full snapshot after reconnect and then continue with live rows.
#[tokio::test]
async fn test_disconnect_after_ack_before_first_initial_batch() {
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
        let table = format!("default.ack_gap_{}", suffix);
        ensure_table(&writer, &table).await;

        for index in 0..12 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('seed-{}', 'seed-value-{}')",
                        table, index, index
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert seed row");
        }

        client.connect().await.expect("connect through proxy");

        let mut config = SubscriptionConfig::new(
            format!("ack-gap-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        );
        config.options = Some(SubscriptionOptions::new().with_batch_size(3));

        let mut sub = client.subscribe_with_config(config).await.expect("subscribe should succeed");

        let ack = timeout(TEST_TIMEOUT, sub.next())
            .await
            .expect("ack wait should not time out")
            .expect("subscription should not end before ack")
            .expect("ack event should not error");

        match ack {
            ChangeEvent::Ack { batch_control, .. } => {
                assert_ne!(
                    batch_control.status,
                    BatchStatus::Ready,
                    "expected a non-ready ack for the large snapshot scenario"
                );
            },
            other => panic!("expected ack before initial batch, got {:?}", other),
        }

        let mut seen_ids = HashSet::<String>::new();
        let disconnects_before = disconnect_count.load(Ordering::SeqCst);
        proxy.pause();
        proxy.drop_active_connections().await;

        for _ in 0..40 {
            match timeout(Duration::from_millis(100), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "ack-gap before disconnect observed",
                    );
                    seen_ids.extend(ids);
                },
                _ => {},
            }
            if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
                break;
            }
        }

        assert!(
            disconnect_count.load(Ordering::SeqCst) > disconnects_before,
            "ack-gap scenario should trigger a disconnect"
        );

        for _ in 0..16 {
            match timeout(Duration::from_millis(100), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(
                        &ev,
                        &mut ids,
                        &mut scratch,
                        None,
                        "ack-gap local tail",
                    );
                    seen_ids.extend(ids);
                },
                _ => break,
            }
        }

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('gap-after-ack', 'during-outage')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert outage row");

        sleep(Duration::from_millis(900)).await;
        proxy.resume();

        for _ in 0..80 {
            if connect_count.load(Ordering::SeqCst) >= 2 && client.is_connected().await {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(client.is_connected().await, "client should reconnect after ack-gap drop");

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('live-after-ack-gap', 'after-reconnect')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert post-reconnect row");

        for _ in 0..40 {
            if seen_ids.contains("gap-after-ack")
                && seen_ids.contains("live-after-ack-gap")
                && (0..12).all(|index| seen_ids.contains(&format!("seed-{}", index)))
            {
                break;
            }

            match timeout(Duration::from_millis(1500), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    let mut scratch = None;
                    let mut ids = Vec::new();
                    collect_ids_and_track_seq(&ev, &mut ids, &mut scratch, None, "ack-gap resumed");
                    seen_ids.extend(ids);
                },
                Ok(Some(Err(e))) => panic!("ack-gap subscription errored after reconnect: {}", e),
                Ok(None) => panic!("ack-gap subscription ended unexpectedly"),
                Err(_) => {},
            }
        }

        for index in 0..12 {
            assert!(
                seen_ids.contains(&format!("seed-{}", index)),
                "seed row {} should be delivered after ack-gap reconnect",
                index
            );
        }
        assert!(
            seen_ids.contains("gap-after-ack"),
            "outage row should be delivered after reconnect"
        );
        assert!(
            seen_ids.contains("live-after-ack-gap"),
            "post-reconnect row should be delivered"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "ack before first batch test timed out");
}
