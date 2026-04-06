use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Close (unsubscribe from) a subscription while the client is disconnected.
/// On reconnect the shared connection must NOT re-subscribe the dropped query,
/// but must still re-subscribe any remaining active ones.
#[tokio::test]
async fn test_unsubscribe_during_outage_prevents_resubscribe() {
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
        let table_keep = format!("default.unsub_keep_{}", suffix);
        let table_drop = format!("default.unsub_drop_{}", suffix);
        ensure_table(&writer, &table_keep).await;
        ensure_table(&writer, &table_drop).await;

        client.connect().await.expect("connect through proxy");

        let mut sub_keep = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("unsub-keep-{}", suffix),
                format!("SELECT id, value FROM {}", table_keep),
            ))
            .await
            .expect("subscribe keep");

        let mut sub_drop = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("unsub-drop-{}", suffix),
                format!("SELECT id, value FROM {}", table_drop),
            ))
            .await
            .expect("subscribe drop");

        // Consume initial events from both.
        let _ = timeout(TEST_TIMEOUT, sub_keep.next()).await;
        let _ = timeout(TEST_TIMEOUT, sub_drop.next()).await;

        // Insert pre-outage rows.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('keep-pre', 'v')", table_keep),
                None,
                None,
                None,
            )
            .await
            .expect("insert keep-pre");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('drop-pre', 'v')", table_drop),
                None,
                None,
                None,
            )
            .await
            .expect("insert drop-pre");

        // Observe pre-outage rows.
        let mut keep_ids = Vec::<String>::new();
        let mut keep_seq = None;
        for _ in 0..12 {
            if keep_ids.iter().any(|id| id == "keep-pre") {
                break;
            }
            match timeout(Duration::from_millis(1200), sub_keep.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut keep_ids,
                        &mut keep_seq,
                        None,
                        "unsub keep pre",
                    );
                },
                _ => {},
            }
        }
        assert!(keep_ids.iter().any(|id| id == "keep-pre"));

        let mut drop_ids = Vec::<String>::new();
        let mut drop_seq = None;
        for _ in 0..12 {
            if drop_ids.iter().any(|id| id == "drop-pre") {
                break;
            }
            match timeout(Duration::from_millis(1200), sub_drop.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut drop_ids,
                        &mut drop_seq,
                        None,
                        "unsub drop pre",
                    );
                },
                _ => {},
            }
        }
        assert!(drop_ids.iter().any(|id| id == "drop-pre"));

        // ── Take the proxy down ─────────────────────────────────────────
        let dc_before = disconnect_count.load(Ordering::SeqCst);
        proxy.simulate_server_down().await;

        for _ in 0..40 {
            if disconnect_count.load(Ordering::SeqCst) > dc_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // ── Drop sub_drop WHILE disconnected ────────────────────────────
        sub_drop.close().await.ok();

        // Insert gap rows into both tables while disconnected.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('keep-gap', 'g')", table_keep),
                None,
                None,
                None,
            )
            .await
            .expect("insert keep-gap");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('drop-gap', 'g')", table_drop),
                None,
                None,
                None,
            )
            .await
            .expect("insert drop-gap (should NOT be delivered)");

        // ── Bring the proxy back ────────────────────────────────────────
        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        proxy.simulate_server_up();

        for _ in 0..100 {
            if connect_count.load(Ordering::SeqCst) >= expected_connects
                && client.is_connected().await
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert!(client.is_connected().await, "client should reconnect");

        // Verify the kept subscription resumes and delivers the gap row.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('keep-post', 'p')", table_keep),
                None,
                None,
                None,
            )
            .await
            .expect("insert keep-post");

        let mut keep_resumed = Vec::<String>::new();
        let mut keep_resumed_seq = None;
        for _ in 0..30 {
            if keep_resumed.iter().any(|id| id == "keep-gap")
                && keep_resumed.iter().any(|id| id == "keep-post")
            {
                break;
            }
            match timeout(Duration::from_millis(2000), sub_keep.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut keep_resumed,
                        &mut keep_resumed_seq,
                        None,
                        "unsub keep resumed",
                    );
                },
                _ => {},
            }
        }
        assert!(
            keep_resumed.iter().any(|id| id == "keep-gap"),
            "kept subscription should deliver gap row after reconnect"
        );
        assert!(
            keep_resumed.iter().any(|id| id == "keep-post"),
            "kept subscription should deliver post-reconnect row"
        );

        // Verify the dropped subscription is NOT listed as active.
        let subs = client.subscriptions().await;
        let drop_sub_active = subs
            .iter()
            .any(|entry| entry.query == format!("SELECT id, value FROM {}", table_drop));
        assert!(!drop_sub_active, "dropped subscription must NOT be re-subscribed on reconnect");

        sub_keep.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "unsubscribe during outage test timed out");
}
