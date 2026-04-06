use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Add a new subscription while the client is actively reconnecting after an
/// outage. The new subscription must eventually be established and deliver its
/// data once the connection stabilises.
#[tokio::test]
#[ntest::timeout(15000)]
async fn test_subscribe_during_reconnect_eventually_delivers() {
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
        let table_a = format!("default.sub_reconn_a_{}", suffix);
        let table_b = format!("default.sub_reconn_b_{}", suffix);
        ensure_table(&writer, &table_a).await;
        ensure_table(&writer, &table_b).await;

        client.connect().await.expect("initial connect");

        // Subscribe to table A and confirm it works.
        let mut sub_a = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("sub-reconn-a-{}", suffix),
                format!("SELECT id, value FROM {}", table_a),
            ))
            .await
            .expect("subscribe A");

        let _ = timeout(TEST_TIMEOUT, sub_a.next()).await;

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('a-pre', 'val')", table_a),
                None,
                None,
                None,
            )
            .await
            .expect("insert a-pre");

        let mut a_ids = Vec::<String>::new();
        let mut a_seq = None;
        for _ in 0..12 {
            if a_ids.iter().any(|id| id == "a-pre") {
                break;
            }
            match timeout(Duration::from_millis(1200), sub_a.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut a_ids,
                        &mut a_seq,
                        None,
                        "sub-reconn A pre",
                    );
                },
                _ => {},
            }
        }
        assert!(a_ids.iter().any(|id| id == "a-pre"), "a-pre should be observed");

        // ── Drop the connection ─────────────────────────────────────────
        let dc_before = disconnect_count.load(Ordering::SeqCst);
        proxy.simulate_server_down().await;

        for _ in 0..40 {
            if disconnect_count.load(Ordering::SeqCst) > dc_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // ── Subscribe to table B WHILE disconnected ─────────────────────
        // This should be queued internally and established once connected.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('b-seed', 'val')", table_b),
                None,
                None,
                None,
            )
            .await
            .expect("insert b-seed");

        // Bring the proxy back so the client can reconnect.
        proxy.simulate_server_up();
        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;

        // Subscribe while reconnect is (potentially) in progress.
        let sub_b_future = client.subscribe_with_config(SubscriptionConfig::new(
            format!("sub-reconn-b-{}", suffix),
            format!("SELECT id, value FROM {}", table_b),
        ));

        // Give the reconnect a moment to progress.
        let mut sub_b = timeout(Duration::from_secs(15), sub_b_future)
            .await
            .expect("subscribe B should not hang forever")
            .expect("subscribe B should succeed after reconnect");

        // Wait for the connection to be fully established.
        wait_for_reconnect(
            &client,
            &connect_count,
            expected_connects,
            "subscribe during reconnect",
        )
        .await;

        // Insert rows after reconnect.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('a-post', 'val')", table_a),
                None,
                None,
                None,
            )
            .await
            .expect("insert a-post");
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('b-post', 'val')", table_b),
                None,
                None,
                None,
            )
            .await
            .expect("insert b-post");

        // Verify table A subscription resumed.
        let mut a_post_ids = Vec::<String>::new();
        let mut a_post_seq = None;
        for _ in 0..30 {
            if a_post_ids.iter().any(|id| id == "a-post") {
                break;
            }
            match timeout(Duration::from_millis(2000), sub_a.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut a_post_ids,
                        &mut a_post_seq,
                        None,
                        "sub-reconn A post",
                    );
                },
                _ => {},
            }
        }
        assert!(
            a_post_ids.iter().any(|id| id == "a-post"),
            "table A subscription should resume and deliver a-post"
        );

        // Verify table B subscription delivers both the seed and post row.
        let mut b_ids = Vec::<String>::new();
        let mut b_seq = None;
        for _ in 0..30 {
            if b_ids.iter().any(|id| id == "b-seed") && b_ids.iter().any(|id| id == "b-post") {
                break;
            }
            match timeout(Duration::from_millis(2000), sub_b.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(&ev, &mut b_ids, &mut b_seq, None, "sub-reconn B");
                },
                _ => {},
            }
        }
        assert!(
            b_ids.iter().any(|id| id == "b-seed"),
            "table B subscription should deliver b-seed"
        );
        assert!(
            b_ids.iter().any(|id| id == "b-post"),
            "table B subscription should deliver b-post"
        );

        sub_a.close().await.ok();
        sub_b.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "subscribe during reconnect test timed out");
}
