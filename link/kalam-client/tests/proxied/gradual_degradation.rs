use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Latency increases in steps until it exceeds the pong timeout, forcing the
/// client to reconnect. Once latency drops back to zero the client should
/// resume and deliver any rows queued on the server during the degraded window.
#[tokio::test]
#[ntest::timeout(35000)]
async fn test_gradual_latency_ramp_forces_reconnect_then_recovers() {
    let result = timeout(Duration::from_secs(30), async {
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
        let table = format!("default.latency_ramp_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("initial connect through proxy");

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("latency-ramp-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe through proxy");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        // Insert baseline.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('baseline', 'ready')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert baseline");

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
                        "ramp baseline",
                    );
                },
                _ => {},
            }
        }
        assert!(baseline_ids.iter().any(|id| id == "baseline"));
        let resume_from = query_max_seq(&writer, &table).await;

        // ── Ramp latency: 100 → 500 → 1500 → 3000ms ───────────────────
        // reconnect_test_timeouts() uses pong_timeout = 2s, receive_timeout = 5s.
        // At 3000ms per chunk the pong will time out and the client will disconnect.
        let dc_before = disconnect_count.load(Ordering::SeqCst);
        let latencies_ms = [100, 500, 1500, 3000];
        for (step, &latency_ms) in latencies_ms.iter().enumerate() {
            proxy.set_latency(Duration::from_millis(latency_ms));
            sleep(Duration::from_millis(2000)).await;

            // Insert a row at each step so we can verify delivery later.
            let _ = writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('ramp-{}', 'at-{}ms')",
                        table, step, latency_ms
                    ),
                    None,
                    None,
                    None,
                )
                .await;
        }

        // Wait for the disconnect that the 3000ms latency should produce.
        for _ in 0..80 {
            if disconnect_count.load(Ordering::SeqCst) > dc_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            disconnect_count.load(Ordering::SeqCst) > dc_before,
            "client should disconnect once latency exceeds pong timeout"
        );

        // ── Clear latency and allow recovery ────────────────────────────
        proxy.clear_latency();
        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        wait_for_reconnect(&client, &connect_count, expected_connects, "gradual latency recovery")
            .await;

        // Insert a post-recovery marker.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('post-ramp', 'recovered')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert post-ramp row");

        // ── Collect resumed events ──────────────────────────────────────
        let mut resumed_ids = Vec::<String>::new();
        let mut resumed_seq = None;
        for _ in 0..60 {
            if resumed_ids.iter().any(|id| id == "post-ramp") {
                break;
            }
            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut resumed_ids,
                        &mut resumed_seq,
                        Some(resume_from),
                        "ramp recovery",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription error after ramp: {}", e),
                Ok(None) => panic!("subscription ended after ramp"),
                Err(_) => {},
            }
        }

        assert!(
            resumed_ids.iter().any(|id| id == "post-ramp"),
            "post-ramp row should arrive after recovery"
        );

        // Baseline must not replay.
        assert!(
            !resumed_ids.iter().any(|id| id == "baseline"),
            "baseline must not replay after ramp recovery"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "gradual latency ramp test timed out");
}
