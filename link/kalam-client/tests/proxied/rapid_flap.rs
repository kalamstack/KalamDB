use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Rapid connection flapping: bring the proxy up and down in quick succession
/// (sub-second cycles). The client must survive without panicking, must not
/// get stuck in a reconnect loop, and must eventually stabilise and resume
/// from the correct seq once the link stays up.
#[tokio::test]
#[ntest::timeout(25000)]
async fn test_rapid_flapping_connection_stabilises_and_resumes() {
    let result = timeout(Duration::from_secs(20), async {
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
        let table = format!("default.rapid_flap_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("initial connect through proxy");

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("rapid-flap-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe through proxy");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        // Insert a baseline row and observe it.
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
                        "rapid-flap baseline",
                    );
                },
                _ => {},
            }
        }
        assert!(baseline_ids.iter().any(|id| id == "baseline"));
        let resume_from = query_max_seq(&writer, &table).await;

        // ── Rapid flap: 5 cycles of down→up with < 500 ms between ──────
        for cycle in 0..5 {
            proxy.simulate_server_down().await;
            sleep(Duration::from_millis(150)).await;
            proxy.simulate_server_up();
            sleep(Duration::from_millis(250)).await;

            // Insert a row during each brief up window.
            let _ = writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('flap-{}', 'cycle')",
                        table, cycle
                    ),
                    None,
                    None,
                    None,
                )
                .await;
        }

        // ── Stabilise: leave the proxy up ───────────────────────────────
        proxy.simulate_server_up();

        // Insert a final row that MUST arrive.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('post-flap', 'stable')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert post-flap row");

        // Wait for reconnect.
        if connect_count.load(Ordering::SeqCst) == 0 {
            panic!("client never established the initial connection event");
        }
        wait_until_connected(&client, "rapid flap stabilisation").await;

        // Client should have seen at least one disconnect during the flap.
        assert!(
            disconnect_count.load(Ordering::SeqCst) >= 1,
            "at least one disconnect event should fire during rapid flapping"
        );

        // Collect all events after the stable resume.
        let mut post_ids = Vec::<String>::new();
        let mut post_seq = None;
        for _ in 0..60 {
            if post_ids.iter().any(|id| id == "post-flap") {
                break;
            }
            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut post_ids,
                        &mut post_seq,
                        Some(resume_from),
                        "rapid-flap recovery",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription error after flap: {}", e),
                Ok(None) => panic!("subscription ended after flap"),
                Err(_) => {},
            }
        }

        assert!(
            post_ids.iter().any(|id| id == "post-flap"),
            "post-flap row must arrive after the connection stabilises"
        );

        // Verify no stale data (baseline must not replay).
        assert!(
            !post_ids.iter().any(|id| id == "baseline"),
            "baseline row must not replay after resume"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "rapid flapping test timed out");
}
