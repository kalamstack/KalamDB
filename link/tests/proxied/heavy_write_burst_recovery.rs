use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_link::SubscriptionConfig;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// A large burst of writes (50 rows) is performed while the client is
/// disconnected. After recovery, every single row must arrive exactly once
/// with no duplicates and no gaps. This stress-tests the resume/replay
/// path under a realistic write backlog.
#[tokio::test]
async fn test_heavy_write_burst_during_outage_all_delivered() {
    let result = timeout(Duration::from_secs(90), async {
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
        let table = format!("default.burst_recovery_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("connect through proxy");

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("burst-recovery-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe through proxy");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        // Insert a baseline row.
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
                        "burst baseline",
                    );
                },
                _ => {},
            }
        }
        assert!(baseline_ids.iter().any(|id| id == "baseline"));
        let resume_from = query_max_seq(&writer, &table).await;

        // ── Take the proxy down ─────────────────────────────────────────
        let dc_before = disconnect_count.load(Ordering::SeqCst);
        proxy.simulate_server_down().await;

        for _ in 0..40 {
            if disconnect_count.load(Ordering::SeqCst) > dc_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // ── Burst-insert 50 rows while the client is disconnected ───────
        let burst_count = 50_u32;
        for i in 0..burst_count {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('burst-{}', 'val-{}')",
                        table, i, i
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap_or_else(|e| panic!("burst insert #{} failed: {}", i, e));
        }

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
        assert!(client.is_connected().await, "client should reconnect after burst outage");

        // Insert one final row after reconnect to mark the end.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('after-burst', 'marker')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert after-burst marker");

        // ── Collect all events and verify completeness ──────────────────
        let mut seen_ids = Vec::<String>::new();
        let mut seen_seq = None;
        for _ in 0..120 {
            if seen_ids.iter().any(|id| id == "after-burst")
                && (0..burst_count).all(|i| seen_ids.iter().any(|id| id == &format!("burst-{}", i)))
            {
                break;
            }
            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut seen_ids,
                        &mut seen_seq,
                        Some(resume_from),
                        "burst recovery",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription error during burst recovery: {}", e),
                Ok(None) => panic!("subscription ended during burst recovery"),
                Err(_) => {},
            }
        }

        // Verify every burst row arrived.
        let missing: Vec<_> = (0..burst_count)
            .filter(|i| !seen_ids.iter().any(|id| id == &format!("burst-{}", i)))
            .collect();
        assert!(
            missing.is_empty(),
            "all {} burst rows should arrive; missing indices: {:?}",
            burst_count,
            missing
        );

        // Verify no duplicates.
        let burst_only: Vec<_> = seen_ids.iter().filter(|id| id.starts_with("burst-")).collect();
        let unique_count = burst_only.iter().collect::<HashSet<_>>().len();
        assert_eq!(
            burst_only.len(),
            unique_count,
            "burst rows must not contain duplicates; total={} unique={}",
            burst_only.len(),
            unique_count
        );

        assert!(
            seen_ids.iter().any(|id| id == "after-burst"),
            "after-burst marker should arrive"
        );

        // baseline must NOT replay.
        assert!(
            !seen_ids.iter().any(|id| id == "baseline"),
            "baseline row must not replay after resume"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "heavy write burst recovery test timed out");
}
