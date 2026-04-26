use std::{sync::atomic::Ordering, time::Duration};

use kalam_client::SubscriptionConfig;
use tokio::time::{sleep, timeout};

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

async fn wait_for_row_after_checkpoint(
    sub: &mut kalam_client::SubscriptionManager,
    checkpoint: kalam_client::SeqId,
    expected_ids: &[&str],
    forbidden_ids: &[&str],
    context: &str,
) {
    let mut seen_ids = Vec::<String>::new();
    let mut resumed_seq = Some(checkpoint);

    for _ in 0..60 {
        if expected_ids.iter().all(|expected| seen_ids.iter().any(|seen| seen == expected)) {
            break;
        }

        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut seen_ids,
                    &mut resumed_seq,
                    Some(checkpoint),
                    context,
                );
            },
            Ok(Some(Err(e))) => panic!("{}: subscription error after impairment: {}", context, e),
            Ok(None) => panic!("{}: subscription ended unexpectedly", context),
            Err(_) => {},
        }
    }

    for expected in expected_ids {
        assert!(
            seen_ids.iter().any(|seen| seen == expected),
            "{}: expected row {} after recovery",
            context,
            expected
        );
    }

    for forbidden in forbidden_ids {
        assert!(
            !seen_ids.iter().any(|seen| seen == forbidden),
            "{}: row {} must not replay after recovery",
            context,
            forbidden
        );
    }
}

/// Traffic can stop flowing while the TCP socket remains open. The client must
/// still detect the dead connection via pong timeout, reconnect, and resume.
#[tokio::test]
async fn test_proxy_blackhole_keeps_socket_open_until_client_times_out() {
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
        let table = format!("default.blackhole_timeout_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("initial connect through proxy");
        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("blackhole-timeout-{}", suffix),
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
                        "blackhole baseline",
                    );
                },
                _ => {},
            }
        }
        assert!(baseline_ids.iter().any(|id| id == "baseline"));
        let resume_from = query_max_seq(&writer, &table).await;

        assert!(
            proxy.wait_for_active_connections(1, Duration::from_secs(2)).await,
            "proxy should have at least one active connection before blackhole"
        );

        let disconnects_before = disconnect_count.load(Ordering::SeqCst);
        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        proxy.blackhole();

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('gap-blackhole', 'during-outage')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap row during blackhole");

        let mut saw_open_socket_during_blackhole = false;
        for _ in 0..40 {
            if proxy.active_count().await >= 1 {
                saw_open_socket_during_blackhole = true;
            }
            if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            saw_open_socket_during_blackhole,
            "proxy should keep the TCP socket open while traffic is blackholed"
        );
        assert!(
            disconnect_count.load(Ordering::SeqCst) > disconnects_before,
            "client should detect timeout even though the proxy did not hard-drop the socket"
        );

        proxy.restore_traffic();

        for _ in 0..80 {
            if connect_count.load(Ordering::SeqCst) >= expected_connects
                && client.is_connected().await
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(client.is_connected().await, "client should reconnect after blackhole clears");

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('live-blackhole', 'after-reconnect')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live row after reconnect");

        wait_for_row_after_checkpoint(
            &mut sub,
            resume_from,
            &["gap-blackhole", "live-blackhole"],
            &["baseline"],
            "blackhole recovery",
        )
        .await;

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "blackhole timeout test timed out");
}

/// High latency below the pong-timeout budget should not trigger false
/// reconnects, and live rows should still flow through the shared connection.
#[tokio::test]
async fn test_proxy_latency_does_not_false_positive_disconnect() {
    let result = timeout(Duration::from_secs(30), async {
        let writer = match create_test_client() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Skipping test (writer client unavailable): {}", e);
                return;
            },
        };

        let proxy = TcpDisconnectProxy::start(upstream_server_url()).await;
        let (client, _connect_count, disconnect_count) =
            match create_test_client_with_events_for_base_url(proxy.base_url()) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Skipping test (proxy client unavailable): {}", e);
                    proxy.shutdown().await;
                    return;
                },
            };

        let suffix = unique_suffix();
        let table = format!("default.latency_tolerant_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("initial connect through proxy");
        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("latency-tolerant-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe through proxy");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        let disconnects_before = disconnect_count.load(Ordering::SeqCst);
        proxy.set_latency(Duration::from_millis(300));
        sleep(Duration::from_secs(4)).await;

        assert!(
            client.is_connected().await,
            "client should stay connected under moderate latency"
        );
        assert_eq!(
            disconnect_count.load(Ordering::SeqCst),
            disconnects_before,
            "moderate latency should not trigger a disconnect"
        );

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('slow-network', 'still-live')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert row under latency");

        let mut ids = Vec::<String>::new();
        let mut seq = None;
        for _ in 0..12 {
            if ids.iter().any(|id| id == "slow-network") {
                break;
            }
            match timeout(Duration::from_millis(1500), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(&ev, &mut ids, &mut seq, None, "moderate latency");
                },
                _ => {},
            }
        }

        assert!(
            ids.iter().any(|id| id == "slow-network"),
            "live row should still arrive under moderate latency"
        );

        proxy.clear_latency();
        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "moderate latency test timed out");
}

/// Simulate packet loss at the TCP level as retransmission-like chunk stalls.
/// The client should reconnect and resume once the stalls are removed.
#[tokio::test]
async fn test_proxy_packet_loss_style_stalls_resume_without_replay() {
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
        let table = format!("default.lossy_stalls_{}", suffix);
        ensure_table(&writer, &table).await;

        client.connect().await.expect("initial connect through proxy");
        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("lossy-stalls-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe through proxy");

        let _ = timeout(TEST_TIMEOUT, sub.next()).await;

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('baseline-lossy', 'ready')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert baseline row");

        let mut baseline_ids = Vec::<String>::new();
        let mut checkpoint = None;
        for _ in 0..12 {
            if baseline_ids.iter().any(|id| id == "baseline-lossy") {
                break;
            }
            match timeout(Duration::from_millis(1200), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut baseline_ids,
                        &mut checkpoint,
                        None,
                        "lossy baseline",
                    );
                },
                _ => {},
            }
        }
        assert!(baseline_ids.iter().any(|id| id == "baseline-lossy"));
        let resume_from = query_max_seq(&writer, &table).await;

        let disconnects_before = disconnect_count.load(Ordering::SeqCst);
        let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
        // This needs to exceed the 2s pong timeout used by reconnect_test_timeouts()
        // or the transport impairment will look like mere latency instead of a dead link.
        proxy.set_chunk_stall_pattern(1, Duration::from_millis(2500));

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('gap-lossy', 'during-stall')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap row during stall pattern");

        for _ in 0..80 {
            if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            disconnect_count.load(Ordering::SeqCst) > disconnects_before,
            "retransmission-style stalls should eventually force a reconnect"
        );

        proxy.clear_chunk_stall_pattern();

        for _ in 0..120 {
            if connect_count.load(Ordering::SeqCst) >= expected_connects
                && client.is_connected().await
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            client.is_connected().await,
            "client should reconnect after stall pattern clears"
        );

        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('live-lossy', 'after-reconnect')",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert live row after stall pattern");

        wait_for_row_after_checkpoint(
            &mut sub,
            resume_from,
            &["gap-lossy", "live-lossy"],
            &["baseline-lossy"],
            "lossy recovery",
        )
        .await;

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "packet-loss-style stall test timed out");
}
