use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Server goes down while the client is receiving live updates.
/// After reconnect the subscription should resume from where it left off
/// and NOT replay rows seen before the drop.
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_proxy_server_down_during_live_updates_resumes() {
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
    let table = format!("default.live_update_drop_{}", suffix);
    ensure_table(&writer, &table).await;

    client.connect().await.expect("connect through proxy");

    let mut sub = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("live-update-drop-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        ))
        .await
        .expect("subscribe");

    // Consume initial ack.
    let _ = timeout(TEST_TIMEOUT, sub.next()).await;

    // Insert a "before" row and observe it.
    let before_ids = ["before-drop-1", "before-drop-2", "before-drop-3"];
    for before_id in before_ids {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'pre')", table, before_id),
                None,
                None,
                None,
            )
            .await
            .expect("insert before row");
    }

    let mut pre_seen = Vec::<String>::new();
    let mut observed_seq = None;
    for _ in 0..12 {
        if before_ids
            .iter()
            .all(|expected_id| pre_seen.iter().any(|seen_id| seen_id == expected_id))
        {
            break;
        }
        match timeout(Duration::from_millis(1200), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut pre_seen,
                    &mut observed_seq,
                    None,
                    "live-update pre",
                );
            },
            _ => {},
        }
    }
    assert!(
        before_ids
            .iter()
            .all(|expected_id| pre_seen.iter().any(|seen_id| seen_id == expected_id)),
        "should observe all before-drop rows"
    );
    let resume_from = query_max_seq(&writer, &table).await;

    // Kill the proxy while updates are flowing.
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
        "disconnect event should fire"
    );

    // Insert rows while the proxy is down.
    let gap_ids = [
        "gap-during-down-1",
        "gap-during-down-2",
        "gap-during-down-3",
        "gap-during-down-4",
    ];
    for gap_id in gap_ids {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'gap')", table, gap_id),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap row");
    }

    // Bring the proxy back.
    proxy.simulate_server_up();
    wait_for_reconnect(&client, &connect_count, 2, "live updates outage").await;

    // Insert another after reconnect.
    let live_ids = [
        "live-after-reconnect-1",
        "live-after-reconnect-2",
        "live-after-reconnect-3",
        "live-after-reconnect-4",
    ];
    for live_id in live_ids {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'live')", table, live_id),
                None,
                None,
                None,
            )
            .await
            .expect("insert live row");
    }

    // Collect resumed events and verify sequencing.
    let mut resumed_ids = Vec::<String>::new();
    let mut resumed_seq = Some(resume_from);
    for _ in 0..28 {
        if gap_ids
            .iter()
            .all(|expected_id| resumed_ids.iter().any(|seen_id| seen_id == expected_id))
            && live_ids
                .iter()
                .all(|expected_id| resumed_ids.iter().any(|seen_id| seen_id == expected_id))
        {
            break;
        }
        match timeout(Duration::from_millis(1200), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut resumed_ids,
                    &mut resumed_seq,
                    Some(resume_from),
                    "live-update resumed",
                );
            },
            _ => {},
        }
    }

    assert!(
        gap_ids
            .iter()
            .all(|expected_id| resumed_ids.iter().any(|seen_id| seen_id == expected_id)),
        "all gap rows written during disconnect should be received"
    );
    assert!(
        live_ids
            .iter()
            .all(|expected_id| resumed_ids.iter().any(|seen_id| seen_id == expected_id)),
        "all live rows written after reconnect should be received"
    );
    assert!(
        before_ids
            .iter()
            .all(|expected_id| !resumed_ids.iter().any(|seen_id| seen_id == expected_id)),
        "rows observed before the drop must NOT be replayed"
    );

    sub.close().await.ok();
    client.disconnect().await;
    proxy.shutdown().await;
}
