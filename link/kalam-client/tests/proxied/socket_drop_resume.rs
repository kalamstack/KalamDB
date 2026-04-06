use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_client::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

/// Simulate a real network/socket drop by routing the client through a local
/// TCP proxy, force-closing the active socket, then allowing the shared
/// connection to auto-reconnect and resume the same subscription.
#[tokio::test]
async fn test_shared_connection_auto_reconnects_after_socket_drop_and_resumes() {
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
    let table = format!("default.socket_drop_resume_{}", suffix);

    ensure_table(&writer, &table).await;

    client.connect().await.expect("connect should succeed through proxy");

    let mut sub = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("socket-drop-sub-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        ))
        .await
        .expect("subscribe should succeed through proxy");

    let _ = timeout(TEST_TIMEOUT, sub.next()).await;
    assert!(
        proxy.wait_for_active_connections(1, TEST_TIMEOUT).await,
        "proxy should observe the shared websocket connection"
    );

    let pre_ids = ["71001", "71002", "71003"];
    let gap_ids = ["71011", "71012", "71013", "71014"];
    let live_ids = ["71021", "71022", "71023", "71024"];

    for pre_id in pre_ids {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'before-drop')", table, pre_id),
                None,
                None,
                None,
            )
            .await
            .expect("insert before drop");
    }

    let mut pre_seen = Vec::<String>::new();
    let mut observed_seq = None;
    let mut last_pre_event = None;
    for _ in 0..12 {
        if pre_ids
            .iter()
            .all(|expected_id| pre_seen.iter().any(|seen_id| seen_id == expected_id))
        {
            break;
        }

        match timeout(Duration::from_millis(1200), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                last_pre_event = Some(format!("{:?}", ev));
                collect_ids_and_track_seq(
                    &ev,
                    &mut pre_seen,
                    &mut observed_seq,
                    None,
                    "socket drop pre",
                );
            },
            Ok(Some(Err(e))) => panic!("subscription error before socket drop: {}", e),
            Ok(None) => panic!("subscription ended unexpectedly before socket drop"),
            Err(_) => {},
        }
    }

    assert!(
        pre_ids
            .iter()
            .all(|expected_id| pre_seen.iter().any(|seen_id| seen_id == expected_id)),
        "all pre rows should be observed before drop"
    );
    let resume_from = query_max_seq(&writer, &table).await;
    assert_eq!(
        observed_seq,
        Some(resume_from),
        "pre-drop live insert should expose _seq to the client before checking shared state; last event: {:?}",
        last_pre_event
    );
    let subs = client.subscriptions().await;
    let tracked_seq = subs
        .iter()
        .find(|entry| entry.query == format!("SELECT id, value FROM {}", table))
        .and_then(|entry| entry.last_seq_id);
    assert_eq!(
        tracked_seq,
        Some(resume_from),
        "shared subscription should track last_seq_id before the socket drop"
    );

    let disconnects_before = disconnect_count.load(Ordering::SeqCst);
    proxy.pause();
    proxy.drop_active_connections().await;

    for _ in 0..40 {
        if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(
        disconnect_count.load(Ordering::SeqCst) > disconnects_before,
        "forced socket close should trigger an on_disconnect event"
    );

    for gap_id in gap_ids {
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'while-disconnected')",
                    table, gap_id
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert while disconnected");
    }

    sleep(Duration::from_millis(300)).await;
    proxy.resume();

    let reconnect_deadline = Instant::now() + RECONNECT_WAIT_TIMEOUT;
    while Instant::now() < reconnect_deadline {
        if connect_count.load(Ordering::SeqCst) >= 2 && client.is_connected().await {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(client.is_connected().await, "client should auto reconnect after socket drop");
    assert!(
        connect_count.load(Ordering::SeqCst) >= 2,
        "shared connection should emit a second on_connect after reconnect"
    );

    for live_id in live_ids {
        writer
            .execute_query(
                &format!(
                    "INSERT INTO {} (id, value) VALUES ('{}', 'after-reconnect')",
                    table, live_id
                ),
                None,
                None,
                None,
            )
            .await
            .expect("insert after reconnect");
    }

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
                    "socket drop resumed",
                );
            },
            Ok(Some(Err(e))) => panic!("subscription error after reconnect: {}", e),
            Ok(None) => panic!("subscription ended unexpectedly after reconnect"),
            Err(_) => {},
        }
    }

    assert!(
        gap_ids
            .iter()
            .all(|expected_id| resumed_ids.iter().any(|seen_id| seen_id == expected_id)),
        "subscription should resume and receive all rows written during the disconnect"
    );
    assert!(
        live_ids
            .iter()
            .all(|expected_id| resumed_ids.iter().any(|seen_id| seen_id == expected_id)),
        "subscription should continue receiving all live rows after reconnect"
    );
    assert!(
        pre_ids
            .iter()
            .all(|expected_id| !resumed_ids.iter().any(|seen_id| seen_id == expected_id)),
        "subscription must not replay rows observed before the socket drop"
    );

    sub.close().await.ok();
    client.disconnect().await;
    proxy.shutdown().await;
}
