use std::{sync::atomic::Ordering, time::Duration};

use kalam_client::SubscriptionConfig;
use tokio::time::{sleep, timeout};

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

/// Server goes down again while the client is in the process of reconnecting.
/// After the second outage clears, the client should still recover.
#[tokio::test]
#[ntest::timeout(46000)]
async fn test_proxy_server_down_while_reconnecting() {
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
    let table = format!("default.reconn_drop_{}", suffix);
    ensure_table(&writer, &table).await;

    client.connect().await.expect("initial connect");

    let mut sub = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("reconn-drop-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        ))
        .await
        .expect("subscribe");

    let _ = timeout(TEST_TIMEOUT, sub.next()).await;

    // Insert a pre-outage row.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('pre-1', 'v')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert pre-1");

    let mut pre_seen = Vec::<String>::new();
    let mut pre_seq = None;
    for _ in 0..12 {
        if pre_seen.iter().any(|id| id == "pre-1") {
            break;
        }
        match timeout(Duration::from_millis(1200), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut pre_seen,
                    &mut pre_seq,
                    None,
                    "reconn-drop pre",
                );
            },
            _ => {},
        }
    }
    assert!(pre_seen.iter().any(|id| id == "pre-1"), "should see pre-1");
    let resume_from = query_max_seq(&writer, &table).await;

    // ── First outage ────────────────────────────────────────────────────
    let dc1 = disconnect_count.load(Ordering::SeqCst);
    proxy.simulate_server_down().await;

    for _ in 0..40 {
        if disconnect_count.load(Ordering::SeqCst) > dc1 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Briefly resume the proxy so the client starts its reconnect attempt,
    // then kill it again immediately.
    proxy.simulate_server_up();
    // Wait for EITHER the client to complete a full WS reconnect (connect_count
    // increases — on_connect fired, so a subsequent disconnect will be properly
    // registered) OR at minimum a TCP connection has been accepted by the proxy.
    // Checking connect_count first avoids a race introduced by relying solely on
    // wait_for_active_connections, which fires at TCP accept time (before the WS
    // handshake), leaving the subscription in an ambiguous mid-handshake state
    // that can cause incorrect seq-resume and replay of pre-1 after the second
    // outage.
    let mut begin_reconnect_seen = false;
    for _ in 0..200 {
        let connects = connect_count.load(Ordering::SeqCst);
        let active = proxy.active_count().await;
        if connects >= 2 || active >= 1 {
            begin_reconnect_seen = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        begin_reconnect_seen,
        "client should begin reconnecting before the second outage"
    );

    // ── Second outage while reconnecting ────────────────────────────────
    let dc2 = disconnect_count.load(Ordering::SeqCst);
    proxy.simulate_server_down().await;

    for _ in 0..40 {
        if disconnect_count.load(Ordering::SeqCst) > dc2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Insert data while doubly-disconnected.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('gap-double', 'g')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert gap-double");

    sleep(Duration::from_millis(900)).await;

    // ── Final recovery ──────────────────────────────────────────────────
    let expected_connects = connect_count.load(Ordering::SeqCst) + 1;
    proxy.simulate_server_up();

    wait_for_reconnect(&client, &connect_count, expected_connects, "double outage final recovery")
        .await;
    assert!(client.is_connected().await, "client should recover after double outage");

    let mut resumed_ids = Vec::<String>::new();
    let mut resumed_seq = Some(resume_from);
    for _ in 0..40 {
        if resumed_ids.iter().any(|id| id == "gap-double") {
            break;
        }
        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut resumed_ids,
                    &mut resumed_seq,
                    Some(resume_from),
                    "reconn-drop gap",
                );
            },
            _ => {},
        }
    }

    assert!(
        resumed_ids.iter().any(|id| id == "gap-double"),
        "should receive gap-double row after double outage"
    );

    // Insert a live row.
    writer
        .execute_query(
            &format!("INSERT INTO {} (id, value) VALUES ('live-after-double', 'l')", table),
            None,
            None,
            None,
        )
        .await
        .expect("insert live-after-double");

    for _ in 0..40 {
        if resumed_ids.iter().any(|id| id == "gap-double")
            && resumed_ids.iter().any(|id| id == "live-after-double")
        {
            break;
        }
        match timeout(Duration::from_millis(2000), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                collect_ids_and_track_seq(
                    &ev,
                    &mut resumed_ids,
                    &mut resumed_seq,
                    Some(resume_from),
                    "reconn-drop resumed",
                );
            },
            _ => {},
        }
    }

    assert!(
        resumed_ids.iter().any(|id| id == "gap-double"),
        "should receive gap-double row after double outage"
    );
    assert!(
        resumed_ids.iter().any(|id| id == "live-after-double"),
        "should receive live-after-double row"
    );
    assert!(
        !resumed_ids.iter().any(|id| id == "pre-1"),
        "pre-1 must NOT be replayed after reconnect"
    );

    let _ = dc2; // suppress warning
    sub.close().await.ok();
    client.disconnect().await;
    proxy.shutdown().await;
}
