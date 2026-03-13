use super::helpers::*;
use crate::common;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_link::SubscriptionConfig;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Three active subscriptions on different tables experience a server bounce
/// (down then up). After recovery ALL three should resume from their respective
/// last-seen seq_ids and only deliver newer rows.
#[tokio::test]
async fn test_proxy_three_subscriptions_resume_after_server_bounce() {
    let writer = match create_test_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test (writer client unavailable): {}", e);
            return;
        },
    };

    let proxy = TcpDisconnectProxy::start(common::server_url()).await;
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
    let table_a = format!("default.bounce3_a_{}", suffix);
    let table_b = format!("default.bounce3_b_{}", suffix);
    let table_c = format!("default.bounce3_c_{}", suffix);

    ensure_table(&writer, &table_a).await;
    ensure_table(&writer, &table_b).await;
    ensure_table(&writer, &table_c).await;

    client.connect().await.expect("connect through proxy");

    let mut sub_a = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("bounce3-a-{}", suffix),
            format!("SELECT id, value FROM {}", table_a),
        ))
        .await
        .expect("subscribe A");
    let mut sub_b = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("bounce3-b-{}", suffix),
            format!("SELECT id, value FROM {}", table_b),
        ))
        .await
        .expect("subscribe B");
    let mut sub_c = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("bounce3-c-{}", suffix),
            format!("SELECT id, value FROM {}", table_c),
        ))
        .await
        .expect("subscribe C");

    // Consume initial events.
    let _ = timeout(TEST_TIMEOUT, sub_a.next()).await;
    let _ = timeout(TEST_TIMEOUT, sub_b.next()).await;
    let _ = timeout(TEST_TIMEOUT, sub_c.next()).await;

    // Insert pre-bounce rows.
    for (table, id) in [
        (&table_a, "pre-a"),
        (&table_b, "pre-b"),
        (&table_c, "pre-c"),
    ] {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'before')", table, id),
                None,
                None,
                None,
            )
            .await
            .expect("insert pre row");
    }

    // Observe pre-bounce rows on all three subscriptions.
    let mut ids_a = Vec::<String>::new();
    let mut ids_b = Vec::<String>::new();
    let mut ids_c = Vec::<String>::new();
    let mut seq_a = None;
    let mut seq_b = None;
    let mut seq_c = None;
    for _ in 0..14 {
        if ids_a.iter().any(|id| id == "pre-a")
            && ids_b.iter().any(|id| id == "pre-b")
            && ids_c.iter().any(|id| id == "pre-c")
        {
            break;
        }
        if !ids_a.iter().any(|id| id == "pre-a") {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_a.next()).await {
                collect_ids_and_track_seq(&ev, &mut ids_a, &mut seq_a, None, "bounce3 pre A");
            }
        }
        if !ids_b.iter().any(|id| id == "pre-b") {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_b.next()).await {
                collect_ids_and_track_seq(&ev, &mut ids_b, &mut seq_b, None, "bounce3 pre B");
            }
        }
        if !ids_c.iter().any(|id| id == "pre-c") {
            if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(900), sub_c.next()).await {
                collect_ids_and_track_seq(&ev, &mut ids_c, &mut seq_c, None, "bounce3 pre C");
            }
        }
    }
    assert!(ids_a.iter().any(|id| id == "pre-a"), "should see pre-a");
    assert!(ids_b.iter().any(|id| id == "pre-b"), "should see pre-b");
    assert!(ids_c.iter().any(|id| id == "pre-c"), "should see pre-c");
    let from_a = query_max_seq(&writer, &table_a).await;
    let from_b = query_max_seq(&writer, &table_b).await;
    let from_c = query_max_seq(&writer, &table_c).await;

    // ── Server bounce: down then up ─────────────────────────────────────
    let dc_before = disconnect_count.load(Ordering::SeqCst);
    proxy.simulate_server_down().await;

    for _ in 0..40 {
        if disconnect_count.load(Ordering::SeqCst) > dc_before {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(disconnect_count.load(Ordering::SeqCst) > dc_before, "disconnect should fire");

    // Insert gap rows while the proxy is down.
    for (table, id) in [
        (&table_a, "gap-a"),
        (&table_b, "gap-b"),
        (&table_c, "gap-c"),
    ] {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'gap')", table, id),
                None,
                None,
                None,
            )
            .await
            .expect("insert gap row");
    }

    // Bring the server back.
    proxy.simulate_server_up();

    for _ in 0..80 {
        if connect_count.load(Ordering::SeqCst) >= 2 && client.is_connected().await {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(client.is_connected().await, "should auto-reconnect");

    // Insert live rows after reconnect.
    for (table, id) in [
        (&table_a, "live-a"),
        (&table_b, "live-b"),
        (&table_c, "live-c"),
    ] {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('{}', 'live')", table, id),
                None,
                None,
                None,
            )
            .await
            .expect("insert live row");
    }

    // Collect resumed events for all three subscriptions.
    let mut resumed_a = Vec::<String>::new();
    let mut resumed_b = Vec::<String>::new();
    let mut resumed_c = Vec::<String>::new();
    let mut rseq_a = Some(from_a);
    let mut rseq_b = Some(from_b);
    let mut rseq_c = Some(from_c);

    for _ in 0..20 {
        if resumed_a.iter().any(|id| id == "gap-a")
            && resumed_a.iter().any(|id| id == "live-a")
            && resumed_b.iter().any(|id| id == "gap-b")
            && resumed_b.iter().any(|id| id == "live-b")
            && resumed_c.iter().any(|id| id == "gap-c")
            && resumed_c.iter().any(|id| id == "live-c")
        {
            break;
        }

        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_a.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut resumed_a,
                &mut rseq_a,
                Some(from_a),
                "bounce3 resumed A",
            );
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_b.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut resumed_b,
                &mut rseq_b,
                Some(from_b),
                "bounce3 resumed B",
            );
        }
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(1200), sub_c.next()).await {
            collect_ids_and_track_seq(
                &ev,
                &mut resumed_c,
                &mut rseq_c,
                Some(from_c),
                "bounce3 resumed C",
            );
        }
    }

    // All three should receive gap + live rows.
    assert!(resumed_a.iter().any(|id| id == "gap-a"), "A should get gap row");
    assert!(resumed_a.iter().any(|id| id == "live-a"), "A should get live row");
    assert!(resumed_b.iter().any(|id| id == "gap-b"), "B should get gap row");
    assert!(resumed_b.iter().any(|id| id == "live-b"), "B should get live row");
    assert!(resumed_c.iter().any(|id| id == "gap-c"), "C should get gap row");
    assert!(resumed_c.iter().any(|id| id == "live-c"), "C should get live row");

    // None should replay pre-bounce rows.
    assert!(!resumed_a.iter().any(|id| id == "pre-a"), "A must NOT replay pre-a");
    assert!(!resumed_b.iter().any(|id| id == "pre-b"), "B must NOT replay pre-b");
    assert!(!resumed_c.iter().any(|id| id == "pre-c"), "C must NOT replay pre-c");

    sub_a.close().await.ok();
    sub_b.close().await.ok();
    sub_c.close().await.ok();
    client.disconnect().await;
    proxy.shutdown().await;
}
