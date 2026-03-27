use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_link::SubscriptionConfig;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Server goes down while the client is receiving the initial data batch.
/// After the proxy resumes, the subscription should reconnect and deliver the data.
#[tokio::test]
async fn test_proxy_server_down_during_initial_load() {
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
    let table = format!("default.init_load_drop_{}", suffix);

    ensure_table(&writer, &table).await;

    // Seed several rows so the initial batch has content.
    for i in 0..5 {
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('seed-{}', 'val-{}')", table, i, i),
                None,
                None,
                None,
            )
            .await
            .expect("seed insert");
    }

    client.connect().await.expect("connect through proxy");

    let mut sub = client
        .subscribe_with_config(SubscriptionConfig::new(
            format!("init-load-drop-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        ))
        .await
        .expect("subscribe should succeed");

    // Immediately kill the proxy to simulate server going down during
    // the initial load transfer.
    proxy.simulate_server_down().await;

    // The subscription should detect the disconnect.
    let disconnects_before = disconnect_count.load(Ordering::SeqCst);
    let mut seen_ids = HashSet::<String>::new();
    for _ in 0..40 {
        if disconnect_count.load(Ordering::SeqCst) > disconnects_before {
            break;
        }
        // Drain any events that arrived before the disconnect.
        if let Ok(Some(Ok(ev))) = timeout(Duration::from_millis(100), sub.next()).await {
            let mut seq = None;
            let mut ids = Vec::new();
            collect_ids_and_track_seq(&ev, &mut ids, &mut seq, None, "init-load-drop pre");
            seen_ids.extend(ids);
        }
    }

    // Bring the proxy back.
    proxy.simulate_server_up();

    for _ in 0..80 {
        if connect_count.load(Ordering::SeqCst) >= 2 && client.is_connected().await {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(client.is_connected().await, "client should auto-reconnect after proxy resumes");

    // After reconnect the subscription should eventually deliver all seed rows.
    for _ in 0..20 {
        if (0..5).all(|i| seen_ids.contains(&format!("seed-{}", i))) {
            break;
        }
        match timeout(Duration::from_millis(1500), sub.next()).await {
            Ok(Some(Ok(ev))) => {
                let mut _seq = None;
                let mut ids = Vec::new();
                collect_ids_and_track_seq(&ev, &mut ids, &mut _seq, None, "init-load-drop post");
                seen_ids.extend(ids);
            },
            _ => {},
        }
    }

    for i in 0..5 {
        let expected = format!("seed-{}", i);
        assert!(
            seen_ids.contains(&expected),
            "seed row {} should be delivered after reconnect",
            expected
        );
    }

    sub.close().await.ok();
    client.disconnect().await;
    proxy.shutdown().await;
}
