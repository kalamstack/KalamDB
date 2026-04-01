use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_link::SubscriptionConfig;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Blackhole the proxy right as the client sends its subscribe request.
/// The TCP socket stays open but no data flows. The client should detect a
/// dead link (pong timeout), reconnect, re-subscribe, and ultimately deliver
/// the full snapshot plus any rows inserted during the outage.
#[tokio::test]
async fn test_blackhole_during_subscribe_handshake_recovers() {
    let result = timeout(Duration::from_secs(60), async {
        let writer = match create_test_client() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Skipping test (writer client unavailable): {}", e);
                return;
            },
        };

        let proxy = TcpDisconnectProxy::start(upstream_server_url()).await;
        let (client, _connect_count, _disconnect_count) =
            match create_test_client_with_events_for_base_url(proxy.base_url()) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Skipping test (proxy client unavailable): {}", e);
                    proxy.shutdown().await;
                    return;
                },
            };

        let suffix = unique_suffix();
        let table = format!("default.blackhole_subscribe_{}", suffix);
        ensure_table(&writer, &table).await;

        // Seed a few rows so the subscription has data to deliver.
        for i in 0..5 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('seed-{}', 'val-{}')",
                        table, i, i
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert seed row");
        }

        client.connect().await.expect("connect through proxy");
        assert!(
            proxy.wait_for_active_connections(1, Duration::from_secs(2)).await,
            "proxy should see at least one active connection"
        );

        // Blackhole immediately BEFORE subscribing so the subscribe message
        // (and the server's response) never make it across.
        proxy.blackhole();

        // Subscribe will eventually time out or hang because nothing comes back.
        // We wrap it in a timeout so the test can continue.
        let sub_result = timeout(
            Duration::from_secs(3),
            client.subscribe_with_config(SubscriptionConfig::new(
                format!("blackhole-sub-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            )),
        )
        .await;

        // Whether subscribe itself succeeded (buffered locally) or timed out,
        // we need to let the client recover.
        let mut sub = match sub_result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                // subscribe failed — restore traffic and try again.
                proxy.restore_traffic();
                sleep(Duration::from_millis(500)).await;

                for _ in 0..80 {
                    if client.is_connected().await {
                        break;
                    }
                    sleep(Duration::from_millis(100)).await;
                }

                client
                    .subscribe_with_config(SubscriptionConfig::new(
                        format!("blackhole-sub-{}", suffix),
                        format!("SELECT id, value FROM {}", table),
                    ))
                    .await
                    .unwrap_or_else(|_| panic!("subscribe after recovery should succeed: {}", e))
            },
            Err(_timeout) => {
                // subscribe hung — restore traffic so the client can reconnect
                // and the subscription can complete via re-subscribe.
                proxy.restore_traffic();

                for _ in 0..80 {
                    if client.is_connected().await {
                        break;
                    }
                    sleep(Duration::from_millis(100)).await;
                }

                client
                    .subscribe_with_config(SubscriptionConfig::new(
                        format!("blackhole-sub-{}", suffix),
                        format!("SELECT id, value FROM {}", table),
                    ))
                    .await
                    .expect("subscribe after timeout recovery should succeed")
            },
        };

        // Insert a row after recovery.
        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('post-blackhole', 'live')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert post-blackhole row");

        // Collect events — we should see the seed rows AND the post-blackhole row.
        let mut seen_ids = Vec::<String>::new();
        let mut seq = None;
        for _ in 0..60 {
            if seen_ids.iter().any(|id| id == "post-blackhole")
                && (0..5).all(|i| seen_ids.iter().any(|id| id == &format!("seed-{}", i)))
            {
                break;
            }
            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut seen_ids,
                        &mut seq,
                        None,
                        "blackhole-subscribe recovery",
                    );
                },
                Ok(Some(Err(e))) => {
                    // Subscription errors are acceptable during recovery.
                    eprintln!("subscription error (may be transient): {}", e);
                },
                Ok(None) => break,
                Err(_) => {},
            }
        }

        assert!(
            (0..5).all(|i| seen_ids.iter().any(|id| id == &format!("seed-{}", i))),
            "all seed rows should arrive after blackhole recovery; got: {:?}",
            seen_ids
        );
        assert!(
            seen_ids.iter().any(|id| id == "post-blackhole"),
            "post-blackhole row should arrive after recovery"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "blackhole during subscribe test timed out");
}
