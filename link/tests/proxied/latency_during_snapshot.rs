use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use kalam_link::SubscriptionConfig;
use std::time::Duration;
use tokio::time::timeout;

/// Inject high latency while the initial snapshot is being loaded.
/// If latency exceeds `initial_data_timeout` the subscription should fail
/// gracefully; if below the timeout, the snapshot should complete normally.
/// After the latency clears the client must resume and deliver live rows.
#[tokio::test]
async fn test_latency_spike_during_initial_snapshot_recovers() {
    let result = timeout(Duration::from_secs(90), async {
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
        let table = format!("default.latency_snapshot_{}", suffix);
        ensure_table(&writer, &table).await;

        // Seed enough rows to produce multiple batches.
        for i in 0..20 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('seed-{}', 'seed-val-{}')",
                        table, i, i
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert seed row");
        }

        client.connect().await.expect("initial connect");

        // Inject moderate latency BEFORE subscribing — the entire handshake and
        // initial snapshot will run through the slow pipe.
        // 200ms per chunk is enough to feel painful but well under the 20s
        // initial_data_timeout.
        proxy.set_latency(Duration::from_millis(200));

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("latency-snapshot-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe should succeed even under latency");

        // Collect the full initial snapshot despite the latency.
        let mut seen_ids = Vec::<String>::new();
        let mut checkpoint = None;
        for _ in 0..60 {
            if (0..20).all(|i| seen_ids.iter().any(|id| id == &format!("seed-{}", i))) {
                break;
            }
            match timeout(Duration::from_millis(3000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut seen_ids,
                        &mut checkpoint,
                        None,
                        "latency-snapshot loading",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription error during latent snapshot: {}", e),
                Ok(None) => panic!("subscription ended during latent snapshot"),
                Err(_) => {},
            }
        }

        assert!(
            (0..20).all(|i| seen_ids.iter().any(|id| id == &format!("seed-{}", i))),
            "all 20 seed rows should arrive despite latency; got {} ids",
            seen_ids.len()
        );
        let _resume_from = query_max_seq(&writer, &table).await;

        // Clear latency and confirm a live row arrives promptly.
        proxy.clear_latency();

        writer
            .execute_query(
                &format!("INSERT INTO {} (id, value) VALUES ('live-after-latency', 'post')", table),
                None,
                None,
                None,
            )
            .await
            .expect("insert live row after latency clears");

        let mut live_ids = Vec::<String>::new();
        let mut live_seq = None;
        for _ in 0..12 {
            if live_ids.iter().any(|id| id == "live-after-latency") {
                break;
            }
            match timeout(Duration::from_millis(2000), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut live_ids,
                        &mut live_seq,
                        None,
                        "post-latency live",
                    );
                },
                _ => {},
            }
        }

        assert!(
            live_ids.iter().any(|id| id == "live-after-latency"),
            "live row should arrive promptly once latency clears"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "latency during snapshot test timed out");
}
