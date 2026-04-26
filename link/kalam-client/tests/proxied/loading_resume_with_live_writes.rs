use std::{collections::HashSet, sync::atomic::Ordering, time::Duration};

use kalam_client::{
    models::BatchStatus, seq_tracking::row_seq, ChangeEvent, SeqId, SubscriptionConfig,
    SubscriptionOptions,
};
use tokio::time::{sleep, timeout, Instant};

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

fn observe_loading_event(
    event: &ChangeEvent,
    seen_ids: &mut HashSet<String>,
    seen_seqs: &mut HashSet<SeqId>,
    max_seq: &mut Option<SeqId>,
    strict_from: Option<SeqId>,
    initial_batch_count: &mut u32,
    highest_batch_num: &mut u32,
    context: &str,
) {
    if let Some(from) = strict_from {
        assert_event_rows_strictly_after(event, from, context);
    }

    if let Some(seq) = event_last_seq(event) {
        *max_seq = Some(max_seq.map_or(seq, |prev| prev.max(seq)));
    }

    if let ChangeEvent::InitialDataBatch { batch_control, .. } = event {
        *initial_batch_count += 1;
        *highest_batch_num = (*highest_batch_num).max(batch_control.batch_num);
    }

    let Some(rows) = change_event_rows(event) else {
        return;
    };

    for row in rows {
        if let Some(id) = row_id(row) {
            assert!(
                seen_ids.insert(id.to_string()),
                "{}: duplicate row id delivered: {}; event={:?}",
                context,
                id,
                event
            );
        }

        if let Some(seq) = row_seq(row) {
            assert!(
                seen_seqs.insert(seq),
                "{}: duplicate _seq delivered: {}; event={:?}",
                context,
                seq,
                event
            );
        }
    }
}

/// Force a long initial snapshot to span many batches, inject new writes while
/// the snapshot is still loading, then drop the connection mid-load. After the
/// reconnect the subscription must continue from the last delivered `_seq`
/// without replaying rows already consumed before the outage.
#[tokio::test]
async fn test_loading_snapshot_with_live_writes_resumes_without_duplicate_rows() {
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
        let table = format!("default.loading_resume_{}", suffix);
        ensure_table(&writer, &table).await;

        for index in 0..30 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('seed-{}', 'seed-value-{}')",
                        table, index, index
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert seed row");
        }

        client.connect().await.expect("connect through proxy");

        let mut config = SubscriptionConfig::new(
            format!("loading-resume-{}", suffix),
            format!("SELECT id, value FROM {}", table),
        );
        config.options = Some(SubscriptionOptions::new().with_batch_size(3));

        let mut sub = client.subscribe_with_config(config).await.expect("subscribe should succeed");

        let ack = timeout(TEST_TIMEOUT, sub.next())
            .await
            .expect("ack wait should not time out")
            .expect("subscription should not end before ack")
            .expect("ack event should not error");

        match ack {
            ChangeEvent::Ack { batch_control, .. } => {
                assert_ne!(
                    batch_control.status,
                    BatchStatus::Ready,
                    "a 30-row snapshot with batch size 3 should not be ready at ack"
                );
            },
            other => panic!("expected ack event, got {:?}", other),
        }

        let mut seen_ids = HashSet::<String>::new();
        let mut seen_seqs = HashSet::<SeqId>::new();
        let mut delivered_seq = None;
        let mut initial_batch_count = 0u32;
        let mut highest_batch_num = 0u32;
        let mut inserted_during_loading = false;

        for _ in 0..20 {
            if inserted_during_loading && initial_batch_count >= 4 {
                break;
            }

            match timeout(Duration::from_millis(1500), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    observe_loading_event(
                        &ev,
                        &mut seen_ids,
                        &mut seen_seqs,
                        &mut delivered_seq,
                        None,
                        &mut initial_batch_count,
                        &mut highest_batch_num,
                        "loading snapshot before outage",
                    );

                    if !inserted_during_loading && initial_batch_count >= 2 {
                        writer
                            .execute_query(
                                &format!(
                                    "INSERT INTO {} (id, value) VALUES ('loading-live-0', \
                                     'during-loading-0'), ('loading-live-1', 'during-loading-1')",
                                    table
                                ),
                                None,
                                None,
                                None,
                            )
                            .await
                            .expect("insert rows during loading");
                        inserted_during_loading = true;
                    }
                },
                Ok(Some(Err(e))) => panic!("loading subscription errored before outage: {}", e),
                Ok(None) => panic!("loading subscription ended before outage"),
                Err(_) => {},
            }
        }

        assert!(
            inserted_during_loading,
            "test should insert additional rows while the snapshot is still loading"
        );
        assert!(
            initial_batch_count >= 4,
            "snapshot should still be mid-load before the forced disconnect"
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
            "forced loading outage should trigger a disconnect"
        );

        for _ in 0..20 {
            match timeout(Duration::from_millis(120), sub.next()).await {
                Ok(Some(Ok(ev))) => observe_loading_event(
                    &ev,
                    &mut seen_ids,
                    &mut seen_seqs,
                    &mut delivered_seq,
                    None,
                    &mut initial_batch_count,
                    &mut highest_batch_num,
                    "loading snapshot local tail",
                ),
                _ => break,
            }
        }

        let resume_from = delivered_seq.expect("should capture delivered _seq before reconnect");

        for index in 0..3 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('gap-{}', 'during-outage-{}')",
                        table, index, index
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert outage row");
        }

        sleep(Duration::from_millis(900)).await;
        proxy.resume();

        let reconnect_deadline = Instant::now() + RECONNECT_WAIT_TIMEOUT;
        while Instant::now() < reconnect_deadline {
            if connect_count.load(Ordering::SeqCst) >= 2 && client.is_connected().await {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(client.is_connected().await, "client should reconnect after loading outage");

        for index in 0..2 {
            writer
                .execute_query(
                    &format!(
                        "INSERT INTO {} (id, value) VALUES ('after-{}', 'after-reconnect-{}')",
                        table, index, index
                    ),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert post-reconnect row");
        }

        for _ in 0..80 {
            let received_all_seed_rows =
                (0..30).all(|index| seen_ids.contains(&format!("seed-{}", index)));
            let received_loading_rows =
                ["loading-live-0", "loading-live-1"].iter().all(|id| seen_ids.contains(*id));
            let received_gap_rows =
                (0..3).all(|index| seen_ids.contains(&format!("gap-{}", index)));
            let received_after_rows =
                (0..2).all(|index| seen_ids.contains(&format!("after-{}", index)));

            if received_all_seed_rows
                && received_loading_rows
                && received_gap_rows
                && received_after_rows
                && initial_batch_count >= 10
            {
                break;
            }

            match timeout(Duration::from_millis(1500), sub.next()).await {
                Ok(Some(Ok(ev))) => observe_loading_event(
                    &ev,
                    &mut seen_ids,
                    &mut seen_seqs,
                    &mut delivered_seq,
                    Some(resume_from),
                    &mut initial_batch_count,
                    &mut highest_batch_num,
                    "loading snapshot resumed",
                ),
                Ok(Some(Err(e))) => panic!("loading subscription errored after reconnect: {}", e),
                Ok(None) => panic!("loading subscription ended after reconnect"),
                Err(_) => {},
            }
        }

        for index in 0..30 {
            assert!(
                seen_ids.contains(&format!("seed-{}", index)),
                "seed row {} should be delivered exactly once",
                index
            );
        }
        assert!(
            seen_ids.contains("loading-live-0") && seen_ids.contains("loading-live-1"),
            "rows inserted during loading should be delivered"
        );
        for index in 0..3 {
            assert!(
                seen_ids.contains(&format!("gap-{}", index)),
                "gap row {} should be delivered after reconnect",
                index
            );
        }
        for index in 0..2 {
            assert!(
                seen_ids.contains(&format!("after-{}", index)),
                "post-reconnect row {} should be delivered",
                index
            );
        }
        assert!(
            initial_batch_count >= 10,
            "30 seed rows with batch size 3 should span at least 10 initial batches, got {}",
            initial_batch_count
        );
        assert!(
            highest_batch_num >= 9,
            "expected to observe at least batch 9, got {}",
            highest_batch_num
        );
        assert_eq!(
            seen_ids.len(),
            30 + 2 + 3 + 2,
            "all snapshot and live rows should be delivered exactly once"
        );
        assert_eq!(
            seen_seqs.len(),
            seen_ids.len(),
            "every delivered row should have a unique _seq"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "loading resume test timed out");
}
