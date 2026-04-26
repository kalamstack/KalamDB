use std::{sync::atomic::Ordering, time::Duration};

use kalam_client::{models::BatchStatus, ChangeEvent, SubscriptionConfig};
use tokio::time::{sleep, timeout};

use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;

/// Resume logic should work for update and delete events, not just inserts.
/// Rows updated or deleted before the drop must not replay, while changes made
/// during the outage and after reconnect must still arrive in order.
#[tokio::test]
async fn test_update_and_delete_events_resume_without_replay() {
    let result = timeout(Duration::from_secs(45), async {
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
        let table = format!("default.update_delete_resume_{}", suffix);
        ensure_table(&writer, &table).await;

        for (id, value) in [
            ("update-pre", "before"),
            ("delete-pre", "before"),
            ("update-gap", "before"),
            ("delete-gap", "before"),
            ("update-after", "before"),
            ("delete-after", "before"),
        ] {
            writer
                .execute_query(
                    &format!("INSERT INTO {} (id, value) VALUES ('{}', '{}')", table, id, value),
                    None,
                    None,
                    None,
                )
                .await
                .expect("insert seed row for update/delete test");
        }

        client.connect().await.expect("connect through proxy");

        let mut sub = client
            .subscribe_with_config(SubscriptionConfig::new(
                format!("update-delete-{}", suffix),
                format!("SELECT id, value FROM {}", table),
            ))
            .await
            .expect("subscribe should succeed");

        let drain_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < drain_deadline {
            match timeout(Duration::from_millis(250), sub.next()).await {
                Ok(Some(Ok(ChangeEvent::Ack { batch_control, .. }))) => {
                    if batch_control.status == BatchStatus::Ready {
                        break;
                    }
                },
                Ok(Some(Ok(ChangeEvent::InitialDataBatch { batch_control, .. }))) => {
                    if batch_control.status == BatchStatus::Ready {
                        break;
                    }
                },
                Ok(Some(Ok(_))) => {},
                Ok(Some(Err(e))) => panic!("subscription errored during initial drain: {}", e),
                Ok(None) => panic!("subscription ended during initial drain"),
                Err(_) => {},
            }
        }

        writer
            .execute_query(
                &format!("UPDATE {} SET value = 'updated-pre' WHERE id = 'update-pre'", table),
                None,
                None,
                None,
            )
            .await
            .expect("pre-drop update should succeed");
        writer
            .execute_query(
                &format!("DELETE FROM {} WHERE id = 'delete-pre'", table),
                None,
                None,
                None,
            )
            .await
            .expect("pre-drop delete should succeed");

        let mut pre_ids = Vec::<String>::new();
        let mut pre_seq = None;
        for _ in 0..16 {
            if pre_ids.iter().any(|id| id == "update-pre")
                && pre_ids.iter().any(|id| id == "delete-pre")
            {
                break;
            }

            match timeout(Duration::from_millis(1200), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut pre_ids,
                        &mut pre_seq,
                        None,
                        "update-delete pre",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription errored before drop: {}", e),
                Ok(None) => panic!("subscription ended unexpectedly before drop"),
                Err(_) => {},
            }
        }

        assert!(
            pre_ids.iter().any(|id| id == "update-pre"),
            "pre-drop update event should arrive"
        );
        assert!(
            pre_ids.iter().any(|id| id == "delete-pre"),
            "pre-drop delete event should arrive"
        );

        writer
            .execute_query(
                &format!(
                    "UPDATE {} SET value = 'anchor-after-pre' WHERE id = 'update-after'",
                    table
                ),
                None,
                None,
                None,
            )
            .await
            .expect("checkpoint anchor update should succeed");

        let mut anchor_seen = false;
        for _ in 0..12 {
            if anchor_seen {
                break;
            }

            match timeout(Duration::from_millis(1200), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut pre_ids,
                        &mut pre_seq,
                        None,
                        "update-delete anchor",
                    );
                    anchor_seen = pre_ids.iter().any(|id| id == "update-after");
                },
                Ok(Some(Err(e))) => panic!("subscription errored while observing anchor: {}", e),
                Ok(None) => panic!("subscription ended unexpectedly while observing anchor"),
                Err(_) => {},
            }
        }

        assert!(anchor_seen, "checkpoint anchor update should be observed before drop");

        for _ in 0..8 {
            match timeout(Duration::from_millis(120), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut pre_ids,
                        &mut pre_seq,
                        None,
                        "update-delete pre-drain",
                    );
                },
                _ => break,
            }
        }

        let resume_from = query_max_seq(&writer, &table).await;

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
            "update/delete scenario should trigger a disconnect"
        );

        writer
            .execute_query(
                &format!("UPDATE {} SET value = 'updated-gap' WHERE id = 'update-gap'", table),
                None,
                None,
                None,
            )
            .await
            .expect("gap update should succeed");

        proxy.simulate_server_up();

        for _ in 0..80 {
            if connect_count.load(Ordering::SeqCst) >= 2 && client.is_connected().await {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        assert!(
            client.is_connected().await,
            "client should reconnect for update/delete scenario"
        );

        let mut resumed_ids = Vec::<String>::new();
        let mut resumed_seq = Some(resume_from);
        for _ in 0..16 {
            if resumed_ids.iter().any(|id| id == "update-gap") {
                break;
            }

            match timeout(Duration::from_millis(1200), sub.next()).await {
                Ok(Some(Ok(ev))) => {
                    collect_ids_and_track_seq(
                        &ev,
                        &mut resumed_ids,
                        &mut resumed_seq,
                        Some(resume_from),
                        "update-delete reconnect gap",
                    );
                },
                Ok(Some(Err(e))) => {
                    panic!("subscription errored while waiting for gap update: {}", e)
                },
                Ok(None) => panic!("subscription ended unexpectedly while waiting for gap update"),
                Err(_) => {},
            }
        }

        assert!(
            resumed_ids.iter().any(|id| id == "update-gap"),
            "gap update should be delivered after reconnect resumes"
        );

        writer
            .execute_query(
                &format!("DELETE FROM {} WHERE id = 'delete-gap'", table),
                None,
                None,
                None,
            )
            .await
            .expect("post-reconnect delete-gap should succeed");
        writer
            .execute_query(
                &format!("UPDATE {} SET value = 'updated-after' WHERE id = 'update-after'", table),
                None,
                None,
                None,
            )
            .await
            .expect("post-reconnect update should succeed");
        writer
            .execute_query(
                &format!("DELETE FROM {} WHERE id = 'delete-after'", table),
                None,
                None,
                None,
            )
            .await
            .expect("post-reconnect delete should succeed");

        for _ in 0..24 {
            if resumed_ids.iter().any(|id| id == "update-gap")
                && resumed_ids.iter().any(|id| id == "delete-gap")
                && resumed_ids.iter().any(|id| id == "update-after")
                && resumed_ids.iter().any(|id| id == "delete-after")
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
                        "update-delete resumed",
                    );
                },
                Ok(Some(Err(e))) => panic!("subscription errored after reconnect: {}", e),
                Ok(None) => panic!("subscription ended unexpectedly after reconnect"),
                Err(_) => {},
            }
        }

        assert!(
            resumed_ids.iter().any(|id| id == "update-gap"),
            "gap update should be delivered"
        );
        assert!(
            resumed_ids.iter().any(|id| id == "delete-gap"),
            "gap delete should be delivered"
        );
        assert!(
            resumed_ids.iter().any(|id| id == "update-after"),
            "post-reconnect update should be delivered"
        );
        assert!(
            resumed_ids.iter().any(|id| id == "delete-after"),
            "post-reconnect delete should be delivered"
        );
        assert!(
            !resumed_ids.iter().any(|id| id == "update-pre"),
            "pre-drop update must not replay"
        );
        assert!(
            !resumed_ids.iter().any(|id| id == "delete-pre"),
            "pre-drop delete must not replay"
        );

        sub.close().await.ok();
        client.disconnect().await;
        proxy.shutdown().await;
    })
    .await;

    assert!(result.is_ok(), "update/delete resume test timed out");
}
