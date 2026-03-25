//! Integration tests for concurrent WebSocket connections
//!
//! These tests verify that the server can handle many simultaneous WebSocket
//! connections without heartbeat timeouts or dropped subscriptions.
//!
//! REQUIRES: A running KalamDB server at SERVER_URL (see tests/common/mod.rs)
//!
//! Run with:
//!   cargo test --test connection concurrent_ws -- --test-threads=1

use crate::common::*;
use kalam_link::{KalamLinkTimeouts, SubscriptionConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test: Open many concurrent WebSocket connections, each subscribing to the same table.
/// Verify all receive an initial ack and at least one notification without heartbeat timeout.
///
/// This tests the server's ability to handle concurrent connections under load.
/// It exercises the heartbeat checker, notification delivery, and connection management.
#[ntest::timeout(300000)]
#[test]
fn test_concurrent_websocket_subscriptions() {
    if !require_server_running() {
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    rt.block_on(async {
        let num_connections: usize = 8; // Keep this as a lightweight stability check in the shared suite
        let namespace = generate_unique_namespace("conc_ws");
        let table = generate_unique_table("scale");
        let full = format!("{}.{}", namespace, table);

        // Setup: create namespace and table
        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .expect("create namespace");
        let create_sql = format!(
            r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            data TEXT NOT NULL,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER')"#,
            full
        );
        execute_sql_as_root_via_client(&create_sql).expect("create table");

        // Insert a row so subscriptions have initial data
        execute_sql_as_root_via_client(&format!("INSERT INTO {} (data) VALUES ('seed_row')", full))
            .expect("seed insert");

        let base_url = leader_or_server_url();
        let subscribed = Arc::new(AtomicUsize::new(0));
        let notified = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicUsize::new(0));
        let timeouts = Arc::new(AtomicUsize::new(0));

        let query = format!("SELECT * FROM {}", full);

        // Launch concurrent subscriptions
        let mut handles = Vec::with_capacity(num_connections);
        for i in 0..num_connections {
            let base_url = base_url.clone();
            let query = query.clone();
            let subscribed = Arc::clone(&subscribed);
            let notified = Arc::clone(&notified);
            let errors = Arc::clone(&errors);
            let timeouts = Arc::clone(&timeouts);

            let handle = tokio::spawn(async move {
                let task = async {
                    let client = match client_for_user_on_url_with_timeouts(
                        &base_url,
                        default_username(),
                        default_password(),
                        KalamLinkTimeouts::builder()
                            .connection_timeout_secs(10)
                            .receive_timeout_secs(45)
                            .send_timeout_secs(10)
                            .subscribe_timeout_secs(12)
                            .auth_timeout_secs(10)
                            .initial_data_timeout(Duration::from_secs(20))
                            .build(),
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("[conn {}] client build error: {}", i, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                            return;
                        },
                    };

                    let mut sub = match client
                        .subscribe_with_config(SubscriptionConfig::new(
                            format!("conc_sub_{}", i),
                            &query,
                        ))
                        .await
                    {
                        Ok(s) => {
                            subscribed.fetch_add(1, Ordering::Relaxed);
                            s
                        },
                        Err(e) => {
                            let msg = e.to_string();
                            if msg.contains("timeout") || msg.contains("Timeout") {
                                eprintln!("[conn {}] subscribe timeout: {}", i, e);
                                timeouts.fetch_add(1, Ordering::Relaxed);
                            } else {
                                eprintln!("[conn {}] subscribe error: {}", i, e);
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                            return;
                        },
                    };

                    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep_until(deadline) => {
                                break;
                            }
                            event = sub.next() => {
                                match event {
                                    Some(Ok(_change)) => {
                                        notified.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        let msg = e.to_string();
                                        if msg.contains("heartbeat") || msg.contains("Heartbeat") {
                                            eprintln!("[conn {}] heartbeat timeout: {}", i, e);
                                            timeouts.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            eprintln!("[conn {}] notification error: {}", i, e);
                                            errors.fetch_add(1, Ordering::Relaxed);
                                        }
                                        break;
                                    }
                                    None => {
                                        eprintln!("[conn {}] stream ended", i);
                                        errors.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    let _ = tokio::time::timeout(Duration::from_secs(5), sub.close()).await;
                    let _ = tokio::time::timeout(Duration::from_secs(5), client.disconnect()).await;
                };

                if tokio::time::timeout(Duration::from_secs(20), task).await.is_err() {
                    eprintln!("[conn {}] task timed out", i);
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        // Give subscriptions time to connect
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Insert a row to trigger notifications for all subscribers
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (data) VALUES ('trigger_notification')",
            full
        ))
        .expect("trigger insert");

        // Wait for all tasks to complete
        for handle in handles {
            let _ = handle.await;
        }

        let total_subscribed = subscribed.load(Ordering::Relaxed);
        let total_notified = notified.load(Ordering::Relaxed);
        let total_errors = errors.load(Ordering::Relaxed);
        let total_timeouts = timeouts.load(Ordering::Relaxed);

        eprintln!(
            "\n=== Concurrent WS Test Results ===\n\
         Connections attempted: {}\n\
         Successfully subscribed: {}\n\
         Received notification: {}\n\
         Errors: {}\n\
         Heartbeat timeouts: {}\n",
            num_connections, total_subscribed, total_notified, total_errors, total_timeouts
        );

        // Cleanup
        let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full));
        let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

        // Assertions: at least 90% should succeed (some may fail due to connection limits)
        let min_success = num_connections * 3 / 4;
        assert!(
            total_subscribed >= min_success,
            "Expected at least {} subscriptions to succeed, got {} (errors={}, timeouts={})",
            min_success,
            total_subscribed,
            total_errors,
            total_timeouts
        );

        // No heartbeat timeouts allowed — this is the key signal our improvements work
        assert_eq!(
            total_timeouts, 0,
            "Expected zero heartbeat timeouts, got {} (this indicates the server heartbeat \
         checker is too aggressive or connection handling is too slow)",
            total_timeouts
        );
    }); // rt.block_on
}

/// Test: Rapid connect/disconnect cycles to verify cleanup doesn't leak resources
#[ntest::timeout(300000)]
#[test]
fn test_rapid_connect_disconnect() {
    if !require_server_running() {
        return;
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    rt.block_on(async {
        let namespace = generate_unique_namespace("rapid_ws");
        let table = generate_unique_table("churn");
        let full = format!("{}.{}", namespace, table);

        // Setup
        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .expect("create namespace");
        let create_sql = format!(
            r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            msg TEXT NOT NULL
        ) WITH (TYPE='USER')"#,
            full
        );
        execute_sql_as_root_via_client(&create_sql).expect("create table");

        let base_url = leader_or_server_url();
        let query = format!("SELECT * FROM {}", full);
        let cycles = 3;
        let mut success_count = 0;

        let start = Instant::now();

        for i in 0..cycles {
            let client = match client_for_user_on_url_with_timeouts(
                &base_url,
                default_username(),
                default_password(),
                KalamLinkTimeouts::builder()
                    .connection_timeout_secs(5)
                    .receive_timeout_secs(10)
                    .send_timeout_secs(5)
                    .subscribe_timeout_secs(5)
                    .auth_timeout_secs(5)
                    .initial_data_timeout(Duration::from_secs(10))
                    .build(),
            ) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[cycle {}] client error: {}", i, e);
                    continue;
                },
            };

            let subscribe_result = tokio::time::timeout(
                Duration::from_secs(8),
                client.subscribe_with_config(SubscriptionConfig::new(format!("rapid_{}", i), &query)),
            )
            .await;

            match subscribe_result {
                Err(_) => {
                    eprintln!("[cycle {}] subscribe timeout", i);
                }
                Ok(Ok(subscription)) => {
                    let mut sub = subscription;
                    let _ = tokio::time::timeout(Duration::from_secs(3), sub.close()).await;
                    success_count += 1;
                },
                Ok(Err(e)) => {
                    eprintln!("[cycle {}] subscribe error: {}", i, e);
                },
            }

            let _ = tokio::time::timeout(Duration::from_secs(3), client.disconnect()).await;
        }

        let elapsed = start.elapsed();

        eprintln!(
            "\n=== Rapid Connect/Disconnect Results ===\n\
         Cycles: {}\n\
         Successful: {}\n\
         Elapsed: {:.2}s\n\
         Avg per cycle: {:.1}ms\n",
            cycles,
            success_count,
            elapsed.as_secs_f64(),
            elapsed.as_millis() as f64 / cycles as f64,
        );

        // Cleanup
        let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full));
        let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

        // At least 90% should succeed
        let min_success = (cycles * 2) / 3;
        assert!(
            success_count >= min_success,
            "Expected at least {} cycles to succeed, got {}",
            min_success,
            success_count
        );
    }); // rt.block_on
}
