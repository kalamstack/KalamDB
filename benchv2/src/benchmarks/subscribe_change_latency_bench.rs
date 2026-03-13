use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kalam_link::{ChangeEvent, SubscriptionConfig};

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Measures the actual latency from INSERT commit to subscriber receiving the change notification.
/// Performs a single subscribe, then times individual writes to measure delivery latency.
pub struct SubscribeChangeLatencyBench;

impl Benchmark for SubscribeChangeLatencyBench {
    fn name(&self) -> &str {
        "subscribe_change_latency"
    }
    fn category(&self) -> &str {
        "Subscribe"
    }
    fn description(&self) -> &str {
        "Latency from INSERT to subscriber receiving the change notification"
    }

    fn setup<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            client
                .sql_ok(&format!("CREATE NAMESPACE IF NOT EXISTS {}", config.namespace))
                .await?;
            let _ = client
                .sql(&format!("DROP USER TABLE IF EXISTS {}.change_latency", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE USER TABLE {}.change_latency (id INT PRIMARY KEY, ts TEXT)",
                    config.namespace
                ))
                .await?;
            Ok(())
        })
    }

    fn run<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
        iteration: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let ns = config.namespace.clone();
            client
                .sql_ok(&format!(
                    "CREATE USER TABLE IF NOT EXISTS {}.change_latency (id INT PRIMARY KEY, ts TEXT)",
                    ns
                ))
                .await?;
            tokio::time::sleep(Duration::from_millis(25)).await;

            let change_received = Arc::new(AtomicU32::new(0));
            let counter = change_received.clone();

            let sub_id = format!("latency_{}", iteration);
            let sql = format!("SELECT * FROM {}.change_latency", ns);
            let sub_config = SubscriptionConfig::new(sub_id, sql);
            let mut sub = client.subscribe_with_config(sub_config).await?;

            // Drain initial data until ready (handled by kalam-link internally)
            loop {
                match tokio::time::timeout(Duration::from_secs(10), sub.next()).await {
                    Ok(Some(Ok(event))) => match &event {
                        ChangeEvent::Ack { batch_control, .. } => {
                            if batch_control.status == kalam_link::models::BatchStatus::Ready {
                                break;
                            }
                        },
                        ChangeEvent::InitialDataBatch { batch_control, .. } => {
                            if batch_control.status == kalam_link::models::BatchStatus::Ready
                                || !batch_control.has_more
                            {
                                break;
                            }
                        },
                        ChangeEvent::Error { message, .. } => {
                            return Err(format!("Server error: {}", message));
                        },
                        _ => break,
                    },
                    _ => break,
                }
            }

            // Spawn a listener task for change events
            let listen_handle = tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, sub.next()).await {
                        Ok(Some(Ok(event))) => {
                            if matches!(event, ChangeEvent::Insert { .. }) {
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                        },
                        _ => break,
                    }
                }
                // Gracefully close the subscription (sends Unsubscribe + WS Close frame)
                let _ = sub.close().await;
            });

            // Brief yield to ensure subscription is fully registered
            tokio::time::sleep(Duration::from_millis(10)).await;

            let write_id = 500_000 + iteration;
            let _start = Instant::now();

            client
                .sql_ok(&format!(
                    "INSERT INTO {}.change_latency (id, ts) VALUES ({}, 'ts_{}')",
                    config.namespace, write_id, iteration
                ))
                .await?;

            // Wait for the change to arrive
            let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                if change_received.load(Ordering::SeqCst) > 0 {
                    break;
                }
                if tokio::time::Instant::now() >= poll_deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Wait for the listener task to finish (bounded by its 5s deadline)
            // instead of aborting, so it can close the WebSocket gracefully
            let _ = tokio::time::timeout(Duration::from_secs(6), listen_handle).await;

            if change_received.load(Ordering::SeqCst) == 0 {
                return Err("Change notification not received within 5s".to_string());
            }

            Ok(())
        })
    }

    fn teardown<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = client
                .sql(&format!("DROP USER TABLE IF EXISTS {}.change_latency", config.namespace))
                .await;
            Ok(())
        })
    }
}
