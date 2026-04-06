use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use kalam_client::{ChangeEvent, SubscriptionConfig};

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Measures how the server handles N concurrent WebSocket live-query subscribers
/// while writes stream into the watched table.
pub struct ConcurrentSubscriberBench;

impl Benchmark for ConcurrentSubscriberBench {
    fn name(&self) -> &str {
        "concurrent_subscribers"
    }
    fn category(&self) -> &str {
        "Load"
    }
    fn description(&self) -> &str {
        "N WebSocket live-query subscribers receiving changes from concurrent writes"
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
                .sql(&format!("DROP USER TABLE IF EXISTS {}.sub_bench", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE USER TABLE {}.sub_bench (id INT PRIMARY KEY, payload TEXT)",
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
            let num_subscribers = config.concurrency;
            let changes_received = Arc::new(AtomicU32::new(0));
            let mut sub_handles = Vec::new();

            // Launch N subscriber connections via kalam-link
            for sub_id in 0..num_subscribers {
                let link = client.link().clone();
                let ns = config.namespace.clone();
                let counter = changes_received.clone();

                sub_handles.push(tokio::spawn(async move {
                    let sub_name = format!("sub_{}_{}", iteration, sub_id);
                    let sql = format!("SELECT * FROM {}.sub_bench", ns);
                    let sub_config = SubscriptionConfig::new(sub_name, sql);

                    let mut sub = link
                        .subscribe_with_config(sub_config)
                        .await
                        .map_err(|e| format!("Subscribe #{} failed: {}", sub_id, e))?;

                    // Read messages for a short window (initial data + changes)
                    let deadline =
                        tokio::time::Instant::now() + std::time::Duration::from_millis(1500);
                    loop {
                        let remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        if remaining.is_zero() {
                            break;
                        }
                        match tokio::time::timeout(remaining, sub.next()).await {
                            Ok(Some(Ok(event))) => match event {
                                ChangeEvent::Insert { .. }
                                | ChangeEvent::InitialDataBatch { .. } => {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                },
                                _ => {},
                            },
                            _ => break,
                        }
                    }

                    let _ = sub.close().await;
                    Ok::<(), String>(())
                }));
            }

            // Give subscribers a moment to connect, then fire writes
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let mut write_handles = Vec::new();
            let writes_per_iter = 10u32;
            for w in 0..writes_per_iter {
                let c = client.clone();
                let ns = config.namespace.clone();
                let id = iteration * 10_000 + w;
                write_handles.push(tokio::spawn(async move {
                    c.sql_ok(&format!(
                        "INSERT INTO {}.sub_bench (id, payload) VALUES ({}, 'load_{}' )",
                        ns, id, id
                    ))
                    .await
                }));
            }
            for h in write_handles {
                h.await.map_err(|e| format!("Write join: {}", e))??;
            }

            // Wait for subscribers to finish
            for h in sub_handles {
                let _ = h.await;
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
                .sql(&format!("DROP USER TABLE IF EXISTS {}.sub_bench", config.namespace))
                .await;
            Ok(())
        })
    }
}
