use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use kalam_client::{ChangeEvent, SubscriptionConfig};

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Measures reconnection overhead: disconnect a subscriber and re-subscribe.
/// Tests how quickly the server can re-establish a live query after a subscriber
/// drops and reconnects.
pub struct ReconnectSubscribeBench;

impl Benchmark for ReconnectSubscribeBench {
    fn name(&self) -> &str {
        "reconnect_subscribe"
    }
    fn category(&self) -> &str {
        "Subscribe"
    }
    fn description(&self) -> &str {
        "Disconnect and re-subscribe to a user table (reconnection overhead)"
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
                .sql(&format!("DROP USER TABLE IF EXISTS {}.reconnect_sub", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE USER TABLE {}.reconnect_sub (id INT PRIMARY KEY, data TEXT)",
                    config.namespace
                ))
                .await?;

            // Seed initial data
            let mut values = Vec::new();
            for i in 0..100 {
                values.push(format!("({}, 'initial_{}')", i, i));
            }
            client
                .sql_ok(&format!(
                    "INSERT INTO {}.reconnect_sub (id, data) VALUES {}",
                    config.namespace,
                    values.join(", ")
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
            let sql = format!("SELECT * FROM {}.reconnect_sub", ns);

            // --- First connection: subscribe, drain initial data, disconnect ---
            {
                let sub_id_a = format!("reconn_a_{}", iteration);
                let sub_config = SubscriptionConfig::new(sub_id_a, sql.clone());
                let mut sub = client.subscribe_with_config(sub_config).await?;

                // Drain until ready
                loop {
                    match tokio::time::timeout(Duration::from_secs(10), sub.next()).await {
                        Ok(Some(Ok(event))) => match &event {
                            ChangeEvent::Ack { batch_control, .. }
                            | ChangeEvent::InitialDataBatch { batch_control, .. } => {
                                if batch_control.status == kalam_client::models::BatchStatus::Ready
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

                // Close (simulate disconnection)
                let _ = sub.close().await;
            }

            // Brief yield to let server process the disconnect
            tokio::time::sleep(Duration::from_millis(5)).await;

            // --- Second connection: re-subscribe, verify initial data arrives ---
            {
                let sub_id_b = format!("reconn_b_{}", iteration);
                let sub_config = SubscriptionConfig::new(sub_id_b, sql);
                let mut sub = client.subscribe_with_config(sub_config).await?;

                let mut got_ack_or_data = false;
                loop {
                    match tokio::time::timeout(Duration::from_secs(10), sub.next()).await {
                        Ok(Some(Ok(event))) => match &event {
                            ChangeEvent::Ack { batch_control, .. } => {
                                got_ack_or_data = true;
                                if batch_control.status == kalam_client::models::BatchStatus::Ready
                                    || !batch_control.has_more
                                {
                                    break;
                                }
                            },
                            ChangeEvent::InitialDataBatch { batch_control, .. } => {
                                got_ack_or_data = true;
                                if batch_control.status == kalam_client::models::BatchStatus::Ready
                                    || !batch_control.has_more
                                {
                                    break;
                                }
                            },
                            ChangeEvent::Error { message, .. } => {
                                return Err(format!("Server error on reconnect: {}", message));
                            },
                            _ => break,
                        },
                        _ => break,
                    }
                }

                let _ = sub.close().await;

                if !got_ack_or_data {
                    return Err("No ack/initial data on reconnect".to_string());
                }
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
                .sql(&format!("DROP USER TABLE IF EXISTS {}.reconnect_sub", config.namespace))
                .await;
            Ok(())
        })
    }
}
