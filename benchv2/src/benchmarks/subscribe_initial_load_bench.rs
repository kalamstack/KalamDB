use std::future::Future;
use std::pin::Pin;

use kalam_client::{ChangeEvent, SubscriptionConfig};

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Measures how quickly a subscriber receives the full initial data batch
/// when subscribing to a user table that already contains 1000 rows.
pub struct SubscribeInitialLoadBench;

const SEED_ROWS: u32 = 1000;

impl Benchmark for SubscribeInitialLoadBench {
    fn name(&self) -> &str {
        "subscribe_initial_load"
    }
    fn category(&self) -> &str {
        "Subscribe"
    }
    fn description(&self) -> &str {
        "Subscribe to a 1000-row user table and receive the full initial data batch"
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
                .sql(&format!("DROP USER TABLE IF EXISTS {}.sub_init_load", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE USER TABLE {}.sub_init_load (id INT PRIMARY KEY, name TEXT, value INT)",
                    config.namespace
                ))
                .await?;

            // Seed 1000 rows in batches of 50
            for batch in 0..(SEED_ROWS / 50) {
                let mut values = Vec::new();
                for i in 0..50 {
                    let id = batch * 50 + i;
                    values.push(format!("({}, 'row_{}', {})", id, id, id * 10));
                }
                client
                    .sql_ok(&format!(
                        "INSERT INTO {}.sub_init_load (id, name, value) VALUES {}",
                        config.namespace,
                        values.join(", ")
                    ))
                    .await?;
            }
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
            let sub_id = format!("init_load_{}", iteration);
            let sql = format!("SELECT * FROM {}.sub_init_load", config.namespace);

            let sub_config = SubscriptionConfig::new(sub_id, sql);
            let mut sub = client.subscribe_with_config(sub_config).await?;

            let mut batches = 0u32;
            let mut got_ready = false;

            // The kalam-link SubscriptionManager handles auth, gzip decompression,
            // and NextBatch pagination internally. We just consume ChangeEvents.
            while let Some(event) = sub.next().await {
                match event {
                    Ok(ChangeEvent::Ack { batch_control, .. }) => {
                        if batch_control.status == kalam_client::models::BatchStatus::Ready
                            && !batch_control.has_more
                        {
                            got_ready = true;
                            break;
                        }
                    },
                    Ok(ChangeEvent::InitialDataBatch { batch_control, .. }) => {
                        batches += 1;
                        if batch_control.status == kalam_client::models::BatchStatus::Ready
                            || !batch_control.has_more
                        {
                            got_ready = true;
                            break;
                        }
                    },
                    Ok(ChangeEvent::Error { message, .. }) => {
                        return Err(format!("Server error: {}", message));
                    },
                    Err(e) => return Err(format!("Subscription error: {}", e)),
                    _ => {},
                }
            }

            let _ = sub.close().await;

            if !got_ready && batches == 0 {
                return Err("No initial data batches received within timeout".to_string());
            }
            if !got_ready {
                return Err(format!("Received {} batches but never got Ready status", batches));
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
                .sql(&format!("DROP USER TABLE IF EXISTS {}.sub_init_load", config.namespace))
                .await;
            Ok(())
        })
    }
}
