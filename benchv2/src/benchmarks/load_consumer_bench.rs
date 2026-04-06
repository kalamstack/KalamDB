use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use kalam_client::AutoOffsetReset;

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Measures throughput of N concurrent topic CONSUME requests.
pub struct ConcurrentConsumerBench;

impl Benchmark for ConcurrentConsumerBench {
    fn name(&self) -> &str {
        "concurrent_consumers"
    }
    fn category(&self) -> &str {
        "Load"
    }
    fn description(&self) -> &str {
        "N concurrent topic CONSUME calls pulling messages in parallel"
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
                .sql(&format!("DROP TABLE IF EXISTS {}.consume_bench", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE TABLE {}.consume_bench (id INT PRIMARY KEY, val TEXT)",
                    config.namespace
                ))
                .await?;

            // Topic names must be namespace-qualified: <namespace>.<topic>
            let topic_name = format!("{}.consume_topic", config.namespace);
            // Ensure clean topic
            let _ = client.sql(&format!("DROP TOPIC IF EXISTS {}", topic_name)).await;
            client.sql_ok(&format!("CREATE TOPIC {}", topic_name)).await?;
            client
                .sql_ok(&format!(
                    "ALTER TOPIC {} ADD SOURCE {}.consume_bench ON INSERT",
                    topic_name, config.namespace
                ))
                .await?;

            // Seed data to produce topic messages
            let mut values = Vec::new();
            for i in 0..200 {
                values.push(format!("({}, 'seed_{}')", i, i));
            }
            client
                .sql_ok(&format!(
                    "INSERT INTO {}.consume_bench (id, val) VALUES {}",
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
            let conc = config.concurrency;
            let topic_name = format!("{}.consume_topic", config.namespace);
            let mut handles = Vec::new();

            for i in 0..conc {
                let link = client.link().clone();
                let topic = topic_name.clone();
                let group = format!("bench_group_{}_{}", iteration, i);
                handles.push(tokio::spawn(async move {
                    let mut consumer = link
                        .consumer()
                        .group_id(&group)
                        .topic(&topic)
                        .auto_offset_reset(AutoOffsetReset::Earliest)
                        .max_poll_records(50)
                        .poll_timeout(Duration::from_secs(5))
                        .build()
                        .map_err(|e| format!("Consumer build: {}", e))?;

                    let _records = consumer
                        .poll_with_timeout(Duration::from_secs(5))
                        .await
                        .map_err(|e| format!("Consumer poll: {}", e))?;

                    consumer.close().await.map_err(|e| format!("Consumer close: {}", e))?;

                    Ok::<(), String>(())
                }));
            }

            for h in handles {
                h.await.map_err(|e| format!("Join: {}", e))??;
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
            let topic_name = format!("{}.consume_topic", config.namespace);
            let _ = client.sql(&format!("DROP TOPIC IF EXISTS {}", topic_name)).await;
            let _ = client
                .sql(&format!("DROP TABLE IF EXISTS {}.consume_bench", config.namespace))
                .await;
            Ok(())
        })
    }
}
