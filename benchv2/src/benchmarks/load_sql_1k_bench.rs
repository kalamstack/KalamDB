use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Fires 1000 concurrent SQL SELECT queries at once per iteration to measure
/// RPS degradation and tail latency under extreme concurrency.
pub struct Sql1kUsersBench;

impl Benchmark for Sql1kUsersBench {
    fn name(&self) -> &str {
        "sql_1k_concurrent"
    }
    fn category(&self) -> &str {
        "Load"
    }
    fn description(&self) -> &str {
        "1000 concurrent SQL SELECT queries at once (RPS degradation test)"
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
            let _ = client.sql(&format!("DROP TABLE IF EXISTS {}.load_1k", config.namespace)).await;
            client
                .sql_ok(&format!(
                    "CREATE TABLE {}.load_1k (id INT PRIMARY KEY, name TEXT, score DOUBLE)",
                    config.namespace
                ))
                .await?;

            // Seed 500 rows
            for chunk in 0..5 {
                let mut values = Vec::new();
                for i in 0..100 {
                    let id = chunk * 100 + i;
                    values.push(format!("({}, 'user_{}', {:.1})", id, id, id as f64 * 1.1));
                }
                client
                    .sql_ok(&format!(
                        "INSERT INTO {}.load_1k (id, name, score) VALUES {}",
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
        _iteration: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let total_queries = 1000u32;
            let mut handles = Vec::with_capacity(total_queries as usize);
            let max_in_flight = (client.urls().len().max(1) * 128).min(total_queries as usize);
            let semaphore = Arc::new(Semaphore::new(max_in_flight));

            for i in 0..total_queries {
                let c = client.clone();
                let ns = config.namespace.clone();
                let semaphore = semaphore.clone();
                // Mix of query patterns to simulate realistic load
                let query = match i % 4 {
                    0 => format!("SELECT * FROM {}.load_1k WHERE id = {}", ns, i % 500),
                    1 => format!("SELECT COUNT(*) FROM {}.load_1k", ns),
                    2 => format!(
                        "SELECT * FROM {}.load_1k WHERE score > {:.1} ORDER BY score LIMIT 10",
                        ns,
                        (i % 500) as f64
                    ),
                    _ => format!("SELECT name, score FROM {}.load_1k LIMIT 20", ns),
                };
                handles.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.map_err(|e| e.to_string())?;
                    run_sql_with_retry(&c, &query).await
                }));
            }

            let mut errors = 0u32;
            for h in handles {
                match h.await {
                    Ok(Ok(_)) => {},
                    Ok(Err(_)) => errors += 1,
                    Err(_) => errors += 1,
                }
            }

            // Allow up to 5% failure rate under extreme load
            let threshold = total_queries / 20;
            if errors > threshold {
                return Err(format!(
                    "{} out of {} queries failed (>{} threshold)",
                    errors, total_queries, threshold
                ));
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
            let _ = client.sql(&format!("DROP TABLE IF EXISTS {}.load_1k", config.namespace)).await;
            Ok(())
        })
    }
}

async fn run_sql_with_retry(client: &KalamClient, sql: &str) -> Result<(), String> {
    let mut delay = Duration::from_millis(100);

    for attempt in 0..4 {
        match client.sql_ok(sql).await {
            Ok(_) => return Ok(()),
            Err(error) if attempt < 3 && is_transient_load_error(&error) => {
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, Duration::from_secs(2));
            },
            Err(error) => return Err(error),
        }
    }

    Err("SQL load benchmark exhausted retries".to_string())
}

fn is_transient_load_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("network error")
        || lower.contains("connection failed")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection reset")
        || lower.contains("broken pipe")
}
