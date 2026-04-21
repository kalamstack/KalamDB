use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use kalam_client::KalamLinkClient;
use tokio::sync::{RwLock, Semaphore};

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;
use crate::metrics::BenchmarkDetail;

/// Fires 1000 concurrent SQL SELECT queries at once per iteration to measure
/// RPS degradation and tail latency under extreme concurrency.
pub struct Sql1kUsersBench {
    query_pool: RwLock<Option<Arc<Vec<KalamLinkClient>>>>,
}

impl Default for Sql1kUsersBench {
    fn default() -> Self {
        Self {
            query_pool: RwLock::new(None),
        }
    }
}

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

    fn report_details(&self, _config: &Config) -> Vec<BenchmarkDetail> {
        vec![
            BenchmarkDetail::new("Baseline", "phase-0 performance"),
            BenchmarkDetail::new("Query Class", "concurrent read burst"),
            BenchmarkDetail::new("Dataset", "500 seeded rows"),
            BenchmarkDetail::new("Burst", "1000 concurrent SQL queries"),
            BenchmarkDetail::new(
                "Query Mix",
                "pk lookup, count, selective order-by limit, narrow projection",
            ),
        ]
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

            // Keep the isolated query clients alive for the full benchmark run
            // so repeated iterations do not churn local HTTP sockets.
            let query_pool = Arc::new(build_sql_query_pool(client)?);
            *self.query_pool.write().await = Some(query_pool);

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
            let query_pool = self.query_pool(client).await?;
            let mut handles = Vec::with_capacity(total_queries as usize);
            let max_in_flight = sql_burst_in_flight_limit(client, total_queries);
            let semaphore = Arc::new(Semaphore::new(max_in_flight));

            for i in 0..total_queries {
                let query_pool = query_pool.clone();
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
                    let query_client = select_query_client(query_pool.as_ref(), i);
                    run_sql_with_retry(query_client, &query).await
                }));
            }

            let mut errors = 0u32;
            let mut error_samples = BTreeMap::new();
            for h in handles {
                match h.await {
                    Ok(Ok(_)) => {},
                    Ok(Err(error)) => {
                        errors += 1;
                        record_error_sample(&mut error_samples, &error);
                    },
                    Err(error) => {
                        errors += 1;
                        record_error_sample(
                            &mut error_samples,
                            &format!("Join error: {}", error),
                        );
                    },
                }
            }

            // Allow up to 5% failure rate under extreme load
            let threshold = total_queries / 20;
            if errors > threshold {
                return Err(format!(
                    "{} out of {} queries failed (>{} threshold); sample errors: {}",
                    errors,
                    total_queries,
                    threshold,
                    format_error_samples(&error_samples)
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
            self.query_pool.write().await.take();
            let _ = client.sql(&format!("DROP TABLE IF EXISTS {}.load_1k", config.namespace)).await;
            Ok(())
        })
    }
}

impl Sql1kUsersBench {
    async fn query_pool(&self, client: &KalamClient) -> Result<Arc<Vec<KalamLinkClient>>, String> {
        if let Some(query_pool) = self.query_pool.read().await.clone() {
            return Ok(query_pool);
        }

        let query_pool = Arc::new(build_sql_query_pool(client)?);
        let mut guard = self.query_pool.write().await;
        Ok(guard.get_or_insert_with(|| query_pool.clone()).clone())
    }
}

fn sql_burst_in_flight_limit(client: &KalamClient, total_queries: u32) -> usize {
    if std::env::var("KALAMDB_BENCH_MANAGED_SERVER").ok().as_deref() == Some("1") {
        return std::env::var("KALAMDB_BENCH_SQL_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(128)
            .min(total_queries as usize);
    }

    (client.urls().len().max(1) * 128).min(total_queries as usize)
}

fn build_sql_query_pool(client: &KalamClient) -> Result<Vec<KalamLinkClient>, String> {
    let pool_size = managed_sql_query_pool_size();
    let mut pool = Vec::with_capacity(pool_size);

    for _ in 0..pool_size {
        pool.push(client.new_isolated_link_for_ws_url(None)?);
    }

    Ok(pool)
}

fn managed_sql_query_pool_size() -> usize {
    if std::env::var("KALAMDB_BENCH_MANAGED_SERVER").ok().as_deref() != Some("1") {
        return 1;
    }

    std::env::var("KALAMDB_BENCH_SQL_QUERY_CLIENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8)
}

fn select_query_client(pool: &[KalamLinkClient], index: u32) -> &KalamLinkClient {
    let pool_len = pool.len().max(1);
    &pool[(index as usize) % pool_len]
}

async fn run_sql_with_retry(client: &KalamLinkClient, sql: &str) -> Result<(), String> {
    let mut delay = Duration::from_millis(100);

    for attempt in 0..4 {
        match run_sql_once(client, sql).await {
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

async fn run_sql_once(client: &KalamLinkClient, sql: &str) -> Result<(), String> {
    let response = client
        .execute_query(sql, None, None, None)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;

    if response.success() {
        Ok(())
    } else {
        Err(format!(
            "SQL error: {}",
            response
                .error
                .map(|error| error.message)
                .unwrap_or_else(|| "unknown error".to_string())
        ))
    }
}

fn is_transient_load_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("network error")
        || lower.contains("connection failed")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection reset")
        || lower.contains("cannot assign requested address")
        || lower.contains("connection refused")
        || lower.contains("connection closed")
        || lower.contains("error trying to connect")
        || lower.contains("resource temporarily unavailable")
        || lower.contains("unexpected eof")
        || lower.contains("broken pipe")
}

fn record_error_sample(samples: &mut BTreeMap<String, u32>, error: &str) {
    let summarized = summarize_error(error);
    *samples.entry(summarized).or_insert(0) += 1;
}

fn summarize_error(error: &str) -> String {
    let single_line = error.lines().next().unwrap_or(error).trim();
    if single_line.len() > 160 {
        format!("{}...", &single_line[..160])
    } else {
        single_line.to_string()
    }
}

fn format_error_samples(samples: &BTreeMap<String, u32>) -> String {
    if samples.is_empty() {
        return "none captured".to_string();
    }

    samples
        .iter()
        .take(5)
        .map(|(error, count)| format!("{}x {}", count, error))
        .collect::<Vec<_>>()
        .join(" | ")
}
