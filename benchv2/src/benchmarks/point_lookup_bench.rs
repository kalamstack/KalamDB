use std::future::Future;
use std::pin::Pin;

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;
use crate::metrics::BenchmarkDetail;

/// Point lookup by primary key from a 10K-row table.
/// Measures the best-case read latency when the key is known.
pub struct PointLookupBench;

const ROWS: u32 = 10_000;

impl Benchmark for PointLookupBench {
    fn name(&self) -> &str {
        "point_lookup"
    }
    fn category(&self) -> &str {
        "Select"
    }
    fn description(&self) -> &str {
        "SELECT by primary key from a 10K-row table (single row lookup)"
    }

    fn report_details(&self, _config: &Config) -> Vec<BenchmarkDetail> {
        vec![
            BenchmarkDetail::new("Baseline", "phase-0 performance"),
            BenchmarkDetail::new("Query Class", "primary-key lookup"),
            BenchmarkDetail::new("Dataset", format!("{} seeded rows", ROWS)),
            BenchmarkDetail::new(
                "Query Shape",
                "SELECT * FROM <ns>.point_lookup WHERE id = ?",
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
            let _ = client
                .sql(&format!("DROP TABLE IF EXISTS {}.point_lookup", config.namespace))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE TABLE {}.point_lookup (id INT PRIMARY KEY, name TEXT, value INT)",
                    config.namespace
                ))
                .await?;

            // Seed 10K rows in batches of 50
            for batch in 0..(ROWS / 50) {
                let mut values = Vec::new();
                for i in 0..50 {
                    let id = batch * 50 + i;
                    values.push(format!("({}, 'name_{}', {})", id, id, id * 3));
                }
                client
                    .sql_ok(&format!(
                        "INSERT INTO {}.point_lookup (id, name, value) VALUES {}",
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
            // Look up a different row each iteration to avoid caching effects
            let target_id = (iteration * 997) % ROWS;
            let resp = client
                .sql_ok(&format!(
                    "SELECT * FROM {}.point_lookup WHERE id = {}",
                    config.namespace, target_id
                ))
                .await?;

            // Verify we got exactly 1 row
            if let Some(result) = resp.results.first() {
                if let Some(rows) = &result.rows {
                    if rows.len() != 1 {
                        return Err(format!(
                            "Expected 1 row for id={}, got {}",
                            target_id,
                            rows.len()
                        ));
                    }
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
                .sql(&format!("DROP TABLE IF EXISTS {}.point_lookup", config.namespace))
                .await;
            Ok(())
        })
    }
}
