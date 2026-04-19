use std::future::Future;
use std::pin::Pin;

use crate::benchmarks::Benchmark;
use crate::client::KalamClient;
use crate::config::Config;

/// Benchmark: explicit transaction containing 50 single-row INSERT statements.
pub struct TransactionMultiInsertBench;

impl Benchmark for TransactionMultiInsertBench {
    fn name(&self) -> &str {
        "transaction_multi_insert"
    }

    fn category(&self) -> &str {
        "Insert"
    }

    fn description(&self) -> &str {
        "Explicit BEGIN/COMMIT with 50 single-row INSERT statements"
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
                .sql(&format!(
                    "DROP TABLE IF EXISTS {}.transaction_multi_insert_bench",
                    config.namespace
                ))
                .await;
            client
                .sql_ok(&format!(
                    "CREATE TABLE {}.transaction_multi_insert_bench (id INT PRIMARY KEY, name TEXT, value DOUBLE)",
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
            let base = iteration * 50;
            let statements: Vec<String> = (0..50)
                .map(|offset| {
                    let id = base + offset;
                    format!(
                        "INSERT INTO {}.transaction_multi_insert_bench (id, name, value) VALUES ({}, 'tx_user_{}', {:.2});",
                        config.namespace,
                        id,
                        id,
                        id as f64 * 0.7
                    )
                })
                .collect();

            client
                .sql_ok(&format!("BEGIN; {} COMMIT;", statements.join(" ")))
                .await?;
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
                .sql(&format!(
                    "DROP TABLE IF EXISTS {}.transaction_multi_insert_bench",
                    config.namespace
                ))
                .await;
            Ok(())
        })
    }
}
