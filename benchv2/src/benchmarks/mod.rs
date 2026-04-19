pub mod bulk_insert_bench;
pub mod concurrent_bench;
pub mod connection_scale_bench;
pub mod ddl_bench;
pub mod delete_bench;
pub mod insert_bench;
pub mod load_connection_storm_bench;
pub mod load_consumer_bench;
pub mod load_create_user_bench;
pub mod load_mixed_rw_bench;
pub mod load_publisher_bench;
pub mod load_sql_1k_bench;
pub mod load_subscriber_bench;
pub mod load_wide_fanout_bench;
pub mod select_bench;
pub mod subscriber_scale_bench;
pub mod update_bench;

// --- New benchmarks ---
pub mod aggregate_query_bench;
pub mod alter_table_bench;
pub mod bulk_delete_bench;
pub mod concurrent_mixed_dml_bench;
pub mod concurrent_update_bench;
pub mod flushed_parquet_query_bench;
pub mod large_payload_insert_bench;
pub mod multi_table_join_bench;
pub mod namespace_isolation_bench;
pub mod point_lookup_bench;
pub mod reconnect_subscribe_bench;
pub mod sequential_crud_bench;
pub mod subscribe_change_latency_bench;
pub mod subscribe_initial_load_bench;
pub mod transaction_multi_insert_bench;
pub mod wide_column_insert_bench;

use std::future::Future;
use std::pin::Pin;

use crate::client::KalamClient;
use crate::config::Config;
use crate::metrics::BenchmarkDetail;

/// Trait that every benchmark must implement.
/// To add a new benchmark: create a file, implement this trait, and register it in `all_benchmarks()`.
pub trait Benchmark: Send + Sync {
    /// Unique name for this benchmark.
    fn name(&self) -> &str;

    /// Category for grouping in reports (e.g. "Insert", "Select").
    fn category(&self) -> &str;

    /// Short human-readable description.
    fn description(&self) -> &str;

    /// Report summary shown in generated artifacts.
    fn report_description(&self, _config: &Config) -> String {
        self.description().to_string()
    }

    /// Full report description shown in tooltips.
    fn report_full_description(&self, config: &Config) -> String {
        self.report_description(config)
    }

    /// Benchmark-specific details shown in generated reports.
    fn report_details(&self, _config: &Config) -> Vec<BenchmarkDetail> {
        Vec::new()
    }

    /// One-time setup (create tables, seed data, etc). Called once before warmup+iterations.
    fn setup<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

    /// The actual operation to benchmark. Called once per iteration.
    fn run<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
        iteration: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

    /// Teardown (drop tables, clean up). Called once after all iterations.
    fn teardown<'a>(
        &'a self,
        client: &'a KalamClient,
        config: &'a Config,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

    /// Scale tests that open many connections should return `true`.
    /// The runner will skip warmup and run exactly 1 iteration.
    fn single_run(&self) -> bool {
        false
    }
}

/// Benchmarks included when no explicit `--bench` or `--filter` selection is provided.
///
/// `connection_scale` remains opt-in because single-host runs often need extra
/// loopback aliases or explicit override flags to avoid macOS ephemeral-port limits.
pub fn enabled_in_default_suite(name: &str) -> bool {
    !matches!(name, "connection_scale")
}

/// Returns all registered benchmarks. Add new benchmarks here.
pub fn all_benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![
        // --- Core operation benchmarks ---
        Box::new(ddl_bench::CreateTableBench {
            counter: std::sync::atomic::AtomicU32::new(0),
        }),
        Box::new(ddl_bench::DropTableBench),
        Box::new(insert_bench::SingleInsertBench),
        Box::new(bulk_insert_bench::BulkInsertBench),
        Box::new(transaction_multi_insert_bench::TransactionMultiInsertBench),
        Box::new(select_bench::SelectAllBench),
        Box::new(select_bench::SelectByFilterBench),
        Box::new(select_bench::SelectCountBench),
        Box::new(select_bench::SelectOrderByBench),
        Box::new(update_bench::SingleUpdateBench),
        Box::new(delete_bench::SingleDeleteBench),
        Box::new(concurrent_bench::ConcurrentInsertBench),
        Box::new(concurrent_bench::ConcurrentSelectBench),
        // --- New benchmarks: queries, DML, DDL ---
        Box::new(point_lookup_bench::PointLookupBench),
        Box::new(aggregate_query_bench::AggregateQueryBench),
        Box::new(multi_table_join_bench::MultiTableJoinBench),
        Box::new(large_payload_insert_bench::LargePayloadInsertBench),
        Box::new(wide_column_insert_bench::WideColumnInsertBench),
        Box::new(bulk_delete_bench::BulkDeleteBench),
        Box::new(sequential_crud_bench::SequentialCrudBench),
        Box::new(alter_table_bench::AlterTableBench {
            counter: std::sync::atomic::AtomicU32::new(0),
        }),
        // --- Concurrent / contention benchmarks ---
        Box::new(concurrent_update_bench::ConcurrentUpdateBench),
        Box::new(concurrent_mixed_dml_bench::ConcurrentMixedDmlBench),
        Box::new(namespace_isolation_bench::NamespaceIsolationBench),
        // --- Subscribe benchmarks ---
        Box::new(subscribe_initial_load_bench::SubscribeInitialLoadBench),
        Box::new(subscribe_change_latency_bench::SubscribeChangeLatencyBench),
        Box::new(reconnect_subscribe_bench::ReconnectSubscribeBench),
        // --- Storage benchmarks ---
        Box::new(flushed_parquet_query_bench::FlushedParquetQueryBench),
        // --- Load / stress benchmarks ---
        Box::new(load_subscriber_bench::ConcurrentSubscriberBench),
        Box::new(load_publisher_bench::ConcurrentPublisherBench),
        Box::new(load_consumer_bench::ConcurrentConsumerBench),
        Box::new(load_sql_1k_bench::Sql1kUsersBench::default()),
        Box::new(load_create_user_bench::CreateUserBench {
            counter: std::sync::atomic::AtomicU64::new(0),
        }),
        Box::new(load_create_user_bench::DropUserBench {
            counter: std::sync::atomic::AtomicU64::new(0),
            created_names: tokio::sync::Mutex::new(Vec::new()),
        }),
        Box::new(load_connection_storm_bench::ConnectionStormBench),
        Box::new(load_mixed_rw_bench::MixedReadWriteBench),
        Box::new(load_wide_fanout_bench::WideFanoutQueryBench),
        // --- Scale tests (run with --iterations 1 --warmup 0 --filter subscriber_scale) ---
        Box::new(connection_scale_bench::ConnectionScaleBench),
        Box::new(subscriber_scale_bench::SubscriberScaleBench::default()),
    ]
}
