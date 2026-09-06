//! Health monitoring and observability utilities for KalamDB.
//!
//! This crate provides lightweight health monitoring capabilities:
//! - System resource metrics (CPU, memory, open files)
//! - CPU detection and monitoring
//! - WebSocket session tracking
//! - Health metrics collection and reporting
//! - Runtime metrics collection (uptime, memory, CPU, threads)
//!
//! This crate is intentionally minimal to reduce compilation dependencies
//! for the core kalamdb-core crate.

pub mod activity;
pub mod allocator_metrics;
pub mod cpu;
pub mod function_metrics;
pub mod health_monitor;
pub mod pubsub_metrics;
pub mod query_metrics;
pub mod runtime_metrics;
pub mod storage_metrics;
pub mod system_stats;
pub mod trace;

pub use activity::{idle_duration, initialize_activity_now, last_activity_ms, record_activity_now};
pub use allocator_metrics::{
    collect_allocator_metrics, force_allocator_collection, AllocatorMetrics,
};
pub use cpu::{get_cpu_count, get_physical_cpu_count};
pub use function_metrics::{
    begin_function_run, finish_function_run, function_metrics_snapshot, FunctionMetricsSnapshot,
};
pub use health_monitor::{
    decrement_websocket_sessions, get_websocket_session_count, get_websocket_session_peak_count,
    increment_websocket_sessions, HealthCounts, HealthMetrics, HealthMonitor,
};
pub use pubsub_metrics::{
    heartbeat_pubsub_consumer, pubsub_metrics_snapshot, record_pubsub_messages_consumed,
    record_pubsub_messages_published, record_subscription_changes_delivered,
    record_subscription_delivery, track_pubsub_consumer, PubSubConsumerGuard,
    PubSubMetricsSnapshot,
};
pub use query_metrics::{
    observe_query, query_metrics_snapshot, should_observe_query_namespace, QueryMetricKind,
    QueryMetricsSnapshot,
};
pub use runtime_metrics::{
    collect_runtime_metrics, RuntimeMetrics, BUILD_DATE, GIT_BRANCH, GIT_COMMIT_HASH,
    SERVER_VERSION,
};
pub use storage_metrics::{
    decrement_manifest_cache_rocksdb_entries, increment_manifest_cache_rocksdb_entries,
    initialize_manifest_cache_rocksdb_entries, record_manifest_read, record_manifest_write,
    record_parquet_file_read, record_parquet_files_written, set_manifest_cache_memory_entries,
    storage_metrics_snapshot, StorageMetricsSnapshot,
};
pub use system_stats::{
    collect_system_stats, CacheMetrics, ClusterMetrics, EntityCounts, LiveQueryMetrics,
    ServerConfigMetrics, SystemStatsSource,
};
pub use trace::NoopSpanGuard;
