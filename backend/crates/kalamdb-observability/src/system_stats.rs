use std::time::Instant;

use crate::{
    cpu::{get_cpu_count, get_physical_cpu_count},
    function_metrics::function_metrics_snapshot,
    health_monitor::HealthMonitor,
    pubsub_metrics::pubsub_metrics_snapshot,
    query_metrics::query_metrics_snapshot,
    runtime_metrics::{
        collect_runtime_metrics, BUILD_DATE, GIT_BRANCH, GIT_COMMIT_HASH, SERVER_VERSION,
    },
    storage_metrics::storage_metrics_snapshot,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMetrics {
    pub cluster_id:       String,
    pub cluster_rpc_addr: String,
    pub cluster_api_addr: String,
    pub user_shards:      u32,
    pub shared_shards:    u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfigMetrics {
    pub node_id: String,
    pub server_workers_configured: usize,
    pub max_connections: usize,
    pub connection_backlog: usize,
    pub worker_max_blocking_threads: usize,
    pub datafusion_query_parallelism: usize,
    pub datafusion_max_partitions: usize,
    pub datafusion_memory_limit_bytes: usize,
    pub cluster: Option<ClusterMetrics>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EntityCounts {
    pub total_users:      usize,
    pub total_namespaces: usize,
    pub total_tables:     usize,
    pub total_jobs:       usize,
    pub jobs_running:     usize,
    pub jobs_queued:      usize,
    pub jobs_failed:      usize,
    pub total_storages:   usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveQueryMetrics {
    pub total_live_queries:         usize,
    pub active_connections:         usize,
    pub active_connections_peak:    usize,
    pub max_connections_configured: usize,
    pub active_subscriptions:       usize,
    pub active_subscriptions_peak:  usize,
    pub websocket_sessions:         usize,
    pub websocket_sessions_peak:    usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMetrics {
    pub schema_cache_size:              usize,
    pub schema_registry_size:           usize,
    pub schema_cache_total_entries:     usize,
    pub plan_cache_size:                Option<usize>,
    pub topic_cache_topic_count:        usize,
    pub topic_cache_table_route_count:  usize,
    pub topic_cache_total_routes:       usize,
    pub topic_consumer_group_count:     usize,
    pub topic_consumer_partition_count: usize,
    pub string_interner_unique_strings: usize,
}

pub trait SystemStatsSource {
    fn server_start_time(&self) -> Instant;
    fn server_config_metrics(&self) -> ServerConfigMetrics;
    fn entity_counts(&self) -> EntityCounts;
    fn storage_stats(&self) -> Vec<(String, String)>;
    fn live_query_metrics(&self) -> LiveQueryMetrics;
    fn cache_metrics(&self) -> CacheMetrics;
}

fn effective_server_workers(configured: usize) -> usize {
    if configured == 0 {
        get_cpu_count().min(8)
    } else {
        configured
    }
}

fn push_metric(metrics: &mut Vec<(String, String)>, name: &str, value: impl ToString) {
    metrics.push((name.to_string(), value.to_string()));
}

pub fn collect_system_stats(source: &impl SystemStatsSource) -> Vec<(String, String)> {
    let mut metrics = Vec::new();

    let runtime = collect_runtime_metrics(source.server_start_time());
    metrics.extend(runtime.as_pairs());
    metrics.extend(query_metrics_snapshot().as_pairs());
    metrics.extend(pubsub_metrics_snapshot().as_pairs());
    metrics.extend(function_metrics_snapshot().as_pairs());
    metrics.extend(storage_metrics_snapshot().as_pairs());

    push_metric(&mut metrics, "cpu_logical_cores", get_cpu_count());
    push_metric(&mut metrics, "cpu_physical_cores", get_physical_cpu_count());

    let server = source.server_config_metrics();
    push_metric(&mut metrics, "server_workers_configured", server.server_workers_configured);
    push_metric(
        &mut metrics,
        "server_workers_effective",
        effective_server_workers(server.server_workers_configured),
    );
    push_metric(&mut metrics, "max_connections", server.max_connections);
    push_metric(&mut metrics, "connection_backlog", server.connection_backlog);
    push_metric(&mut metrics, "worker_max_blocking_threads", server.worker_max_blocking_threads);
    push_metric(
        &mut metrics,
        "datafusion_query_parallelism",
        server.datafusion_query_parallelism,
    );
    push_metric(&mut metrics, "datafusion_max_partitions", server.datafusion_max_partitions);
    push_metric(
        &mut metrics,
        "datafusion_memory_limit_mb",
        server.datafusion_memory_limit_bytes / (1024 * 1024),
    );

    let (open_files_total, open_file_breakdown) = HealthMonitor::collect_open_file_metrics();
    push_metric(&mut metrics, "open_files_total", open_files_total);
    if let Some(breakdown) = open_file_breakdown {
        push_metric(&mut metrics, "open_files_regular", breakdown.regular);
        push_metric(&mut metrics, "open_files_directories", breakdown.directories);
        push_metric(&mut metrics, "open_files_kqueue", breakdown.kqueue);
        push_metric(&mut metrics, "open_files_unix", breakdown.unix);
        push_metric(&mut metrics, "open_files_ipv4", breakdown.ipv4);
        push_metric(&mut metrics, "open_files_other", breakdown.other);
    }

    let counts = source.entity_counts();
    push_metric(&mut metrics, "total_users", counts.total_users);
    push_metric(&mut metrics, "total_namespaces", counts.total_namespaces);
    push_metric(&mut metrics, "total_tables", counts.total_tables);
    push_metric(&mut metrics, "total_jobs", counts.total_jobs);
    push_metric(&mut metrics, "jobs_running", counts.jobs_running);
    push_metric(&mut metrics, "jobs_queued", counts.jobs_queued);
    push_metric(&mut metrics, "jobs_failed", counts.jobs_failed);
    push_metric(&mut metrics, "total_storages", counts.total_storages);

    metrics.extend(source.storage_stats());

    let live = source.live_query_metrics();
    push_metric(&mut metrics, "total_live_queries", live.total_live_queries);
    push_metric(&mut metrics, "active_connections", live.active_connections);
    push_metric(&mut metrics, "active_connections_peak", live.active_connections_peak);
    push_metric(&mut metrics, "max_connections_configured", live.max_connections_configured);
    push_metric(&mut metrics, "active_subscriptions", live.active_subscriptions);
    push_metric(&mut metrics, "active_subscriptions_peak", live.active_subscriptions_peak);
    push_metric(&mut metrics, "websocket_sessions", live.websocket_sessions);
    push_metric(&mut metrics, "websocket_sessions_peak", live.websocket_sessions_peak);

    let cache = source.cache_metrics();
    push_metric(&mut metrics, "schema_cache_size", cache.schema_cache_size);
    push_metric(&mut metrics, "schema_registry_size", cache.schema_registry_size);
    push_metric(&mut metrics, "schema_cache_total_entries", cache.schema_cache_total_entries);
    if let Some(plan_cache_size) = cache.plan_cache_size {
        push_metric(&mut metrics, "plan_cache_size", plan_cache_size);
    }
    push_metric(&mut metrics, "topic_cache_topic_count", cache.topic_cache_topic_count);
    push_metric(
        &mut metrics,
        "topic_cache_table_route_count",
        cache.topic_cache_table_route_count,
    );
    push_metric(&mut metrics, "topic_cache_total_routes", cache.topic_cache_total_routes);
    push_metric(&mut metrics, "topic_consumer_group_count", cache.topic_consumer_group_count);
    push_metric(
        &mut metrics,
        "topic_consumer_partition_count",
        cache.topic_consumer_partition_count,
    );
    push_metric(
        &mut metrics,
        "string_interner_unique_strings",
        cache.string_interner_unique_strings,
    );

    push_metric(&mut metrics, "node_id", server.node_id);
    push_metric(&mut metrics, "server_version", SERVER_VERSION);
    push_metric(&mut metrics, "server_build_date", BUILD_DATE);
    push_metric(&mut metrics, "server_git_branch", GIT_BRANCH);
    push_metric(&mut metrics, "server_git_commit", GIT_COMMIT_HASH);
    push_metric(&mut metrics, "cluster_mode", server.cluster.is_some());

    if let Some(cluster) = server.cluster {
        push_metric(&mut metrics, "cluster_id", cluster.cluster_id);
        push_metric(&mut metrics, "cluster_rpc_addr", cluster.cluster_rpc_addr);
        push_metric(&mut metrics, "cluster_api_addr", cluster.cluster_api_addr);
        push_metric(&mut metrics, "user_shards", cluster.user_shards);
        push_metric(&mut metrics, "shared_shards", cluster.shared_shards);
        push_metric(
            &mut metrics,
            "raft_group_count",
            1usize + cluster.user_shards as usize + cluster.shared_shards as usize,
        );
    }

    metrics
}
