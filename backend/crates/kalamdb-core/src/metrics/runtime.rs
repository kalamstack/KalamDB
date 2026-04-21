// Re-export runtime metrics from kalamdb-observability
pub use kalamdb_observability::{
    collect_runtime_metrics, RuntimeMetrics, BUILD_DATE, GIT_BRANCH, GIT_COMMIT_HASH,
    SERVER_VERSION,
};
use kalamdb_system::JobStatus;

fn effective_server_workers(configured: usize) -> usize {
    if configured == 0 {
        kalamdb_observability::cpu::get_cpu_count().min(8)
    } else {
        configured
    }
}

/// Compute all server metrics from the application context.
///
/// Returns a vector of (metric_name, metric_value) pairs covering:
/// - Runtime metrics (uptime, memory, CPU, threads)
/// - Entity counts (users, namespaces, tables, jobs, storages, live queries)
/// - Connection metrics (active connections, subscriptions)
/// - Schema cache metrics (size, hit rate)
/// - Manifest cache metrics (memory, RocksDB, breakdown)
/// - Server metadata (version, node ID, cluster info)
pub fn compute_metrics(ctx: &crate::app_context::AppContext) -> Vec<(String, String)> {
    let mut metrics = Vec::new();
    let config = ctx.config();

    // Runtime metrics from sysinfo (shared with console logging)
    let runtime = collect_runtime_metrics(ctx.server_start_time());
    metrics.extend(runtime.as_pairs());
    metrics.push((
        "cpu_logical_cores".to_string(),
        kalamdb_observability::cpu::get_cpu_count().to_string(),
    ));
    metrics.push((
        "cpu_physical_cores".to_string(),
        kalamdb_observability::cpu::get_physical_cpu_count().to_string(),
    ));

    let configured_workers = config.server.workers;
    metrics.push(("server_workers_configured".to_string(), configured_workers.to_string()));
    metrics.push((
        "server_workers_effective".to_string(),
        effective_server_workers(configured_workers).to_string(),
    ));
    metrics.push(("max_connections".to_string(), config.performance.max_connections.to_string()));
    metrics.push(("connection_backlog".to_string(), config.performance.backlog.to_string()));
    metrics.push((
        "worker_max_blocking_threads".to_string(),
        config.performance.worker_max_blocking_threads.to_string(),
    ));
    metrics.push((
        "datafusion_query_parallelism".to_string(),
        config.datafusion.query_parallelism.to_string(),
    ));
    metrics.push((
        "datafusion_max_partitions".to_string(),
        config.datafusion.max_partitions.to_string(),
    ));
    metrics.push((
        "datafusion_memory_limit_mb".to_string(),
        (config.datafusion.memory_limit / (1024 * 1024)).to_string(),
    ));

    let (open_files_total, open_file_breakdown) =
        kalamdb_observability::HealthMonitor::collect_open_file_metrics();
    metrics.push(("open_files_total".to_string(), open_files_total.to_string()));
    if let Some(breakdown) = open_file_breakdown {
        metrics.push(("open_files_regular".to_string(), breakdown.regular.to_string()));
        metrics.push(("open_files_directories".to_string(), breakdown.directories.to_string()));
        metrics.push(("open_files_kqueue".to_string(), breakdown.kqueue.to_string()));
        metrics.push(("open_files_unix".to_string(), breakdown.unix.to_string()));
        metrics.push(("open_files_ipv4".to_string(), breakdown.ipv4.to_string()));
        metrics.push(("open_files_other".to_string(), breakdown.other.to_string()));
    }

    // Count entities from system tables
    // Users count
    if let Ok(batch) = ctx.system_tables().users().scan_all_users() {
        metrics.push(("total_users".to_string(), batch.num_rows().to_string()));
    } else {
        metrics.push(("total_users".to_string(), "0".to_string()));
    }

    // Namespaces count
    if let Ok(namespaces) = ctx.system_tables().namespaces().scan_all() {
        metrics.push(("total_namespaces".to_string(), namespaces.len().to_string()));
    } else {
        metrics.push(("total_namespaces".to_string(), "0".to_string()));
    }

    // Tables count
    if let Ok(tables) = ctx.system_tables().tables().scan_all() {
        metrics.push(("total_tables".to_string(), tables.len().to_string()));
    } else {
        metrics.push(("total_tables".to_string(), "0".to_string()));
    }

    metrics.extend(ctx.storage_backend().stats());

    // Jobs count
    if let Ok(jobs) = ctx.system_tables().jobs().list_jobs() {
        let running_jobs = jobs.iter().filter(|job| job.status == JobStatus::Running).count();
        let queued_jobs = jobs.iter().filter(|job| job.status == JobStatus::Queued).count();
        let failed_jobs = jobs.iter().filter(|job| job.status == JobStatus::Failed).count();
        metrics.push(("total_jobs".to_string(), jobs.len().to_string()));
        metrics.push(("jobs_running".to_string(), running_jobs.to_string()));
        metrics.push(("jobs_queued".to_string(), queued_jobs.to_string()));
        metrics.push(("jobs_failed".to_string(), failed_jobs.to_string()));
    } else {
        metrics.push(("total_jobs".to_string(), "0".to_string()));
        metrics.push(("jobs_running".to_string(), "0".to_string()));
        metrics.push(("jobs_queued".to_string(), "0".to_string()));
        metrics.push(("jobs_failed".to_string(), "0".to_string()));
    }

    // Storages count
    if let Ok(batch) = ctx.system_tables().storages().scan_all_storages() {
        metrics.push(("total_storages".to_string(), batch.num_rows().to_string()));
    } else {
        metrics.push(("total_storages".to_string(), "0".to_string()));
    }

    metrics.push((
        "total_live_queries".to_string(),
        ctx.connection_registry().subscription_count().to_string(),
    ));

    // Active WebSocket connections
    let active_connections = ctx.connection_registry().connection_count();
    metrics.push(("active_connections".to_string(), active_connections.to_string()));
    metrics.push((
        "active_connections_peak".to_string(),
        ctx.connection_registry().peak_connection_count().to_string(),
    ));
    metrics.push((
        "max_connections_configured".to_string(),
        ctx.connection_registry().max_connection_limit().to_string(),
    ));

    // Active Subscriptions
    let active_subscriptions = ctx.connection_registry().subscription_count();
    metrics.push(("active_subscriptions".to_string(), active_subscriptions.to_string()));
    metrics.push((
        "active_subscriptions_peak".to_string(),
        ctx.connection_registry().peak_subscription_count().to_string(),
    ));
    metrics.push((
        "websocket_sessions".to_string(),
        kalamdb_observability::get_websocket_session_count().to_string(),
    ));
    metrics.push((
        "websocket_sessions_peak".to_string(),
        kalamdb_observability::get_websocket_session_peak_count().to_string(),
    ));

    // Schema cache size and stats
    let cache_size = ctx.schema_registry().len();
    metrics.push(("schema_cache_size".to_string(), cache_size.to_string()));

    // Schema registry size (returns usize now, not a tuple)
    let registry_size = ctx.schema_registry().stats();
    metrics.push(("schema_registry_size".to_string(), registry_size.to_string()));
    metrics.push((
        "schema_cache_total_entries".to_string(),
        ctx.schema_registry().total_len().to_string(),
    ));

    if let Some(sql_executor) = ctx.try_sql_executor() {
        metrics.push(("plan_cache_size".to_string(), sql_executor.plan_cache_len().to_string()));
    }

    let topic_cache_stats = ctx.topic_publisher().cache_stats();
    metrics
        .push(("topic_cache_topic_count".to_string(), topic_cache_stats.topic_count.to_string()));
    metrics.push((
        "topic_cache_table_route_count".to_string(),
        topic_cache_stats.table_route_count.to_string(),
    ));
    metrics.push((
        "topic_cache_total_routes".to_string(),
        topic_cache_stats.total_routes.to_string(),
    ));

    metrics.push((
        "string_interner_unique_strings".to_string(),
        kalamdb_commons::helpers::string_interner::stats().unique_strings.to_string(),
    ));

    // Node ID
    metrics.push(("node_id".to_string(), ctx.node_id().to_string()));

    // Server build/version
    metrics.push(("server_version".to_string(), SERVER_VERSION.to_string()));
    metrics.push(("server_build_date".to_string(), BUILD_DATE.to_string()));
    metrics.push(("server_git_branch".to_string(), GIT_BRANCH.to_string()));
    metrics.push(("server_git_commit".to_string(), GIT_COMMIT_HASH.to_string()));

    // Cluster info
    metrics.push(("cluster_mode".to_string(), config.cluster.is_some().to_string()));
    if let Some(cluster) = &config.cluster {
        metrics.push(("cluster_id".to_string(), cluster.cluster_id.clone()));
        metrics.push(("cluster_rpc_addr".to_string(), cluster.rpc_addr.clone()));
        metrics.push(("cluster_api_addr".to_string(), cluster.api_addr.clone()));
        metrics.push(("user_shards".to_string(), cluster.user_shards.to_string()));
        metrics.push(("shared_shards".to_string(), cluster.shared_shards.to_string()));
        metrics.push((
            "raft_group_count".to_string(),
            (1usize + cluster.user_shards as usize + cluster.shared_shards as usize).to_string(),
        ));
    }

    metrics
}
