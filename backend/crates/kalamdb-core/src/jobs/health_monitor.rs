use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::jobs::JobsManager;
use kalamdb_system::providers::jobs::models::JobFilter;
use kalamdb_system::JobStatus;
use std::sync::Arc;

// Re-export the WebSocket session tracking functions from kalamdb-observability
pub use kalamdb_observability::{
    decrement_websocket_sessions, get_websocket_session_count, increment_websocket_sessions,
};

/// Monitor for system health and job statistics
///
/// This is a thin wrapper around kalamdb-observability that collects
/// KalamDB-specific metrics (jobs, namespaces, tables, subscriptions)
/// and delegates to the observability crate for system metrics.
pub struct HealthMonitor;

impl HealthMonitor {
    /// Log system health metrics for monitoring
    ///
    /// Logs memory usage, CPU usage, open files count, namespace/table counts, and active job statistics.
    pub async fn log_metrics(
        jobs_manager: &JobsManager,
        app_context: Arc<AppContext>,
    ) -> Result<(), KalamDbError> {
        // Collect system metrics (CPU, memory, open files)
        let (memory_mb, cpu_usage, open_files, open_file_breakdown) =
            kalamdb_observability::HealthMonitor::collect_system_metrics();

        // Get job statistics
        let all_jobs = jobs_manager.list_jobs(JobFilter::default()).await?;
        let running_count = all_jobs.iter().filter(|j| j.status == JobStatus::Running).count();
        let queued_count = all_jobs.iter().filter(|j| j.status == JobStatus::Queued).count();
        let failed_count = all_jobs.iter().filter(|j| j.status == JobStatus::Failed).count();

        // Get namespace and table counts from system tables
        let namespaces_provider = app_context.system_tables().namespaces();
        let tables_provider = app_context.system_tables().tables();

        let namespace_count = namespaces_provider.list_namespaces().map(|ns| ns.len()).unwrap_or(0);
        let table_count = tables_provider.list_tables().map(|ts| ts.len()).unwrap_or(0);
        let storage_partition_count = app_context
            .storage_backend()
            .list_partitions()
            .map(|partitions| partitions.len())
            .ok();

        // Get live subscription stats
        let live_stats = app_context.live_query_manager().get_stats().await;
        let subscription_count = live_stats.total_subscriptions;
        let connection_count = live_stats.total_connections;

        // Build complete metrics snapshot
        let counts = kalamdb_observability::HealthCounts {
            namespace_count,
            table_count,
            storage_partition_count,
            subscription_count,
            connection_count,
            jobs_running: running_count,
            jobs_queued: queued_count,
            jobs_failed: failed_count,
            jobs_total: all_jobs.len(),
        };

        let metrics = kalamdb_observability::HealthMonitor::build_metrics(
            memory_mb,
            cpu_usage,
            open_files,
            open_file_breakdown,
            counts,
        );

        // Log the metrics
        kalamdb_observability::HealthMonitor::log_metrics(&metrics);

        Ok(())
    }
}
