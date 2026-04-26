use std::{sync::Arc, time::Duration};

use kalamdb_core::{app_context::AppContext, error::KalamDbError};
// Re-export the WebSocket session tracking functions from kalamdb-observability
pub use kalamdb_observability::{
    decrement_websocket_sessions, get_websocket_session_count, idle_duration,
    increment_websocket_sessions, record_activity_now,
};

const IDLE_TRIM_GRACE: Duration = Duration::from_secs(60);

/// Monitor for system health and job statistics
///
/// This is a thin wrapper around kalamdb-observability that collects
/// KalamDB-specific metrics (jobs, namespaces, tables, subscriptions)
/// and delegates to the observability crate for system metrics.
pub struct HealthMonitor;

impl HealthMonitor {
    fn maybe_trim_idle_memory(app_context: &AppContext) {
        let active_connections = app_context.connection_registry().connection_count();
        let active_subscriptions = app_context.connection_registry().subscription_count();
        let ws_sessions = get_websocket_session_count();

        if active_connections > 0 || active_subscriptions > 0 || ws_sessions > 0 {
            return;
        }

        let idle_for = idle_duration().unwrap_or(Duration::MAX);
        if idle_for < IDLE_TRIM_GRACE {
            return;
        }

        let mut cleared_plan_cache = 0usize;
        if let Some(sql_executor) = app_context.try_sql_executor() {
            cleared_plan_cache = sql_executor.plan_cache_len();
            if cleared_plan_cache > 0 {
                sql_executor.clear_plan_cache();
            }
        }

        app_context.connection_registry().trim_idle_capacity();
        kalamdb_observability::force_allocator_collection(true);
        record_activity_now();

        if cleared_plan_cache > 0 {
            log::info!(
                "Idle trim cleared {} SQL plans after {:?} idle",
                cleared_plan_cache,
                idle_for,
            );
        } else {
            log::debug!("Idle trim forced allocator collection after {:?} idle", idle_for);
        }
    }

    /// Log system health metrics for monitoring
    ///
    /// Logs a curated summary rendered from the same key/value rows exposed by system.stats.
    pub async fn log_metrics(app_context: Arc<AppContext>) -> Result<(), KalamDbError> {
        Self::maybe_trim_idle_memory(app_context.as_ref());
        let metrics = app_context.compute_metrics_async().await?;
        kalamdb_observability::HealthMonitor::log_system_stats(&metrics);
        Ok(())
    }
}
