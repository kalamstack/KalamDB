use crate::app_context::AppContext;
use crate::error::KalamDbError;
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
    /// Logs a curated summary rendered from the same key/value rows exposed by system.stats.
    pub async fn log_metrics(app_context: Arc<AppContext>) -> Result<(), KalamDbError> {
        let metrics = app_context.compute_metrics_async().await?;
        kalamdb_observability::HealthMonitor::log_system_stats(&metrics);
        Ok(())
    }
}
