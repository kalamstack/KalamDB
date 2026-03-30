//! Lightweight slow query logger
//!
//! Logs queries that exceed a configurable threshold to a separate slow.log file.
//! Designed for minimal performance overhead using async file I/O.

use crate::schema_registry::TableType;
use kalamdb_commons::models::{TableName, UserId};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Slow query log entry
#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    pub query: String,
    pub duration_secs: f64,
    pub row_count: usize,
    pub user_id: UserId,
    pub table_type: TableType, //use backend/crates/kalamdb-commons/src/models/schemas/table_type.rs
    pub table_name: Option<TableName>,
    pub timestamp: i64,
}

/// Lightweight slow query logger using async channel
pub struct SlowQueryLogger {
    sender: mpsc::UnboundedSender<SlowQueryEntry>,
    threshold_ms: u64,
}

impl SlowQueryLogger {
    /// Create a new slow query logger
    ///
    /// # Arguments
    /// * `log_path` - Path to slow.log file
    /// * `threshold_ms` - Minimum duration to log in milliseconds (queries faster than this are ignored)
    ///
    /// # Returns
    /// Arc-wrapped logger instance
    pub fn new(log_path: String, threshold_ms: u64) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel::<SlowQueryEntry>();

        // Spawn background task only if a Tokio runtime is available.
        // In unit tests without a runtime, we gracefully degrade by dropping logs.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                if let Some(parent) = Path::new(&log_path).parent() {
                    let _ = std::fs::create_dir_all(parent);
                }

                while let Some(entry) = receiver.recv().await {
                    if let Ok(mut file) =
                        OpenOptions::new().create(true).append(true).open(&log_path)
                    {
                        let timestamp = chrono::DateTime::from_timestamp_millis(entry.timestamp)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                            .unwrap_or_else(|| "unknown".to_string());

                        let table_info = entry
                            .table_name
                            .as_ref()
                            .map(|t| format!("{}", t))
                            .unwrap_or_else(|| "unknown".to_string());

                        let log_line = format!(
                            "[{}] SLOW QUERY - user={}, table={} ({}), duration={:.3}s, rows={}, query={}\n",
                            timestamp,
                            entry.user_id,
                            table_info,
                            entry.table_type,
                            entry.duration_secs,
                            entry.row_count,
                            entry.query.replace('\n', " ").replace('\r', "")
                        );

                        let _ = file.write_all(log_line.as_bytes());
                    }
                }
            });
        } else {
            // No runtime available: drop receiver so sends are no-ops.
            drop(receiver);
        }

        Arc::new(Self {
            sender,
            threshold_ms,
        })
    }

    /// Create a test logger that doesn't spawn background tasks
    ///
    /// This version is safe to use in unit tests that don't have a Tokio runtime.
    /// It creates a channel but doesn't spawn the background task, so logs are
    /// simply dropped (tests don't typically check slow query logs anyway).
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn new_test() -> Self {
        let (sender, _receiver) = mpsc::unbounded_channel::<SlowQueryEntry>();
        // Note: We drop the receiver immediately, so logs will just be discarded
        // This is fine for tests since we don't need actual slow query logging
        Self {
            sender,
            threshold_ms: 1000, // Default 1 second threshold
        }
    }

    /// Log a query if it exceeds the threshold
    ///
    /// This method is designed to be extremely lightweight:
    /// - Early return if query is fast (no allocation)
    /// - Channel send is non-blocking (O(1) operation)
    /// - Actual I/O happens in background task
    ///
    /// # Arguments
    /// * `query` - SQL query text
    /// * `duration_secs` - Query execution time in seconds
    /// * `row_count` - Number of rows returned
    /// * `user_id` - User who executed the query
    /// * `table_type` - Type of table (user/shared/stream/system)
    /// * `table_name` - Optional table name
    pub fn log_if_slow(
        &self,
        query: String,
        duration_secs: f64,
        row_count: usize,
        user_id: UserId,
        table_type: TableType,
        table_name: Option<TableName>,
    ) {
        // Convert duration to milliseconds for comparison
        let duration_ms = (duration_secs * 1000.0) as u64;

        // Fast path: skip if query is faster than threshold (no allocations)
        if duration_ms < self.threshold_ms {
            return;
        }

        // Log to console as warning
        log::warn!(
            "SLOW QUERY: {:.3}s | user={} | table={} ({}) | rows={} | query={}",
            duration_secs,
            user_id,
            table_name
                .as_ref()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            table_type,
            row_count,
            query.chars().take(100).collect::<String>()
        );

        let entry = SlowQueryEntry {
            query,
            duration_secs,
            row_count,
            user_id,
            table_type,
            table_name,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        // Non-blocking send (if channel is full, this is a no-op to avoid backpressure)
        let _ = self.sender.send(entry);
    }

    /// Get the configured threshold in milliseconds
    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ms
    }

    /// Get the configured threshold in seconds (for backwards compatibility)
    pub fn threshold_secs(&self) -> f64 {
        self.threshold_ms as f64 / 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::UserId;

    #[tokio::test]
    async fn test_slow_query_logger_threshold() {
        let logger = SlowQueryLogger::new("/tmp/test_slow.log".to_string(), 1200); // 1.2 seconds

        // Fast query - should not log
        logger.log_if_slow(
            "SELECT * FROM fast_table".to_string(),
            0.5,
            100,
            UserId::new("user1"),
            TableType::User,
            None,
        );

        // Slow query - should log
        logger.log_if_slow(
            "SELECT * FROM slow_table".to_string(),
            2.5,
            1000,
            UserId::new("user1"),
            TableType::Shared,
            None,
        );

        // Wait for background task to process
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check log file exists and contains slow query
        let content = std::fs::read_to_string("/tmp/test_slow.log").unwrap();
        assert!(content.contains("SLOW QUERY"));
        assert!(content.contains("slow_table"));
        assert!(content.contains("duration=2.5"));
        assert!(!content.contains("fast_table"));
    }

    #[test]
    fn test_table_type_display() {
        assert_eq!(format!("{}", TableType::User), "user");
        assert_eq!(format!("{}", TableType::Shared), "shared");
        assert_eq!(format!("{}", TableType::Stream), "stream");
        assert_eq!(format!("{}", TableType::System), "system");
    }
}
