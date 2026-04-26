//! system.settings virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides server configuration settings as a queryable table with columns:
//! - name: Setting name (e.g., "server.host", "storage.data_path")
//! - value: Current setting value as string
//! - description: Human-readable description of the setting
//! - category: Setting category (e.g., "server", "storage", "limits")
//!
//! **DataFusion Pattern**: Implements VirtualView trait for consistent view behavior
//! Memoizes schema via `OnceLock`.
//!
//! **Schema**: TableDefinition provides consistent metadata for views

use std::sync::{Arc, OnceLock};

use datafusion::arrow::{
    array::{ArrayRef, StringBuilder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, TableName,
};
use kalamdb_configs::ServerConfig;
use kalamdb_system::SystemTable;
use parking_lot::RwLock;

use crate::view_base::VirtualView;

/// Get the settings schema (memoized)
fn settings_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            SettingsView::definition()
                .to_arrow_schema()
                .expect("Failed to convert settings TableDefinition to Arrow schema")
        })
        .clone()
}

/// Macro to add multiple settings in a concise way
/// Usage: add_settings!(builders, [ (name, value, description, category), ... ])
macro_rules! add_settings {
    ($names:expr, $values:expr, $descs:expr, $cats:expr, [ $( ($n:expr, $v:expr, $d:expr, $c:expr) ),* $(,)? ]) => {
        $(
            $names.append_value($n);
            $values.append_value(&$v.to_string());
            $descs.append_value($d);
            $cats.append_value($c);
        )*
    };
}

/// Virtual view that displays server configuration settings
///
/// **DataFusion Design**:
/// - Implements VirtualView trait
/// - Returns TableType::View
/// - Computes batch dynamically from ServerConfig
#[derive(Debug)]
pub struct SettingsView {
    config: Arc<RwLock<Option<ServerConfig>>>,
}

impl SettingsView {
    /// Get the TableDefinition for system.settings view
    ///
    /// Schema:
    /// - name TEXT NOT NULL (setting name)
    /// - value TEXT NOT NULL (current value)
    /// - description TEXT NOT NULL (human-readable description)
    /// - category TEXT NOT NULL (setting category)
    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "name",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Setting name (e.g., server.host, storage.data_path)".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "value",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Current setting value".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "description",
                3,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Human-readable description of the setting".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "category",
                4,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Setting category (server, storage, limits, etc.)".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Settings.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Server configuration settings (read-only view)".to_string()),
        )
        .expect("Failed to create system.settings view definition")
    }

    /// Create a new settings view
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new settings view with config
    pub fn with_config(config: ServerConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(Some(config))),
        }
    }
}

impl Default for SettingsView {
    fn default() -> Self {
        Self::new()
    }
}

impl VirtualView for SettingsView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Settings
    }

    fn schema(&self) -> SchemaRef {
        settings_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, crate::error::RegistryError> {
        let mut names = StringBuilder::new();
        let mut values = StringBuilder::new();
        let mut descriptions = StringBuilder::new();
        let mut categories = StringBuilder::new();

        let config_guard = self.config.read();

        if let Some(config) = config_guard.as_ref() {
            // Server Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    ("server.host", config.server.host, "Server bind address", "server"),
                    ("server.port", config.server.port, "Server port number", "server"),
                    (
                        "server.workers",
                        config.server.workers,
                        "Number of worker threads (0 = auto-detect CPU cores)",
                        "server"
                    ),
                    (
                        "server.enable_http2",
                        config.server.enable_http2,
                        "Enable HTTP/2 protocol support",
                        "server"
                    ),
                    (
                        "server.public_origin",
                        config.server.effective_public_origin(),
                        "Public origin used by the Admin UI for API and WebSocket traffic",
                        "server"
                    ),
                    (
                        "server.api_version",
                        config.server.api_version,
                        "API version prefix (e.g., v1)",
                        "server"
                    ),
                ]
            );

            // Cluster Settings (optional)
            if let Some(cluster) = &config.cluster {
                add_settings!(
                    names,
                    values,
                    descriptions,
                    categories,
                    [
                        (
                            "storage.data_path",
                            config.storage.data_path,
                            "Base data directory (auto-creates rocksdb/, storage/, snapshots/ \
                             subdirs)",
                            "storage"
                        ),
                        (
                            "cluster.cluster_id",
                            cluster.cluster_id,
                            "Unique cluster identifier",
                            "cluster"
                        ),
                    ]
                );
            }

            // Storage Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "storage.data_path",
                        config.storage.data_path,
                        "Base data directory (auto-creates rocksdb/, storage/, snapshots/ subdirs)",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.system_meta.write_buffer_size",
                        config.storage.rocksdb.cf_profiles.system_meta.write_buffer_size,
                        "Write buffer size for low-write system metadata column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.system_meta.max_write_buffers",
                        config.storage.rocksdb.cf_profiles.system_meta.max_write_buffers,
                        "Maximum number of write buffers for system metadata column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.system_index.write_buffer_size",
                        config.storage.rocksdb.cf_profiles.system_index.write_buffer_size,
                        "Write buffer size for system secondary index column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.system_index.max_write_buffers",
                        config.storage.rocksdb.cf_profiles.system_index.max_write_buffers,
                        "Maximum number of write buffers for system secondary indexes",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.hot_data.write_buffer_size",
                        config.storage.rocksdb.cf_profiles.hot_data.write_buffer_size,
                        "Write buffer size for user/shared/stream/topic data column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.hot_data.max_write_buffers",
                        config.storage.rocksdb.cf_profiles.hot_data.max_write_buffers,
                        "Maximum number of write buffers for hot data column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.hot_index.write_buffer_size",
                        config.storage.rocksdb.cf_profiles.hot_index.write_buffer_size,
                        "Write buffer size for PK and vector index column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.hot_index.max_write_buffers",
                        config.storage.rocksdb.cf_profiles.hot_index.max_write_buffers,
                        "Maximum number of write buffers for hot index column families",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.raft.write_buffer_size",
                        config.storage.rocksdb.cf_profiles.raft.write_buffer_size,
                        "Write buffer size for the raft_data column family",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.cf_profiles.raft.max_write_buffers",
                        config.storage.rocksdb.cf_profiles.raft.max_write_buffers,
                        "Maximum number of write buffers for the raft_data column family",
                        "storage"
                    ),
                    (
                        "storage.rocksdb.block_cache_size",
                        config.storage.rocksdb.block_cache_size,
                        "Block cache size for reads in bytes",
                        "storage"
                    ),
                ]
            );

            // DataFusion Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "datafusion.memory_limit",
                        config.datafusion.memory_limit,
                        "Memory limit for query execution in bytes",
                        "datafusion"
                    ),
                    (
                        "datafusion.batch_size",
                        config.datafusion.batch_size,
                        "Batch size for query processing",
                        "datafusion"
                    ),
                    (
                        "datafusion.query_parallelism",
                        config.datafusion.query_parallelism,
                        "Number of parallel threads for query execution",
                        "datafusion"
                    ),
                    (
                        "datafusion.max_partitions",
                        config.datafusion.max_partitions,
                        "Maximum number of partitions per query",
                        "datafusion"
                    ),
                ]
            );

            // Limits Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "limits.max_message_size",
                        config.limits.max_message_size,
                        "Maximum message size for REST API requests",
                        "limits"
                    ),
                    (
                        "limits.max_query_limit",
                        config.limits.max_query_limit,
                        "Maximum rows returned in a single query",
                        "limits"
                    ),
                    (
                        "limits.default_query_limit",
                        config.limits.default_query_limit,
                        "Default LIMIT for queries",
                        "limits"
                    ),
                ]
            );

            // Logging Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    ("logging.level", config.logging.level, "Log level filter", "logging"),
                    ("logging.format", config.logging.format, "Log format (text, json)", "logging"),
                    (
                        "logging.logs_path",
                        config.logging.logs_path,
                        "Directory for log files",
                        "logging"
                    ),
                ]
            );

            // Flush Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "flush.default_row_limit",
                        config.flush.default_row_limit,
                        "Default row limit for flush policies",
                        "flush"
                    ),
                    (
                        "flush.default_time_interval",
                        config.flush.default_time_interval,
                        "Default time interval for flush (seconds)",
                        "flush"
                    ),
                    (
                        "flush.check_interval_seconds",
                        config.flush.check_interval_seconds,
                        "How often the background scheduler checks for pending flushes (seconds, \
                         0 = disabled)",
                        "flush"
                    ),
                ]
            );

            // Manifest Cache Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "manifest_cache.max_entries",
                        config.manifest_cache.max_entries,
                        "Maximum manifest cache entries",
                        "manifest_cache"
                    ),
                    (
                        "manifest_cache.eviction_ttl_days",
                        config.manifest_cache.eviction_ttl_days,
                        "Cache eviction TTL in days",
                        "manifest_cache"
                    ),
                ]
            );

            // Retention Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [(
                    "retention.default_deleted_retention_hours",
                    config.retention.default_deleted_retention_hours,
                    "Default retention hours for soft-deleted rows",
                    "retention"
                ),]
            );

            // Stream Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "stream.default_ttl_seconds",
                        config.stream.default_ttl_seconds,
                        "Default TTL for stream rows (seconds)",
                        "stream"
                    ),
                    (
                        "stream.default_max_buffer",
                        config.stream.default_max_buffer,
                        "Default maximum buffer size",
                        "stream"
                    ),
                    (
                        "stream.eviction_interval_seconds",
                        config.stream.eviction_interval_seconds,
                        "Stream eviction check interval (seconds)",
                        "stream"
                    ),
                ]
            );

            // WebSocket Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "websocket.client_timeout_secs",
                        config.websocket.client_timeout_secs.unwrap_or(10),
                        "WebSocket client timeout (seconds)",
                        "websocket"
                    ),
                    (
                        "websocket.heartbeat_interval_secs",
                        config.websocket.heartbeat_interval_secs.unwrap_or(5),
                        "WebSocket heartbeat interval (seconds)",
                        "websocket"
                    ),
                ]
            );

            // Rate Limit Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "rate_limit.max_queries_per_sec",
                        config.rate_limit.max_queries_per_sec,
                        "Maximum SQL queries per second per user",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.max_messages_per_sec",
                        config.rate_limit.max_messages_per_sec,
                        "Maximum WebSocket messages per second per connection",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.max_subscriptions_per_user",
                        config.rate_limit.max_subscriptions_per_user,
                        "Maximum concurrent live query subscriptions",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.max_auth_requests_per_ip_per_sec",
                        config.rate_limit.max_auth_requests_per_ip_per_sec,
                        "Maximum auth requests per second per IP (applies to /auth/login, \
                         /auth/refresh, /setup)",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.max_connections_per_ip",
                        config.rate_limit.max_connections_per_ip,
                        "Maximum concurrent connections per IP address",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.max_requests_per_ip_per_sec",
                        config.rate_limit.max_requests_per_ip_per_sec,
                        "Maximum requests per second per IP BEFORE authentication (if exceeded → \
                         IP BAN)",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.request_body_limit_bytes",
                        config.rate_limit.request_body_limit_bytes,
                        "Maximum request body size in bytes",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.ban_duration_seconds",
                        config.rate_limit.ban_duration_seconds,
                        "Duration in seconds to ban abusive IPs (default: 300 = 5 minutes)",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.enable_connection_protection",
                        config.rate_limit.enable_connection_protection,
                        "Enable connection protection middleware",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.cache_max_entries",
                        config.rate_limit.cache_max_entries,
                        "Maximum cached entries for rate limiting state",
                        "rate_limit"
                    ),
                    (
                        "rate_limit.cache_ttl_seconds",
                        config.rate_limit.cache_ttl_seconds,
                        "Time-to-idle for cached entries in seconds",
                        "rate_limit"
                    ),
                ]
            );

            // Auth Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "auth.bcrypt_cost",
                        config.auth.bcrypt_cost,
                        "Bcrypt cost factor for password hashing",
                        "auth"
                    ),
                    (
                        "auth.min_password_length",
                        config.auth.min_password_length,
                        "Minimum password length",
                        "auth"
                    ),
                ]
            );

            // Jobs Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "jobs.max_concurrent",
                        config.jobs.max_concurrent,
                        "Maximum number of concurrent jobs",
                        "jobs"
                    ),
                    (
                        "jobs.max_retries",
                        config.jobs.max_retries,
                        "Maximum retry attempts per job",
                        "jobs"
                    ),
                    (
                        "jobs.wal_cleanup_interval_seconds",
                        config.jobs.wal_cleanup_interval_seconds,
                        "Interval for periodic RocksDB WAL cleanup flushes",
                        "jobs"
                    ),
                ]
            );

            // Execution Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "execution.handler_timeout_seconds",
                        config.execution.handler_timeout_seconds,
                        "Handler execution timeout in seconds",
                        "execution"
                    ),
                    (
                        "execution.max_parameters",
                        config.execution.max_parameters,
                        "Maximum number of parameters per statement",
                        "execution"
                    ),
                ]
            );

            // Security Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [
                    (
                        "security.max_request_body_size",
                        config.security.max_request_body_size,
                        "Maximum request body size in bytes",
                        "security"
                    ),
                    (
                        "security.max_ws_message_size",
                        config.security.max_ws_message_size,
                        "Maximum WebSocket message size in bytes",
                        "security"
                    ),
                ]
            );

            // Shutdown Settings
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [(
                    "shutdown.flush.timeout",
                    config.shutdown.flush.timeout,
                    "Timeout in seconds to wait for flush jobs during shutdown",
                    "shutdown"
                ),]
            );
        } else {
            // No config available yet
            add_settings!(
                names,
                values,
                descriptions,
                categories,
                [("status", "not_initialized", "Configuration not yet loaded", "system"),]
            );
        }

        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(names.finish()) as ArrayRef,
                Arc::new(values.finish()) as ArrayRef,
                Arc::new(descriptions.finish()) as ArrayRef,
                Arc::new(categories.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            crate::error::RegistryError::Other(format!("Failed to build settings batch: {}", e))
        })
    }
}

// Re-export as SettingsTableProvider for consistency
pub type SettingsTableProvider = crate::view_base::ViewTableProvider<SettingsView>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_view_schema() {
        let view = SettingsView::new();
        let schema = view.schema();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(2).name(), "description");
        assert_eq!(schema.field(3).name(), "category");
    }

    #[test]
    fn test_settings_view_without_config() {
        let view = SettingsView::new();
        let batch = view.compute_batch().unwrap();

        // Should have one row with "not_initialized" status
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_settings_view_with_config() {
        let config = ServerConfig::default();
        let view = SettingsView::with_config(config);
        let batch = view.compute_batch().unwrap();

        // Should have multiple settings rows
        assert!(batch.num_rows() > 10);
    }
}
