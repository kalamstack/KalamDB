use super::defaults::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSettings,
    pub storage: StorageSettings,
    pub limits: LimitsSettings,
    pub logging: LoggingSettings,
    pub performance: PerformanceSettings,
    #[serde(default)]
    pub datafusion: DataFusionSettings,
    #[serde(default)]
    pub flush: FlushSettings,
    #[serde(default)]
    pub manifest_cache: ManifestCacheSettings,
    #[serde(default)]
    pub retention: RetentionSettings,
    #[serde(default)]
    pub stream: StreamSettings,
    #[serde(default)]
    pub websocket: WebSocketSettings,
    #[serde(default)]
    pub rate_limit: RateLimitSettings,
    #[serde(default, alias = "authentication")]
    pub auth: AuthSettings,
    #[serde(default)]
    pub oauth: OAuthSettings,
    #[serde(default)]
    pub user_management: UserManagementSettings,
    #[serde(default)]
    pub shutdown: ShutdownSettings,
    #[serde(default)]
    pub jobs: JobsSettings,
    #[serde(default)]
    pub execution: ExecutionSettings,
    #[serde(default)]
    pub security: SecuritySettings,
    #[serde(default)]
    pub files: FileUploadSettings,
    #[serde(default)]
    pub cluster: Option<super::cluster::ClusterConfig>,
    /// Unified TLS/mTLS configuration for the gRPC listener.
    /// Serves both cluster inter-node traffic and PG extension connections.
    /// When absent, falls back to `cluster.rpc_tls` for backward compatibility.
    #[serde(default)]
    pub rpc_tls: super::rpc_tls::RpcTlsConfig,
}

/// CORS configuration that maps directly to actix-cors options
/// See: https://docs.rs/actix-cors/latest/actix_cors/struct.Cors.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsSettings {
    /// Allowed origins. Use ["*"] for any origin, or specify exact origins.
    /// Example: ["https://app.example.com", "https://admin.example.com"]
    /// Empty list = same as ["*"] (allow any origin)
    #[serde(default)]
    pub allowed_origins: Vec<String>,

    /// Allowed HTTP methods. Default: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
    #[serde(default = "default_cors_methods")]
    pub allowed_methods: Vec<String>,

    /// Allowed HTTP headers. Use ["*"] for any header.
    /// Default: ["Authorization", "Content-Type", "Accept", "Origin", "X-Requested-With"]
    #[serde(default = "default_cors_headers")]
    pub allowed_headers: Vec<String>,

    /// Headers to expose to the browser. Default: []
    #[serde(default)]
    pub expose_headers: Vec<String>,

    /// Allow credentials (cookies, authorization headers). Default: true
    #[serde(default = "default_true")]
    pub allow_credentials: bool,

    /// Preflight cache max age in seconds. Default: 3600 (1 hour)
    #[serde(default = "default_cors_max_age")]
    pub max_age: u64,

    /// Allow requests without Origin header. Default: false
    #[serde(default)]
    pub allow_private_network: bool,
}

fn default_cors_methods() -> Vec<String> {
    vec![
        "GET".to_string(),
        "POST".to_string(),
        "PUT".to_string(),
        "DELETE".to_string(),
        "PATCH".to_string(),
        "OPTIONS".to_string(),
    ]
}

fn default_cors_headers() -> Vec<String> {
    vec![
        "Authorization".to_string(),
        "Content-Type".to_string(),
        "Accept".to_string(),
        "Origin".to_string(),
        "X-Requested-With".to_string(),
    ]
}

fn default_cors_max_age() -> u64 {
    3600 // 1 hour
}

impl Default for CorsSettings {
    fn default() -> Self {
        Self {
            // SECURITY: Default to empty which means allow-any-origin.
            // When allow_credentials is true and origins is empty, the CORS
            // middleware will NOT reflect arbitrary origins — actix-cors
            // rejects allow_any_origin+supports_credentials at startup.
            // Users must configure explicit origins for credentialed requests.
            allowed_origins: Vec::new(),
            allowed_methods: default_cors_methods(),
            allowed_headers: default_cors_headers(),
            expose_headers: Vec::new(),
            // SECURITY: Default to false to prevent unsafe wildcard+credentials.
            // Users must explicitly enable credentials AND configure allowed_origins.
            allow_credentials: false,
            max_age: default_cors_max_age(),
            allow_private_network: false,
        }
    }
}

/// Security settings for CORS, WebSocket, and request limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySettings {
    /// CORS configuration
    #[serde(default)]
    pub cors: CorsSettings,

    /// Direct peer IPs or CIDR ranges allowed to supply proxy headers such as
    /// X-Forwarded-For and X-Real-IP.
    #[serde(default)]
    pub trusted_proxy_ranges: Vec<String>,

    /// Allowed WebSocket origins for connection validation
    /// If empty, falls back to CORS allowed_origins
    #[serde(default)]
    pub allowed_ws_origins: Vec<String>,

    /// Maximum WebSocket message size in bytes (default: 1MB)
    #[serde(default = "default_max_ws_message_size")]
    pub max_ws_message_size: usize,

    /// Enable strict origin checking for WebSocket (reject requests with no Origin header)
    #[serde(default)]
    pub strict_ws_origin_check: bool,

    /// Maximum request body size in bytes (default: 10MB)
    #[serde(default = "default_max_request_body_size")]
    pub max_request_body_size: usize,
}

impl Default for SecuritySettings {
    fn default() -> Self {
        Self {
            cors: CorsSettings::default(),
            trusted_proxy_ranges: Vec::new(),
            allowed_ws_origins: Vec::new(),
            max_ws_message_size: default_max_ws_message_size(),
            strict_ws_origin_check: false,
            max_request_body_size: default_max_request_body_size(),
        }
    }
}

/// File upload settings for FILE datatype
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUploadSettings {
    /// Maximum file size in bytes (default: 25MB)
    #[serde(default = "default_file_max_size_bytes")]
    pub max_size_bytes: usize,

    /// Maximum number of files per upload request (default: 20)
    #[serde(default = "default_file_max_files_per_request")]
    pub max_files_per_request: usize,

    /// Maximum number of files per subfolder before rotation (default: 5000)
    #[serde(default = "default_file_max_files_per_folder")]
    pub max_files_per_folder: u32,

    /// Staging directory for temp file uploads (default: "./data/tmp")
    #[serde(default = "default_file_staging_path")]
    pub staging_path: String,

    /// Allowed MIME types for file uploads.
    /// Empty list = allow all types.
    /// Supports wildcards like "image/*" for all image types.
    /// Example: ["image/*", "application/pdf", "text/plain"]
    #[serde(default = "default_file_allowed_mime_types")]
    pub allowed_mime_types: Vec<String>,
}

impl Default for FileUploadSettings {
    fn default() -> Self {
        Self {
            max_size_bytes: default_file_max_size_bytes(),
            max_files_per_request: default_file_max_files_per_request(),
            max_files_per_folder: default_file_max_files_per_folder(),
            staging_path: default_file_staging_path(),
            allowed_mime_types: default_file_allowed_mime_types(),
        }
    }
}

/// Server settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// API version prefix for endpoints (default: "v1")
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Enable HTTP/2 protocol support (default: true)
    /// When true, server uses bind_auto_h2c() for automatic HTTP/1.1 and HTTP/2 cleartext negotiation
    /// When false, server only supports HTTP/1.1
    #[serde(default = "default_enable_http2")]
    pub enable_http2: bool,
    /// Path to the Admin UI static files (e.g., "./ui/dist")
    /// When set, the server will serve the UI at /ui route
    /// Set to None/null to disable UI serving
    #[serde(default = "default_ui_path")]
    pub ui_path: Option<String>,
}

/// Storage settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    /// Base data directory for all KalamDB storage
    /// Default: "./data"
    /// Subdirectories are automatically created:
    /// - {data_path}/rocksdb - RocksDB hot storage
    /// - {data_path}/storage - Parquet cold storage
    /// - {data_path}/snapshots - Raft snapshots
    #[serde(default = "default_data_path")]
    pub data_path: String,
    /// Template for shared table paths (placeholders: {namespace}, {tableName})
    #[serde(default = "default_shared_tables_template")]
    pub shared_tables_template: String,
    /// Template for user table paths (placeholders: {namespace}, {tableName}, {userId})
    #[serde(default = "default_user_tables_template")]
    pub user_tables_template: String,
    #[serde(default)]
    pub rocksdb: RocksDbSettings,
    /// Remote storage timeout settings (S3, GCS, Azure)
    #[serde(default)]
    pub remote_timeouts: RemoteStorageTimeouts,
}

impl StorageSettings {
    /// Get RocksDB directory path (data_path/rocksdb)
    pub fn rocksdb_dir(&self) -> std::path::PathBuf {
        let base = crate::file_helpers::normalize_dir_path(&self.data_path);
        crate::file_helpers::join_path(base, "rocksdb")
    }

    /// Get Parquet storage directory path (data_path/storage)
    pub fn storage_dir(&self) -> std::path::PathBuf {
        let base = crate::file_helpers::normalize_dir_path(&self.data_path);
        crate::file_helpers::join_path(base, "storage")
    }

    /// Get stream log directory path (data_path/streams)
    pub fn streams_dir(&self) -> std::path::PathBuf {
        let base = crate::file_helpers::normalize_dir_path(&self.data_path);
        crate::file_helpers::join_path(base, "streams")
    }

    /// Get Raft snapshots directory path (data_path/snapshots)
    pub fn resolved_snapshots_dir(&self) -> std::path::PathBuf {
        let base = crate::file_helpers::normalize_dir_path(&self.data_path);
        crate::file_helpers::join_path(base, "snapshots")
    }

    /// Get user exports directory path (data_path/exports)
    pub fn exports_dir(&self) -> std::path::PathBuf {
        let base = crate::file_helpers::normalize_dir_path(&self.data_path);
        crate::file_helpers::join_path(base, "exports")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbCfProfileSettings {
    /// Write buffer size per column family in bytes.
    pub write_buffer_size: usize,

    /// Maximum number of memtables RocksDB may keep per CF before stalling.
    pub max_write_buffers: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbCfProfilesSettings {
    /// System metadata tables and compatibility partitions.
    #[serde(default = "default_rocksdb_system_meta_profile")]
    pub system_meta: RocksDbCfProfileSettings,

    /// Secondary indexes for system tables.
    #[serde(default = "default_rocksdb_system_index_profile")]
    pub system_index: RocksDbCfProfileSettings,

    /// User/shared/stream data partitions and topic message payloads.
    #[serde(default = "default_rocksdb_hot_data_profile")]
    pub hot_data: RocksDbCfProfileSettings,

    /// PK and vector indexes on user-facing tables.
    #[serde(default = "default_rocksdb_hot_index_profile")]
    pub hot_index: RocksDbCfProfileSettings,

    /// Raft log/state partition.
    #[serde(default = "default_rocksdb_raft_profile")]
    pub raft: RocksDbCfProfileSettings,
}

impl Default for RocksDbCfProfileSettings {
    fn default() -> Self {
        default_rocksdb_hot_index_profile()
    }
}

impl Default for RocksDbCfProfilesSettings {
    fn default() -> Self {
        Self {
            system_meta: default_rocksdb_system_meta_profile(),
            system_index: default_rocksdb_system_index_profile(),
            hot_data: default_rocksdb_hot_data_profile(),
            hot_index: default_rocksdb_hot_index_profile(),
            raft: default_rocksdb_raft_profile(),
        }
    }
}

/// RocksDB-specific settings (MEMORY OPTIMIZED DEFAULTS)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbSettings {
    /// Per-profile tuning for different categories of column families.
    #[serde(default)]
    pub cf_profiles: RocksDbCfProfilesSettings,

    /// Block cache size for reads in bytes (default: 4MB, SHARED across all CFs)
    /// Reduced from 256MB. This cache is shared, so adding CFs doesn't multiply memory.
    #[serde(default = "default_rocksdb_block_cache_size")]
    pub block_cache_size: usize,

    /// Maximum number of background jobs (default: 4)
    #[serde(default = "default_rocksdb_max_background_jobs")]
    pub max_background_jobs: i32,

    /// Maximum number of open files RocksDB can keep open (default: 512)
    /// Set to -1 for unlimited. Lower values reduce memory usage but may impact performance.
    /// If you see "Too many open files" errors, increase your OS limits or reduce this value.
    #[serde(default = "default_rocksdb_max_open_files")]
    pub max_open_files: i32,

    /// Sync writes to WAL on each write (default: false for performance)
    /// When false, writes are buffered and synced periodically by OS.
    /// Setting to true guarantees durability but reduces write throughput 10-100x.
    /// Data is still safe with WAL enabled - only ~1 second of data could be lost on crash.
    #[serde(default = "default_rocksdb_sync_writes")]
    pub sync_writes: bool,

    /// Disable WAL for maximum write performance (default: false)
    /// WARNING: Setting to true means data loss on crash. Only for ephemeral/cacheable data.
    #[serde(default)]
    pub disable_wal: bool,

    /// Compact all column families on startup (default: true)
    /// This reduces the number of SST files and prevents "Too many open files" errors.
    /// May increase startup time for large databases.
    #[serde(default = "default_rocksdb_compact_on_startup")]
    pub compact_on_startup: bool,
}

impl Default for RocksDbSettings {
    fn default() -> Self {
        Self {
            cf_profiles: RocksDbCfProfilesSettings::default(),
            block_cache_size: default_rocksdb_block_cache_size(),
            max_background_jobs: default_rocksdb_max_background_jobs(),
            max_open_files: default_rocksdb_max_open_files(),
            sync_writes: default_rocksdb_sync_writes(),
            disable_wal: false,
            compact_on_startup: default_rocksdb_compact_on_startup(),
        }
    }
}

/// Remote storage timeout settings for S3, GCS, Azure backends
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStorageTimeouts {
    /// Request timeout in seconds for remote storage operations (default: 60s)
    /// Applies to read, write, list, and delete operations
    #[serde(default = "default_remote_request_timeout")]
    pub request_timeout_secs: u64,

    /// Connect timeout in seconds for establishing connections (default: 10s)
    /// Lower than request timeout to fail fast on connection issues
    #[serde(default = "default_remote_connect_timeout")]
    pub connect_timeout_secs: u64,
}

impl Default for RemoteStorageTimeouts {
    fn default() -> Self {
        Self {
            request_timeout_secs: default_remote_request_timeout(),
            connect_timeout_secs: default_remote_connect_timeout(),
        }
    }
}

/// Limits settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsSettings {
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    #[serde(default = "default_max_query_limit")]
    pub max_query_limit: usize,
    #[serde(default = "default_query_limit")]
    pub default_query_limit: usize,
}

impl Default for LimitsSettings {
    fn default() -> Self {
        Self {
            max_message_size: default_max_message_size(),
            max_query_limit: default_max_query_limit(),
            default_query_limit: default_query_limit(),
        }
    }
}

/// Logging settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingSettings {
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Directory for all log files (default: "./logs")
    /// Used for app.log, slow.log, and other log files
    #[serde(default = "default_logs_path")]
    pub logs_path: String,
    #[serde(default = "default_true")]
    pub log_to_console: bool,
    #[serde(default = "default_log_format")]
    pub format: String,
    /// Optional per-target log level overrides (e.g., datafusion="info", arrow="warn")
    /// Configure via a TOML table:
    /// [logging.targets]
    /// datafusion = "info"
    /// arrow = "warn"
    /// parquet = "warn"
    #[serde(default)]
    pub targets: HashMap<String, String>,
    /// Slow query logging threshold in milliseconds (default: 1000ms = 1 second)
    /// Queries taking longer than this threshold will be logged to slow.log
    #[serde(default = "default_slow_query_threshold_ms")]
    pub slow_query_threshold_ms: u64,
    /// OpenTelemetry OTLP export settings (Jaeger/Tempo/Collector)
    #[serde(default)]
    pub otlp: OtlpSettings,
}

/// OpenTelemetry OTLP trace export settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpSettings {
    /// Enable OTLP trace exporting
    #[serde(default = "default_otlp_enabled")]
    pub enabled: bool,
    /// OTLP endpoint.
    /// - gRPC: "http://127.0.0.1:4317"
    /// - HTTP: "http://127.0.0.1:4318" ("/v1/traces" appended automatically)
    #[serde(default = "default_otlp_endpoint")]
    pub endpoint: String,
    /// Protocol: "grpc" or "http"
    #[serde(default = "default_otlp_protocol")]
    pub protocol: String,
    /// Service name shown in Jaeger
    #[serde(default = "default_otlp_service_name")]
    pub service_name: String,
    /// Export timeout in milliseconds
    #[serde(default = "default_otlp_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for OtlpSettings {
    fn default() -> Self {
        Self {
            enabled: default_otlp_enabled(),
            endpoint: default_otlp_endpoint(),
            protocol: default_otlp_protocol(),
            service_name: default_otlp_service_name(),
            timeout_ms: default_otlp_timeout_ms(),
        }
    }
}

/// Performance settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSettings {
    #[serde(default = "default_request_timeout")]
    pub request_timeout: u64,
    #[serde(default = "default_keepalive_timeout")]
    pub keepalive_timeout: u64,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Backlog size for pending connections (default: 2048)
    /// Increase for high-traffic servers
    #[serde(default = "default_backlog")]
    pub backlog: u32,
    /// Number of tokio runtime worker threads (default: 0 = auto, uses num_cpus capped at 4)
    /// Lower values reduce idle RSS from thread stacks (~2MB per thread).
    /// Set to 0 for auto-detection, or an explicit count for Docker/constrained environments.
    #[serde(default)]
    pub tokio_worker_threads: usize,
    /// Max blocking threads per worker for CPU-intensive operations (default: 32)
    /// Used for RocksDB and other synchronous operations
    #[serde(default = "default_worker_max_blocking_threads")]
    pub worker_max_blocking_threads: usize,
    /// Client request timeout in seconds (default: 5)
    /// Time allowed for client to send complete request headers
    #[serde(default = "default_client_request_timeout")]
    pub client_request_timeout: u64,
    /// Client disconnect timeout in seconds (default: 2)
    /// Time allowed for graceful connection shutdown
    #[serde(default = "default_client_disconnect_timeout")]
    pub client_disconnect_timeout: u64,
    /// Maximum HTTP header size in bytes (default: 16384 = 16KB)
    /// Increase if you have large JWT tokens or custom headers
    #[serde(default = "default_max_header_size")]
    pub max_header_size: usize,
}

/// DataFusion settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFusionSettings {
    /// Memory limit for query execution in bytes (default: 256MB)
    #[serde(default = "default_datafusion_memory_limit")]
    pub memory_limit: usize,

    /// Number of parallel threads for query execution (default: number of CPU cores)
    #[serde(default = "default_datafusion_parallelism")]
    pub query_parallelism: usize,

    /// Maximum number of partitions per query (default: 8)
    #[serde(default = "default_datafusion_max_partitions")]
    pub max_partitions: usize,

    /// Batch size for record processing (default: 2048)
    #[serde(default = "default_datafusion_batch_size")]
    pub batch_size: usize,
}

/// Flush policy defaults
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushSettings {
    /// Default row limit for flush (default: 10000 rows)
    #[serde(default = "default_flush_row_limit")]
    pub default_row_limit: usize,

    /// Default time interval for flush in seconds (default: 300s = 5 minutes)
    #[serde(default = "default_flush_time_interval")]
    pub default_time_interval: u64,

    /// Batch size for flush operations (default: 10000 rows)
    /// Controls how many rows are loaded into memory at once during flush
    /// Lower values reduce memory usage but may increase flush duration
    #[serde(default = "default_flush_batch_size")]
    pub flush_batch_size: usize,

    /// How often (in seconds) the background scheduler checks for tables with
    /// pending writes and creates flush jobs (default: 60s). Set to 0 to disable.
    #[serde(default = "default_flush_check_interval")]
    pub check_interval_seconds: u64,
}

/// Manifest cache settings (Phase 4 - US6)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCacheSettings {
    /// Eviction job interval in seconds (default: 300s = 5 minutes)
    #[serde(default = "default_manifest_cache_eviction_interval")]
    pub eviction_interval_seconds: i64,

    /// Maximum number of cached manifest entries (default: 500)
    #[serde(default = "default_manifest_cache_max_entries")]
    pub max_entries: usize,

    /// TTL in days for manifest eviction (default: 7 days)
    /// Manifests not accessed for this many days will be removed from cache
    #[serde(default = "default_manifest_cache_eviction_ttl_days")]
    pub eviction_ttl_days: u64,

    /// Weight factor for user table manifests (default: 10)
    /// User tables are evicted N times faster than shared tables.
    /// Set to 1 to treat all tables equally.
    /// Higher values give stronger preference to keeping shared tables in memory.
    #[serde(default = "default_user_table_weight_factor")]
    pub user_table_weight_factor: u32,
}

impl ManifestCacheSettings {
    /// Get TTL in seconds (converts eviction_ttl_days to seconds)
    pub fn ttl_seconds(&self) -> i64 {
        (self.eviction_ttl_days * 24 * 60 * 60) as i64
    }

    /// Get TTL in milliseconds (converts eviction_ttl_days to milliseconds)
    pub fn ttl_millis(&self) -> i64 {
        self.ttl_seconds() * 1000
    }
}

impl Default for ManifestCacheSettings {
    fn default() -> Self {
        Self {
            eviction_interval_seconds: default_manifest_cache_eviction_interval(),
            max_entries: default_manifest_cache_max_entries(),
            eviction_ttl_days: default_manifest_cache_eviction_ttl_days(),
            user_table_weight_factor: default_user_table_weight_factor(),
        }
    }
}

/// Retention policy defaults
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionSettings {
    /// Default retention hours for soft-deleted rows (default: 168 hours = 7 days)
    #[serde(default = "default_deleted_retention_hours")]
    pub default_deleted_retention_hours: i32,

    /// Enable periodic dba.stats collection (default: true)
    /// When false, the background stats recorder is not started, saving memory
    /// and CPU in resource-constrained environments (e.g. Docker containers).
    #[serde(default = "default_true")]
    pub enable_dba_stats: bool,

    /// Number of days to preserve dba.stats samples (default: 7 days)
    /// Set to 0 to disable automatic cleanup.
    #[serde(default = "default_dba_stats_retention_days")]
    pub dba_stats_retention_days: u64,
}

/// Stream table defaults
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSettings {
    /// Default TTL for stream table rows in seconds (default: 10 seconds)
    #[serde(default = "default_stream_ttl")]
    pub default_ttl_seconds: u64,

    /// Default maximum buffer size for stream tables (default: 10000 rows)
    #[serde(default = "default_stream_max_buffer")]
    pub default_max_buffer: usize,

    /// Stream eviction interval in seconds (default: 60 seconds = 1 minute)
    /// How often to check and evict expired events from stream tables
    #[serde(default = "default_stream_eviction_interval")]
    pub eviction_interval_seconds: u64,
}

/// WebSocket settings for connection management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketSettings {
    /// Client heartbeat timeout in seconds (default: 10)
    /// How long to wait for client pong before disconnecting
    #[serde(default = "default_websocket_client_timeout")]
    pub client_timeout_secs: Option<u64>,

    /// Authentication timeout in seconds (default: 3)
    /// How long to wait for auth message before disconnecting
    #[serde(default = "default_websocket_auth_timeout")]
    pub auth_timeout_secs: Option<u64>,

    /// Heartbeat check interval in seconds (default: 5)
    /// How often the shared heartbeat manager checks all connections
    #[serde(default = "default_websocket_heartbeat_interval")]
    pub heartbeat_interval_secs: Option<u64>,
}

impl Default for WebSocketSettings {
    fn default() -> Self {
        Self {
            client_timeout_secs: default_websocket_client_timeout(),
            auth_timeout_secs: default_websocket_auth_timeout(),
            heartbeat_interval_secs: default_websocket_heartbeat_interval(),
        }
    }
}

/// User management cleanup settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserManagementSettings {
    /// Grace period (in days) before soft-deleted users are purged
    #[serde(default = "default_user_deletion_grace_period")]
    pub deletion_grace_period_days: i64,

    /// Cron expression for scheduling the cleanup job
    #[serde(default = "default_cleanup_job_schedule")]
    pub cleanup_job_schedule: String,
}

/// Job management settings (Phase 9, T163)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsSettings {
    /// Maximum number of concurrent jobs (default: 10)
    #[serde(default = "default_jobs_max_concurrent")]
    pub max_concurrent: u32,

    /// Maximum number of retry attempts (default: 3)
    #[serde(default = "default_jobs_max_retries")]
    pub max_retries: u32,

    /// Initial retry backoff delay in milliseconds (default: 100ms)
    #[serde(default = "default_jobs_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

/// SQL execution settings (Phase 11, T026)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSettings {
    /// Handler execution timeout in seconds (default: 30)
    #[serde(default = "default_handler_timeout_seconds")]
    pub handler_timeout_seconds: u64,

    /// Maximum number of query parameters (default: 50)
    #[serde(default = "default_max_parameters")]
    pub max_parameters: usize,

    /// Maximum size of a single parameter in bytes (default: 524288 = 512KB)
    #[serde(default = "default_max_parameter_size_bytes")]
    pub max_parameter_size_bytes: usize,

    /// Maximum number of cached SQL logical plans (default: 1000)
    #[serde(default = "default_sql_plan_cache_max_entries")]
    pub sql_plan_cache_max_entries: u64,

    /// Time-to-idle TTL in seconds for cached SQL plans (default: 900 = 15m)
    #[serde(default = "default_sql_plan_cache_ttl_seconds")]
    pub sql_plan_cache_ttl_seconds: u64,
}

/// Shutdown settings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShutdownSettings {
    /// Flush job timeout settings
    pub flush: ShutdownFlushSettings,
}

/// Flush job shutdown timeout settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownFlushSettings {
    /// Timeout in seconds to wait for flush jobs to complete during graceful shutdown
    #[serde(default = "default_flush_job_shutdown_timeout")]
    pub timeout: u32,
}

/// Rate limiter settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitSettings {
    /// Maximum queries per second per user (default: 100)
    #[serde(default = "default_rate_limit_queries_per_sec")]
    pub max_queries_per_sec: u32,

    /// Maximum WebSocket messages per second per connection (default: 50)
    #[serde(default = "default_rate_limit_messages_per_sec")]
    pub max_messages_per_sec: u32,

    /// Maximum concurrent subscriptions per user (default: 10)
    #[serde(default = "default_rate_limit_max_subscriptions")]
    pub max_subscriptions_per_user: u32,

    /// Maximum auth requests per second per IP (default: 20)
    #[serde(default = "default_rate_limit_auth_requests_per_ip_per_sec")]
    pub max_auth_requests_per_ip_per_sec: u32,

    /// Maximum concurrent connections per IP address (default: 100)
    /// Prevents a single IP from exhausting all server connections
    #[serde(default = "default_max_connections_per_ip")]
    pub max_connections_per_ip: u32,

    /// Maximum requests per second per IP before authentication (default: 200)
    /// Applied before auth to protect against unauthenticated floods
    #[serde(default = "default_max_requests_per_ip_per_sec")]
    pub max_requests_per_ip_per_sec: u32,

    /// Maximum request body size in bytes (default: 10MB)
    /// Prevents memory exhaustion from huge request payloads
    #[serde(default = "default_request_body_limit_bytes")]
    pub request_body_limit_bytes: usize,

    /// Duration in seconds to ban abusive IPs (default: 300 = 5 minutes)
    #[serde(default = "default_ban_duration_seconds")]
    pub ban_duration_seconds: u64,

    /// Enable connection protection middleware (default: true)
    #[serde(default = "default_enable_connection_protection")]
    pub enable_connection_protection: bool,

    /// Maximum cached entries for rate limiting state (default: 100,000)
    #[serde(default = "default_rate_limit_cache_max_entries")]
    pub cache_max_entries: u64,

    /// Time-to-idle for cached entries in seconds (default: 600 = 10 minutes)
    #[serde(default = "default_rate_limit_cache_ttl_seconds")]
    pub cache_ttl_seconds: u64,
}

/// Authentication settings (T105 - Phase 7, User Story 5)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSettings {
    /// Root password set via config file (alternative to KALAMDB_ROOT_PASSWORD env var)
    /// If set, root user will have remote access enabled with this password.
    #[serde(default)]
    pub root_password: Option<String>,

    /// JWT secret for token validation (required for JWT auth)
    #[serde(default = "default_auth_jwt_secret")]
    pub jwt_secret: String,

    /// Trusted JWT issuers (comma-separated list of domains)
    #[serde(default = "default_auth_jwt_trusted_issuers")]
    pub jwt_trusted_issuers: String,

    /// JWT expiry in hours (default: 24)
    #[serde(default = "default_auth_jwt_expiry_hours")]
    pub jwt_expiry_hours: i64,

    /// Whether auth cookies require HTTPS (default: true)
    #[serde(default = "default_auth_cookie_secure")]
    pub cookie_secure: bool,

    /// Minimum password length (default: 8)
    #[serde(default = "default_auth_min_password_length")]
    pub min_password_length: usize,

    /// Maximum password length (default: 1024)
    #[serde(default = "default_auth_max_password_length")]
    pub max_password_length: usize,

    /// Bcrypt cost factor (default: 12, range: 4-31)
    #[serde(default = "default_auth_bcrypt_cost")]
    pub bcrypt_cost: u32,

    /// Enforce password complexity policy (uppercase, lowercase, digit, special)
    #[serde(default = "default_auth_enforce_password_complexity")]
    pub enforce_password_complexity: bool,

    /// Allow initial setup from non-localhost clients (default: false)
    #[serde(default = "default_auth_allow_remote_setup")]
    pub allow_remote_setup: bool,

    /// Auto-create local users from trusted external auth provider subject when missing.
    #[serde(default = "default_auth_auto_create_users_from_provider")]
    pub auto_create_users_from_provider: bool,

    /// Pre-shared token for authenticating pg_kalam FDW gRPC calls.
    /// When set, the PG extension must send this value in the `authorization` gRPC metadata.
    /// Override via `KALAMDB_PG_AUTH_TOKEN` env var.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pg_auth_token: Option<String>,
}

/// OAuth settings (Phase 10, User Story 8)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthSettings {
    /// Enable OAuth authentication (default: false)
    #[serde(default = "default_oauth_enabled")]
    pub enabled: bool,

    /// Auto-provision users on first OAuth login (default: false)
    #[serde(default = "default_oauth_auto_provision")]
    pub auto_provision: bool,

    /// Default role for auto-provisioned OAuth users (default: "user")
    #[serde(default = "default_oauth_default_role")]
    pub default_role: String,

    /// OAuth providers configuration
    #[serde(default)]
    pub providers: OAuthProvidersSettings,
}

/// OAuth providers configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OAuthProvidersSettings {
    #[serde(default)]
    pub google: OAuthProviderConfig,
    #[serde(default)]
    pub github: OAuthProviderConfig,
    #[serde(default)]
    pub azure: OAuthProviderConfig,
    /// Firebase Authentication (backed by Google Identity Platform / securetoken.google.com)
    #[serde(default)]
    pub firebase: OAuthProviderConfig,
}

/// Individual OAuth provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthProviderConfig {
    /// Enable this provider (default: false)
    #[serde(default = "default_oauth_provider_enabled")]
    pub enabled: bool,

    /// OAuth provider's issuer URL
    #[serde(default)]
    pub issuer: String,

    /// JSON Web Key Set endpoint for public keys
    #[serde(default)]
    pub jwks_uri: String,

    /// OAuth application client ID (optional)
    #[serde(default)]
    pub client_id: Option<String>,

    /// OAuth application client secret (optional, for GitHub)
    #[serde(default)]
    pub client_secret: Option<String>,

    /// Azure tenant ID (optional, for Azure)
    #[serde(default)]
    pub tenant: Option<String>,
}

impl Default for OAuthSettings {
    fn default() -> Self {
        Self {
            enabled: default_oauth_enabled(),
            auto_provision: default_oauth_auto_provision(),
            default_role: default_oauth_default_role(),
            providers: OAuthProvidersSettings::default(),
        }
    }
}

impl Default for OAuthProviderConfig {
    fn default() -> Self {
        Self {
            enabled: default_oauth_provider_enabled(),
            issuer: String::new(),
            jwks_uri: String::new(),
            client_id: None,
            client_secret: None,
            tenant: None,
        }
    }
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            root_password: None,
            jwt_secret: default_auth_jwt_secret(),
            jwt_trusted_issuers: default_auth_jwt_trusted_issuers(),
            jwt_expiry_hours: default_auth_jwt_expiry_hours(),
            cookie_secure: default_auth_cookie_secure(),
            min_password_length: default_auth_min_password_length(),
            max_password_length: default_auth_max_password_length(),
            bcrypt_cost: default_auth_bcrypt_cost(),
            enforce_password_complexity: default_auth_enforce_password_complexity(),
            allow_remote_setup: default_auth_allow_remote_setup(),
            auto_create_users_from_provider: default_auth_auto_create_users_from_provider(),
            pg_auth_token: None,
        }
    }
}

impl Default for DataFusionSettings {
    fn default() -> Self {
        Self {
            memory_limit: default_datafusion_memory_limit(),
            query_parallelism: default_datafusion_parallelism(),
            max_partitions: default_datafusion_max_partitions(),
            batch_size: default_datafusion_batch_size(),
        }
    }
}

impl Default for FlushSettings {
    fn default() -> Self {
        Self {
            default_row_limit: default_flush_row_limit(),
            default_time_interval: default_flush_time_interval(),
            flush_batch_size: default_flush_batch_size(),
            check_interval_seconds: default_flush_check_interval(),
        }
    }
}

impl Default for RetentionSettings {
    fn default() -> Self {
        Self {
            default_deleted_retention_hours: default_deleted_retention_hours(),
            enable_dba_stats: true,
            dba_stats_retention_days: default_dba_stats_retention_days(),
        }
    }
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            default_ttl_seconds: default_stream_ttl(),
            default_max_buffer: default_stream_max_buffer(),
            eviction_interval_seconds: default_stream_eviction_interval(),
        }
    }
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            max_queries_per_sec: default_rate_limit_queries_per_sec(),
            max_messages_per_sec: default_rate_limit_messages_per_sec(),
            max_subscriptions_per_user: default_rate_limit_max_subscriptions(),
            max_auth_requests_per_ip_per_sec: default_rate_limit_auth_requests_per_ip_per_sec(),
            max_connections_per_ip: default_max_connections_per_ip(),
            max_requests_per_ip_per_sec: default_max_requests_per_ip_per_sec(),
            request_body_limit_bytes: default_request_body_limit_bytes(),
            ban_duration_seconds: default_ban_duration_seconds(),
            enable_connection_protection: default_enable_connection_protection(),
            cache_max_entries: default_rate_limit_cache_max_entries(),
            cache_ttl_seconds: default_rate_limit_cache_ttl_seconds(),
        }
    }
}

impl Default for UserManagementSettings {
    fn default() -> Self {
        Self {
            deletion_grace_period_days: default_user_deletion_grace_period(),
            cleanup_job_schedule: default_cleanup_job_schedule(),
        }
    }
}

impl Default for JobsSettings {
    fn default() -> Self {
        Self {
            max_concurrent: default_jobs_max_concurrent(),
            max_retries: default_jobs_max_retries(),
            retry_backoff_ms: default_jobs_retry_backoff_ms(),
        }
    }
}

impl Default for ExecutionSettings {
    fn default() -> Self {
        Self {
            handler_timeout_seconds: default_handler_timeout_seconds(),
            max_parameters: default_max_parameters(),
            max_parameter_size_bytes: default_max_parameter_size_bytes(),
            sql_plan_cache_max_entries: default_sql_plan_cache_max_entries(),
            sql_plan_cache_ttl_seconds: default_sql_plan_cache_ttl_seconds(),
        }
    }
}

impl Default for ShutdownFlushSettings {
    fn default() -> Self {
        Self {
            timeout: default_flush_job_shutdown_timeout(),
        }
    }
}

/// Get default configuration (useful for testing)
///
/// Note: Prefer `Default::default()` constructor pattern.
impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            server: ServerSettings {
                host: "127.0.0.1".to_string(),
                port: 8080,
                workers: 0,
                api_version: default_api_version(),
                enable_http2: default_enable_http2(),
                ui_path: default_ui_path(),
            },
            storage: StorageSettings {
                data_path: default_data_path(),
                shared_tables_template: default_shared_tables_template(),
                user_tables_template: default_user_tables_template(),
                rocksdb: RocksDbSettings::default(),
                remote_timeouts: RemoteStorageTimeouts::default(),
            },
            limits: LimitsSettings {
                max_message_size: 1048576,
                max_query_limit: 1000,
                default_query_limit: 50,
            },
            logging: LoggingSettings {
                level: "info".to_string(),
                logs_path: default_logs_path(),
                log_to_console: true,
                format: "compact".to_string(),
                targets: HashMap::new(),
                slow_query_threshold_ms: default_slow_query_threshold_ms(),
                otlp: OtlpSettings::default(),
            },
            performance: PerformanceSettings {
                request_timeout: 30,
                keepalive_timeout: 75,
                max_connections: 25000,
                backlog: default_backlog(),
                tokio_worker_threads: 0,
                worker_max_blocking_threads: default_worker_max_blocking_threads(),
                client_request_timeout: default_client_request_timeout(),
                client_disconnect_timeout: default_client_disconnect_timeout(),
                max_header_size: default_max_header_size(),
            },
            datafusion: DataFusionSettings::default(),
            flush: FlushSettings::default(),
            manifest_cache: ManifestCacheSettings::default(),
            retention: RetentionSettings::default(),
            stream: StreamSettings::default(),
            websocket: WebSocketSettings::default(),
            rate_limit: RateLimitSettings::default(),
            auth: AuthSettings::default(),
            oauth: OAuthSettings::default(),
            user_management: UserManagementSettings::default(),
            shutdown: ShutdownSettings::default(),
            jobs: JobsSettings::default(),
            execution: ExecutionSettings::default(),
            security: SecuritySettings::default(),
            files: FileUploadSettings::default(),
            cluster: None, // Standalone mode by default
            rpc_tls: super::rpc_tls::RpcTlsConfig::default(),
        }
    }
}
