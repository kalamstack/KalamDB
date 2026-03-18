use num_cpus;

// Default value functions
pub fn default_workers() -> usize {
    0
}

pub fn default_api_version() -> String {
    "v1".to_string()
}

pub fn default_enable_http2() -> bool {
    true // HTTP/2 enabled by default for better performance
}

pub fn default_ui_path() -> Option<String> {
    None // UI disabled by default; set to path like "./ui/dist" to enable
}

pub fn default_flush_job_shutdown_timeout() -> u32 {
    300 // 5 minutes (T158j)
}

pub fn default_true() -> bool {
    true
}

pub fn default_data_path() -> String {
    "./data".to_string() // Default dev path; normalized to absolute at runtime
}

pub fn default_shared_tables_template() -> String {
    "{namespace}/{tableName}".to_string()
}

pub fn default_user_tables_template() -> String {
    "{namespace}/{tableName}/{userId}".to_string()
}

pub fn default_max_message_size() -> usize {
    1048576 // 1MB
}

pub fn default_max_query_limit() -> usize {
    1000
}

pub fn default_query_limit() -> usize {
    50
}

pub fn default_log_level() -> String {
    "info".to_string()
}

pub fn default_log_format() -> String {
    "compact".to_string()
}

pub fn default_otlp_enabled() -> bool {
    false
}

pub fn default_otlp_endpoint() -> String {
    "http://127.0.0.1:4317".to_string()
}

pub fn default_otlp_protocol() -> String {
    "grpc".to_string()
}

pub fn default_otlp_service_name() -> String {
    "kalamdb-server".to_string()
}

pub fn default_otlp_timeout_ms() -> u64 {
    3000
}

pub fn default_slow_query_threshold_ms() -> u64 {
    1200 // 1.2 seconds
}

pub fn default_logs_path() -> String {
    "./logs".to_string()
}

pub fn default_request_timeout() -> u64 {
    30
}

pub fn default_keepalive_timeout() -> u64 {
    75
}

pub fn default_max_connections() -> usize {
    25000
}

pub fn default_backlog() -> u32 {
    4096 // Pending connection queue size (increased from 2048 for better burst handling)
}

pub fn default_worker_max_blocking_threads() -> usize {
    512 // Max blocking threads per worker (actix default: 512 / parallelism)
}

pub fn default_client_request_timeout() -> u64 {
    5 // 5 seconds to receive request headers
}

pub fn default_client_disconnect_timeout() -> u64 {
    2 // 2 seconds for graceful disconnect
}

pub fn default_max_header_size() -> usize {
    16384 // 16KB - increased from default 8KB to support large JWT tokens
}

// DataFusion defaults
pub fn default_datafusion_memory_limit() -> usize {
    256 * 1024 * 1024 // 256MB (lower idle footprint)
}

pub fn default_datafusion_parallelism() -> usize {
    num_cpus::get()
}

pub fn default_datafusion_max_partitions() -> usize {
    8
}

pub fn default_datafusion_batch_size() -> usize {
    2048 // Reduced from 8192 for lower memory usage
}

// Flush defaults
pub fn default_flush_row_limit() -> usize {
    10000
}

pub fn default_flush_time_interval() -> u64 {
    300 // 5 minutes
}

pub fn default_flush_batch_size() -> usize {
    10000 // 10k rows per batch to avoid loading all into memory
}

pub fn default_flush_check_interval() -> u64 {
    60 // Check for pending writes every 60 seconds
}

// Manifest cache defaults (Phase 4 - US6)
pub fn default_manifest_cache_eviction_interval() -> i64 {
    300 // 5 minutes
}

pub fn default_manifest_cache_max_entries() -> usize {
    500 // Reduced from 50000 for lower memory footprint
}

/// Default TTL in days for manifest eviction (default: 7 days)
/// Manifests not accessed for this many days will be removed from cache
pub fn default_manifest_cache_eviction_ttl_days() -> u64 {
    7 // 7 days
}

/// Default weight factor for user table manifests (default: 10)
/// User tables are evicted N times faster than shared tables.
/// Higher values give stronger preference to keeping shared tables in memory.
pub fn default_user_table_weight_factor() -> u32 {
    10 // User tables are evicted 10x faster than shared tables
}

// Retention defaults
pub fn default_deleted_retention_hours() -> i32 {
    168 // 7 days
}

pub fn default_dba_stats_retention_days() -> u64 {
    7
}

// Stream defaults
pub fn default_stream_ttl() -> u64 {
    10 // 10 seconds
}

pub fn default_stream_max_buffer() -> usize {
    10000
}

pub fn default_stream_eviction_interval() -> u64 {
    60 // 60 seconds = 1 minute
}

pub fn default_user_deletion_grace_period() -> i64 {
    30 // 30 days
}

pub fn default_cleanup_job_schedule() -> String {
    "0 2 * * *".to_string()
}

// Jobs defaults (Phase 9, T163)
pub fn default_jobs_max_concurrent() -> u32 {
    10 // 10 concurrent jobs
}

pub fn default_jobs_max_retries() -> u32 {
    3 // 3 retry attempts
}

pub fn default_jobs_retry_backoff_ms() -> u64 {
    100 // 100ms initial backoff
}

// Execution defaults (Phase 11, T026)
pub fn default_handler_timeout_seconds() -> u64 {
    30 // 30 seconds
}

pub fn default_max_parameters() -> usize {
    50 // Maximum 50 query parameters
}

pub fn default_max_parameter_size_bytes() -> usize {
    524288 // 512KB maximum parameter size
}

pub fn default_sql_plan_cache_max_entries() -> u64 {
    1000 // bounded SQL logical plan cache entries
}

pub fn default_sql_plan_cache_ttl_seconds() -> u64 {
    900 // 15 minutes idle TTL for unused cached plans
}

// Rate limiter defaults
pub fn default_rate_limit_queries_per_sec() -> u32 {
    100 // 100 queries per second per user
}

pub fn default_rate_limit_messages_per_sec() -> u32 {
    50 // 50 messages per second per WebSocket connection
}

pub fn default_rate_limit_max_subscriptions() -> u32 {
    10 // 10 concurrent subscriptions per user
}

pub fn default_rate_limit_auth_requests_per_ip_per_sec() -> u32 {
    20 // 20 auth requests per second per IP (login/refresh/setup)
}

// Connection protection defaults (DoS prevention)
pub fn default_max_connections_per_ip() -> u32 {
    100 // Maximum 100 concurrent connections per IP address
}

pub fn default_max_requests_per_ip_per_sec() -> u32 {
    200 // Maximum 200 requests per second per IP (before auth)
}

pub fn default_request_body_limit_bytes() -> usize {
    10 * 1024 * 1024 // 10MB maximum request body size
}

pub fn default_ban_duration_seconds() -> u64 {
    300 // 5 minutes ban for abusive IPs
}

pub fn default_enable_connection_protection() -> bool {
    true // Enable connection protection by default
}

pub fn default_rate_limit_cache_max_entries() -> u64 {
    10_000 // Maximum 10k cached rate limit entries
           // MEMORY OPTIMIZATION: reduced from 100k. Each moka cache has internal
           // overhead proportional to max_capacity. 5 caches × 100k entries was
           // over-provisioned for most deployments. 10k handles typical load.
}

pub fn default_rate_limit_cache_ttl_seconds() -> u64 {
    600 // 10 minutes TTL for rate limit cache entries
}

/// Default JWT secret - MUST be overridden in production.
///
/// # Security
/// This default value is intentionally insecure and will be rejected by the server
/// when running on non-localhost addresses. The server startup in main.rs validates
/// that the JWT secret is not in the list of known insecure defaults.
///
/// Insecure values that will be rejected:
/// - "CHANGE_ME_IN_PRODUCTION"
/// - "kalamdb-dev-secret-key-change-in-production"
/// - "your-secret-key-at-least-32-chars-change-me-in-production"
/// - Any secret shorter than 32 characters
pub fn default_auth_jwt_secret() -> String {
    "CHANGE_ME_IN_PRODUCTION".to_string()
}

/// Default trusted JWT issuers (empty = no issuer validation)
pub fn default_auth_jwt_trusted_issuers() -> String {
    String::new()
}

/// Default JWT expiry in hours
pub fn default_auth_jwt_expiry_hours() -> i64 {
    24
}

/// Default cookie secure flag (HTTPS-only cookies)
pub fn default_auth_cookie_secure() -> bool {
    true
}

/// Default minimum password length
pub fn default_auth_min_password_length() -> usize {
    8
}

/// Default maximum password length
pub fn default_auth_max_password_length() -> usize {
    1024
}

/// Default bcrypt cost factor
pub fn default_auth_bcrypt_cost() -> u32 {
    12
}

/// Default password complexity enforcement
pub fn default_auth_enforce_password_complexity() -> bool {
    false
}

/// Default: only allow setup from localhost
pub fn default_auth_allow_remote_setup() -> bool {
    false
}

/// Default: do not auto-create local users from external auth provider identities.
pub fn default_auth_auto_create_users_from_provider() -> bool {
    false
}

// OAuth defaults
pub fn default_oauth_enabled() -> bool {
    false
}

pub fn default_oauth_auto_provision() -> bool {
    false
}

pub fn default_oauth_default_role() -> String {
    "user".to_string()
}

pub fn default_oauth_provider_enabled() -> bool {
    false
}

// WebSocket defaults
pub fn default_websocket_client_timeout() -> Option<u64> {
    Some(10)
}

pub fn default_websocket_auth_timeout() -> Option<u64> {
    Some(3)
}

pub fn default_websocket_heartbeat_interval() -> Option<u64> {
    Some(5)
}

// RocksDB defaults (MEMORY OPTIMIZED)
pub fn default_rocksdb_write_buffer_size() -> usize {
    512 * 1024 // 512KB (reduced from 2MB for lower memory footprint with many CFs)
               // With 50+ column families, write buffers dominate memory usage:
               // 512KB × 2 buffers × 50 CFs = 50MB vs 2MB × 2 × 50 = 200MB
}

pub fn default_rocksdb_max_write_buffers() -> i32 {
    2 // Reduced from 3 for lower memory usage
}

pub fn default_rocksdb_block_cache_size() -> usize {
    4 * 1024 * 1024 // 4MB (reduced from 256MB, SHARED across all column families)
}

pub fn default_rocksdb_max_background_jobs() -> i32 {
    4
}

pub fn default_rocksdb_sync_writes() -> bool {
    false
}

pub fn default_rocksdb_max_open_files() -> i32 {
    512 // Reasonable default that stays under typical OS limits
}

pub fn default_rocksdb_compact_on_startup() -> bool {
    false // Compact all column families on startup to reduce file count
}

// Security defaults
pub fn default_max_ws_message_size() -> usize {
    1024 * 1024 // 1MB
}

pub fn default_max_request_body_size() -> usize {
    10 * 1024 * 1024 // 10MB
}

// File upload defaults
pub fn default_file_max_size_bytes() -> usize {
    25 * 1024 * 1024 // 25MB
}

pub fn default_file_max_files_per_request() -> usize {
    20
}

pub fn default_file_max_files_per_folder() -> u32 {
    5000
}

pub fn default_file_staging_path() -> String {
    "./data/tmp".to_string()
}

pub fn default_file_allowed_mime_types() -> Vec<String> {
    vec![] // Empty = allow all
}

// Remote storage timeout defaults (S3, GCS, Azure)
pub fn default_remote_request_timeout() -> u64 {
    60 // 60 seconds for all remote storage operations (unified timeout)
}

pub fn default_remote_connect_timeout() -> u64 {
    10 // 10 seconds for connection establishment
}
