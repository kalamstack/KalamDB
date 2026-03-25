use super::cluster::{ClusterConfig, PeerConfig};
use super::types::ServerConfig;
use std::env;
use std::path::Path;

impl ServerConfig {
    /// Apply environment variable overrides for sensitive configuration
    ///
    /// Supported environment variables (T030):
    /// - KALAMDB_SERVER_HOST: Override server.host
    /// - KALAMDB_SERVER_PORT: Override server.port
    /// - KALAMDB_LOG_LEVEL: Override logging.level
    /// - KALAMDB_LOGS_DIR: Override logging.logs_path
    /// - KALAMDB_LOG_FILE: Override logging.file_path (legacy, extracts parent dir)
    /// - KALAMDB_LOG_TO_CONSOLE: Override logging.log_to_console
    /// - KALAMDB_OTLP_ENABLED: Override logging.otlp.enabled
    /// - KALAMDB_OTLP_ENDPOINT: Override logging.otlp.endpoint
    /// - KALAMDB_OTLP_PROTOCOL: Override logging.otlp.protocol ("grpc" | "http")
    /// - KALAMDB_OTLP_SERVICE_NAME: Override logging.otlp.service_name
    /// - KALAMDB_OTLP_TIMEOUT_MS: Override logging.otlp.timeout_ms
    /// - KALAMDB_DATA_DIR: Override storage.data_path (base directory for rocksdb, storage, snapshots)
    /// - KALAMDB_ROCKSDB_PATH: Override storage.data_path (legacy, extracts parent dir)
    /// - KALAMDB_LOG_FILE_PATH: Override logging.file_path (legacy, prefer KALAMDB_LOG_FILE)
    /// - KALAMDB_HOST: Override server.host (legacy, prefer KALAMDB_SERVER_HOST)
    /// - KALAMDB_PORT: Override server.port (legacy, prefer KALAMDB_SERVER_PORT)
    /// - KALAMDB_CLUSTER_ID: Override cluster.cluster_id
    /// - KALAMDB_NODE_ID: Override cluster.node_id (alias: KALAMDB_CLUSTER_NODE_ID)
    /// - KALAMDB_CLUSTER_RPC_ADDR: Override cluster.rpc_addr
    /// - KALAMDB_CLUSTER_API_ADDR: Override cluster.api_addr
    /// - KALAMDB_CLUSTER_PEERS: Override cluster.peers
    /// - KALAMDB_CLUSTER_RPC_TLS_ENABLED: Override cluster.rpc_tls.enabled
    /// - KALAMDB_CLUSTER_RPC_TLS_CA_CERT_PATH: Override cluster.rpc_tls.ca_cert_path
    /// - KALAMDB_CLUSTER_RPC_TLS_NODE_CERT_PATH: Override cluster.rpc_tls.node_cert_path
    /// - KALAMDB_CLUSTER_RPC_TLS_NODE_KEY_PATH: Override cluster.rpc_tls.node_key_path
    /// - KALAMDB_JWT_SECRET: Override auth.jwt_secret
    /// - KALAMDB_PG_AUTH_TOKEN: Override auth.pg_auth_token
    /// - KALAMDB_JWT_TRUSTED_ISSUERS: Override auth.jwt_trusted_issuers
    /// - KALAMDB_JWT_EXPIRY_HOURS: Override auth.jwt_expiry_hours
    /// - KALAMDB_COOKIE_SECURE: Override auth.cookie_secure
    /// - KALAMDB_ALLOW_REMOTE_SETUP: Override auth.allow_remote_setup
    /// - KALAMDB_AUTH_AUTO_CREATE_USERS_FROM_PROVIDER: Override auth.auto_create_users_from_provider
    /// - KALAMDB_SECURITY_TRUSTED_PROXY_RANGES: Override security.trusted_proxy_ranges
    /// - KALAMDB_RATE_LIMIT_AUTH_REQUESTS_PER_IP_PER_SEC: Override rate_limit.max_auth_requests_per_ip_per_sec
    /// - KALAMDB_WEBSOCKET_CLIENT_TIMEOUT_SECS: Override websocket.client_timeout_secs
    /// - KALAMDB_WEBSOCKET_AUTH_TIMEOUT_SECS: Override websocket.auth_timeout_secs
    /// - KALAMDB_WEBSOCKET_HEARTBEAT_INTERVAL_SECS: Override websocket.heartbeat_interval_secs
    /// - KALAMDB_RPC_TLS_ENABLED: Override rpc_tls.enabled
    /// - KALAMDB_RPC_TLS_CA_CERT: Override rpc_tls.ca_cert (file path or inline PEM)
    /// - KALAMDB_RPC_TLS_SERVER_CERT: Override rpc_tls.server_cert (file path or inline PEM)
    /// - KALAMDB_RPC_TLS_SERVER_KEY: Override rpc_tls.server_key (file path or inline PEM)
    /// - KALAMDB_RPC_TLS_REQUIRE_CLIENT_CERT: Override rpc_tls.require_client_cert
    /// - KALAMDB_SERVER_WORKERS: Override server.workers (actix-web worker threads)
    ///
    /// Environment variables take precedence over server.toml values (T031)
    pub fn apply_env_overrides(&mut self) -> anyhow::Result<()> {
        // Server workers (actix-web worker thread count)
        if let Ok(val) = env::var("KALAMDB_SERVER_WORKERS") {
            self.server.workers = val.parse().map_err(|_| {
                anyhow::anyhow!("Invalid KALAMDB_SERVER_WORKERS value: {}", val)
            })?;
        }

        // Server host (new naming convention)
        if let Ok(host) = env::var("KALAMDB_SERVER_HOST") {
            self.server.host = host;
        } else if let Ok(host) = env::var("KALAMDB_HOST") {
            // Legacy fallback
            self.server.host = host;
        }

        // Server port (new naming convention)
        if let Ok(port_str) = env::var("KALAMDB_SERVER_PORT") {
            self.server.port = port_str
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid KALAMDB_SERVER_PORT value: {}", port_str))?;
        } else if let Ok(port_str) = env::var("KALAMDB_PORT") {
            // Legacy fallback
            self.server.port = port_str
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid KALAMDB_PORT value: {}", port_str))?;
        }

        // Log level
        if let Ok(level) = env::var("KALAMDB_LOG_LEVEL") {
            self.logging.level = level;
        }

        // Logs directory path (new naming convention)
        if let Ok(path) = env::var("KALAMDB_LOGS_DIR") {
            self.logging.logs_path = path;
        } else if let Ok(path) = env::var("KALAMDB_LOG_FILE") {
            // Legacy fallback - extract directory from file path
            if let Some(parent) = Path::new(&path).parent() {
                self.logging.logs_path = parent.to_string_lossy().to_string();
            }
        } else if let Ok(path) = env::var("KALAMDB_LOG_FILE_PATH") {
            // Legacy fallback - extract directory from file path
            if let Some(parent) = Path::new(&path).parent() {
                self.logging.logs_path = parent.to_string_lossy().to_string();
            }
        }

        // Log to console
        if let Ok(val) = env::var("KALAMDB_LOG_TO_CONSOLE") {
            self.logging.log_to_console =
                val.to_lowercase() == "true" || val == "1" || val.to_lowercase() == "yes";
        }

        // OTLP tracing settings
        if let Ok(val) = env::var("KALAMDB_OTLP_ENABLED") {
            self.logging.otlp.enabled =
                val.to_lowercase() == "true" || val == "1" || val.to_lowercase() == "yes";
        }
        if let Ok(val) = env::var("KALAMDB_OTLP_ENDPOINT") {
            self.logging.otlp.endpoint = val;
        }
        if let Ok(val) = env::var("KALAMDB_OTLP_PROTOCOL") {
            self.logging.otlp.protocol = val;
        }
        if let Ok(val) = env::var("KALAMDB_OTLP_SERVICE_NAME") {
            self.logging.otlp.service_name = val;
        }
        if let Ok(val) = env::var("KALAMDB_OTLP_TIMEOUT_MS") {
            self.logging.otlp.timeout_ms = val
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid KALAMDB_OTLP_TIMEOUT_MS value: {}", val))?;
        }

        // JWT secret
        if let Ok(secret) = env::var("KALAMDB_JWT_SECRET") {
            self.auth.jwt_secret = secret;
        }

        // JWT trusted issuers
        if let Ok(issuers) = env::var("KALAMDB_JWT_TRUSTED_ISSUERS") {
            self.auth.jwt_trusted_issuers = issuers;
        }

        // JWT expiry hours
        if let Ok(hours) = env::var("KALAMDB_JWT_EXPIRY_HOURS") {
            self.auth.jwt_expiry_hours = hours.parse().map_err(|_| {
                anyhow::anyhow!("Invalid KALAMDB_JWT_EXPIRY_HOURS value: {}", hours)
            })?;
        }

        // Cookie secure flag
        if let Ok(val) = env::var("KALAMDB_COOKIE_SECURE") {
            self.auth.cookie_secure =
                val.to_lowercase() != "false" && val != "0" && val.to_lowercase() != "no";
        }

        // Allow remote setup (default: false)
        if let Ok(val) = env::var("KALAMDB_ALLOW_REMOTE_SETUP") {
            self.auth.allow_remote_setup =
                val.to_lowercase() == "true" || val == "1" || val.to_lowercase() == "yes";
        }

        // Auto-create users from trusted auth provider identity if missing
        if let Ok(val) = env::var("KALAMDB_AUTH_AUTO_CREATE_USERS_FROM_PROVIDER") {
            self.auth.auto_create_users_from_provider =
                val.to_lowercase() == "true" || val == "1" || val.to_lowercase() == "yes";
        }

        // Pre-shared token for pg_kalam FDW gRPC authentication
        if let Ok(val) = env::var("KALAMDB_PG_AUTH_TOKEN") {
            self.auth.pg_auth_token = Some(val);
        }

        // Trusted proxy ranges used for X-Forwarded-For / X-Real-IP trust.
        if let Ok(val) = env::var("KALAMDB_SECURITY_TRUSTED_PROXY_RANGES")
            .or_else(|_| env::var("KALAMDB_TRUSTED_PROXY_RANGES"))
        {
            self.security.trusted_proxy_ranges = val
                .split(',')
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect();
        }

        // Auth rate limit per IP
        if let Ok(val) = env::var("KALAMDB_RATE_LIMIT_AUTH_REQUESTS_PER_IP_PER_SEC") {
            self.rate_limit.max_auth_requests_per_ip_per_sec = val.parse().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid KALAMDB_RATE_LIMIT_AUTH_REQUESTS_PER_IP_PER_SEC value: {}",
                    val
                )
            })?;
        }

        // WebSocket settings
        if let Ok(val) = env::var("KALAMDB_WEBSOCKET_CLIENT_TIMEOUT_SECS") {
            self.websocket.client_timeout_secs = Some(val.parse().map_err(|_| {
                anyhow::anyhow!("Invalid KALAMDB_WEBSOCKET_CLIENT_TIMEOUT_SECS value: {}", val)
            })?);
        }
        if let Ok(val) = env::var("KALAMDB_WEBSOCKET_AUTH_TIMEOUT_SECS") {
            self.websocket.auth_timeout_secs = Some(val.parse().map_err(|_| {
                anyhow::anyhow!("Invalid KALAMDB_WEBSOCKET_AUTH_TIMEOUT_SECS value: {}", val)
            })?);
        }
        if let Ok(val) = env::var("KALAMDB_WEBSOCKET_HEARTBEAT_INTERVAL_SECS") {
            self.websocket.heartbeat_interval_secs = Some(val.parse().map_err(|_| {
                anyhow::anyhow!("Invalid KALAMDB_WEBSOCKET_HEARTBEAT_INTERVAL_SECS value: {}", val)
            })?);
        }

        // Data directory (new naming convention)
        if let Ok(path) = env::var("KALAMDB_DATA_DIR") {
            self.storage.data_path = path;
        } else if let Ok(path) = env::var("KALAMDB_ROCKSDB_PATH") {
            // Legacy fallback - KALAMDB_ROCKSDB_PATH now sets the parent data dir
            if let Some(parent) = Path::new(&path).parent() {
                self.storage.data_path = parent.to_string_lossy().to_string();
            }
        }

        // Cluster overrides
        let cluster_id = env::var("KALAMDB_CLUSTER_ID").ok();
        let node_id = env::var("KALAMDB_NODE_ID")
            .ok()
            .or_else(|| env::var("KALAMDB_CLUSTER_NODE_ID").ok());
        let rpc_addr = env::var("KALAMDB_CLUSTER_RPC_ADDR").ok();
        let api_addr = env::var("KALAMDB_CLUSTER_API_ADDR").ok();
        let peers_env = env::var("KALAMDB_CLUSTER_PEERS").ok();

        let has_cluster_env = cluster_id.is_some()
            || node_id.is_some()
            || rpc_addr.is_some()
            || api_addr.is_some()
            || peers_env.is_some();

        if has_cluster_env {
            let parsed_node_id = match node_id {
                Some(ref val) => val
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("Invalid KALAMDB_NODE_ID value: {}", val))?,
                None => self.cluster.as_ref().map(|c| c.node_id).ok_or_else(|| {
                    anyhow::anyhow!("KALAMDB_NODE_ID is required when overriding cluster settings")
                })?,
            };

            let cluster = self.cluster.get_or_insert_with(|| ClusterConfig {
                cluster_id: cluster_id.clone().unwrap_or_else(|| "cluster".to_string()),
                node_id: parsed_node_id,
                rpc_addr: rpc_addr.clone().unwrap_or_else(|| "127.0.0.1:9188".to_string()),
                api_addr: api_addr.clone().unwrap_or_else(|| "127.0.0.1:8080".to_string()),
                peers: Vec::new(),
                user_shards: 12,
                shared_shards: 1,
                heartbeat_interval_ms: 250,
                election_timeout_ms: (500, 1000),
                snapshot_policy: "LogsSinceLast(1000)".to_string(),
                max_snapshots_to_keep: 3,
                replication_timeout_ms: 5000,
                reconnect_interval_ms: 3000,
                peer_wait_max_retries: None,
                peer_wait_initial_delay_ms: None,
                peer_wait_max_delay_ms: None,
            });

            if let Some(val) = cluster_id {
                cluster.cluster_id = val;
            }

            cluster.node_id = parsed_node_id;

            if let Some(val) = rpc_addr {
                cluster.rpc_addr = val;
            }

            if let Some(val) = api_addr {
                cluster.api_addr = val;
            }

            if let Some(val) = peers_env {
                cluster.peers = parse_cluster_peers(&val)?;
            }
        }

        // Top-level RPC TLS overrides (unified mTLS config)
        if let Ok(val) = env::var("KALAMDB_RPC_TLS_ENABLED") {
            self.rpc_tls.enabled = val.eq_ignore_ascii_case("true")
                || val == "1"
                || val.eq_ignore_ascii_case("yes");
        }
        if let Ok(val) = env::var("KALAMDB_RPC_TLS_CA_CERT") {
            self.rpc_tls.ca_cert = Some(val);
        }
        if let Ok(val) = env::var("KALAMDB_RPC_TLS_SERVER_CERT") {
            self.rpc_tls.server_cert = Some(val);
        }
        if let Ok(val) = env::var("KALAMDB_RPC_TLS_SERVER_KEY") {
            self.rpc_tls.server_key = Some(val);
        }
        if let Ok(val) = env::var("KALAMDB_RPC_TLS_REQUIRE_CLIENT_CERT") {
            self.rpc_tls.require_client_cert = val.eq_ignore_ascii_case("true")
                || val == "1"
                || val.eq_ignore_ascii_case("yes");
        }

        Ok(())
    }
}

fn parse_cluster_peers(value: &str) -> anyhow::Result<Vec<PeerConfig>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut peers = Vec::new();
    for entry in trimmed.split(';') {
        let parts: Vec<&str> = entry.split('@').collect();
        if parts.len() < 3 || parts.len() > 4 {
            return Err(anyhow::anyhow!(
                "Invalid KALAMDB_CLUSTER_PEERS entry '{}'. Expected format: node_id@rpc_addr@api_addr[@rpc_server_name]",
                entry
            ));
        }

        let node_id = parts[0]
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid peer node_id in '{}': {}", entry, parts[0]))?;

        peers.push(PeerConfig {
            node_id,
            rpc_addr: parts[1].trim().to_string(),
            api_addr: parts[2].trim().to_string(),
            rpc_server_name: parts.get(3).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
        });
    }

    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

    fn acquire_env_lock() -> MutexGuard<'static, ()> {
        ENV_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn test_env_override_server_host() {
        let _guard = acquire_env_lock();
        env::set_var("KALAMDB_SERVER_HOST", "0.0.0.0");

        let mut config = ServerConfig::default();
        config.apply_env_overrides().unwrap();

        assert_eq!(config.server.host, "0.0.0.0");

        env::remove_var("KALAMDB_SERVER_HOST");
    }

    #[test]
    fn test_env_override_server_port() {
        let _guard = acquire_env_lock();
        env::set_var("KALAMDB_SERVER_PORT", "9090");

        let mut config = ServerConfig::default();
        config.apply_env_overrides().unwrap();

        assert_eq!(config.server.port, 9090);

        env::remove_var("KALAMDB_SERVER_PORT");
    }

    #[test]
    fn test_env_override_log_level() {
        let _guard = acquire_env_lock();
        env::set_var("KALAMDB_LOG_LEVEL", "debug");

        let mut config = ServerConfig::default();
        config.apply_env_overrides().unwrap();

        assert_eq!(config.logging.level, "debug");

        env::remove_var("KALAMDB_LOG_LEVEL");
    }

    #[test]
    fn test_env_override_trusted_proxy_ranges() {
        let _guard = acquire_env_lock();
        env::set_var("KALAMDB_SECURITY_TRUSTED_PROXY_RANGES", "10.0.1.9,192.168.0.0/24");

        let mut config = ServerConfig::default();
        config.apply_env_overrides().unwrap();

        assert_eq!(
            config.security.trusted_proxy_ranges,
            vec!["10.0.1.9".to_string(), "192.168.0.0/24".to_string()]
        );

        env::remove_var("KALAMDB_SECURITY_TRUSTED_PROXY_RANGES");
    }
}
