use super::trusted_proxies::parse_trusted_proxy_entries;
use super::types::ServerConfig;
use crate::file_helpers::normalize_dir_path;
use std::fs;
use std::net::IpAddr;
use std::path::Path;

fn is_localhost_host(host: &str) -> bool {
    let trimmed = host.trim().trim_matches('[').trim_matches(']');
    trimmed.eq_ignore_ascii_case("localhost")
        || trimmed.parse::<IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
}

fn extract_host_component(value: &str) -> &str {
    let trimmed = value.trim();
    let without_scheme = trimmed.split_once("://").map(|(_, rest)| rest).unwrap_or(trimmed);
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);

    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }

    if authority.matches(':').count() == 1 {
        return authority.rsplit_once(':').map(|(host, _)| host).unwrap_or(authority);
    }

    authority
}

fn endpoint_is_localhost(value: &str) -> bool {
    is_localhost_host(extract_host_component(value))
}

fn has_configured_origin_policy(origins: &[String]) -> bool {
    origins.iter().any(|origin| !origin.trim().is_empty())
}

fn uses_wildcard_origin_policy(origins: &[String]) -> bool {
    origins.iter().any(|origin| origin.trim() == "*")
}

fn is_non_local_http_exposure(config: &ServerConfig) -> bool {
    if !is_localhost_host(&config.server.host) {
        return true;
    }

    config
        .server
        .configured_public_origin()
        .is_some_and(|origin| !endpoint_is_localhost(&origin))
}

fn cluster_uses_non_local_networking(cluster: &super::cluster::ClusterConfig) -> bool {
    !endpoint_is_localhost(&cluster.rpc_addr)
        || !endpoint_is_localhost(&cluster.api_addr)
        || cluster.peers.iter().any(|peer| {
            !endpoint_is_localhost(&peer.rpc_addr) || !endpoint_is_localhost(&peer.api_addr)
        })
}

impl ServerConfig {
    /// Load configuration from a TOML file
    ///
    /// Note: Environment overrides are applied separately via `apply_env_overrides()`.
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to read config file: {}", e))?;

        let mut config: ServerConfig = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file: {}", e))?;

        config.finalize()?;

        Ok(config)
    }

    /// Normalize directory-like paths to absolute paths for consistent runtime behavior.
    ///
    /// This keeps semantics the same (relative paths remain relative to current working dir),
    /// but avoids each subsystem re-implementing path handling.
    fn normalize_paths(&mut self) {
        self.storage.data_path = normalize_dir_path(&self.storage.data_path);
        self.logging.logs_path = normalize_dir_path(&self.logging.logs_path);
    }

    /// Normalize local filesystem paths and validate configuration.
    ///
    /// Call this after applying environment overrides.
    pub fn finalize(&mut self) -> anyhow::Result<()> {
        // Normalize local filesystem paths (rocksdb/logs/storage/snapshots)
        self.normalize_paths();

        self.validate()?;

        Ok(())
    }

    /// Returns true when startup should emit a security warning because the
    /// server is reachable from non-localhost clients while CORS is configured
    /// to accept any browser origin.
    pub fn should_warn_on_non_local_http_wildcard_cors(&self) -> bool {
        is_non_local_http_exposure(self)
            && uses_wildcard_origin_policy(&self.security.cors.allowed_origins)
    }

    /// Validate configuration settings
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate port range
        if self.server.port == 0 {
            return Err(anyhow::anyhow!("Server port cannot be 0"));
        }

        if let Some(public_origin) = self
            .server
            .public_origin
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let valid_scheme =
                public_origin.starts_with("http://") || public_origin.starts_with("https://");
            if !valid_scheme {
                return Err(anyhow::anyhow!(
                    "server.public_origin must start with http:// or https://"
                ));
            }
        }

        // Validate log level
        let valid_levels = ["error", "warn", "info", "debug", "trace"];
        if !valid_levels.contains(&self.logging.level.as_str()) {
            return Err(anyhow::anyhow!(
                "Invalid log level '{}'. Must be one of: {}",
                self.logging.level,
                valid_levels.join(", ")
            ));
        }

        // Validate log format
        let valid_formats = ["compact", "pretty", "json"];
        if !valid_formats.contains(&self.logging.format.as_str()) {
            return Err(anyhow::anyhow!(
                "Invalid log format '{}'. Must be one of: {}",
                self.logging.format,
                valid_formats.join(", ")
            ));
        }

        // Validate per-target log levels if provided
        let valid_levels = ["error", "warn", "info", "debug", "trace"];
        for (target, level) in &self.logging.targets {
            if !valid_levels.contains(&level.as_str()) {
                return Err(anyhow::anyhow!(
                    "Invalid log level '{}' for target '{}'. Must be one of: {}",
                    level,
                    target,
                    valid_levels.join(", ")
                ));
            }
        }

        // Validate message size limit
        if self.limits.max_message_size == 0 {
            return Err(anyhow::anyhow!("max_message_size cannot be 0"));
        }

        // Validate query limits
        if self.limits.max_query_limit == 0 {
            return Err(anyhow::anyhow!("max_query_limit cannot be 0"));
        }

        if self.limits.default_query_limit > self.limits.max_query_limit {
            return Err(anyhow::anyhow!(
                "default_query_limit ({}) cannot exceed max_query_limit ({})",
                self.limits.default_query_limit,
                self.limits.max_query_limit
            ));
        }

        if self.user_management.deletion_grace_period_days < 0 {
            return Err(anyhow::anyhow!("deletion_grace_period_days cannot be negative"));
        }

        if self.user_management.cleanup_job_schedule.trim().is_empty() {
            return Err(anyhow::anyhow!("cleanup_job_schedule cannot be empty"));
        }

        parse_trusted_proxy_entries(&self.security.trusted_proxy_ranges).map_err(|error| {
            anyhow::anyhow!("Invalid security.trusted_proxy_ranges configuration: {}", error)
        })?;

        self.rpc_tls
            .validate()
            .map_err(|error| anyhow::anyhow!("Invalid rpc_tls configuration: {}", error))?;

        if let Some(cluster) = &self.cluster {
            cluster
                .validate()
                .map_err(|error| anyhow::anyhow!("Invalid cluster configuration: {}", error))?;

            if !self.rpc_tls.enabled
                && !cluster.peers.is_empty()
                && cluster_uses_non_local_networking(cluster)
            {
                return Err(anyhow::anyhow!(
                    "Non-localhost multi-node clusters require rpc_tls.enabled = true"
                ));
            }
        }

        if is_non_local_http_exposure(self)
            && !has_configured_origin_policy(&self.security.cors.allowed_origins)
        {
            return Err(anyhow::anyhow!(
                "Non-localhost HTTP exposure requires security.cors.allowed_origins to be configured (empty is not allowed)"
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::cluster::{ClusterConfig, PeerConfig};
    use super::*;

    fn local_cluster_config() -> ClusterConfig {
        ClusterConfig {
            cluster_id: "test-cluster".to_string(),
            node_id: 1,
            rpc_addr: "127.0.0.1:9188".to_string(),
            api_addr: "127.0.0.1:8080".to_string(),
            peers: vec![PeerConfig {
                node_id: 2,
                rpc_addr: "127.0.0.2:9188".to_string(),
                api_addr: "127.0.0.2:8080".to_string(),
                rpc_server_name: None,
            }],
            user_shards: 8,
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
        }
    }

    #[test]
    fn test_default_config_is_valid() {
        let config = ServerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_port() {
        let mut config = ServerConfig::default();
        config.server.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_effective_public_origin_uses_localhost_fallback() {
        let mut config = ServerConfig::default();
        config.server.port = 9090;

        assert_eq!(config.server.effective_public_origin(), "http://localhost:9090");

        config.server.public_origin = Some("https://db.example.com/".to_string());

        assert_eq!(config.server.effective_public_origin(), "https://db.example.com");
    }

    #[test]
    fn test_configured_public_origin_treats_blank_as_unset() {
        let mut config = ServerConfig::default();

        config.server.public_origin = Some("   ".to_string());

        assert_eq!(config.server.configured_public_origin(), None);

        config.server.public_origin = Some("https://db.example.com/".to_string());

        assert_eq!(
            config.server.configured_public_origin().as_deref(),
            Some("https://db.example.com")
        );
    }

    #[test]
    fn test_invalid_log_level() {
        let mut config = ServerConfig::default();
        config.logging.level = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_trusted_proxy_ranges() {
        let mut config = ServerConfig::default();
        config.security.trusted_proxy_ranges = vec!["nope".to_string()];
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_public_origin_scheme() {
        let mut config = ServerConfig::default();
        config.server.public_origin = Some("db.example.com".to_string());

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_non_localhost_bind_requires_configured_browser_origins() {
        let mut config = ServerConfig::default();
        config.server.host = "0.0.0.0".to_string();

        let err = config
            .validate()
            .expect_err("non-localhost bind should require configured origins");
        assert!(err.to_string().contains("configured"));
    }

    #[test]
    fn test_non_localhost_bind_allows_wildcard_browser_origins_with_warning() {
        let mut config = ServerConfig::default();
        config.server.host = "0.0.0.0".to_string();
        config.security.cors.allowed_origins = vec!["*".to_string()];

        assert!(config.validate().is_ok());
        assert!(config.should_warn_on_non_local_http_wildcard_cors());
    }

    #[test]
    fn test_non_localhost_bind_allows_explicit_browser_origins() {
        let mut config = ServerConfig::default();
        config.server.host = "0.0.0.0".to_string();
        config.security.cors.allowed_origins = vec![
            "http://localhost:8080".to_string(),
            "http://127.0.0.1:8080".to_string(),
        ];

        assert!(config.validate().is_ok());
        assert!(!config.should_warn_on_non_local_http_wildcard_cors());
    }

    #[test]
    fn test_localhost_bind_does_not_warn_on_wildcard_browser_origins() {
        let mut config = ServerConfig::default();
        config.security.cors.allowed_origins = vec!["*".to_string()];

        assert!(config.validate().is_ok());
        assert!(!config.should_warn_on_non_local_http_wildcard_cors());
    }

    #[test]
    fn test_external_cluster_requires_rpc_tls() {
        let mut config = ServerConfig::default();
        config.cluster = Some(ClusterConfig {
            rpc_addr: "10.0.0.1:9188".to_string(),
            api_addr: "http://10.0.0.1:8080".to_string(),
            peers: vec![PeerConfig {
                node_id: 2,
                rpc_addr: "10.0.0.2:9188".to_string(),
                api_addr: "http://10.0.0.2:8080".to_string(),
                rpc_server_name: None,
            }],
            ..local_cluster_config()
        });

        let err = config.validate().expect_err("external cluster should require rpc_tls");
        assert!(err.to_string().contains("rpc_tls.enabled = true"));
    }

    #[test]
    fn test_local_cluster_allows_disabled_rpc_tls() {
        let mut config = ServerConfig::default();
        config.cluster = Some(local_cluster_config());

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_enabled_rpc_tls_requires_material() {
        let mut config = ServerConfig::default();
        config.rpc_tls.enabled = true;

        let err = config.validate().expect_err("enabled rpc_tls without certs must be rejected");
        assert!(err.to_string().contains("ca_cert"));
    }
}
