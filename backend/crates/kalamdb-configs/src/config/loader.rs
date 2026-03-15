use super::types::ServerConfig;
use super::trusted_proxies::parse_trusted_proxy_entries;
use crate::file_helpers::normalize_dir_path;
use std::fs;
use std::path::Path;

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

    /// Validate configuration settings
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate port range
        if self.server.port == 0 {
            return Err(anyhow::anyhow!("Server port cannot be 0"));
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
            anyhow::anyhow!(
                "Invalid security.trusted_proxy_ranges configuration: {}",
                error
            )
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
