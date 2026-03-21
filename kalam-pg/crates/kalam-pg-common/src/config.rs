use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Remote server configuration consumed by the PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for RemoteServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 50051,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddedHttpConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
}

impl Default for EmbeddedHttpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

/// Minimal embedded runtime configuration consumed by the PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddedRuntimeConfig {
    /// Base directory for embedded state and cold storage files.
    pub storage_base_path: PathBuf,
    /// Explicit node id override used for embedded runtimes.
    pub node_id: String,
    /// Optional in-process HTTP listener for the embedded runtime.
    pub http: EmbeddedHttpConfig,
}

impl Default for EmbeddedRuntimeConfig {
    fn default() -> Self {
        Self {
            storage_base_path: PathBuf::from("data/embedded"),
            node_id: "pg-embedded".to_string(),
            http: EmbeddedHttpConfig::default(),
        }
    }
}
