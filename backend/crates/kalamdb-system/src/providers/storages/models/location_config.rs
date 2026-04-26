use serde::{Deserialize, Serialize};

use super::{AzureStorageConfig, GcsStorageConfig, LocalStorageConfig, S3StorageConfig};

/// Type-safe JSON configuration for `system.storages` locations.
///
/// Stored as raw JSON text in the `config_json` column, but can be decoded
/// into this strongly typed enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageLocationConfig {
    Local(LocalStorageConfig),
    S3(S3StorageConfig),
    Gcs(GcsStorageConfig),
    Azure(AzureStorageConfig),
}

impl StorageLocationConfig {
    pub fn as_type_str(&self) -> &'static str {
        match self {
            StorageLocationConfig::Local(_) => "local",
            StorageLocationConfig::S3(_) => "s3",
            StorageLocationConfig::Gcs(_) => "gcs",
            StorageLocationConfig::Azure(_) => "azure",
        }
    }
}
