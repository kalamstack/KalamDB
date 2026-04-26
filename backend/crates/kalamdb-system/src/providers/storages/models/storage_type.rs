use std::fmt;

use serde::{Deserialize, Serialize};

/// Enum representing the type of storage backend in KalamDB.
///
/// - Filesystem: Local or network filesystem storage
/// - S3: Amazon S3 or S3-compatible object storage
/// - Gcs: Google Cloud Storage
/// - Azure: Azure Blob Storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    /// Local or network filesystem storage
    Filesystem,
    /// Amazon S3 or S3-compatible object storage
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
}

impl StorageType {
    pub fn as_str(self) -> &'static str {
        match self {
            StorageType::Filesystem => "filesystem",
            StorageType::S3 => "s3",
            StorageType::Gcs => "gcs",
            StorageType::Azure => "azure",
        }
    }
}

impl fmt::Display for StorageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for StorageType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "s3" => StorageType::S3,
            "gcs" | "gs" => StorageType::Gcs,
            "azure" | "az" => StorageType::Azure,
            _ => StorageType::Filesystem,
        }
    }
}

impl From<String> for StorageType {
    fn from(s: String) -> Self {
        StorageType::from(s.as_str())
    }
}
