//! Storage configuration entity for system.storages table.

use super::{StorageLocationConfig, StorageLocationConfigError, StorageType};
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::ids::StorageId;
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Storage configuration in system_storages table
#[table(
    name = "storages",
    comment = "Storage configurations for data persistence"
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Storage {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Storage identifier"
    )]
    pub storage_id: StorageId, // PK
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Human-readable storage name"
    )]
    pub storage_name: String,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Storage description"
    )]
    pub description: Option<String>,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Storage type: Local, S3, Azure, GCS"
    )]
    pub storage_type: StorageType, // Enum: Filesystem, S3, Gcs, Azure
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Base directory path for storage"
    )]
    pub base_directory: String,
    #[serde(default)]
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Storage credentials JSON (WARNING: stored as plaintext - use environment variables for sensitive credentials)"
    )]
    pub credentials: Option<Value>,
    /// Storage backend parameters encoded as JSON.
    ///
    /// This is the canonical place for backend-specific configuration (S3/GCS/Azure/local).
    ///
    /// Example (S3):
    /// `{ "type": "s3", "region": "us-east-1", "endpoint": "https://s3.amazonaws.com" }`
    #[serde(default)]
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Backend-specific storage configuration JSON"
    )]
    pub config_json: Option<Value>,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Path template for shared tables"
    )]
    pub shared_tables_template: String,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Path template for user tables"
    )]
    pub user_tables_template: String,
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Storage creation timestamp"
    )]
    pub created_at: i64,
    #[column(
        id = 11,
        ordinal = 11,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last update timestamp"
    )]
    pub updated_at: i64,
}

impl Storage {
    /// Decode `config_json` into a type-safe `StorageLocationConfig`.
    pub fn location_config(&self) -> Result<StorageLocationConfig, StorageLocationConfigError> {
        let raw = self
            .config_json
            .as_ref()
            .ok_or(StorageLocationConfigError::MissingConfigJson)?;

        serde_json::from_value::<StorageLocationConfig>(raw.clone())
            .map_err(|e| StorageLocationConfigError::InvalidJson(e.to_string()))
    }
}
