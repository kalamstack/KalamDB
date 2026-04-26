//! Error types for KalamDB Core.
//!
//! This module provides a unified error hierarchy for all KalamDB operations.
//! Errors are structured to provide context while remaining ergonomic to use.
//!
//! # Error Categories
//!
//! - **Storage errors**: RocksDB, Parquet, and IO operations
//! - **Schema errors**: Table definitions, schema evolution, and validation
//! - **Authentication errors**: Permission denied, unauthorized access
//! - **Operational errors**: Invalid operations, resource conflicts
//!
//! # Example
//!
//! ```rust,ignore
//! use kalamdb_core::error::KalamDbError;
//!
//! fn create_table(name: &str) -> Result<(), KalamDbError> {
//!     if name.is_empty() {
//!         return Err(KalamDbError::InvalidOperation("Table name cannot be empty".into()));
//!     }
//!     Ok(())
//! }
//! ```

use thiserror::Error;

/// Main error type for KalamDB.
///
/// This enum encompasses all possible errors that can occur during KalamDB operations.
/// Use the specific variants to provide context-rich error messages.
#[derive(Error, Debug)]
#[must_use = "errors should be handled or propagated"]
pub enum KalamDbError {
    #[error("Storage error: {0}")]
    Storage(#[from] CoreStorageError),

    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("API error: {0}")]
    Api(#[from] ApiError),

    #[error("Configuration file error: {0}")]
    ConfigError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Catalog error: {0}")]
    CatalogError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// IO error with string message (for errors that can't carry std::io::Error)
    #[error("IO error: {message}")]
    IoMessage {
        message: String,
        /// Optional source context (e.g., file path, operation)
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Invalid SQL: {0}")]
    InvalidSql(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Namespace not found: {0}")]
    NamespaceNotFound(String),

    #[error("Schema version not found: table={table}, version={version}")]
    SchemaVersionNotFound { table: String, version: i32 },

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Invalid schema evolution: {0}")]
    InvalidSchemaEvolution(String),

    #[error("System column violation: {0}")]
    SystemColumnViolation(String),

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Not the leader for this shard (Raft cluster mode)
    ///
    /// Client should retry the request against the leader node.
    /// The `leader_addr` field contains the API address of the current leader
    /// (if known) to help with request redirection.
    #[error("Not leader for shard. Leader: {leader_addr:?}")]
    NotLeader {
        /// API address of the current leader (e.g., "192.168.1.100:8080")
        /// May be None if leader is unknown (during election)
        leader_addr: Option<String>,
    },

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Idempotent conflict: {0}")]
    IdempotentConflict(String),

    #[error("Column family error: {0}")]
    ColumnFamily(#[from] ColumnFamilyError),

    #[error("Flush error: {0}")]
    Flush(#[from] FlushError),

    #[error("Backup error: {0}")]
    Backup(#[from] BackupError),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    // Parameter validation errors
    #[error("Parameter count exceeded: maximum {max} parameters allowed, got {actual}")]
    ParamCountExceeded { max: usize, actual: usize },

    #[error(
        "Parameter size exceeded: parameter at index {index} is {actual_bytes} bytes (max \
         {max_bytes} bytes)"
    )]
    ParamSizeExceeded {
        index: usize,
        max_bytes: usize,
        actual_bytes: usize,
    },

    #[error("Parameter count mismatch: expected {expected} parameters, got {actual}")]
    ParamCountMismatch { expected: usize, actual: usize },

    #[error("Parameters not supported for {statement_type} statements")]
    ParamsNotSupported { statement_type: String },

    #[error("Parameter binding error: {message}")]
    ParameterBindingError { message: String },

    #[error("Handler execution timeout: exceeded {timeout_seconds}s")]
    Timeout { timeout_seconds: u64 },

    #[error("Not implemented: {feature} - {message}")]
    NotImplemented { feature: String, message: String },

    #[error("{0}")]
    Other(String),
}

// Convert kalamdb_live::LiveError to KalamDbError
impl From<kalamdb_live::error::LiveError> for KalamDbError {
    fn from(err: kalamdb_live::error::LiveError) -> Self {
        match err {
            kalamdb_live::error::LiveError::InvalidSql(msg) => KalamDbError::InvalidSql(msg),
            kalamdb_live::error::LiveError::InvalidOperation(msg) => {
                KalamDbError::InvalidOperation(msg)
            },
            kalamdb_live::error::LiveError::NotFound(msg) => KalamDbError::NotFound(msg),
            kalamdb_live::error::LiveError::TableNotFound(msg) => KalamDbError::TableNotFound(msg),
            kalamdb_live::error::LiveError::PermissionDenied(msg) => {
                KalamDbError::PermissionDenied(msg)
            },
            kalamdb_live::error::LiveError::SerializationError(msg) => {
                KalamDbError::SerializationError(msg)
            },
            kalamdb_live::error::LiveError::ExecutionError(msg) => {
                KalamDbError::ExecutionError(msg)
            },
            kalamdb_live::error::LiveError::Other(msg) => KalamDbError::Other(msg),
        }
    }
}

// Convert kalamdb_store::StorageError to KalamDbError
impl From<kalamdb_store::StorageError> for KalamDbError {
    fn from(err: kalamdb_store::StorageError) -> Self {
        match err {
            kalamdb_store::StorageError::PartitionNotFound(msg) => KalamDbError::NotFound(msg),
            kalamdb_store::StorageError::IoError(msg) => KalamDbError::io_message(msg),
            kalamdb_store::StorageError::SerializationError(msg) => {
                KalamDbError::SerializationError(msg)
            },
            kalamdb_store::StorageError::Unsupported(msg) => KalamDbError::InvalidOperation(msg),
            kalamdb_store::StorageError::UniqueConstraintViolation(msg) => {
                KalamDbError::AlreadyExists(msg)
            },
            kalamdb_store::StorageError::LockPoisoned(msg) => {
                KalamDbError::Other(format!("Lock poisoned: {}", msg))
            },
            kalamdb_store::StorageError::Other(msg) => KalamDbError::Other(msg),
        }
    }
}

// Convert kalamdb_filestore::FilestoreError to KalamDbError (Phase 13.8)
impl From<kalamdb_filestore::FilestoreError> for KalamDbError {
    fn from(err: kalamdb_filestore::FilestoreError) -> Self {
        match err {
            kalamdb_filestore::FilestoreError::Io(e) => KalamDbError::Io(e),
            kalamdb_filestore::FilestoreError::Arrow(e) => {
                KalamDbError::Other(format!("Arrow error: {}", e))
            },
            kalamdb_filestore::FilestoreError::Parquet(msg) => {
                KalamDbError::Other(format!("Parquet error: {}", msg))
            },
            kalamdb_filestore::FilestoreError::Path(msg) => {
                KalamDbError::Other(format!("Path error: {}", msg))
            },
            kalamdb_filestore::FilestoreError::PathTraversal(msg) => {
                KalamDbError::InvalidOperation(format!("Security: Path traversal blocked: {}", msg))
            },
            kalamdb_filestore::FilestoreError::BatchNotFound(msg) => KalamDbError::NotFound(msg),
            kalamdb_filestore::FilestoreError::InvalidBatchFile(msg) => {
                KalamDbError::InvalidOperation(format!("Invalid batch file: {}", msg))
            },
            kalamdb_filestore::FilestoreError::Serialization(e) => {
                KalamDbError::SerializationError(e.to_string())
            },
            kalamdb_filestore::FilestoreError::Config(msg) => {
                KalamDbError::InvalidOperation(format!("Storage config error: {}", msg))
            },
            kalamdb_filestore::FilestoreError::ObjectStore(msg) => {
                KalamDbError::Other(format!("ObjectStore error: {}", msg))
            },
            kalamdb_filestore::FilestoreError::StorageError(msg) => {
                KalamDbError::Other(format!("Storage error: {}", msg))
            },
            kalamdb_filestore::FilestoreError::InvalidTemplate(msg) => {
                KalamDbError::InvalidOperation(format!("Invalid storage template: {}", msg))
            },
            kalamdb_filestore::FilestoreError::Format(msg) => {
                KalamDbError::Other(format!("Format error: {}", msg))
            },
            kalamdb_filestore::FilestoreError::NotFound(msg) => KalamDbError::NotFound(msg),
            kalamdb_filestore::FilestoreError::Other(msg) => KalamDbError::Other(msg),
            kalamdb_filestore::FilestoreError::HealthCheckFailed(msg) => {
                KalamDbError::Other(format!("Storage health check failed: {}", msg))
            },
        }
    }
}

impl From<kalamdb_sql::parser::query_parser::QueryParseError> for KalamDbError {
    fn from(err: kalamdb_sql::parser::query_parser::QueryParseError) -> Self {
        match err {
            kalamdb_sql::parser::query_parser::QueryParseError::ParseError(msg)
            | kalamdb_sql::parser::query_parser::QueryParseError::InvalidSql(msg) => {
                KalamDbError::InvalidSql(msg)
            },
        }
    }
}

// Convert DataFusion errors
impl From<datafusion::error::DataFusionError> for KalamDbError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        KalamDbError::Other(format!("DataFusion error: {}", err))
    }
}

// Convert Arrow errors
impl From<datafusion::arrow::error::ArrowError> for KalamDbError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        KalamDbError::Other(format!("Arrow error: {}", err))
    }
}

// Convert schema_registry::RegistryError to KalamDbError
impl From<crate::schema_registry::RegistryError> for KalamDbError {
    fn from(err: crate::schema_registry::RegistryError) -> Self {
        match err {
            crate::schema_registry::RegistryError::TableNotFound { namespace, table } => {
                KalamDbError::TableNotFound(format!("{}.{}", namespace, table))
            },
            crate::schema_registry::RegistryError::StorageNotFound { storage_id } => {
                KalamDbError::NotFound(format!("Storage not found: {}", storage_id))
            },
            crate::schema_registry::RegistryError::SchemaConversion { message } => {
                KalamDbError::SchemaError(message)
            },
            crate::schema_registry::RegistryError::SchemaError(msg) => {
                KalamDbError::SchemaError(msg)
            },
            crate::schema_registry::RegistryError::ArrowError { message } => {
                KalamDbError::Other(format!("Registry Arrow error: {}", message))
            },
            crate::schema_registry::RegistryError::DataFusionError { message } => {
                KalamDbError::Other(format!("Registry DataFusion error: {}", message))
            },
            crate::schema_registry::RegistryError::StorageError { message } => {
                KalamDbError::Other(format!("Registry storage error: {}", message))
            },
            crate::schema_registry::RegistryError::InvalidConfig { message } => {
                KalamDbError::ConfigError(message)
            },
            crate::schema_registry::RegistryError::CacheFailed { message } => {
                KalamDbError::Other(format!("Cache failed: {}", message))
            },
            crate::schema_registry::RegistryError::ViewError { message } => {
                KalamDbError::Other(format!("View error: {}", message))
            },
            crate::schema_registry::RegistryError::InvalidOperation(msg) => {
                KalamDbError::InvalidOperation(msg)
            },
            crate::schema_registry::RegistryError::Other(msg) => {
                KalamDbError::Other(format!("Registry error: {}", msg))
            },
        }
    }
}

// Convert kalamdb_system::SystemError to KalamDbError
impl From<kalamdb_system::SystemError> for KalamDbError {
    fn from(err: kalamdb_system::SystemError) -> Self {
        match err {
            kalamdb_system::SystemError::Storage(msg) => {
                KalamDbError::Other(format!("System table storage error: {}", msg))
            },
            kalamdb_system::SystemError::NotFound(msg) => KalamDbError::NotFound(msg),
            kalamdb_system::SystemError::AlreadyExists(msg) => KalamDbError::AlreadyExists(msg),
            kalamdb_system::SystemError::InvalidOperation(msg) => {
                KalamDbError::InvalidOperation(msg)
            },
            kalamdb_system::SystemError::SerializationError(msg) => {
                KalamDbError::SerializationError(msg)
            },
            kalamdb_system::SystemError::DataFusion(msg) => {
                KalamDbError::Other(format!("DataFusion error: {}", msg))
            },
            kalamdb_system::SystemError::Arrow(e) => {
                KalamDbError::Other(format!("Arrow error: {}", e))
            },
            kalamdb_system::SystemError::Other(msg) => KalamDbError::Other(msg),
        }
    }
}

/// Storage-related errors
#[derive(Error, Debug)]
pub enum CoreStorageError {
    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Message not found: {0}")]
    MessageNotFound(i64),

    #[error("Invalid key format: {0}")]
    InvalidKey(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database is closed")]
    DatabaseClosed,

    #[error("Storage error: {0}")]
    Other(String),
}

/// Configuration-related errors
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    ReadError(#[from] std::io::Error),

    #[error("Failed to parse config: {0}")]
    ParseError(String),

    #[error("Invalid configuration: {0}")]
    ValidationError(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid value for {field}: {value}")]
    InvalidValue { field: String, value: String },
}

/// API-related errors
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Storage error: {0}")]
    Storage(#[from] CoreStorageError),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Message too large: {size} bytes (max: {max} bytes)")]
    MessageTooLarge { size: usize, max: usize },

    #[error("Query limit exceeded: {requested} (max: {max})")]
    QueryLimitExceeded { requested: usize, max: usize },

    #[error("Internal server error: {0}")]
    Internal(String),
}

/// Column family operation errors
#[derive(Error, Debug)]
pub enum ColumnFamilyError {
    #[error("Column family not found: {0}")]
    NotFound(String),

    #[error("Failed to create column family: {0}")]
    CreateFailed(String),

    #[error("Failed to drop column family: {0}")]
    DropFailed(String),

    #[error("Column family already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid column family name: {0}")]
    InvalidName(String),

    #[error("RocksDB error: {0}")]
    RocksDb(String),
}

/// Flush operation errors
#[derive(Error, Debug)]
pub enum FlushError {
    #[error("Failed to read from RocksDB buffer: {0}")]
    ReadFailed(String),

    #[error("Failed to write Parquet file: {0}")]
    WriteFailed(String),

    #[error("No data to flush for table: {0}")]
    NoData(String),

    #[error("Flush policy not configured for table: {0}")]
    NoPolicyConfigured(String),

    #[error("Failed to update flush metadata: {0}")]
    MetadataUpdateFailed(String),

    #[error("IO error during flush: {0}")]
    Io(String),

    #[error("Serialization error during flush: {0}")]
    Serialization(String),
}

/// Backup/restore operation errors
#[derive(Error, Debug)]
pub enum BackupError {
    #[error("Backup not found: {0}")]
    NotFound(String),

    #[error("Failed to create backup: {0}")]
    CreateFailed(String),

    #[error("Failed to restore backup: {0}")]
    RestoreFailed(String),

    #[error("Backup manifest is corrupt: {0}")]
    CorruptManifest(String),

    #[error("Backup validation failed: {0}")]
    ValidationFailed(String),

    #[error("Failed to copy Parquet files: {0}")]
    FileCopyFailed(String),

    #[error("Checksum mismatch: expected={expected}, actual={actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("IO error during backup/restore: {0}")]
    Io(String),
}

impl CoreStorageError {
    /// Create a validation error
    pub fn validation<S: Into<String>>(msg: S) -> Self {
        CoreStorageError::Validation(msg.into())
    }

    /// Create a generic error
    pub fn other<S: Into<String>>(msg: S) -> Self {
        CoreStorageError::Other(msg.into())
    }
}

impl ConfigError {
    /// Create a parse error
    pub fn parse<S: Into<String>>(msg: S) -> Self {
        ConfigError::ParseError(msg.into())
    }

    /// Create a validation error
    pub fn validation<S: Into<String>>(msg: S) -> Self {
        ConfigError::ValidationError(msg.into())
    }
}

impl ApiError {
    /// Create an invalid request error
    pub fn invalid_request<S: Into<String>>(msg: S) -> Self {
        ApiError::InvalidRequest(msg.into())
    }

    /// Create an internal error
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        ApiError::Internal(msg.into())
    }
}

impl ColumnFamilyError {
    /// Create a not found error
    pub fn not_found<S: Into<String>>(name: S) -> Self {
        ColumnFamilyError::NotFound(name.into())
    }

    /// Create a create failed error
    pub fn create_failed<S: Into<String>>(msg: S) -> Self {
        ColumnFamilyError::CreateFailed(msg.into())
    }

    /// Create a drop failed error
    pub fn drop_failed<S: Into<String>>(msg: S) -> Self {
        ColumnFamilyError::DropFailed(msg.into())
    }
}

impl FlushError {
    /// Create a read failed error
    pub fn read_failed<S: Into<String>>(msg: S) -> Self {
        FlushError::ReadFailed(msg.into())
    }

    /// Create a write failed error
    pub fn write_failed<S: Into<String>>(msg: S) -> Self {
        FlushError::WriteFailed(msg.into())
    }

    /// Create a no data error
    pub fn no_data<S: Into<String>>(table: S) -> Self {
        FlushError::NoData(table.into())
    }
}

impl BackupError {
    /// Create a not found error
    pub fn not_found<S: Into<String>>(backup: S) -> Self {
        BackupError::NotFound(backup.into())
    }

    /// Create a create failed error
    pub fn create_failed<S: Into<String>>(msg: S) -> Self {
        BackupError::CreateFailed(msg.into())
    }

    /// Create a restore failed error
    pub fn restore_failed<S: Into<String>>(msg: S) -> Self {
        BackupError::RestoreFailed(msg.into())
    }

    /// Create a validation failed error
    pub fn validation_failed<S: Into<String>>(msg: S) -> Self {
        BackupError::ValidationFailed(msg.into())
    }
}

impl KalamDbError {
    /// Create a table not found error
    pub fn table_not_found<S: Into<String>>(table: S) -> Self {
        KalamDbError::TableNotFound(table.into())
    }

    /// Create a namespace not found error
    pub fn namespace_not_found<S: Into<String>>(namespace: S) -> Self {
        KalamDbError::NamespaceNotFound(namespace.into())
    }

    /// Create a schema version not found error
    pub fn schema_version_not_found<S: Into<String>>(table: S, version: i32) -> Self {
        KalamDbError::SchemaVersionNotFound {
            table: table.into(),
            version,
        }
    }

    /// Create an invalid schema evolution error
    pub fn invalid_schema_evolution<S: Into<String>>(msg: S) -> Self {
        KalamDbError::InvalidSchemaEvolution(msg.into())
    }

    /// Create an IO error with a message (without source error).
    ///
    /// Use this when the original IO error has already been converted to a string,
    /// or when creating an IO-like error from other contexts.
    pub fn io_message<S: Into<String>>(message: S) -> Self {
        KalamDbError::IoMessage {
            message: message.into(),
            source: None,
        }
    }

    /// Create an IO error with a message and source error.
    ///
    /// Use this when you have access to the original error for better error chains.
    pub fn io_with_source<S, E>(message: S, source: E) -> Self
    where
        S: Into<String>,
        E: std::error::Error + Send + Sync + 'static,
    {
        KalamDbError::IoMessage {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

// Conversion from StorageError validation to string
impl From<String> for CoreStorageError {
    fn from(msg: String) -> Self {
        CoreStorageError::Validation(msg)
    }
}

// Conversion from String to KalamDbError
impl From<String> for KalamDbError {
    fn from(msg: String) -> Self {
        KalamDbError::Other(msg)
    }
}

// Conversion from anyhow::Error to KalamDbError
impl From<anyhow::Error> for KalamDbError {
    fn from(err: anyhow::Error) -> Self {
        KalamDbError::Other(err.to_string())
    }
}

// Conversion from TableError (kalamdb-tables) to KalamDbError
impl From<kalamdb_tables::TableError> for KalamDbError {
    fn from(err: kalamdb_tables::TableError) -> Self {
        use kalamdb_tables::TableError;
        match err {
            TableError::Storage(msg) => KalamDbError::Storage(CoreStorageError::Other(msg)),
            TableError::AlreadyExists(msg) => KalamDbError::AlreadyExists(msg),
            TableError::NotFound(msg) => KalamDbError::NotFound(msg),
            TableError::TableNotFound(msg) => KalamDbError::TableNotFound(msg),
            TableError::InvalidOperation(msg) => KalamDbError::InvalidOperation(msg),
            TableError::Serialization(msg) => KalamDbError::SerializationError(msg),
            TableError::DataFusion(msg) => {
                KalamDbError::Other(format!("DataFusion error: {}", msg))
            },
            TableError::Arrow(e) => KalamDbError::Other(format!("Arrow error: {}", e)),
            TableError::Filestore(msg) => KalamDbError::Other(format!("Filestore error: {}", msg)),
            TableError::SchemaError(msg) => KalamDbError::SchemaError(msg),
            TableError::NotLeader { leader_addr } => KalamDbError::NotLeader { leader_addr },
            TableError::Other(msg) => KalamDbError::Other(msg),
            TableError::ConstraintViolation(msg) => {
                KalamDbError::InvalidOperation(format!("Constraint violation: {}", msg))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_error_display() {
        let err = CoreStorageError::MessageNotFound(12345);
        assert_eq!(err.to_string(), "Message not found: 12345");
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::MissingField("port".to_string());
        assert_eq!(err.to_string(), "Missing required field: port");
    }

    #[test]
    fn test_api_error_display() {
        let err = ApiError::MessageTooLarge {
            size: 2_000_000,
            max: 1_048_576,
        };
        assert_eq!(err.to_string(), "Message too large: 2000000 bytes (max: 1048576 bytes)");
    }

    #[test]
    fn test_storage_error_validation() {
        let err = CoreStorageError::validation("invalid data");
        assert!(matches!(err, CoreStorageError::Validation(_)));
    }

    #[test]
    fn test_config_error_parse() {
        let err = ConfigError::parse("invalid TOML");
        assert!(matches!(err, ConfigError::ParseError(_)));
    }

    #[test]
    fn test_table_not_found_error() {
        let err = KalamDbError::table_not_found("messages");
        assert_eq!(err.to_string(), "Table not found: messages");
    }

    #[test]
    fn test_namespace_not_found_error() {
        let err = KalamDbError::namespace_not_found("default");
        assert_eq!(err.to_string(), "Namespace not found: default");
    }

    #[test]
    fn test_schema_version_not_found_error() {
        let err = KalamDbError::schema_version_not_found("messages", 3);
        assert_eq!(err.to_string(), "Schema version not found: table=messages, version=3");
    }

    #[test]
    fn test_invalid_schema_evolution_error() {
        let err = KalamDbError::invalid_schema_evolution("cannot drop required column");
        assert_eq!(err.to_string(), "Invalid schema evolution: cannot drop required column");
    }

    #[test]
    fn test_column_family_not_found_error() {
        let err = ColumnFamilyError::not_found("user_table:messages");
        assert_eq!(err.to_string(), "Column family not found: user_table:messages");
    }

    #[test]
    fn test_flush_error_no_data() {
        let err = FlushError::no_data("messages");
        assert_eq!(err.to_string(), "No data to flush for table: messages");
    }

    #[test]
    fn test_backup_checksum_mismatch() {
        let err = BackupError::ChecksumMismatch {
            expected: "abc123".to_string(),
            actual: "def456".to_string(),
        };
        assert_eq!(err.to_string(), "Checksum mismatch: expected=abc123, actual=def456");
    }
}
