//! Error types for KalamDB views and registry

use std::fmt;

use kalamdb_commons::models::StorageId;

/// Registry-specific errors
#[derive(Debug, Clone)]
pub enum RegistryError {
    /// Table not found in cache
    TableNotFound { namespace: String, table: String },

    /// Storage not found
    StorageNotFound { storage_id: StorageId },

    /// Schema conversion error
    SchemaConversion { message: String },

    /// Schema error (generic)
    SchemaError(String),

    /// Arrow error
    ArrowError { message: String },

    /// DataFusion error
    DataFusionError { message: String },

    /// Storage backend error
    StorageError { message: String },

    /// Invalid configuration
    InvalidConfig { message: String },

    /// Cache operation failed
    CacheFailed { message: String },

    /// View error
    ViewError { message: String },

    /// Invalid operation
    InvalidOperation(String),

    /// Other errors
    Other(String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::TableNotFound { namespace, table } => {
                write!(f, "Table not found: {}.{}", namespace, table)
            },
            RegistryError::StorageNotFound { storage_id } => {
                write!(f, "Storage not found: {}", storage_id)
            },
            RegistryError::SchemaConversion { message } => {
                write!(f, "Schema conversion error: {}", message)
            },
            RegistryError::SchemaError(message) => {
                write!(f, "Schema error: {}", message)
            },
            RegistryError::ArrowError { message } => {
                write!(f, "Arrow error: {}", message)
            },
            RegistryError::DataFusionError { message } => {
                write!(f, "DataFusion error: {}", message)
            },
            RegistryError::StorageError { message } => {
                write!(f, "Storage error: {}", message)
            },
            RegistryError::InvalidConfig { message } => {
                write!(f, "Invalid configuration: {}", message)
            },
            RegistryError::CacheFailed { message } => {
                write!(f, "Cache operation failed: {}", message)
            },
            RegistryError::ViewError { message } => {
                write!(f, "View error: {}", message)
            },
            RegistryError::InvalidOperation(message) => {
                write!(f, "Invalid operation: {}", message)
            },
            RegistryError::Other(message) => {
                write!(f, "{}", message)
            },
        }
    }
}

impl std::error::Error for RegistryError {}

// Conversions from other error types
impl From<datafusion::error::DataFusionError> for RegistryError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        RegistryError::DataFusionError {
            message: err.to_string(),
        }
    }
}

impl From<arrow::error::ArrowError> for RegistryError {
    fn from(err: arrow::error::ArrowError) -> Self {
        RegistryError::ArrowError {
            message: err.to_string(),
        }
    }
}
