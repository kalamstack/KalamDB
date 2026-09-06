use thiserror::Error;

/// Errors that can occur in system table operations
#[derive(Error, Debug)]
pub enum SystemError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Other error: {0}")]
    Other(String),
}

/// Result type for system table operations
pub type Result<T> = std::result::Result<T, SystemError>;

// Convert from kalamdb_store::StorageError
impl From<kalamdb_store::StorageError> for SystemError {
    fn from(err: kalamdb_store::StorageError) -> Self {
        SystemError::Storage(err.to_string())
    }
}

/// Extension trait for Result types to simplify SystemError conversions.
///
/// Reduces boilerplate by providing convenient methods to convert errors
/// from external crates into SystemError variants.
pub trait SystemResultExt<T> {
    /// Convert any error into SystemError::Other with context message.
    fn into_system_error(self, context: &str) -> Result<T>;

    /// Convert errors into SystemError::Storage with context.
    fn into_storage_error(self, context: &str) -> Result<T>;

    /// Convert Arrow errors into SystemError::Arrow.
    fn into_arrow_error(self, context: &str) -> Result<T>;
}

impl<T, E: std::fmt::Display> SystemResultExt<T> for std::result::Result<T, E> {
    #[inline]
    fn into_system_error(self, context: &str) -> Result<T> {
        self.map_err(|e| SystemError::Other(format!("{}: {}", context, e)))
    }

    #[inline]
    fn into_storage_error(self, context: &str) -> Result<T> {
        self.map_err(|e| SystemError::Storage(format!("{}: {}", context, e)))
    }

    #[inline]
    fn into_arrow_error(self, context: &str) -> Result<T> {
        self.map_err(|e| {
            SystemError::Arrow(arrow::error::ArrowError::ExternalError(Box::new(
                std::io::Error::other(format!("{}: {}", context, e)),
            )))
        })
    }
}
