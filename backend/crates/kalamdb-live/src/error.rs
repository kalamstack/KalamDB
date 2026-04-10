//! Error types for the KalamDB live query subsystem.

use kalamdb_sql::parser::query_parser::QueryParseError;
use thiserror::Error;

/// Error type for live query operations.
///
/// Covers the subset of errors that can occur during subscription management,
/// notification dispatch, filter evaluation, and initial data fetching.
#[derive(Error, Debug)]
pub enum LiveError {
    #[error("Invalid SQL: {0}")]
    InvalidSql(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("{0}")]
    Other(String),
}

impl From<QueryParseError> for LiveError {
    fn from(err: QueryParseError) -> Self {
        LiveError::InvalidSql(err.to_string())
    }
}

/// Extension trait for Result types to simplify error conversions into LiveError.
pub trait LiveResultExt<T> {
    /// Convert any error into LiveError::Other with context message.
    fn into_live_error(self, context: &str) -> Result<T, LiveError>;

    /// Convert any error into LiveError::SerializationError with context message.
    fn into_serialization_error(self, context: &str) -> Result<T, LiveError>;

    /// Convert any Arrow/DataFusion error into LiveError with context.
    fn into_arrow_error_ctx(self, context: &str) -> Result<T, LiveError>;
}

impl<T, E: std::fmt::Display> LiveResultExt<T> for Result<T, E> {
    fn into_live_error(self, context: &str) -> Result<T, LiveError> {
        self.map_err(|e| LiveError::Other(format!("{}: {}", context, e)))
    }

    fn into_serialization_error(self, context: &str) -> Result<T, LiveError> {
        self.map_err(|e| LiveError::SerializationError(format!("{}: {}", context, e)))
    }

    fn into_arrow_error_ctx(self, context: &str) -> Result<T, LiveError> {
        self.map_err(|e| LiveError::Other(format!("{}: {}", context, e)))
    }
}
