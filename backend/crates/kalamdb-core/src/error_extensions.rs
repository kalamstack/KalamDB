//! Error conversion trait extensions for reducing boilerplate.
//!
//! This module provides convenient trait methods to convert errors from external
//! crates (like Arrow, Parquet, RocksDB, serde_json) into KalamDB error types without repetitive
//! .map_err() calls.
//!
//! # Examples
//!
//! ```rust,ignore
//! // Serialization errors
//! let json = serde_json::to_string(&data)
//!     .into_serialization_error("Failed to serialize config")?;
//!
//! // Arrow/DataFusion errors
//! let batch = RecordBatch::try_new(schema, columns)
//!     .into_arrow_error_ctx("Failed to create record batch")?;
//!
//! // Generic errors with context
//! file_operation()
//!     .into_kalamdb_error("File operation failed")?;
//!
//! // Invalid operation errors
//! validate_input()
//!     .into_invalid_operation("Input validation failed")?;
//!
//! ```

use crate::error::KalamDbError;

/// Extension trait for Result types to simplify error conversions.
///
/// Provides convenience methods to convert any error type into specific
/// KalamDbError variants without verbose .map_err() closures.
pub trait KalamDbResultExt<T> {
    /// Convert any error into KalamDbError::Other with context message.
    ///
    /// # Example
    /// ```rust,ignore
    /// file.read_to_string()
    ///     .into_kalamdb_error("Failed to read config file")?;
    /// ```
    fn into_kalamdb_error(self, context: &str) -> Result<T, KalamDbError>;

    /// Convert Arrow errors into KalamDbError with generic message.
    ///
    /// # Example
    /// ```rust,ignore
    /// RecordBatch::try_new(schema, columns)
    ///     .into_arrow_error()?;
    /// ```
    fn into_arrow_error(self) -> Result<T, KalamDbError>;

    /// Convert Arrow errors into KalamDbError with context.
    ///
    /// # Example
    /// ```rust,ignore
    /// cast(&array, &new_type)
    ///     .into_arrow_error_ctx("Schema evolution cast failed")?;
    /// ```
    fn into_arrow_error_ctx(self, context: &str) -> Result<T, KalamDbError>;

    /// Convert errors into KalamDbError::ExecutionError with context.
    ///
    /// # Example
    /// ```rust,ignore
    /// execute_query()
    ///     .into_execution_error("Query execution failed")?;
    /// ```
    fn into_execution_error(self, context: &str) -> Result<T, KalamDbError>;

    /// Convert errors into KalamDbError::SerializationError.
    ///
    /// # Example
    /// ```rust,ignore
    /// serde_json::to_string(&config)
    ///     .into_serialization_error("Config serialization failed")?;
    /// ```
    fn into_serialization_error(self, context: &str) -> Result<T, KalamDbError>;

    /// Convert errors into KalamDbError::SchemaError.
    ///
    /// # Example
    /// ```rust,ignore
    /// validate_schema_change()
    ///     .into_schema_error("Invalid schema evolution")?;
    /// ```
    fn into_schema_error(self, context: &str) -> Result<T, KalamDbError>;

    /// Convert errors into KalamDbError::InvalidOperation.
    ///
    /// # Example
    /// ```rust,ignore
    /// validate_table_options()
    ///     .into_invalid_operation("Invalid table configuration")?;
    /// ```
    fn into_invalid_operation(self, context: &str) -> Result<T, KalamDbError>;

}

impl<T, E: std::fmt::Display> KalamDbResultExt<T> for Result<T, E> {
    #[inline]
    fn into_kalamdb_error(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::Other(format!("{}: {}", context, e)))
    }

    #[inline]
    fn into_arrow_error(self) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::Other(format!("Arrow error: {}", e)))
    }

    #[inline]
    fn into_arrow_error_ctx(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::Other(format!("Arrow error - {}: {}", context, e)))
    }

    #[inline]
    fn into_execution_error(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::ExecutionError(format!("{}: {}", context, e)))
    }

    #[inline]
    fn into_serialization_error(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::SerializationError(format!("{}: {}", context, e)))
    }

    #[inline]
    fn into_schema_error(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::SchemaError(format!("{}: {}", context, e)))
    }

    #[inline]
    fn into_invalid_operation(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::InvalidOperation(format!("{}: {}", context, e)))
    }

}

/// Specialized extension methods for commonly-used types.
///
/// These provide more ergonomic conversions for specific error types
/// like serde_json.
pub trait SerdeJsonResultExt<T> {
    /// Convert serde_json errors into KalamDbError::SerializationError.
    ///
    /// # Example
    /// ```rust,ignore
    /// let json = serde_json::to_string(&data)
    ///     .into_serde_error("Config serialization")?;
    /// ```
    fn into_serde_error(self, context: &str) -> Result<T, KalamDbError>;
}

impl<T> SerdeJsonResultExt<T> for Result<T, serde_json::Error> {
    #[inline]
    fn into_serde_error(self, context: &str) -> Result<T, KalamDbError> {
        self.map_err(|e| KalamDbError::SerializationError(format!("{}: {}", context, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_kalamdb_error() {
        let result: Result<(), &str> = Err("something failed");
        let err = result.into_kalamdb_error("Test operation").unwrap_err();

        match err {
            KalamDbError::Other(msg) => {
                assert!(msg.contains("Test operation"));
                assert!(msg.contains("something failed"));
            },
            _ => panic!("Expected KalamDbError::Other"),
        }
    }

    #[test]
    fn test_into_arrow_error() {
        let result: Result<(), &str> = Err("arrow failed");
        let err = result.into_arrow_error().unwrap_err();

        match err {
            KalamDbError::Other(msg) => assert!(msg.contains("arrow failed")),
            _ => panic!("Expected KalamDbError::Other"),
        }
    }

    #[test]
    fn test_into_execution_error() {
        let result: Result<(), &str> = Err("execution issue");
        let err = result.into_execution_error("Query execution").unwrap_err();

        match err {
            KalamDbError::ExecutionError(msg) => {
                assert!(msg.contains("Query execution"));
                assert!(msg.contains("execution issue"));
            },
            _ => panic!("Expected KalamDbError::ExecutionError"),
        }
    }

    #[test]
    fn test_into_serialization_error() {
        let result: Result<(), &str> = Err("json parse error");
        let err = result.into_serialization_error("Parsing config").unwrap_err();

        match err {
            KalamDbError::SerializationError(msg) => {
                assert!(msg.contains("Parsing config"));
                assert!(msg.contains("json parse error"));
            },
            _ => panic!("Expected KalamDbError::SerializationError"),
        }
    }

    #[test]
    fn test_into_schema_error() {
        let result: Result<(), &str> = Err("incompatible types");
        let err = result.into_schema_error("Schema validation").unwrap_err();

        match err {
            KalamDbError::SchemaError(msg) => {
                assert!(msg.contains("Schema validation"));
                assert!(msg.contains("incompatible types"));
            },
            _ => panic!("Expected KalamDbError::SchemaError"),
        }
    }

    #[test]
    fn test_into_invalid_operation() {
        let result: Result<(), &str> = Err("operation not allowed");
        let err = result.into_invalid_operation("Validation").unwrap_err();

        match err {
            KalamDbError::InvalidOperation(msg) => {
                assert!(msg.contains("Validation"));
                assert!(msg.contains("operation not allowed"));
            },
            _ => panic!("Expected KalamDbError::InvalidOperation"),
        }
    }

    #[test]
    fn test_serde_json_error() {
        // Test with actual serde_json error
        let invalid_json = "{invalid json}";
        let result: Result<serde_json::Value, _> = serde_json::from_str(invalid_json);
        let err = result.into_serde_error("JSON parsing").unwrap_err();

        match err {
            KalamDbError::SerializationError(msg) => {
                assert!(msg.contains("JSON parsing"));
            },
            _ => panic!("Expected KalamDbError::SerializationError"),
        }
    }
}
