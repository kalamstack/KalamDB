//! Topic error response model

use serde::Serialize;

/// Error response for topic operations
#[derive(Debug, Serialize)]
pub struct TopicErrorResponse {
    /// Human-readable error message
    pub error: String,
    /// Error code
    pub code: String,
}

impl TopicErrorResponse {
    /// Create a new error response
    pub fn new(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: code.into(),
        }
    }

    /// Create a forbidden error
    pub fn forbidden(message: &str) -> Self {
        Self::new(message, "FORBIDDEN")
    }

    /// Create a not found error
    pub fn not_found(message: &str) -> Self {
        Self::new(message, "NOT_FOUND")
    }

    /// Create an internal error
    pub fn internal_error(message: &str) -> Self {
        Self::new(message, "INTERNAL_ERROR")
    }
}
