//! Authentication error response model

use serde::Serialize;

/// Error response body for authentication failures
#[derive(Debug, Serialize)]
pub struct AuthErrorResponse {
    /// Error type identifier (e.g., "unauthorized", "forbidden")
    pub error: String,
    /// Human-readable error message
    pub message: String,
}

impl AuthErrorResponse {
    /// Create a new error response
    #[inline]
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}
