//! Error types for KalamDB SDK client operations.
//!
//! Provides a comprehensive error enum covering all failure scenarios
//! including network errors, authentication failures, and protocol violations.

use std::fmt;

/// Result type alias using [`KalamLinkError`]
pub type Result<T> = std::result::Result<T, KalamLinkError>;

/// Errors that can occur during KalamDB SDK operations.
///
/// # Examples
///
/// ```rust,no_run
/// use kalam_client::{KalamLinkClient, KalamLinkError};
///
/// # async fn example() -> kalam_client::Result<()> {
/// let client = KalamLinkClient::builder().base_url("http://invalid-host:9999").build()?;
///
/// match client.execute_query("SELECT 1", None, None, None).await {
///     Ok(response) => println!("Success: {:?}", response),
///     Err(KalamLinkError::NetworkError(msg)) => {
///         eprintln!("Connection failed: {}", msg);
///     },
///     Err(e) => eprintln!("Other error: {}", e),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub enum KalamLinkError {
    /// Network or HTTP request errors
    NetworkError(String),

    /// Authentication failures (invalid token, expired credentials)
    AuthenticationError(String),

    /// Server requires initial setup (root password not configured)
    /// The CLI should prompt the user to complete server setup
    SetupRequired(String),

    /// Server returned an error response
    ServerError {
        /// HTTP status code
        status_code: u16,
        /// Error message from server
        message: String,
    },

    /// Invalid configuration (missing URL, invalid settings)
    ConfigurationError(String),

    /// JSON serialization/deserialization errors
    SerializationError(String),

    /// WebSocket connection or protocol errors
    WebSocketError(String),

    /// Connection timeout
    TimeoutError(String),

    /// The operation was cancelled
    Cancelled,
}

impl fmt::Display for KalamLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NetworkError(msg) => write!(f, "Network error: {}", msg),
            Self::AuthenticationError(msg) => write!(f, "Authentication failed: {}", msg),
            Self::SetupRequired(msg) => write!(f, "Server setup required: {}", msg),
            Self::ServerError {
                status_code,
                message,
            } => write!(f, "Server error ({}): {}", status_code, message),
            Self::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            Self::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            Self::WebSocketError(msg) => write!(f, "WebSocket error: {}", msg),
            Self::TimeoutError(msg) => write!(f, "Timeout: {}", msg),
            Self::Cancelled => write!(f, "Operation cancelled"),
        }
    }
}

impl std::error::Error for KalamLinkError {}

// Conversions from external error types (only for native builds with tokio-runtime)
#[cfg(feature = "tokio-runtime")]
impl From<reqwest::Error> for KalamLinkError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            Self::TimeoutError(err.to_string())
        } else if err.is_connect() {
            Self::NetworkError(format!("Connection failed: {}", err))
        } else if err.is_status() {
            if let Some(status) = err.status() {
                Self::ServerError {
                    status_code: status.as_u16(),
                    message: err.to_string(),
                }
            } else {
                Self::NetworkError(err.to_string())
            }
        } else {
            Self::NetworkError(err.to_string())
        }
    }
}

impl From<serde_json::Error> for KalamLinkError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerializationError(err.to_string())
    }
}

#[cfg(feature = "tokio-runtime")]
impl From<tokio_tungstenite::tungstenite::Error> for KalamLinkError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocketError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = KalamLinkError::NetworkError("connection refused".to_string());
        assert_eq!(err.to_string(), "Network error: connection refused");

        let err = KalamLinkError::ServerError {
            status_code: 500,
            message: "Internal server error".to_string(),
        };
        assert_eq!(err.to_string(), "Server error (500): Internal server error");

        let err = KalamLinkError::Cancelled;
        assert_eq!(err.to_string(), "Operation cancelled");
    }
}
