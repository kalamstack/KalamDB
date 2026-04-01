//! WebSocket error codes

use serde::Serialize;

/// WebSocket error codes for type-safe error handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WsErrorCode {
    /// Authentication required before subscribing
    AuthRequired,
    /// Invalid subscription ID format
    InvalidSubscriptionId,
    /// Subscription limit exceeded
    SubscriptionLimitExceeded,
    /// Unauthorized access
    Unauthorized,
    /// Resource not found
    NotFound,
    /// Invalid SQL syntax
    InvalidSql,
    /// Unsupported operation
    Unsupported,
    /// Subscription registration failed
    SubscriptionFailed,
    /// Conversion error
    ConversionError,
    /// Batch fetch failed
    BatchFetchFailed,
    /// Message too large
    MessageTooLarge,
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Unsupported data format
    UnsupportedData,
    /// Generic protocol error
    Protocol,
}

impl WsErrorCode {
    /// Get the string representation of the error code
    pub fn as_str(&self) -> &'static str {
        match self {
            WsErrorCode::AuthRequired => "AUTH_REQUIRED",
            WsErrorCode::InvalidSubscriptionId => "INVALID_SUBSCRIPTION_ID",
            WsErrorCode::SubscriptionLimitExceeded => "SUBSCRIPTION_LIMIT_EXCEEDED",
            WsErrorCode::Unauthorized => "UNAUTHORIZED",
            WsErrorCode::NotFound => "NOT_FOUND",
            WsErrorCode::InvalidSql => "INVALID_SQL",
            WsErrorCode::Unsupported => "UNSUPPORTED",
            WsErrorCode::SubscriptionFailed => "SUBSCRIPTION_FAILED",
            WsErrorCode::ConversionError => "CONVERSION_ERROR",
            WsErrorCode::BatchFetchFailed => "BATCH_FETCH_FAILED",
            WsErrorCode::MessageTooLarge => "MESSAGE_TOO_LARGE",
            WsErrorCode::RateLimitExceeded => "RATE_LIMIT_EXCEEDED",
            WsErrorCode::UnsupportedData => "UNSUPPORTED_DATA",
            WsErrorCode::Protocol => "PROTOCOL",
        }
    }
}

impl std::fmt::Display for WsErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
