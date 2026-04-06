//! Data operation responses
//!
//! Shared response type for both user and shared data operations.
use serde::{Deserialize, Serialize};

/// Response for data operations
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum DataResponse {
    #[default]
    Ok,

    /// Number of rows affected by the operation
    RowsAffected(usize),

    /// Subscription registered
    Subscribed { subscription_id: String },

    /// Error
    Error { message: String },
}

impl DataResponse {
    /// Create an error response with the given message
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error {
            message: msg.into(),
        }
    }

    /// Returns true if this is not an error response
    pub fn is_ok(&self) -> bool {
        !matches!(self, Self::Error { .. })
    }

    /// Returns the number of rows affected, or 0 if not applicable
    pub fn rows_affected(&self) -> usize {
        match self {
            DataResponse::RowsAffected(n) => *n,
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_response_is_ok() {
        assert!(DataResponse::Ok.is_ok());
        assert!(DataResponse::RowsAffected(5).is_ok());
        assert!(DataResponse::Subscribed {
            subscription_id: "sub_1".to_string()
        }
        .is_ok());
        assert!(!DataResponse::Error {
            message: "error".to_string()
        }
        .is_ok());
    }

    #[test]
    fn test_data_response_rows_affected() {
        assert_eq!(DataResponse::Ok.rows_affected(), 0);
        assert_eq!(DataResponse::RowsAffected(10).rows_affected(), 10);
        assert_eq!(
            DataResponse::Error {
                message: "err".to_string()
            }
            .rows_affected(),
            0
        );
    }

    #[test]
    fn test_data_response_error_constructor() {
        let response = DataResponse::error("test error");
        assert!(!response.is_ok());
        match response {
            DataResponse::Error { message } => assert_eq!(message, "test error"),
            _ => panic!("Expected error response"),
        }
    }
}
