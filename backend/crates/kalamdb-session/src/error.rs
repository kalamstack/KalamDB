//! Session error types

use kalamdb_commons::models::{NamespaceId, Role, TableName};
use std::fmt;

/// Result type for session operations
pub type SessionResult<T> = Result<T, SessionError>;

/// Errors that can occur during session and permission operations
#[derive(Debug, Clone)]
pub enum SessionError {
    /// Access denied to a table
    AccessDenied {
        namespace_id: NamespaceId,
        table_name: TableName,
        role: Role,
        reason: String,
    },
    /// Session context not found in DataFusion session
    SessionContextNotFound,
    /// Invalid session state
    InvalidSessionState(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::AccessDenied {
                namespace_id,
                table_name,
                role,
                reason,
            } => {
                write!(
                    f,
                    "Access denied: Role '{:?}' cannot access table '{}.{}'. {}",
                    role,
                    namespace_id.as_str(),
                    table_name.as_str(),
                    reason
                )
            },
            SessionError::SessionContextNotFound => {
                write!(f, "SessionUserContext not found in session extensions")
            },
            SessionError::InvalidSessionState(msg) => {
                write!(f, "Invalid session state: {}", msg)
            },
        }
    }
}

impl std::error::Error for SessionError {}
