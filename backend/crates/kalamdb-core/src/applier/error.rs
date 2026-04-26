//! Error types for the unified applier

use std::fmt;

use thiserror::Error;

/// Errors that can occur during command application
#[derive(Debug, Error)]
pub enum ApplierError {
    /// Command validation failed
    #[error("Validation error: {0}")]
    Validation(String),

    /// No leader available for the Raft group
    #[error("No leader available for Raft group")]
    NoLeader,

    /// Raft consensus error
    #[error("Raft error: {0}")]
    Raft(String),

    /// Command execution failed
    #[error("Execution error: {0}")]
    Execution(String),

    /// Resource not found
    #[error("{resource_type} not found: {id}")]
    NotFound { resource_type: String, id: String },

    /// Resource already exists
    #[error("{resource_type} already exists: {id}")]
    AlreadyExists { resource_type: String, id: String },

    /// Authorization failed
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
}

impl ApplierError {
    /// Create a not found error
    pub fn not_found(resource_type: impl Into<String>, id: impl fmt::Display) -> Self {
        Self::NotFound {
            resource_type: resource_type.into(),
            id: id.to_string(),
        }
    }
}

impl From<crate::error::KalamDbError> for ApplierError {
    fn from(err: crate::error::KalamDbError) -> Self {
        match err {
            crate::error::KalamDbError::NotFound(msg) => ApplierError::NotFound {
                resource_type: "Resource".to_string(),
                id: msg,
            },
            crate::error::KalamDbError::AlreadyExists(msg) => ApplierError::AlreadyExists {
                resource_type: "Resource".to_string(),
                id: msg,
            },
            crate::error::KalamDbError::Unauthorized(msg) => ApplierError::Unauthorized(msg),
            other => ApplierError::Execution(other.to_string()),
        }
    }
}

impl From<ApplierError> for crate::error::KalamDbError {
    fn from(err: ApplierError) -> Self {
        match err {
            ApplierError::Validation(msg) => crate::error::KalamDbError::InvalidOperation(msg),
            ApplierError::NotFound { resource_type, id } => {
                crate::error::KalamDbError::NotFound(format!("{} {}", resource_type, id))
            },
            ApplierError::AlreadyExists { resource_type, id } => {
                crate::error::KalamDbError::AlreadyExists(format!("{} {}", resource_type, id))
            },
            ApplierError::Unauthorized(msg) => crate::error::KalamDbError::Unauthorized(msg),
            ApplierError::NoLeader => {
                crate::error::KalamDbError::ExecutionError("No Raft leader available".to_string())
            },
            other => crate::error::KalamDbError::ExecutionError(other.to_string()),
        }
    }
}
