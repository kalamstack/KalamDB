use datafusion::error::DataFusionError;
use thiserror::Error;
use tonic::Status;

use crate::applier::ApplierError;

/// Errors from `OperationService` domain-typed execution.
#[derive(Debug, Error)]
pub enum OperationError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table provider not available: {0}")]
    ProviderNotAvailable(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("applier error: {0}")]
    Applier(#[from] ApplierError),

    #[error("datafusion error: {0}")]
    DataFusion(#[from] DataFusionError),
}

impl From<OperationError> for Status {
    fn from(err: OperationError) -> Self {
        match err {
            OperationError::TableNotFound(msg) => Status::not_found(msg),
            OperationError::ProviderNotAvailable(msg) => Status::unavailable(msg),
            OperationError::InvalidArgument(msg) => Status::invalid_argument(msg),
            OperationError::PermissionDenied(msg) => Status::permission_denied(msg),
            OperationError::Applier(e) => Status::internal(e.to_string()),
            OperationError::DataFusion(e) => Status::internal(e.to_string()),
        }
    }
}
