use thiserror::Error;

/// Shared error type used across the PostgreSQL extension workspace.
#[derive(Debug, Error)]
pub enum KalamPgError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("{0}")]
    Execution(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    /// The KalamDB server could not be reached at the given address.
    #[error(
        "KalamDB server is not running or unreachable at {0} – \
         start the server and verify the host/port in CREATE SERVER OPTIONS"
    )]
    ServerUnreachable(String),
}

#[cfg(feature = "datafusion")]
impl From<datafusion_common::DataFusionError> for KalamPgError {
    fn from(value: datafusion_common::DataFusionError) -> Self {
        Self::Execution(value.to_string())
    }
}
