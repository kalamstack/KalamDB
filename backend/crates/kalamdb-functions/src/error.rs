//! Errors from the function runtime and activation path.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FunctionsError {
    #[error("{0}")]
    Invalid(String),
    #[error("procedure not found: {0}")]
    UnknownProcedure(String),
    #[error("abi mismatch: artifact {artifact}, runtime {runtime}")]
    AbiMismatch { artifact: u32, runtime: u32 },
    #[error("invocation timed out")]
    Timeout,
    #[error("invocation cancelled")]
    Cancelled,
    #[error("isolate memory limit exceeded")]
    MemoryLimit,
    #[error("function engine capacity exhausted")]
    Capacity,
    #[error("function resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("javascript exception: {0}")]
    Javascript(String),
    #[error("stale function revision (expected {expected}, actual {actual})")]
    StaleRevision { expected: String, actual: String },
    #[error("{0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, FunctionsError>;
