//! Core execution models for SQL handlers (v3)
//!
//! This module consolidates the core types previously defined in
//! `handlers/types.rs` into dedicated files under `executor/models/`.
//!
//! Types:
//! - ExecutionContext
//! - ExecutionResult
//!
//! Note: Parameters use DataFusion's native `datafusion::scalar::ScalarValue` directly.

mod execution_context;
mod execution_result;

// Re-export DataFusion's ScalarValue for convenience
pub use datafusion::scalar::ScalarValue;
pub use execution_context::ExecutionContext;
pub use execution_result::ExecutionResult;
// Re-export SessionUserContext from kalamdb-session-datafusion for TableProviders.
pub use kalamdb_session_datafusion::SessionUserContext;
