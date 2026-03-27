//! SQL query execution with HTTP transport.
//!
//! `models` (query models) is always available — no `tokio-runtime` needed.
//! The HTTP executor requires the `tokio-runtime` feature.

pub mod models;

pub use models::{QueryRequest, QueryResponse, QueryResult};

#[cfg(feature = "tokio-runtime")]
mod executor;
#[cfg(feature = "tokio-runtime")]
pub use executor::{AuthRefreshCallback, QueryExecutor, UploadProgressCallback};
