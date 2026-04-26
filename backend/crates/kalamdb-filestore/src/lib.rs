//! # kalamdb-filestore
//!
//! Async-first object-store operations for KalamDB cold storage.
//!
//! All file I/O goes through [`StorageCached`] which wraps a `Storage` config
//! with a lazy `ObjectStore` instance. Supports local filesystem, S3, GCS, and
//! Azure backends transparently.
//!
//! ## Example
//!
//! ```rust,ignore
//! use kalamdb_filestore::StorageCached;
//!
//! let cached = StorageCached::with_default_timeouts(storage);
//! let result = cached.put(table_type, &table_id, None, "batch-0.parquet", data).await?;
//! let files  = cached.list_parquet_files(table_type, &table_id, None).await?;
//! ```

mod core;
pub mod error;
pub mod files;
pub mod health;
pub mod manifest;
pub mod parquet;
pub mod paths;
pub mod registry;

#[cfg(test)]
mod tests;

// Re-export commonly used types
pub use error::{FilestoreError, Result};
pub use files::{FileStorageService, StagedFile, StagingManager};
pub use health::{ConnectivityTestResult, HealthStatus, StorageHealthResult, StorageHealthService};
pub use manifest::json::{manifest_exists, read_manifest_json, write_manifest_json};
pub use parquet::{
    reader::{parse_parquet_stream, RecordBatchFileStream},
    writer::ParquetWriteResult,
};
pub use registry::{StorageCached, StorageRegistry};
