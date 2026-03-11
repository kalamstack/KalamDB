//! KalamDB Core Library
//!
//! This crate provides the core storage functionality for KalamDB, a high-performance
//! distributed database with:
//!
//! - **Namespace/Table Management**: Multi-tenant data isolation with user, shared, and stream tables
//! - **Arrow Integration**: Native Apache Arrow columnar storage for efficient analytics
//! - **RocksDB Backend**: Fast write path with sub-millisecond latency
//! - **Parquet Storage**: Compressed columnar format for flushed segments
//! - **Live Query Subscriptions**: Real-time data synchronization via WebSocket
//! - **SQL Support**: Full SQL via Apache DataFusion with custom extensions
//!
//! # Core Components
//!
//! - [`app_context::AppContext`]: Global singleton for accessing all core services
//! - [`sql::SqlExecutor`]: SQL statement execution engine
//! - [`providers`]: Table providers with BaseTableProvider trait
//! - [`schema_registry::SchemaRegistry`]: Schema caching and Arrow schema management
//! - [`jobs::JobsManager`]: Background job scheduling and execution
//! - [`live::LiveQueryManager`]: Real-time query subscription management
//!
//! # Example
//!
//! ```rust,ignore
//! use kalamdb_core::app_context::AppContext;
//! use kalamdb_store::StorageBackend;
//! use std::sync::Arc;
//!
//! // Initialize the AppContext singleton
//! let backend: Arc<dyn StorageBackend> = create_backend();
//! let ctx = AppContext::init(backend, node_id, storage_path, config);
//!
//! // Execute SQL
//! let executor = ctx.sql_executor();
//! let result = executor.execute("SELECT * FROM my_table", None, None).await?;
//! ```

pub mod app_context;
pub mod applier;
pub mod error;
pub mod error_extensions;
pub mod jobs;
pub mod live;
pub mod manifest;
pub mod metrics;
pub mod providers;
pub mod schema_registry;
pub mod slow_query_logger;
pub mod sql;
pub mod vector;
pub mod views;

// Re-export commonly used items
pub use error_extensions::KalamDbResultExt;

// Re-export modules that were moved to other crates
pub mod auth {
    pub use kalamdb_session::permissions;
}

pub mod live_query {
    pub use crate::live::*;
}

pub mod system_columns {
    pub use crate::schema_registry::SystemColumnsService;
}

// Test helpers module for unit tests inside this crate.
// Integration tests should include their own helpers or use path-based modules.
#[cfg(test)]
pub mod test_helpers;
