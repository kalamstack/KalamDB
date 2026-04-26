//! Stream tables module
//!
//! Provides commit log-based storage for stream tables with:
//! - TTL-based automatic eviction (via time-bucketed log cleanup)
//! - MVCC architecture with user_id and _seq system columns
//! - Persistent append-only logs for fast replay

pub mod stream_table_provider;
pub mod stream_table_store;

// Re-export StreamTableRowId from kalamdb_commons for convenience
pub use kalamdb_commons::{ids::StreamTableRowId, models::StreamTableRow};
pub use stream_table_provider::StreamTableProvider;
pub use stream_table_store::{
    new_stream_table_store, StreamTableStorageMode, StreamTableStore, StreamTableStoreConfig,
};
