//! # kalamdb-tables
//!
//! Table storage layer for user, shared, and stream tables in KalamDB.
//!
//! This crate contains the storage abstractions (stores) for:
//! - **UserTableStore**: Per-user partitioned table storage with RocksDB backend
//! - **SharedTableStore**: Global table storage accessible to all users
//! - **StreamTableStore**: Time-windowed streaming table storage with TTL
//!
//! **Note**: DataFusion TableProvider implementations are in `kalamdb-core/providers/`,
//! not in this crate. This crate provides only the storage layer.
//!
//! ## Architecture
//!
//! ### User Tables Storage
//! - Data partitioned by `user_id` for efficient per-user queries
//! - Implements EntityStore trait for type-safe key-value operations
//! - RocksDB backend with binary serialization
//!
//! ### Shared Tables Storage
//! - Global storage with no user partitioning
//! - Shared across all users in a namespace
//! - Same EntityStore implementation as user tables
//!
//! ### Stream Tables Storage
//! - Time-series data storage with configurable TTL
//! - Automatic eviction of old data via background jobs
//! - Optimized for append-only workloads
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use kalamdb_tables::{UserTableStore, SharedTableStore, StreamTableStore};
//! use kalamdb_store::EntityStore;
//!
//! // Create and use user table store
//! let store = UserTableStore::new(backend, "user_data");
//! store.put(&key, &row)?;
//! let row = store.get(&key)?;
//! ```

pub mod common;
pub mod error;
pub mod error_extensions;
pub mod manifest;
pub mod shared_tables;
pub mod store_ext;
pub mod stream_tables;
pub mod topics;
pub mod user_tables;
pub mod utils;

// Re-export commonly used types
pub use error::KalamDbError;
pub use error::{Result, TableError};

// Re-export table stores
pub use kalamdb_commons::models::StreamTableRow;
pub use kalamdb_commons::models::UserTableRow;
pub use kalamdb_vector::{
    new_indexed_shared_vector_hot_store, new_indexed_user_vector_hot_store,
    normalize_vector_column_name, SharedVectorHotOpId, SharedVectorHotStore, UserVectorHotOpId,
    UserVectorHotStore, VectorHotOp, VectorHotOpType,
};
pub use shared_tables::pk_index::{create_shared_table_pk_index, SharedTablePkIndex};
pub use shared_tables::shared_table_store::{
    new_indexed_shared_table_store, new_shared_table_store, SharedTableIndexedStore,
    SharedTableRow, SharedTableStore,
};
pub use stream_tables::stream_table_store::{
    new_stream_table_store, StreamTableStorageMode, StreamTableStore, StreamTableStoreConfig,
};
pub use topics::topic_message_models::{TopicMessage, TopicMessageId};
pub use topics::topic_message_store::TopicMessageStore;
pub use user_tables::pk_index::{create_user_table_pk_index, UserTablePkIndex};
pub use user_tables::user_table_store::{
    new_indexed_user_table_store, new_user_table_store, UserTableIndexedStore, UserTableStore,
};

// Re-export extension traits
pub use store_ext::{SharedTableStoreExt, StreamTableStoreExt, UserTableStoreExt};

// Re-export providers for core integration
pub use utils::{
    BaseTableProvider, KalamTableProvider, SharedTableProvider, StreamTableProvider,
    TableProviderCore, UserTableProvider,
};
