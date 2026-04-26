//! # kalamdb-store
//!
//! Low-level key-value store abstraction for user, shared, and stream tables.
//! This crate isolates all direct RocksDB interactions for non-system tables,
//! allowing kalamdb-core to remain free of RocksDB dependencies.
//!
//! ## Architecture
//!
//! ```text
//! kalamdb-core (business logic)
//!     ↓
//! kalamdb-store (K/V operations)
//!     ↓
//! kalamdb-store::backends::* (storage engines)
//! ```
//!
//! ## Table Types
//!
//! - **User Tables**: Isolated per user with key format `{user_id}:{row_id}`
//! - **Shared Tables**: Global data with key format `{row_id}`
//! - **Stream Tables**: Ephemeral events with key format `{timestamp_ms}:{row_id}`

mod async_utils;
pub mod backends;
pub mod entity_store; // Phase 14: Type-safe EntityStore<K, V> with generic keys
pub mod index; // Generic secondary index support
pub mod indexed_store; // Phase 15: Automatic secondary index management
pub mod raft_storage; // Phase 17: Raft log/meta persistence
pub mod storage_trait;

#[cfg(feature = "rocksdb")]
pub use backends::rocksdb::{RocksDBBackend, RocksDbInit};
// Phase 14: Export new type-safe EntityStore traits
pub use entity_store::{
    CrossUserTableStore,
    EntityStore,
    EntityStoreAsync, // Async versions using spawn_blocking internally
};
// Export index types
pub use index::{FunctionExtractor, IndexKeyExtractor, SecondaryIndex};
#[cfg(feature = "datafusion")]
pub use indexed_store::{extract_i64_equality, extract_string_equality};
// Phase 15: Export IndexedEntityStore for automatic index management
pub use indexed_store::{IndexDefinition, IndexedEntityStore};
// Re-export StorageKey from kalamdb-commons to avoid import inconsistency
pub use kalamdb_commons::StorageKey;
// Phase 17: Export Raft storage types
pub use raft_storage::{
    GroupId, RaftLogEntry, RaftLogId, RaftPartitionStore, RaftSnapshotData, RaftSnapshotMeta,
    RaftVote, RAFT_PARTITION_NAME,
};
pub use storage_trait::{
    Operation, Partition, StorageBackend, StorageBackendAsync, StorageError, StorageStats,
};

// Make test_utils available for testing in dependent crates
pub mod test_utils;

/// Open the configured storage backend and return it as a generic [`StorageBackend`].
///
/// The current default implementation uses RocksDB internally, but callers
/// outside this crate only depend on the storage abstraction.
pub fn open_storage_backend(
    db_path: &std::path::Path,
    settings: &kalamdb_configs::RocksDbSettings,
) -> anyhow::Result<(std::sync::Arc<dyn StorageBackend>, usize)> {
    #[cfg(feature = "rocksdb")]
    {
        return backends::rocksdb::open_storage_backend(db_path, settings);
    }

    #[cfg(not(feature = "rocksdb"))]
    {
        let _ = (db_path, settings);
        anyhow::bail!("kalamdb-store was built without the rocksdb backend feature enabled")
    }
}
