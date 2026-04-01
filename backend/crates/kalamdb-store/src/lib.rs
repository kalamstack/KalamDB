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
//! RocksDB (storage engine)
//! ```
//!
//! ## Table Types
//!
//! - **User Tables**: Isolated per user with key format `{user_id}:{row_id}`
//! - **Shared Tables**: Global data with key format `{row_id}`
//! - **Stream Tables**: Ephemeral events with key format `{timestamp_ms}:{row_id}`

mod async_utils;
mod cf_tuning;
pub mod entity_store; // Phase 14: Type-safe EntityStore<K, V> with generic keys
pub mod index; // Generic secondary index support
pub mod indexed_store; // Phase 15: Automatic secondary index management
pub mod raft_storage; // Phase 17: Raft log/meta persistence
pub mod rocksdb_impl;
pub mod rocksdb_init;
pub mod storage_trait;

pub use rocksdb_impl::RocksDBBackend;
pub use rocksdb_init::RocksDbInit;
pub use storage_trait::{Operation, Partition, StorageBackend, StorageBackendAsync, StorageError};

// Re-export StorageKey from kalamdb-commons to avoid import inconsistency
pub use kalamdb_commons::StorageKey;

// Phase 14: Export new type-safe EntityStore traits
pub use entity_store::{
    CrossUserTableStore,
    EntityStore,
    EntityStoreAsync, // Async versions using spawn_blocking internally
};

// Export index types
pub use index::{FunctionExtractor, IndexKeyExtractor, SecondaryIndex};

// Phase 15: Export IndexedEntityStore for automatic index management
pub use indexed_store::{IndexDefinition, IndexedEntityStore};

#[cfg(feature = "datafusion")]
pub use indexed_store::{extract_i64_equality, extract_string_equality};

// Phase 17: Export Raft storage types
pub use raft_storage::{
    GroupId, RaftLogEntry, RaftLogId, RaftPartitionStore, RaftSnapshotData, RaftSnapshotMeta,
    RaftVote, RAFT_PARTITION_NAME,
};

// Make test_utils available for testing in dependent crates
pub mod test_utils;

/// Open the configured storage backend and return it as a generic [`StorageBackend`].
///
/// The current implementation uses RocksDB internally, but callers outside this
/// crate only depend on the storage abstraction.
pub fn open_storage_backend(
    db_path: &std::path::Path,
    settings: &kalamdb_configs::RocksDbSettings,
) -> anyhow::Result<(std::sync::Arc<dyn StorageBackend>, usize)> {
    let db_init = RocksDbInit::new(db_path.to_string_lossy().into_owned(), settings.clone());
    let (db, cf_names) = db_init.open_with_cf_names()?;
    let backend = std::sync::Arc::new(RocksDBBackend::with_options_and_settings(
        db,
        settings.sync_writes,
        settings.disable_wal,
        settings.clone(),
    ));
    backend.set_known_cf_names(cf_names.clone());
    Ok((backend, cf_names.len()))
}

/// Attempt to extract a RocksDB handle from a generic `StorageBackend`.
///
/// Returns `Some(Arc<rocksdb::DB>)` when the backend is a RocksDB-backed implementation,
/// otherwise returns `None`.
pub fn try_extract_rocksdb_db(
    backend: &std::sync::Arc<dyn crate::storage_trait::StorageBackend>,
) -> Option<std::sync::Arc<rocksdb::DB>> {
    backend
        .as_any()
        .downcast_ref::<crate::rocksdb_impl::RocksDBBackend>()
        .map(|rb| rb.db().clone())
}
