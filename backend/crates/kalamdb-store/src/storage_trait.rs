//! Storage backend abstraction for pluggable storage implementations.
//!
//! This module provides a trait-based abstraction layer that allows KalamDB to support
//! multiple storage backends (RocksDB, Sled, Redis, in-memory, etc.) without changing
//! core logic.
//!
//! ## Architecture
//!
//! The abstraction uses a `StorageBackend` trait that defines common operations:
//! - get/put/delete for key-value access
//! - batch for atomic multi-operation transactions
//! - scan for range queries
//! - partition management (mapped to backend-native keyspaces)
//!
//! ## Partition Model
//!
//! Since different backends have different concepts for data organization:
//! - **RocksDB**: Partition = key prefix inside a fixed physical column-family set
//! - **Sled**: Partition = Tree
//! - **Redis**: Partition = Key Prefix
//! - **In-Memory**: Partition = HashMap namespace
//!
//! We use a generic `Partition` abstraction that backends map to their native concepts.
//!
//! ## Example Usage
//!
//! ```rust
//! use kalamdb_store::storage_trait::{Operation, Partition, StorageBackend};
//!
//! fn store_user_data<S: StorageBackend>(backend: &S, user_id: &str, data: &[u8]) {
//!     let partition = Partition::new(format!("user_{}", user_id));
//!     backend.put(&partition, b"profile", data).expect("Failed to store");
//! }
//! ```
//!
//! ## Implementing a Custom Backend
//!
//! To implement a new storage backend:
//!
//! ```rust,ignore
//! use kalamdb_store::storage_trait::{StorageBackend, Partition, Operation};
//!
//! struct MyBackend {
//!     // Your backend's connection/state
//! }
//!
//! impl StorageBackend for MyBackend {
//!     fn get(&self, partition: &Partition, key: &[u8]) -> Result<Option<Vec<u8>>> {
//!         // Implement key lookup in your backend
//!         todo!()
//!     }
//!     
//!     fn put(&self, partition: &Partition, key: &[u8], value: &[u8]) -> Result<()> {
//!         // Implement key write in your backend
//!         todo!()
//!     }
//!     
//!     // ... implement other required methods
//! }
//! ```

use std::{collections::BTreeMap, path::Path};

pub use kalamdb_commons::storage::{KvIterator, Operation, Partition, Result, StorageError};

/// Deterministic key/value stats emitted by a storage backend for `system.stats`.
pub type StorageStats = BTreeMap<String, String>;

/// Trait for pluggable storage backend implementations.
///
/// Implementations must be thread-safe (Send + Sync) to allow concurrent access.
///
/// ## Performance Considerations
///
/// - `get` operations should be fast (typically <1ms)
/// - `put` operations may be buffered (check backend documentation)
/// - `batch` operations should be atomic (all-or-nothing)
/// - `scan` operations should return an iterator for memory efficiency
/// - `stats` should stay low-cost because it feeds the synchronous `system.stats` callback
///
/// ## Error Handling
///
/// Implementations should:
/// - Return `PartitionNotFound` if partition doesn't exist
/// - Return `IoError` for underlying storage failures
/// - Return `Unsupported` for operations not supported by the backend
pub trait StorageBackend: Send + Sync {
    /// Retrieves a value by key from the specified partition.
    ///
    /// Returns `Ok(None)` if the key doesn't exist.
    fn get(&self, partition: &Partition, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Stores a key-value pair in the specified partition.
    ///
    /// If the key already exists, its value is updated.
    fn put(&self, partition: &Partition, key: &[u8], value: &[u8]) -> Result<()>;

    /// Deletes a key from the specified partition.
    ///
    /// Returns `Ok(())` even if the key doesn't exist (idempotent).
    fn delete(&self, partition: &Partition, key: &[u8]) -> Result<()>;

    /// Executes multiple operations atomically in a batch.
    ///
    /// Either all operations succeed or none are applied.
    fn batch(&self, operations: Vec<Operation>) -> Result<()>;

    /// Scans keys in a partition, optionally filtered by prefix and limit.
    ///
    /// Returns an iterator of (key, value) pairs. The iterator should be
    /// memory-efficient (not loading all data at once).
    ///
    /// ## Parameters
    /// - `prefix`: If Some, only return keys starting with this prefix
    /// - `start_key`: If Some, start scanning from this key (inclusive). Must be >= prefix if both
    ///   are set.
    /// - `limit`: If Some, return at most this many entries
    fn scan(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<KvIterator<'_>>;

    /// Scans keys in reverse order, optionally filtered by prefix and limit.
    ///
    /// This is used by hot-path latest-version lookups where the newest entry is
    /// stored last in the keyspace. Backends with native reverse iterators should
    /// override this; the default implementation falls back to a forward scan and
    /// reverses the collected rows.
    fn scan_reverse(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<KvIterator<'_>> {
        if matches!(limit, Some(0)) {
            return Ok(Box::new(std::iter::empty()));
        }

        let mut items: Vec<(Vec<u8>, Vec<u8>)> =
            self.scan(partition, prefix, None, None)?.collect();

        if let Some(start) = start_key {
            items.retain(|(key, _)| key.as_slice() <= start);
        }

        if let Some(prefix) = prefix {
            items.retain(|(key, _)| key.starts_with(prefix));
        }

        items.reverse();

        if let Some(limit) = limit {
            items.truncate(limit);
        }

        Ok(Box::new(items.into_iter()))
    }

    /// Checks if a partition exists.
    fn partition_exists(&self, partition: &Partition) -> bool;

    /// Creates a new partition.
    ///
    /// Returns `Ok(())` if the partition already exists (idempotent).
    fn create_partition(&self, partition: &Partition) -> Result<()>;

    /// Lists all partitions in the storage backend.
    fn list_partitions(&self) -> Result<Vec<Partition>>;

    /// Deletes a partition and all its data.
    ///
    /// **Warning**: This is a destructive operation and cannot be undone.
    fn drop_partition(&self, partition: &Partition) -> Result<()>;

    /// Compacts a partition to clean up tombstones and optimize storage.
    ///
    /// This is a blocking operation that can take significant time depending on
    /// partition size. It's typically called after flush operations to reclaim
    /// space from deleted/updated rows.
    ///
    /// Returns `Ok(())` if compaction succeeds or if the backend doesn't support
    /// compaction (no-op for backends without compaction).
    fn compact_partition(&self, partition: &Partition) -> Result<()>;

    /// Flush all memtables across the backend to SST files.
    ///
    /// This allows RocksDB to reclaim WAL files that are pinned by unflushed
    /// memtables in idle physical keyspaces. Without periodic flushing, WAL files
    /// can accumulate indefinitely when only a subset of physical stores receive writes.
    ///
    /// Default implementation is a no-op for backends without WAL.
    fn flush_all_memtables(&self) -> Result<()> {
        Ok(())
    }

    /// Creates a native backup of the storage engine to the specified backup directory.
    ///
    /// For RocksDB this uses [`BackupEngine`](rocksdb::backup::BackupEngine) for an efficient
    /// hot, consistent backup with deduplication across incremental snapshots.
    /// The backup directory will be created if it does not exist.
    ///
    /// Returns `Err` for backends that do not support native backup.
    fn backup_to(&self, _backup_dir: &Path) -> Result<()> {
        Err(StorageError::Other(
            "backup_to is not supported by this storage backend".to_string(),
        ))
    }

    /// Restores the storage engine from a backup directory created by
    /// [`StorageBackend::backup_to`].
    ///
    /// For RocksDB this calls `BackupEngine::restore_from_latest_backup`, overwriting the
    /// current data directory. **The server must be restarted after this call** to reload
    /// the restored data into memory.
    ///
    /// Returns `Err` for backends that do not support native restore.
    fn restore_from(&self, _backup_dir: &Path) -> Result<()> {
        Err(StorageError::Other(
            "restore_from is not supported by this storage backend".to_string(),
        ))
    }

    /// Returns low-cost backend stats exposed through `system.stats`.
    ///
    /// Implementations should return backend-agnostic keys under `storage_*`
    /// (for example `storage_backend` and `storage_partition_count`) and any
    /// engine-specific keys under a stable engine prefix such as `rocksdb_*`.
    fn stats(&self) -> StorageStats;
}

/// Extension trait providing async versions of StorageBackend methods.
///
/// These methods internally use `tokio::task::spawn_blocking` to offload
/// synchronous storage operations to a blocking thread pool, preventing
/// the async runtime from being blocked.
///
/// ## Usage
///
/// ```rust,ignore
/// use kalamdb_store::storage_trait::{StorageBackend, StorageBackendAsync, Partition};
/// use std::sync::Arc;
///
/// async fn store_data(backend: Arc<dyn StorageBackend>, partition: &Partition) {
///     // Use async variants in async contexts:
///     let value = backend.get_async(partition, b"key").await.unwrap();
///     backend.put_async(partition, b"key", b"value").await.unwrap();
/// }
/// ```
#[async_trait::async_trait]
pub trait StorageBackendAsync: Send + Sync {
    /// Async version of `get()` - retrieves a value by key.
    async fn get_async(&self, partition: &Partition, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Async version of `put()` - stores a key-value pair.
    async fn put_async(&self, partition: &Partition, key: &[u8], value: &[u8]) -> Result<()>;

    /// Async version of `delete()` - removes a key.
    async fn delete_async(&self, partition: &Partition, key: &[u8]) -> Result<()>;

    /// Async version of `batch()` - executes multiple operations atomically.
    async fn batch_async(&self, operations: Vec<Operation>) -> Result<()>;

    /// Async version of `scan()` - scans keys in a partition.
    /// Returns collected results since iterators can't cross spawn_blocking boundary.
    async fn scan_async(
        &self,
        partition: &Partition,
        prefix: Option<Vec<u8>>,
        start_key: Option<Vec<u8>>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Async version of `compact_partition()` - compacts a partition to clean up tombstones.
    async fn compact_partition_async(&self, partition: &Partition) -> Result<()>;
}

// Blanket implementation for Arc<dyn StorageBackend>
#[async_trait::async_trait]
impl StorageBackendAsync for std::sync::Arc<dyn StorageBackend> {
    async fn get_async(&self, partition: &Partition, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let backend = self.clone();
        let partition = partition.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || backend.get(&partition, &key))
            .await
            .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }

    async fn put_async(&self, partition: &Partition, key: &[u8], value: &[u8]) -> Result<()> {
        let backend = self.clone();
        let partition = partition.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || backend.put(&partition, &key, &value))
            .await
            .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }

    async fn delete_async(&self, partition: &Partition, key: &[u8]) -> Result<()> {
        let backend = self.clone();
        let partition = partition.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || backend.delete(&partition, &key))
            .await
            .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }

    async fn batch_async(&self, operations: Vec<Operation>) -> Result<()> {
        let backend = self.clone();
        tokio::task::spawn_blocking(move || backend.batch(operations))
            .await
            .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }

    async fn scan_async(
        &self,
        partition: &Partition,
        prefix: Option<Vec<u8>>,
        start_key: Option<Vec<u8>>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let backend = self.clone();
        let partition = partition.clone();
        tokio::task::spawn_blocking(move || {
            let iter = backend.scan(&partition, prefix.as_deref(), start_key.as_deref(), limit)?;
            Ok(iter.collect())
        })
        .await
        .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }

    async fn compact_partition_async(&self, partition: &Partition) -> Result<()> {
        let backend = self.clone();
        let partition = partition.clone();
        tokio::task::spawn_blocking(move || backend.compact_partition(&partition))
            .await
            .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_creation() {
        let p1 = Partition::new("users");
        assert_eq!(p1.name(), "users");

        let p2 = Partition::from("tables");
        assert_eq!(p2.name(), "tables");
    }

    #[test]
    fn test_operation_construction() {
        let op = Operation::Put {
            partition: Partition::new("test"),
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };

        match op {
            Operation::Put {
                partition,
                key,
                value,
            } => {
                assert_eq!(partition.name(), "test");
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            },
            _ => panic!("Wrong operation type"),
        }
    }

    #[test]
    fn test_error_display() {
        let err = StorageError::PartitionNotFound("users".to_string());
        assert_eq!(err.to_string(), "Partition not found: users");

        let err = StorageError::IoError("disk full".to_string());
        assert_eq!(err.to_string(), "I/O error: disk full");
    }
}
