//! Type-safe entity storage with generic key types.
//!
//! This module provides the new `EntityStore<K, V>` trait which uses type-safe keys
//! (instead of strings) to provide compile-time safety and prevent wrong-key bugs.
//!
//! ## Architecture
//!
//! ```text
//! EntityStore<K, V>        ← Typed entity CRUD with generic keys (this file)
//!     ↓
//! StorageBackend           ← Generic K/V operations (storage_trait.rs)
//!     ↓
//! RocksDB/Sled/etc         ← Actual storage implementation
//! ```
//!
//! ## Key Differences from Old EntityStore<T>
//!
//! - **Old**: `EntityStore<T>` used string keys, single type parameter
//! - **New**: `EntityStore<K, V>` uses type-safe keys, two type parameters
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use kalamdb_store::{EntityStore, StorageBackend};
//! use kalamdb_commons::{StorageKey, UserId};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct User {
//!     user_id: UserId,
//!     name: String,
//! }
//! use std::sync::Arc;
//!
//! struct UserStore {
//!     backend: Arc<dyn StorageBackend>,
//! }
//!
//! impl EntityStore<UserId, User> for UserStore {
//!     fn backend(&self) -> &Arc<dyn StorageBackend> {
//!         &self.backend
//!     }
//!
//!     fn partition(&self) -> Partition {
//!         Partition::new("system_users")
//!     }
//! }
//!
//! // Type-safe usage:
//! let user_id = UserId::new("u1");
//! let user = User { user_id: user_id.clone(), name: "Alice".into(), ... };
//! store.put(&user_id, &user).unwrap();
//! let retrieved = store.get(&user_id).unwrap().unwrap();
//! ```

use std::{collections::VecDeque, sync::Arc};

use kalamdb_commons::{next_storage_key_bytes, KSerializable, StorageKey};

use crate::{
    async_utils::run_blocking_result,
    storage_trait::{Partition, Result, StorageBackend, StorageError},
};

/// Directional scanning for entity stores
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Older,
    Newer,
}

pub type EntityIterator<'a, K, V> = Box<dyn Iterator<Item = Result<(K, V)>> + Send + 'a>;

/// Trait for typed entity storage with type-safe keys and automatic serialization.
///
/// This trait provides strongly-typed CRUD operations with compile-time key safety.
/// Unlike the old `EntityStore<T>` which used string keys, this version uses
/// generic key types (K) that implement `StorageKey`.
///
/// ## Type Parameters
/// - `K`: Key type that implements StorageKey (UserId, RowId, TableId, etc.)
/// - `V`: Value/entity type that must be Serialize + Deserialize
///
/// ## Required Methods
/// - `backend()`: Returns reference to the storage backend
/// - `partition()`: Returns partition name for this entity type
///
/// ## Provided Methods (with [`KSerializable`] delegation)
/// - `serialize()`: Entity → bytes (delegates to [`KSerializable::encode`])
/// - `deserialize()`: bytes → Entity (delegates to [`KSerializable::decode`])
/// - `put()`: Store an entity by key
/// - `get()`: Retrieve an entity by key
/// - `delete()`: Remove an entity by key
/// - `scan_prefix()`: Scan entities with key prefix
/// - `scan_all()`: Scan all entities in partition
///
/// ## Type Safety Benefits
///
/// ```rust,ignore
/// // Compile-time safety prevents wrong keys:
/// let user_id = UserId::new("u1");
/// let table_id = TableId::new(...);
///
/// user_store.get(&user_id)   // ✓ Compiles
/// user_store.get(&table_id)  // ✗ Compile error - wrong key type!
/// ```
pub trait EntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    /// Returns a reference to the storage backend.
    ///
    /// ⚠️ **INTERNAL USE ONLY** - Do not call this method directly from outside the trait!
    /// This method is only meant to be used internally by EntityStore trait methods.
    /// Use the provided EntityStore methods (get, put, delete, scan_*, etc.) instead.
    ///
    /// **Rationale**: Direct backend access bypasses type safety and proper key serialization.
    /// All operations should go through EntityStore methods which ensure:
    /// - Proper key serialization via StorageKey trait
    /// - Type-safe deserialization via KSerializable trait
    /// - Consistent error handling
    /// - Future optimizations (caching, batching, etc.)
    #[doc(hidden)]
    fn backend(&self) -> &Arc<dyn StorageBackend>;

    /// Returns the partition for this entity type.
    ///
    /// Examples: "system_users", "system_jobs", "user_table:default:users"
    fn partition(&self) -> Partition;

    /// Scan rows relative to a starting key in a specified direction, returning an iterator.
    fn scan_directional(
        &self,
        start_key: Option<&K>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<EntityIterator<'_, K, V>> {
        const MAX_PREALLOC_CAPACITY: usize = 100_000;
        if limit == 0 {
            return Ok(Box::new(Vec::<Result<(K, V)>>::new().into_iter()));
        }

        let partition = self.partition();
        match direction {
            ScanDirection::Newer => {
                let start_bytes = start_key.map(|k| next_storage_key_bytes(&k.storage_key()));
                let iter =
                    self.backend().scan(&partition, None, start_bytes.as_deref(), Some(limit))?;

                let mut rows = Vec::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));
                for (key_bytes, value_bytes) in iter {
                    let key = K::from_storage_key(&key_bytes)
                        .map_err(StorageError::SerializationError)?;
                    let entity = self.deserialize(&value_bytes)?;
                    rows.push(Ok((key, entity)));
                    if rows.len() >= limit {
                        break;
                    }
                }

                Ok(Box::new(rows.into_iter()))
            },
            ScanDirection::Older => {
                let start_bytes = start_key.map(|k| k.storage_key());
                let iter = self.backend().scan(&partition, None, None, None)?;
                let mut collected: VecDeque<(Vec<u8>, Vec<u8>)> =
                    VecDeque::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));

                for (key_bytes, value_bytes) in iter {
                    if let Some(start) = &start_bytes {
                        if key_bytes.as_slice() >= start.as_slice() {
                            break;
                        }
                    }
                    if collected.len() == limit {
                        collected.pop_front();
                    }
                    collected.push_back((key_bytes, value_bytes));
                }

                let mut rows = Vec::with_capacity(collected.len().min(MAX_PREALLOC_CAPACITY));
                for (key_bytes, value_bytes) in collected.into_iter().rev() {
                    let key = K::from_storage_key(&key_bytes)
                        .map_err(StorageError::SerializationError)?;
                    let entity = self.deserialize(&value_bytes)?;
                    rows.push(Ok((key, entity)));
                }

                Ok(Box::new(rows.into_iter()))
            },
        }
    }

    /// Returns a lazy iterator over all entities in the partition.
    fn scan_iterator(
        &self,
        prefix: Option<&K>,
        start_key: Option<&K>,
    ) -> Result<EntityIterator<'_, K, V>> {
        let partition = self.partition();
        let prefix_bytes = prefix.map(|k| k.storage_key());
        let start_key_bytes = start_key.map(|k| k.storage_key());

        let iter = self.backend().scan(
            &partition,
            prefix_bytes.as_deref(),
            start_key_bytes.as_deref(),
            None,
        )?;

        let mapped = iter.map(|(key_bytes, value_bytes)| {
            let key = K::from_storage_key(&key_bytes).map_err(StorageError::SerializationError)?;
            let value = V::decode(&value_bytes)?;
            Ok((key, value))
        });

        Ok(Box::new(mapped))
    }

    /// Serializes an entity to bytes.
    ///
    /// Default implementation delegates to [`KSerializable::encode`].
    fn serialize(&self, entity: &V) -> Result<Vec<u8>> {
        entity.encode()
    }

    /// Deserializes bytes to an entity.
    ///
    /// Default implementation delegates to [`KSerializable::decode`].
    fn deserialize(&self, bytes: &[u8]) -> Result<V> {
        V::decode(bytes)
    }

    /// Stores an entity with the given key.
    ///
    /// The entity is serialized using `serialize()` before storage.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let user_id = UserId::new("u1");
    /// let user = User { user_id: user_id.clone(), ... };
    /// store.put(&user_id, &user)?;
    /// ```
    fn put(&self, key: &K, entity: &V) -> Result<()> {
        let partition = self.partition();
        let value = self.serialize(entity)?;
        self.backend().put(&partition, &key.storage_key(), &value)
    }

    /// Stores multiple entities atomically in a batch.
    ///
    /// All writes succeed or all fail (atomic operation). This is 100× faster
    /// than individual `put()` calls for bulk inserts (e.g., Parquet restore).
    ///
    /// ## Performance
    ///
    /// - Individual puts: 1000 rows = 1000 RocksDB writes (~100ms)
    /// - Batch put: 1000 rows = 1 RocksDB write batch (~1ms)
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let entries = vec![
    ///     (UserId::new("u1"), User { ... }),
    ///     (UserId::new("u2"), User { ... }),
    ///     (UserId::new("u3"), User { ... }),
    /// ];
    /// store.batch_put(&entries)?; // Atomic write
    /// ```
    fn batch_put(&self, entries: &[(K, V)]) -> Result<()> {
        use crate::storage_trait::Operation;

        let partition = self.partition();
        let operations: Result<Vec<Operation>> = entries
            .iter()
            .map(|(key, entity)| {
                let value = self.serialize(entity)?;
                Ok(Operation::Put {
                    partition: partition.clone(),
                    key: key.storage_key(),
                    value,
                })
            })
            .collect();

        self.backend().batch(operations?)
    }

    /// Retrieves an entity by key.
    ///
    /// Returns `Ok(None)` if the key doesn't exist.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let user_id = UserId::new("u1");
    /// if let Some(user) = store.get(&user_id)? {
    ///     println!("Found user: {}", user.name);
    /// }
    /// ```
    #[must_use = "the retrieved entity should be used"]
    fn get(&self, key: &K) -> Result<Option<V>> {
        let partition = self.partition();
        match self.backend().get(&partition, &key.storage_key())? {
            Some(bytes) => Ok(Some(self.deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Deletes an entity by key.
    ///
    /// Returns `Ok(())` even if the key doesn't exist (idempotent).
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let user_id = UserId::new("u1");
    /// store.delete(&user_id)?;
    /// ```
    fn delete(&self, key: &K) -> Result<()> {
        let partition = self.partition();
        self.backend().delete(&partition, &key.storage_key())
    }

    /// Scans entities with keys matching the given prefix.
    ///
    /// Returns a vector of (K, V) pairs with properly deserialized keys.
    /// Entries with malformed keys are skipped with a warning log.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let prefix = UserId::new("user_");
    /// let results = store.scan_prefix_typed(&prefix, None)?;
    /// for (user_id, user) in results {
    ///     println!("User {}: {}", user_id, user.name);
    /// }
    /// ```
    fn scan_prefix_typed(&self, prefix: &K, limit: Option<usize>) -> Result<Vec<(K, V)>> {
        const MAX_SCAN_LIMIT: usize = 10000;
        let effective_limit = limit.unwrap_or(MAX_SCAN_LIMIT);
        let partition = self.partition();
        let iter = self.backend().scan(
            &partition,
            Some(&prefix.storage_key()),
            None,
            Some(effective_limit),
        )?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key = match K::from_storage_key(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("Skipping entry with malformed key: {}", e);
                    continue;
                },
            };
            let entity = match self.deserialize(&value_bytes) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("Skipping entry with malformed value: {}", e);
                    continue;
                },
            };
            results.push((key, entity));
            if results.len() >= effective_limit {
                break;
            }
        }

        Ok(results)
    }

    /// Scans all entities in the partition with typed key deserialization.
    ///
    /// Returns a vector of (K, V) pairs with properly deserialized keys.
    /// Entries with malformed keys are skipped with a warning log.
    ///
    /// ## Arguments
    /// * `limit` - Maximum entries to return (defaults to 100,000)
    /// * `prefix` - Optional key prefix to filter by
    /// * `start_key` - Optional key to start scanning from
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let all_users = store.scan_all_typed(None, None, None)?;
    /// for (user_id, user) in all_users {
    ///     println!("User {}: {}", user_id, user.name);
    /// }
    /// ```
    fn scan_all_typed(
        &self,
        limit: Option<usize>,
        prefix: Option<&K>,
        start_key: Option<&K>,
    ) -> Result<Vec<(K, V)>> {
        const MAX_SCAN_LIMIT: usize = 10000;
        let effective_limit = limit.unwrap_or(MAX_SCAN_LIMIT);

        let partition = self.partition();
        let prefix_bytes = prefix.map(|k| k.storage_key());
        let start_key_bytes = start_key.map(|k| k.storage_key());

        let iter = self.backend().scan(
            &partition,
            prefix_bytes.as_deref(),
            start_key_bytes.as_deref(),
            Some(effective_limit),
        )?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key = match K::from_storage_key(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("Skipping entry with malformed key: {}", e);
                    continue;
                },
            };
            let entity = match self.deserialize(&value_bytes) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("Skipping entry with malformed value: {}", e);
                    continue;
                },
            };
            results.push((key, entity));
            if results.len() >= effective_limit {
                break;
            }
        }

        Ok(results)
    }

    /// Scans entities with raw byte prefix (for partial key prefixes).
    ///
    /// This method accepts a raw byte prefix (e.g., for scanning with partial composite keys)
    /// but always returns properly typed (K, V) pairs. Use this when you have a raw byte prefix
    /// that doesn't correspond to a complete key (e.g., JobNodeId::prefix_for_node returns only
    /// the node_id component, not a complete JobNodeId).
    ///
    /// ## Example
    /// ```rust,ignore
    /// // When prefix_for_node returns Vec<u8> (partial key)
    /// let prefix_bytes = JobNodeId::prefix_for_node(&node_id);
    /// let results = store.scan_with_raw_prefix(&prefix_bytes, None, limit)?;
    /// // Returns Vec<(JobNodeId, JobNode)> with properly typed keys
    /// ```
    fn scan_with_raw_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(K, V)>> {
        const MAX_PREALLOC_CAPACITY: usize = 100_000;
        let partition = self.partition();
        let iter = self.backend().scan(&partition, Some(prefix), start_key, Some(limit))?;

        let mut results = Vec::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));
        for (key_bytes, value_bytes) in iter {
            let key = match K::from_storage_key(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("Skipping entry with malformed key: {}", e);
                    continue;
                },
            };
            let entity = match self.deserialize(&value_bytes) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("Skipping entry with malformed value: {}", e);
                    continue;
                },
            };
            results.push((key, entity));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    /// Scans entities with typed prefix and start key (returns typed keys).
    ///
    /// Similar to `scan_all_typed` but uses typed prefix and start_key.
    fn scan_typed_with_prefix_and_start(
        &self,
        prefix: Option<&K>,
        start_key: Option<&K>,
        limit: usize,
    ) -> Result<Vec<(K, V)>> {
        const MAX_PREALLOC_CAPACITY: usize = 100_000;
        let partition = self.partition();
        let prefix_bytes = prefix.map(|k| k.storage_key());
        let start_bytes = start_key.map(|k| k.storage_key());
        let iter = self.backend().scan(
            &partition,
            prefix_bytes.as_deref(),
            start_bytes.as_deref(),
            Some(limit),
        )?;

        let mut results = Vec::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));
        for (key_bytes, value_bytes) in iter {
            let key = match K::from_storage_key(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("Skipping entry with malformed key: {}", e);
                    continue;
                },
            };
            let entity = match self.deserialize(&value_bytes) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("Skipping entry with malformed value: {}", e);
                    continue;
                },
            };
            results.push((key, entity));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    /// Scans keys with typed prefix and start key (returns typed keys).
    ///
    /// This avoids deserializing values, which is useful for TTL eviction and key-only scans.
    fn scan_keys_typed(
        &self,
        prefix: Option<&K>,
        start_key: Option<&K>,
        limit: usize,
    ) -> Result<Vec<K>> {
        const MAX_PREALLOC_CAPACITY: usize = 100_000;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let partition = self.partition();
        let prefix_bytes = prefix.map(|k| k.storage_key());
        let start_bytes = start_key.map(|k| k.storage_key());
        let iter = self.backend().scan(
            &partition,
            prefix_bytes.as_deref(),
            start_bytes.as_deref(),
            Some(limit),
        )?;

        let mut keys = Vec::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));
        for (key_bytes, _) in iter {
            let key = match K::from_storage_key(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("Skipping entry with malformed key: {}", e);
                    continue;
                },
            };
            keys.push(key);
            if keys.len() >= limit {
                break;
            }
        }
        Ok(keys)
    }

    /// Scans keys with raw byte prefix (returns typed keys, skips values).
    ///
    /// This method accepts a raw byte prefix (e.g., for scanning with partial composite keys)
    /// but only returns properly typed keys without deserializing values. This is more efficient
    /// than `scan_with_raw_prefix` when you don't need the values.
    ///
    /// Use this when you have a raw byte prefix that doesn't correspond to a complete key
    /// (e.g., JobNodeId::prefix_for_node returns only the node_id component) and you only
    /// need to collect the keys.
    ///
    /// ## Example
    /// ```rust,ignore
    /// // When prefix_for_node returns Vec<u8> (partial key)
    /// let prefix_bytes = JobNodeId::prefix_for_node(&node_id);
    /// let keys = store.scan_keys_with_raw_prefix(&prefix_bytes, None, limit)?;
    /// // Returns Vec<JobNodeId> without deserializing JobNode values
    /// ```
    fn scan_keys_with_raw_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<K>> {
        const MAX_PREALLOC_CAPACITY: usize = 100_000;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let partition = self.partition();
        let iter = self.backend().scan(&partition, Some(prefix), start_key, Some(limit))?;

        let mut keys = Vec::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));
        for (key_bytes, _) in iter {
            let key = match K::from_storage_key(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("Skipping entry with malformed key: {}", e);
                    continue;
                },
            };
            keys.push(key);
            if keys.len() >= limit {
                break;
            }
        }
        Ok(keys)
    }

    /// Scans keys relative to a starting key in a specified direction.
    ///
    /// This avoids deserializing values, which reduces CPU and allocation pressure.
    fn scan_keys_directional(
        &self,
        start_key: Option<&K>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<K>> {
        const MAX_PREALLOC_CAPACITY: usize = 100_000;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let partition = self.partition();

        match direction {
            ScanDirection::Newer => {
                let start_bytes = start_key.map(|k| next_storage_key_bytes(&k.storage_key()));
                let iter =
                    self.backend().scan(&partition, None, start_bytes.as_deref(), Some(limit))?;

                let mut keys = Vec::with_capacity(limit.min(MAX_PREALLOC_CAPACITY));
                for (key_bytes, _) in iter {
                    let key = K::from_storage_key(&key_bytes)
                        .map_err(StorageError::SerializationError)?;
                    keys.push(key);
                    if keys.len() >= limit {
                        break;
                    }
                }

                Ok(keys)
            },
            ScanDirection::Older => {
                let start_bytes = start_key.map(|k| k.storage_key());
                let iter = self.backend().scan(&partition, None, None, None)?;
                let mut collected: Vec<Vec<u8>> = Vec::new();

                for (key_bytes, _) in iter {
                    if let Some(start) = &start_bytes {
                        if key_bytes.as_slice() >= start.as_slice() {
                            break;
                        }
                    }
                    collected.push(key_bytes);
                }

                if collected.len() > limit {
                    let drain_end = collected.len() - limit;
                    collected.drain(0..drain_end);
                }

                let mut keys = Vec::with_capacity(collected.len());
                for key_bytes in collected {
                    let key = K::from_storage_key(&key_bytes)
                        .map_err(StorageError::SerializationError)?;
                    keys.push(key);
                }
                Ok(keys)
            },
        }
    }

    /// Count all entities in the partition.
    ///
    /// This is useful for monitoring and validation. Note that this does a full scan
    /// and can be expensive for large datasets.
    fn count_all(&self) -> Result<usize> {
        const MAX_COUNT_LIMIT: usize = 1_000_000;
        let partition = self.partition();
        let iter = self.backend().scan(&partition, None, None, Some(MAX_COUNT_LIMIT))?;
        let mut count = 0usize;
        for _ in iter {
            count += 1;
        }
        Ok(count)
    }

    /// Compact the partition to reclaim space and optimize reads.
    ///
    /// This triggers RocksDB compaction which removes deleted entries (tombstones)
    /// and reorganizes SSTables for better read performance. This is typically called
    /// after bulk deletes or flushes.
    fn compact(&self) -> Result<()> {
        let partition = self.partition();
        self.backend().compact_partition(&partition)
    }

    /// Create the partition if it doesn't exist.
    ///
    /// This creates the RocksDB column family for this entity store. It's safe to call
    /// multiple times - if the partition already exists, this is a no-op.
    fn ensure_partition_exists(&self) -> Result<()> {
        let partition = self.partition();
        self.backend().create_partition(&partition)
    }

    /// Delete multiple entities by keys.
    ///
    /// This is useful for bulk deletion operations. Unlike `batch_put`, this doesn't
    /// return the deleted values.
    fn delete_batch(&self, keys: &[K]) -> Result<()> {
        use crate::storage_trait::Operation;
        let partition = self.partition();

        let operations: Vec<Operation> = keys
            .iter()
            .map(|key| Operation::Delete {
                partition: partition.clone(),
                key: key.storage_key(),
            })
            .collect();

        self.backend().batch(operations)
    }
}

/// Extension trait providing async versions of EntityStore methods.
///
/// These methods internally use `tokio::task::spawn_blocking` to offload
/// synchronous RocksDB operations to a blocking thread pool, preventing
/// the async runtime from being blocked.
///
/// ## Usage
///
/// ```rust,ignore
/// use kalamdb_store::{EntityStore, EntityStoreAsync};
///
/// // In async context, use the async variants:
/// let user = store.get_async(&user_id).await?;
/// store.put_async(&user_id, &user).await?;
///
/// // In sync context (e.g., blocking thread), use the sync variants:
/// let user = store.get(&user_id)?;
/// store.put(&user_id, &user)?;
/// ```
#[async_trait::async_trait]
pub trait EntityStoreAsync<K, V>: EntityStore<K, V> + Send + Sync
where
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
    Self: Clone + 'static,
{
    /// Async version of `get()` - retrieves an entity by key.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    async fn get_async(&self, key: &K) -> Result<Option<V>> {
        let store = self.clone();
        let key = key.clone();
        run_blocking_result(move || store.get(&key)).await
    }

    /// Async version of `put()` - stores an entity by key.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    async fn put_async(&self, key: &K, entity: &V) -> Result<()> {
        let store = self.clone();
        let key = key.clone();
        let entity = entity.clone();
        run_blocking_result(move || store.put(&key, &entity)).await
    }

    /// Async version of `delete()` - removes an entity by key.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    async fn delete_async(&self, key: &K) -> Result<()> {
        let store = self.clone();
        let key = key.clone();
        run_blocking_result(move || store.delete(&key)).await
    }

    /// Async version of `scan_all()` - scans all entities in partition.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    async fn scan_all_async(
        &self,
        limit: Option<usize>,
        prefix: Option<K>,
        start_key: Option<K>,
    ) -> Result<Vec<(Vec<u8>, V)>> {
        let store = self.clone();
        run_blocking_result(move || {
            let typed_results = store.scan_all_typed(limit, prefix.as_ref(), start_key.as_ref())?;
            Ok(typed_results.into_iter().map(|(k, v)| (k.storage_key(), v)).collect())
        })
        .await
    }

    /// Async version of `scan_with_raw_prefix()` - scans entities by raw byte prefix.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    /// This is useful when you have a pre-computed raw prefix (e.g., for user-scoped scans).
    async fn scan_with_raw_prefix_async(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(K, V)>> {
        let store = self.clone();
        let prefix = prefix.to_vec();
        let start_key = start_key.map(|s| s.to_vec());
        run_blocking_result(move || {
            store.scan_with_raw_prefix(&prefix, start_key.as_deref(), limit)
        })
        .await
    }

    /// Async version of `scan_typed_with_prefix_and_start()` - scans entities with typed prefix and
    /// start key.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    async fn scan_typed_with_prefix_and_start_async(
        &self,
        prefix: Option<&K>,
        start_key: Option<&K>,
        limit: usize,
    ) -> Result<Vec<(K, V)>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        let start_key = start_key.cloned();
        run_blocking_result(move || {
            store.scan_typed_with_prefix_and_start(prefix.as_ref(), start_key.as_ref(), limit)
        })
        .await
    }
}

// Blanket implementation for any type that implements EntityStore + Clone + Send + Sync
impl<T, K, V> EntityStoreAsync<K, V> for T
where
    T: EntityStore<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
}

/// Trait for cross-user table stores with access control.
///
/// This trait extends `EntityStore<K, V>` to add access control functionality
/// for tables that are shared across users (system tables and shared tables).
///
/// ## Access Control Model
///
/// - **System tables** (e.g., system.users, system.jobs):
///   - `table_access()` returns `None`
///   - Only admin roles (dba, system) can read
///
/// - **Shared tables** with access levels:
///   - `table_access()` returns `Some(TableAccess::Public/Private/Restricted)`
///   - Access rules depend on table configuration
///
/// ## Example Usage
///
/// ```rust,ignore
/// use kalamdb_store::CrossUserTableStore;
/// use kalamdb_commons::models::{Role, TableAccess};
///
/// fn check_access<S: CrossUserTableStore<K, V>>(
///     store: &S,
///     user_role: &Role
/// ) -> bool {
///     store.can_read(user_role)
/// }
/// ```
pub trait CrossUserTableStore<K, V>: EntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    /// Returns the table access level, if any.
    ///
    /// - `None`: System table (admin-only, e.g., system.users)
    /// - `Some(TableAccess::Public)`: Publicly readable
    /// - `Some(TableAccess::Private)`: Owner-only
    /// - `Some(TableAccess::Restricted)`: Service+ roles only
    /// - `Some(TableAccess::Dba)`: DBA-only
    fn table_access(&self) -> Option<TableAccess>;

    /// Checks if a user with the given role can read this table.
    ///
    /// ## Access Rules
    ///
    /// - **System tables** (`table_access() = None`): Only dba/system roles
    /// - **Public tables**: All roles can read
    /// - **Private tables**: No cross-user access (owner only)
    /// - **Restricted tables**: Service/dba/system roles only
    /// - **DBA tables**: DBA role only
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use kalamdb_commons::models::Role;
    ///
    /// let can_access = store.can_read(&Role::User);  // false for system tables
    /// let admin_access = store.can_read(&Role::Dba); // true for system tables
    /// ```
    fn can_read(&self, user_role: &Role) -> bool {
        match self.table_access() {
            // System tables: service and admin roles
            // Service role is allowed read-only access to system tables for operational telemetry.
            None => matches!(user_role, Role::Service | Role::Dba | Role::System),

            // Shared tables: check access level
            Some(TableAccess::Public) => true,
            Some(TableAccess::Private) => false, // Owner-only, not cross-user
            Some(TableAccess::Restricted) => {
                matches!(user_role, Role::Service | Role::Dba | Role::System)
            },
            Some(TableAccess::Dba) => matches!(user_role, Role::System | Role::Dba),
        }
    }
}

// Import Role and TableAccess for the trait
use kalamdb_commons::models::{Role, TableAccess};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kalamdb_commons::models::{Role, TableAccess, UserId};
    use serde_json::Value as JsonValue;

    use super::*;

    // Mock implementation for testing
    struct MockStore {
        backend: Arc<dyn StorageBackend>,
        access: Option<TableAccess>,
    }

    impl EntityStore<UserId, String> for MockStore {
        fn backend(&self) -> &Arc<dyn StorageBackend> {
            &self.backend
        }

        fn partition(&self) -> Partition {
            Partition::new("test_partition")
        }
    }

    impl CrossUserTableStore<UserId, String> for MockStore {
        fn table_access(&self) -> Option<TableAccess> {
            self.access
        }
    }

    struct BinaryStore {
        backend: Arc<dyn StorageBackend>,
        partition: String,
    }

    impl EntityStore<UserId, String> for BinaryStore {
        fn backend(&self) -> &Arc<dyn StorageBackend> {
            &self.backend
        }

        fn partition(&self) -> Partition {
            Partition::new(&self.partition)
        }
    }

    #[test]
    fn test_system_table_access() {
        // System table (table_access = None)
        let store = MockStore {
            backend: Arc::new(crate::test_utils::InMemoryBackend::new()),
            access: None,
        };

        assert!(!store.can_read(&Role::User)); // Regular users cannot read system tables
        assert!(store.can_read(&Role::Service)); // Service accounts CAN read for operational telemetry
        assert!(store.can_read(&Role::Dba));
        assert!(store.can_read(&Role::System));
    }

    #[test]
    fn test_public_table_access() {
        let store = MockStore {
            backend: Arc::new(crate::test_utils::InMemoryBackend::new()),
            access: Some(TableAccess::Public),
        };

        assert!(store.can_read(&Role::User));
        assert!(store.can_read(&Role::Service));
        assert!(store.can_read(&Role::Dba));
        assert!(store.can_read(&Role::System));
    }

    #[test]
    fn test_private_table_access() {
        let store = MockStore {
            backend: Arc::new(crate::test_utils::InMemoryBackend::new()),
            access: Some(TableAccess::Private),
        };

        assert!(!store.can_read(&Role::User));
        assert!(!store.can_read(&Role::Service));
        assert!(!store.can_read(&Role::Dba));
        assert!(!store.can_read(&Role::System));
    }

    #[test]
    fn test_restricted_table_access() {
        let store = MockStore {
            backend: Arc::new(crate::test_utils::InMemoryBackend::new()),
            access: Some(TableAccess::Restricted),
        };

        assert!(!store.can_read(&Role::User));
        assert!(store.can_read(&Role::Service));
        assert!(store.can_read(&Role::Dba));
        assert!(store.can_read(&Role::System));
    }

    #[test]
    fn default_kserializable_is_bincode() {
        let backend: Arc<dyn StorageBackend> = Arc::new(crate::test_utils::InMemoryBackend::new());
        let store = BinaryStore {
            backend,
            partition: "any_partition".to_string(),
        };

        let partition = store.partition();
        store.backend().create_partition(&partition).unwrap();

        let key = UserId::new("user1");
        let value = "hello".to_string();
        store.put(&key, &value).unwrap();

        let raw = store
            .backend()
            .get(&partition, &key.storage_key())
            .unwrap()
            .expect("value stored");

        assert!(serde_json::from_slice::<JsonValue>(&raw).is_err());
        let roundtrip = String::decode(&raw).unwrap();
        assert_eq!(roundtrip, value);
    }
}
