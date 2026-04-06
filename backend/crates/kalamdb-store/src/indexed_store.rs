//! Indexed Entity Store - Automatic secondary index management.
//!
//! This module provides `IndexedEntityStore<K, V>` which extends `EntityStore`
//! with automatic secondary index maintenance using RocksDB's atomic WriteBatch.
//!
//! ## Features
//!
//! - **Automatic Index Management**: Indexes are updated automatically on insert/update/delete
//! - **Atomic Operations**: Entity + all indexes updated in single WriteBatch
//! - **Async Support**: spawn_blocking wrappers for async contexts
//! - **DataFusion Integration**: Filter pushdown support via `IndexDefinition` trait
//!
//! ## Architecture
//!
//! ```text
//! IndexedEntityStore<K, V>
//!     │
//!     ├── insert(key, entity)
//!     │       │
//!     │       ▼
//!     │   backend.batch([
//!     │       Put { entity },
//!     │       Put { index1 },
//!     │       Put { index2 },
//!     │   ])
//!     │
//!     ├── update(key, entity)
//!     │       │
//!     │       ▼
//!     │   1. Fetch old entity
//!     │   2. backend.batch([
//!     │       Delete { old_index1 },  // if changed
//!     │       Delete { old_index2 },  // if changed
//!     │       Put { entity },
//!     │       Put { new_index1 },
//!     │       Put { new_index2 },
//!     │   ])
//!     │
//!     └── delete(key)
//!             │
//!             ▼
//!         1. Fetch entity
//!         2. backend.batch([
//!             Delete { entity },
//!             Delete { index1 },
//!             Delete { index2 },
//!         ])
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use kalamdb_store::{IndexedEntityStore, IndexDefinition};
//! use kalamdb_commons::JobId;
//! use kalamdb_system::{Job, JobStatus};
//!
//! // Define an index
//! struct JobStatusIndex;
//!
//! impl IndexDefinition<JobId, Job> for JobStatusIndex {
//!     fn partition(&self) -> Partition {
//!         Partition::new("system_jobs_status_idx")
//!     }
//!
//!     fn indexed_columns(&self) -> Vec<&str> {
//!         vec!["status"]
//!     }
//!
//!     fn extract_key(&self, _pk: &JobId, job: &Job) -> Option<Vec<u8>> {
//!         let mut key = Vec::new();
//!         key.push(job.status as u8);
//!         key.extend_from_slice(&job.created_at.to_be_bytes());
//!         key.extend_from_slice(job.job_id.as_bytes());
//!         Some(key)
//!     }
//! }
//!
//! // Create store with indexes
//! let store = IndexedEntityStore::new(
//!     backend,
//!     "system_jobs",
//!     vec![Arc::new(JobStatusIndex)],
//! );
//!
//! // Insert - automatically updates indexes
//! store.insert(&job.job_id, &job)?;
//!
//! // Update - automatically removes old index entries, adds new ones
//! job.status = JobStatus::Completed;
//! store.update(&job.job_id, &job)?;
//!
//! // Delete - automatically removes all index entries
//! store.delete(&job.job_id)?;
//!
//! // Query by index
//! let running_jobs = store.scan_by_index(0, Some(&[JobStatus::Running as u8]), Some(10))?;
//! ```

use crate::async_utils::run_blocking_result;
use crate::entity_store::{EntityIterator, EntityStore};
use crate::storage_trait::{Operation, Partition, Result, StorageBackend, StorageError};
use kalamdb_commons::{KSerializable, StorageKey};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(feature = "datafusion")]
use datafusion::logical_expr::{Expr, Operator};
#[cfg(feature = "datafusion")]
use datafusion::scalar::ScalarValue;

// ============================================================================
// IndexDefinition Trait
// ============================================================================

/// Defines how to extract index keys from an entity.
///
/// Each index is defined by:
/// - A partition name where index entries are stored
/// - Column names covered by the index (for DataFusion filter pushdown)
/// - A function to extract the index key from the entity
/// - Optional: Custom index value (default is primary key for reverse lookup)
/// - Optional: Filter-to-prefix conversion for DataFusion
///
/// ## Index Key Design Guidelines
///
/// For range queries, design composite keys with most selective field first:
/// ```text
/// [status][created_at_be][job_id]
///    1B       8B          var
/// ```
///
/// - Use big-endian for numeric fields (ensures lexicographic = numeric order)
/// - Append primary key to ensure uniqueness
/// - Return `None` from `extract_key()` to skip indexing (e.g., conditional indexes)
pub trait IndexDefinition<K, V>: Send + Sync
where
    K: StorageKey,
    V: KSerializable,
{
    /// Returns the partition for this index.
    ///
    /// This should be unique across all indexes in the system.
    /// Convention: `{main_partition}_idx_{columns}` e.g., `system_jobs_idx_status`
    fn partition(&self) -> Partition;

    /// Returns the column names this index covers.
    ///
    /// Used by DataFusion to determine if a filter can use this index.
    /// Should match the order of fields in the index key.
    fn indexed_columns(&self) -> Vec<&str>;

    /// Extracts the index key from the entity.
    ///
    /// Returns `None` if this entity should not be indexed (e.g., conditional index).
    ///
    /// ## Key Design
    ///
    /// The returned bytes should be designed for efficient prefix scanning:
    /// - Put the most frequently filtered field first
    /// - Use big-endian encoding for numbers (preserves sort order)
    /// - Append primary key to ensure uniqueness
    fn extract_key(&self, primary_key: &K, entity: &V) -> Option<Vec<u8>>;

    /// Returns the value to store in the index.
    ///
    /// Default: Store the primary key bytes for reverse lookup.
    /// Override for covering indexes that include additional data.
    fn index_value(&self, primary_key: &K, _entity: &V) -> Vec<u8> {
        primary_key.storage_key()
    }

    /// Converts a DataFusion filter expression to an index scan prefix.
    ///
    /// Returns `Some(prefix)` if this index can satisfy the filter.
    /// Returns `None` if the filter cannot use this index.
    ///
    /// ## Example
    ///
    /// For a status index, convert `status = 'Running'` to prefix `[2]` (Running = 2).
    #[cfg(feature = "datafusion")]
    fn filter_to_prefix(&self, _filter: &Expr) -> Option<Vec<u8>> {
        None
    }

    /// Checks if this index can satisfy the given filter.
    ///
    /// Used by DataFusion's `supports_filters_pushdown()`.
    #[cfg(feature = "datafusion")]
    fn supports_filter(&self, filter: &Expr) -> bool {
        self.filter_to_prefix(filter).is_some()
    }
}

pub type IndexRawIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + 'a>;
pub type IndexKeyIterator<'a, K> = Box<dyn Iterator<Item = Result<K>> + Send + 'a>;
pub type IndexRawTypedIterator<'a, K> = Box<dyn Iterator<Item = Result<(Vec<u8>, K)>> + Send + 'a>;

// ============================================================================
// IndexedEntityStore
// ============================================================================

/// An EntityStore that automatically manages secondary indexes.
///
/// All write operations (insert/update/delete) atomically update the entity
/// and all defined indexes using RocksDB's WriteBatch.
///
/// ## Type Parameters
///
/// - `K`: Primary key type (implements `StorageKey`)
/// - `V`: Entity value type (implements `KSerializable`)
///
/// ## Thread Safety
///
/// This struct is `Send + Sync` and can be safely shared across threads.
/// The underlying `StorageBackend` handles concurrent access.
pub struct IndexedEntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    backend: Arc<dyn StorageBackend>,
    partition: String,
    main_partition: Partition,
    indexes: Vec<Arc<dyn IndexDefinition<K, V>>>,
    index_partitions: Vec<Partition>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> IndexedEntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    /// Creates a new IndexedEntityStore.
    ///
    /// # Arguments
    ///
    /// * `backend` - Storage backend (RocksDB or mock)
    /// * `partition` - Partition name for the main entity table
    /// * `indexes` - List of index definitions
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let store = IndexedEntityStore::new(
    ///     backend,
    ///     "system_jobs",
    ///     vec![Arc::new(JobStatusIndex)],
    /// );
    /// ```
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        partition: impl Into<String>,
        indexes: Vec<Arc<dyn IndexDefinition<K, V>>>,
    ) -> Self {
        let partition_str = partition.into();

        // Ensure main partition exists
        let main_partition = Partition::new(&partition_str);
        let _ = backend.create_partition(&main_partition); // Ignore error if already exists

        // Ensure all index partitions exist
        let mut index_partitions = Vec::with_capacity(indexes.len());
        for index in &indexes {
            let index_partition = index.partition();
            let _ = backend.create_partition(&index_partition); // Ignore error if already exists
            index_partitions.push(index_partition);
        }

        Self {
            backend,
            partition: partition_str,
            main_partition,
            indexes,
            index_partitions,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the index definitions.
    ///
    /// Useful for DataFusion integration to check which filters can be pushed down.
    pub fn indexes(&self) -> &[Arc<dyn IndexDefinition<K, V>>] {
        &self.indexes
    }

    /// Returns an index definition by index.
    pub fn get_index(&self, idx: usize) -> Option<&Arc<dyn IndexDefinition<K, V>>> {
        self.indexes.get(idx)
    }

    /// Drops all partitions owned by this store: index partitions first (best-effort),
    /// then the main partition (hard error on failure, "not found" is silently OK).
    ///
    /// Use this in DROP TABLE cleanup so every CF that belongs to the store is removed
    /// in one call, derived directly from the store's own partition list rather than
    /// re-constructing names by hand.
    pub fn drop_all_partitions(&self) -> kalamdb_commons::storage::Result<()> {
        // Index partitions: best-effort (missing == already clean)
        for partition in &self.index_partitions {
            if let Err(e) = self.backend.drop_partition(partition) {
                if !e.to_string().to_lowercase().contains("not found") {
                    log::warn!(
                        "[IndexedEntityStore] Failed to drop index partition '{}': {}",
                        partition.name(),
                        e
                    );
                }
            }
        }
        // Main partition: propagate errors except "not found" (already clean)
        match self.backend.drop_partition(&self.main_partition) {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().to_lowercase().contains("not found") => {
                log::warn!(
                    "[IndexedEntityStore] Partition '{}' not found during drop (already clean)",
                    self.main_partition.name()
                );
                Ok(())
            },
            Err(e) => Err(e),
        }
    }
    /// Finds the best index for a given filter.
    ///
    /// Returns `Some((index_idx, prefix))` if an index can satisfy the filter.
    #[cfg(feature = "datafusion")]
    pub fn find_index_for_filter(&self, filter: &Expr) -> Option<(usize, Vec<u8>)> {
        for (idx, index) in self.indexes.iter().enumerate() {
            if let Some(prefix) = index.filter_to_prefix(filter) {
                return Some((idx, prefix));
            }
        }
        None
    }

    /// Find an index by its partition name.
    ///
    /// Useful to avoid hard-coding numeric index positions (0, 1, ...) in providers.
    pub fn find_index_by_partition(&self, partition: &str) -> Option<usize> {
        self.indexes
            .iter()
            .enumerate()
            .find(|(_idx, index)| index.partition().name() == partition)
            .map(|(idx, _)| idx)
    }

    /// Find the first index that covers a column (based on `indexed_columns()`).
    ///
    /// Note: `indexed_columns()` returns an owned Vec, so this is intended for
    /// provider wiring / occasional lookups (not a tight loop).
    pub fn find_index_covering_column(&self, column: &str) -> Option<usize> {
        self.indexes
            .iter()
            .enumerate()
            .find(|(_idx, index)| index.indexed_columns().contains(&column))
            .map(|(idx, _)| idx)
    }

    /// Finds the "best" index for a set of DataFusion filters.
    ///
    /// Strategy: pick the index that yields the longest prefix (more selective).
    #[cfg(feature = "datafusion")]
    pub fn find_best_index_for_filters(&self, filters: &[Expr]) -> Option<(usize, Vec<u8>)> {
        let mut best: Option<(usize, Vec<u8>)> = None;

        for filter in filters {
            if let Some((idx, prefix)) = self.find_index_for_filter(filter) {
                match &best {
                    Some((_best_idx, best_prefix)) if best_prefix.len() >= prefix.len() => {},
                    _ => best = Some((idx, prefix)),
                }
            }
        }

        best
    }

    // ========================================================================
    // Sync Write Operations (Atomic with Indexes)
    // ========================================================================

    /// Inserts a new entity with all indexes atomically.
    ///
    /// Uses RocksDB WriteBatch - all operations succeed or none are applied.
    ///
    /// # Arguments
    ///
    /// * `key` - Primary key for the entity
    /// * `entity` - Entity to insert
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Serialization fails
    /// - Storage backend fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// store.insert(&job.job_id, &job)?;
    /// // Entity and all indexes are now written atomically
    /// ```
    pub fn insert(&self, key: &K, entity: &V) -> Result<()> {
        let mut operations = Vec::with_capacity(1 + self.indexes.len());

        // 1. Main entity write
        let partition = self.main_partition.clone();
        let value = entity.encode()?;
        operations.push(Operation::Put {
            partition,
            key: key.storage_key(),
            value,
        });

        // 2. Index writes
        for (index, index_partition) in self.indexes.iter().zip(self.index_partitions.iter()) {
            if let Some(index_key) = index.extract_key(key, entity) {
                let index_value = index.index_value(key, entity);
                operations.push(Operation::Put {
                    partition: index_partition.clone(),
                    key: index_key,
                    value: index_value,
                });
            }
        }

        // Atomic batch write
        self.backend.batch(operations)
    }

    /// Inserts multiple entities with all indexes atomically in a single WriteBatch.
    ///
    /// This is significantly more efficient than calling `insert()` N times,
    /// as it performs a single disk write instead of N writes.
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of (key, entity) pairs to insert
    ///
    /// # Returns
    ///
    /// `Ok(())` if all entities were inserted successfully.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Serialization fails for any entity
    /// - Storage backend fails
    /// - The entire batch is rolled back on any failure (atomic)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let entries = vec![
    ///     (row_id1, entity1),
    ///     (row_id2, entity2),
    ///     (row_id3, entity3),
    /// ];
    /// store.insert_batch(&entries)?;  // Single atomic write for all
    /// ```
    pub fn insert_batch(&self, entries: &[(K, V)]) -> Result<()> {
        let _span = tracing::debug_span!("store.insert_batch", count = entries.len()).entered();
        if entries.is_empty() {
            return Ok(());
        }

        // Pre-allocate: each entry = 1 entity write + N index writes
        let ops_per_entry = 1 + self.indexes.len();
        let mut operations = Vec::with_capacity(entries.len() * ops_per_entry);

        let partition = self.main_partition.clone();

        for (key, entity) in entries {
            // 1. Main entity write
            let value = entity.encode()?;
            operations.push(Operation::Put {
                partition: partition.clone(),
                key: key.storage_key(),
                value,
            });

            // 2. Index writes for this entity
            for (index, index_partition) in self.indexes.iter().zip(self.index_partitions.iter()) {
                if let Some(index_key) = index.extract_key(key, entity) {
                    let index_value = index.index_value(key, entity);
                    operations.push(Operation::Put {
                        partition: index_partition.clone(),
                        key: index_key,
                        value: index_value,
                    });
                }
            }
        }

        // Single atomic batch write for ALL entities
        self.backend.batch(operations)
    }

    /// Batch insert with pre-encoded entity bytes.
    ///
    /// Unlike [`insert_batch`] which calls `entity.encode()` per row (creating
    /// a new FlatBufferBuilder each time), this method accepts pre-encoded
    /// bytes produced by batch encoders like `batch_encode_user_table_rows()`
    /// that reuse FlatBufferBuilders across rows.
    ///
    /// The caller is responsible for ensuring that `encoded_values[i]`
    /// corresponds to `entries[i]`. The entries are still needed for index
    /// key extraction.
    ///
    /// ## Performance
    ///
    /// - Saves N–1 FlatBufferBuilder allocations for N rows
    /// - Eliminates per-row `.to_vec()` overhead from inner builder
    pub fn insert_batch_preencoded(
        &self,
        entries: &[(K, V)],
        encoded_values: Vec<Vec<u8>>,
    ) -> Result<()> {
        let _span =
            tracing::debug_span!("store.insert_batch_preencoded", count = entries.len()).entered();
        if entries.is_empty() {
            return Ok(());
        }
        debug_assert_eq!(entries.len(), encoded_values.len());

        let ops_per_entry = 1 + self.indexes.len();
        let mut operations = Vec::with_capacity(entries.len() * ops_per_entry);
        let partition = self.main_partition.clone();

        for ((key, entity), value) in entries.iter().zip(encoded_values.into_iter()) {
            // 1. Main entity write (pre-encoded)
            operations.push(Operation::Put {
                partition: partition.clone(),
                key: key.storage_key(),
                value,
            });

            // 2. Index writes for this entity
            for (index, index_partition) in self.indexes.iter().zip(self.index_partitions.iter()) {
                if let Some(index_key) = index.extract_key(key, entity) {
                    let index_value = index.index_value(key, entity);
                    operations.push(Operation::Put {
                        partition: index_partition.clone(),
                        key: index_key,
                        value: index_value,
                    });
                }
            }
        }

        self.backend.batch(operations)
    }

    /// Updates an entity and its indexes atomically.
    ///
    /// 1. Fetches old entity to determine stale index entries
    /// 2. Deletes old index entries (if keys changed)
    /// 3. Writes new entity
    /// 4. Writes new index entries
    ///
    /// All in a single atomic WriteBatch.
    ///
    /// # Arguments
    ///
    /// * `key` - Primary key for the entity
    /// * `new_entity` - Updated entity
    ///
    /// # Note
    ///
    /// This method fetches the old entity first to determine which index
    /// entries need to be deleted. If you already have the old entity,
    /// use `update_with_old()` for better performance.
    pub fn update(&self, key: &K, new_entity: &V) -> Result<()> {
        // Fetch old entity to determine which index entries to remove
        let old_entity = self.get(key)?;
        self.update_internal(key, old_entity.as_ref(), new_entity)
    }

    /// Updates an entity when you already have the old entity.
    ///
    /// More efficient than `update()` when you've already fetched the entity.
    pub fn update_with_old(&self, key: &K, old_entity: Option<&V>, new_entity: &V) -> Result<()> {
        self.update_internal(key, old_entity, new_entity)
    }

    fn update_internal(&self, key: &K, old_entity: Option<&V>, new_entity: &V) -> Result<()> {
        let mut operations = Vec::with_capacity(1 + self.indexes.len() * 2);

        // 1. Delete stale index entries (if entity existed and index key changed)
        // 2. Write new entity
        let partition = self.main_partition.clone();
        let value = new_entity.encode()?;
        operations.push(Operation::Put {
            partition,
            key: key.storage_key(),
            value,
        });

        // 3. Write new index entries (only if changed or new entity)
        for (index, index_partition) in self.indexes.iter().zip(self.index_partitions.iter()) {
            let old_index_key = old_entity.and_then(|old| index.extract_key(key, old));
            let new_index_key = index.extract_key(key, new_entity);

            if old_index_key != new_index_key {
                if let Some(old_key) = old_index_key {
                    operations.push(Operation::Delete {
                        partition: index_partition.clone(),
                        key: old_key,
                    });
                }

                if let Some(idx_key) = new_index_key {
                    let index_value = index.index_value(key, new_entity);
                    operations.push(Operation::Put {
                        partition: index_partition.clone(),
                        key: idx_key,
                        value: index_value,
                    });
                }
            }
        }

        // Atomic batch write
        self.backend.batch(operations)
    }

    /// Deletes an entity and all its index entries atomically.
    ///
    /// # Note
    ///
    /// This method fetches the entity first to determine which index
    /// entries need to be deleted. If you already have the entity,
    /// use `delete_with_entity()` for better performance.
    pub fn delete(&self, key: &K) -> Result<()> {
        // Fetch entity to determine which index entries to remove
        let entity = match self.get(key)? {
            Some(e) => e,
            None => return Ok(()), // Already deleted
        };

        self.delete_with_entity(key, &entity)
    }

    /// Deletes an entity when you already have it.
    ///
    /// More efficient than `delete()` when you've already fetched the entity.
    pub fn delete_with_entity(&self, key: &K, entity: &V) -> Result<()> {
        let mut operations = Vec::with_capacity(1 + self.indexes.len());

        // 1. Delete main entity
        let partition = self.main_partition.clone();
        operations.push(Operation::Delete {
            partition,
            key: key.storage_key(),
        });

        // 2. Delete all index entries
        for (index, index_partition) in self.indexes.iter().zip(self.index_partitions.iter()) {
            if let Some(index_key) = index.extract_key(key, entity) {
                operations.push(Operation::Delete {
                    partition: index_partition.clone(),
                    key: index_key,
                });
            }
        }

        // Atomic batch write
        self.backend.batch(operations)
    }

    // ========================================================================
    // Sync Read/Scan Operations
    // ========================================================================

    /// Scans an index by prefix and returns matching entities.
    ///
    /// # Arguments
    ///
    /// * `index_idx` - Index number (0-based)
    /// * `prefix` - Optional prefix to filter by
    /// * `limit` - Optional limit on number of results
    ///
    /// # Returns
    ///
    /// Vector of (primary_key, entity) tuples.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get all Running jobs (assuming status=Running is encoded as byte 2)
    /// let running_jobs = store.scan_by_index(0, Some(&[2]), Some(100))?;
    /// ```
    pub fn scan_by_index(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(K, V)>> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        let iter = self.backend.scan(&index_partition, prefix, None, limit)?;

        let mut results = Vec::with_capacity(limit.unwrap_or(0));
        for (_index_key, primary_key_bytes) in iter {
            // Deserialize primary key from index value
            let primary_key = K::from_storage_key(&primary_key_bytes)
                .map_err(StorageError::SerializationError)?;

            // Fetch actual entity
            if let Some(entity) = self.get(&primary_key)? {
                results.push((primary_key, entity));
            }
        }

        Ok(results)
    }

    /// Scans an index by prefix and returns matching entities as an iterator.
    ///
    /// This avoids loading all results into memory at once.
    pub fn scan_by_index_iter(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<EntityIterator<'_, K, V>> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        let mut iter = self.backend.scan(&index_partition, prefix, None, limit)?;
        let mut remaining = limit.unwrap_or(usize::MAX);

        let mapped = std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }

            loop {
                let next = iter.next()?;
                let (_index_key, primary_key_bytes) = next;
                let primary_key = match K::from_storage_key(&primary_key_bytes) {
                    Ok(key) => key,
                    Err(e) => return Some(Err(StorageError::SerializationError(e))),
                };

                match self.get(&primary_key) {
                    Ok(Some(entity)) => {
                        remaining -= 1;
                        return Some(Ok((primary_key, entity)));
                    },
                    Ok(None) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
        });

        Ok(Box::new(mapped))
    }

    /// Returns the newest entity matching an index prefix.
    ///
    /// This uses reverse index iteration so hot-path PK lookups can fetch the
    /// latest MVCC version without walking every historical version.
    pub fn get_latest_by_index_prefix(
        &self,
        index_idx: usize,
        prefix: &[u8],
    ) -> Result<Option<(K, V)>> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        let mut iter =
            match self.backend.scan_reverse(&index_partition, Some(prefix), None, Some(1)) {
                Ok(iter) => iter,
                Err(StorageError::PartitionNotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };

        while let Some((_index_key, primary_key_bytes)) = iter.next() {
            let primary_key = K::from_storage_key(&primary_key_bytes)
                .map_err(StorageError::SerializationError)?;

            if let Some(entity) = self.get(&primary_key)? {
                return Ok(Some((primary_key, entity)));
            }
        }

        Ok(None)
    }

    /// Scans an index and returns only the primary keys (no entity fetch).
    ///
    /// More efficient when you only need the keys.
    pub fn scan_index_keys(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<K>> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        let iter = self.backend.scan(&index_partition, prefix, None, limit)?;

        let mut results = Vec::with_capacity(limit.unwrap_or(0));
        for (_index_key, primary_key_bytes) in iter {
            let primary_key = K::from_storage_key(&primary_key_bytes)
                .map_err(StorageError::SerializationError)?;
            results.push(primary_key);
        }

        Ok(results)
    }

    /// Checks if any entry exists in an index with the given prefix.
    ///
    /// This is the most efficient check - only scans index, no entity fetch,
    /// stops at first match. Use this for PK uniqueness validation.
    ///
    /// # Arguments
    ///
    /// * `index_idx` - Index number (0-based, typically 0 for PK index)
    /// * `prefix` - Prefix to search for
    ///
    /// # Returns
    ///
    /// `true` if at least one entry exists with this prefix
    pub fn exists_by_index(&self, index_idx: usize, prefix: &[u8]) -> Result<bool> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        // Only fetch 1 result - we just need to know if anything exists
        let mut iter = self.backend.scan(&index_partition, Some(prefix), None, Some(1))?;

        // If we got any result, the prefix exists
        Ok(iter.next().is_some())
    }

    /// Batch check for existence of multiple prefixes in an index.
    ///
    /// **Performance**: More efficient than N individual `exists_by_index` calls
    /// when checking multiple PKs for uniqueness validation. Uses a single scan
    /// with the common prefix and builds a HashSet of existing values.
    ///
    /// # Arguments
    ///
    /// * `index_idx` - Index number (0-based, typically 0 for PK index)
    /// * `common_prefix` - Common prefix for all values (e.g., user_id prefix)
    /// * `prefixes` - List of full prefixes to check for existence
    ///
    /// # Returns
    ///
    /// HashSet of prefixes that exist in the index
    pub fn exists_batch_by_index(
        &self,
        index_idx: usize,
        common_prefix: &[u8],
        prefixes: &[Vec<u8>],
    ) -> Result<std::collections::HashSet<Vec<u8>>> {
        if prefixes.is_empty() {
            return Ok(HashSet::new());
        }

        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();

        // With no common prefix, a shared scan would walk the entire index.
        // Use point prefix checks instead.
        if common_prefix.is_empty() {
            let mut found: HashSet<Vec<u8>> = HashSet::new();
            let unique_prefixes: HashSet<Vec<u8>> = prefixes.iter().cloned().collect();
            for prefix in unique_prefixes {
                let mut iter = self.backend.scan(&index_partition, Some(&prefix), None, Some(1))?;
                if iter.next().is_some() {
                    found.insert(prefix);
                }
            }
            return Ok(found);
        }

        // For scoped prefixes (e.g., all PKs for one user), a single contiguous scan
        // is typically faster than N individual seeks.
        let iter = self.backend.scan(&index_partition, Some(common_prefix), None, None)?;

        let mut prefixes_by_len: HashMap<usize, HashSet<&[u8]>> = HashMap::new();
        for prefix in prefixes {
            prefixes_by_len.entry(prefix.len()).or_default().insert(prefix.as_slice());
        }
        let mut lengths: Vec<usize> = prefixes_by_len.keys().copied().collect();
        lengths.sort_unstable();

        let mut remaining: usize = prefixes_by_len.values().map(|set| set.len()).sum();
        if remaining == 0 {
            return Ok(HashSet::new());
        }

        let mut found: HashSet<Vec<u8>> = HashSet::with_capacity(remaining);
        'outer: for (key, _value) in iter {
            for len in &lengths {
                if key.len() < *len {
                    break;
                }

                let prefix_slice = &key[..*len];
                if let Some(prefix_set) = prefixes_by_len.get(len) {
                    if prefix_set.contains(prefix_slice) {
                        let prefix_owned = prefix_slice.to_vec();
                        if found.insert(prefix_owned) {
                            remaining -= 1;
                            if remaining == 0 {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }

        Ok(found)
    }

    /// Scans an index returning raw (index_key, primary_key) pairs.
    ///
    /// Useful when you need access to the index key itself.
    pub fn scan_index_raw(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iter = self.scan_index_raw_iter(index_idx, prefix, start_key, limit)?;
        let mut results = Vec::new();
        for item in iter {
            results.push(item?);
        }

        Ok(results)
    }

    /// Scans an index returning raw (index_key, primary_key) pairs as an iterator.
    ///
    /// Useful for large scans without loading all results into memory.
    pub fn scan_index_raw_iter(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<IndexRawIterator<'_>> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        let iter = self.backend.scan(&index_partition, prefix, start_key, limit)?;

        Ok(Box::new(iter.map(Ok)))
    }

    /// Scans an index returning (index_key, primary_key) pairs with typed primary key.
    pub fn scan_index_raw_typed_iter(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<IndexRawTypedIterator<'_, K>> {
        let iter = self.scan_index_raw_iter(index_idx, prefix, start_key, limit)?;

        let mapped = iter.map(|res| {
            let (index_key, primary_key_bytes) = res?;
            let primary_key = K::from_storage_key(&primary_key_bytes)
                .map_err(StorageError::SerializationError)?;
            Ok((index_key, primary_key))
        });

        Ok(Box::new(mapped))
    }

    /// Scans an index and returns only the primary keys as an iterator.
    ///
    /// More efficient for large scans since it avoids collecting into memory.
    pub fn scan_index_keys_iter(
        &self,
        index_idx: usize,
        prefix: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<IndexKeyIterator<'_, K>> {
        let index_partition = self
            .index_partitions
            .get(index_idx)
            .ok_or_else(|| StorageError::Other(format!("Index {} not found", index_idx)))?
            .clone();
        let iter = self.backend.scan(&index_partition, prefix, None, limit)?;

        let mapped = iter.map(|(_index_key, primary_key_bytes)| {
            K::from_storage_key(&primary_key_bytes).map_err(StorageError::SerializationError)
        });

        Ok(Box::new(mapped))
    }
}

// ============================================================================
// EntityStore Implementation
// ============================================================================

impl<K, V> EntityStore<K, V> for IndexedEntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    fn partition(&self) -> Partition {
        self.main_partition.clone()
    }
}

// ============================================================================
// Clone Implementation
// ============================================================================

impl<K, V> Clone for IndexedEntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            partition: self.partition.clone(),
            main_partition: self.main_partition.clone(),
            indexes: self.indexes.clone(),
            index_partitions: self.index_partitions.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

// ============================================================================
// Async Support
// ============================================================================

impl<K, V> IndexedEntityStore<K, V>
where
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    /// Async version of `insert()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn insert_async(&self, key: K, entity: V) -> Result<()> {
        let store = self.clone();
        run_blocking_result(move || store.insert(&key, &entity)).await
    }

    /// Async version of `insert_batch()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn insert_batch_async(&self, entries: Vec<(K, V)>) -> Result<()> {
        let store = self.clone();
        run_blocking_result(move || store.insert_batch(&entries)).await
    }

    /// Async version of `update()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn update_async(&self, key: K, entity: V) -> Result<()> {
        let store = self.clone();
        run_blocking_result(move || store.update(&key, &entity)).await
    }

    /// Async version of `delete()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn delete_async(&self, key: K) -> Result<()> {
        let store = self.clone();
        run_blocking_result(move || store.delete(&key)).await
    }

    /// Async version of `scan_by_index()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn scan_by_index_async(
        &self,
        index_idx: usize,
        prefix: Option<Vec<u8>>,
        limit: Option<usize>,
    ) -> Result<Vec<(K, V)>> {
        let store = self.clone();
        run_blocking_result(move || store.scan_by_index(index_idx, prefix.as_deref(), limit)).await
    }

    /// Async version of `get_latest_by_index_prefix()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn get_latest_by_index_prefix_async(
        &self,
        index_idx: usize,
        prefix: Vec<u8>,
    ) -> Result<Option<(K, V)>> {
        let store = self.clone();
        run_blocking_result(move || store.get_latest_by_index_prefix(index_idx, &prefix)).await
    }

    /// Async version of `insert_batch_preencoded()`.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn insert_batch_preencoded_async(
        &self,
        entries: Vec<(K, V)>,
        encoded_values: Vec<Vec<u8>>,
    ) -> Result<()> {
        let store = self.clone();
        run_blocking_result(move || store.insert_batch_preencoded(&entries, encoded_values)).await
    }

    /// Async version of `get()` from EntityStore.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn get_async(&self, key: K) -> Result<Option<V>> {
        let store = self.clone();
        run_blocking_result(move || store.get(&key)).await
    }

    /// Async version of `scan_all()` from EntityStore.
    ///
    /// Uses `spawn_blocking` to avoid blocking the async runtime.
    pub async fn scan_all_async(
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
}

// ============================================================================
// Helper: Extract equality filters for DataFusion integration
// ============================================================================

/// Helper function to extract column equality from a DataFusion Expr.
///
/// Returns `Some((column_name, string_value))` for expressions like `col = 'value'`.
#[cfg(feature = "datafusion")]
pub fn extract_string_equality(filter: &Expr) -> Option<(&str, &str)> {
    match filter {
        Expr::BinaryExpr(binary) => {
            if binary.op != Operator::Eq {
                return None;
            }

            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(val)), _)) => {
                    return Some((col.name.as_str(), val.as_str()));
                },
                (Expr::Literal(ScalarValue::Utf8(Some(val)), _), Expr::Column(col)) => {
                    return Some((col.name.as_str(), val.as_str()));
                },
                _ => {},
            }
            None
        },
        _ => None,
    }
}

/// Helper function to extract column equality with i64 from a DataFusion Expr.
#[cfg(feature = "datafusion")]
pub fn extract_i64_equality(filter: &Expr) -> Option<(&str, i64)> {
    match filter {
        Expr::BinaryExpr(binary) => {
            if binary.op != Operator::Eq {
                return None;
            }

            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(ScalarValue::Int64(Some(val)), _)) => {
                    return Some((col.name.as_str(), *val));
                },
                (Expr::Literal(ScalarValue::Int64(Some(val)), _), Expr::Column(col)) => {
                    return Some((col.name.as_str(), *val));
                },
                _ => {},
            }
            None
        },
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::InMemoryBackend;
    use kalamdb_commons::{JobId, NodeId};
    use kalamdb_system::providers::jobs::models::Job;
    use kalamdb_system::{JobStatus, JobType};

    // Test index: Jobs by status
    struct TestStatusIndex;

    impl IndexDefinition<JobId, Job> for TestStatusIndex {
        fn partition(&self) -> Partition {
            Partition::new("test_jobs_status_idx")
        }

        fn indexed_columns(&self) -> Vec<&str> {
            vec!["status"]
        }

        fn extract_key(&self, _pk: &JobId, job: &Job) -> Option<Vec<u8>> {
            let status_byte = match job.status {
                JobStatus::New => 0u8,
                JobStatus::Queued => 1,
                JobStatus::Running => 2,
                JobStatus::Retrying => 3,
                JobStatus::Completed => 4,
                JobStatus::Failed => 5,
                JobStatus::Cancelled => 6,
                JobStatus::Skipped => 7,
            };
            let mut key = Vec::with_capacity(1 + 8 + job.job_id.as_bytes().len());
            key.push(status_byte);
            key.extend_from_slice(&job.created_at.to_be_bytes());
            key.extend_from_slice(job.job_id.as_bytes());
            Some(key)
        }
    }

    fn create_test_job(id: &str, status: JobStatus) -> Job {
        let now = chrono::Utc::now().timestamp_millis();
        Job {
            job_id: JobId::new(id),
            job_type: JobType::Flush,
            status,
            parameters: None,
            message: None,
            exception_trace: None,
            idempotency_key: None,
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now,
            updated_at: now,
            started_at: if status != JobStatus::New && status != JobStatus::Queued {
                Some(now)
            } else {
                None
            },
            finished_at: if status == JobStatus::Completed
                || status == JobStatus::Failed
                || status == JobStatus::Cancelled
                || status == JobStatus::Skipped
            {
                Some(now)
            } else {
                None
            },
            node_id: NodeId::from(1u64),
            leader_node_id: None,
            leader_status: None,
            queue: None,
            priority: None,
        }
    }

    #[test]
    fn test_insert_creates_entity_and_index() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        let job = create_test_job("job1", JobStatus::Running);
        store.insert(&job.job_id, &job).unwrap();

        // Verify entity exists
        let retrieved = store.get(&job.job_id).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().job_id, job.job_id);

        // Verify index entry exists (status=Running=2)
        let running_jobs = store.scan_by_index(0, Some(&[2]), None).unwrap();
        assert_eq!(running_jobs.len(), 1);
        assert_eq!(running_jobs[0].0, job.job_id);
    }

    #[test]
    fn test_get_latest_by_index_prefix_returns_none_when_index_partition_is_missing() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        backend.drop_partition(&Partition::new("test_jobs_status_idx")).unwrap();

        let result = store.get_latest_by_index_prefix(0, &[2]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update_changes_index_entry() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        // Insert with Running status
        let mut job = create_test_job("job1", JobStatus::Running);
        store.insert(&job.job_id, &job).unwrap();

        // Update to Completed
        job.status = JobStatus::Completed;
        job.finished_at = Some(job.created_at + 1000);
        job.message = Some("Done".to_string());
        store.update(&job.job_id, &job).unwrap();

        // Verify old index entry removed (Running=2)
        let running_jobs = store.scan_by_index(0, Some(&[2]), None).unwrap();
        assert_eq!(running_jobs.len(), 0);

        // Verify new index entry exists (Completed=4)
        let completed_jobs = store.scan_by_index(0, Some(&[4]), None).unwrap();
        assert_eq!(completed_jobs.len(), 1);
        assert_eq!(completed_jobs[0].0, job.job_id);
    }

    #[test]
    fn test_delete_removes_entity_and_index() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        let job = create_test_job("job1", JobStatus::Running);
        store.insert(&job.job_id, &job).unwrap();

        // Delete
        store.delete(&job.job_id).unwrap();

        // Verify entity gone
        let retrieved = store.get(&job.job_id).unwrap();
        assert!(retrieved.is_none());

        // Verify index entry gone
        let running_jobs = store.scan_by_index(0, Some(&[2]), None).unwrap();
        assert_eq!(running_jobs.len(), 0);
    }

    #[test]
    fn test_scan_by_index_with_multiple_statuses() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        // Insert jobs with different statuses
        let job1 = create_test_job("job1", JobStatus::Running);
        let job2 = create_test_job("job2", JobStatus::Running);
        let job3 = create_test_job("job3", JobStatus::Completed);

        store.insert(&job1.job_id, &job1).unwrap();
        store.insert(&job2.job_id, &job2).unwrap();
        store.insert(&job3.job_id, &job3).unwrap();

        // Query Running jobs
        let running = store.scan_by_index(0, Some(&[2]), None).unwrap();
        assert_eq!(running.len(), 2);

        // Query Completed jobs
        let completed = store.scan_by_index(0, Some(&[4]), None).unwrap();
        assert_eq!(completed.len(), 1);

        // Query New jobs (none)
        let new_jobs = store.scan_by_index(0, Some(&[0]), None).unwrap();
        assert_eq!(new_jobs.len(), 0);
    }

    #[test]
    fn test_get_latest_by_index_prefix_returns_last_matching_entry() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        let mut older = create_test_job("job-old", JobStatus::Running);
        older.created_at = 100;
        older.updated_at = 100;

        let mut newer = create_test_job("job-new", JobStatus::Running);
        newer.created_at = 200;
        newer.updated_at = 200;

        let mut completed = create_test_job("job-done", JobStatus::Completed);
        completed.created_at = 300;
        completed.updated_at = 300;

        store.insert(&older.job_id, &older).unwrap();
        store.insert(&newer.job_id, &newer).unwrap();
        store.insert(&completed.job_id, &completed).unwrap();

        let latest = store.get_latest_by_index_prefix(0, &[2]).unwrap().expect("latest running");
        assert_eq!(latest.0, newer.job_id);
        assert_eq!(latest.1.created_at, 200);
    }

    #[tokio::test]
    async fn test_async_operations() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let store =
            IndexedEntityStore::new(backend.clone(), "test_jobs", vec![Arc::new(TestStatusIndex)]);

        let job = create_test_job("job1", JobStatus::Running);
        let job_id = job.job_id.clone();

        // Async insert
        store.insert_async(job_id.clone(), job.clone()).await.unwrap();

        // Async get
        let retrieved = store.get_async(job_id.clone()).await.unwrap();
        assert!(retrieved.is_some());

        // Async scan
        let running = store.scan_by_index_async(0, Some(vec![2]), None).await.unwrap();
        assert_eq!(running.len(), 1);

        let latest = store.get_latest_by_index_prefix_async(0, vec![2]).await.unwrap();
        assert_eq!(latest.expect("latest running").0, job_id);

        let queued_job = create_test_job("job2", JobStatus::Queued);
        let completed_job = create_test_job("job3", JobStatus::Completed);
        store
            .insert_batch_async(vec![
                (queued_job.job_id.clone(), queued_job.clone()),
                (completed_job.job_id.clone(), completed_job.clone()),
            ])
            .await
            .unwrap();

        let queued = store.scan_by_index_async(0, Some(vec![1]), None).await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].0, queued_job.job_id);

        // Async delete
        store.delete_async(job_id.clone()).await.unwrap();

        let retrieved = store.get_async(job_id).await.unwrap();
        assert!(retrieved.is_none());
    }
}
