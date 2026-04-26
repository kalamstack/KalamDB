//! Test utilities for kalamdb-store.
//!
//! Provides generic test helpers plus feature-gated backend-specific helpers.

use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

#[cfg(feature = "rocksdb")]
pub use crate::backends::rocksdb::test_utils::TestDb;
use crate::storage_trait::{Operation, Partition, StorageBackend, StorageStats};

/// In-memory implementation of StorageBackend for testing.
///
/// This provides a fast, thread-safe storage backend that doesn't require
/// disk I/O, making it ideal for unit tests.
///
/// ## Example
///
/// ```rust,ignore
/// use kalamdb_store::test_utils::InMemoryBackend;
/// use kalamdb_store::{StorageBackend, Partition};
///
/// let backend = InMemoryBackend::new();
/// let partition = Partition::new("test");
/// backend.create_partition(&partition).unwrap();
/// backend.put(&partition, b"key", b"value").unwrap();
/// assert_eq!(backend.get(&partition, b"key").unwrap(), Some(b"value".to_vec()));
/// ```
type PartitionMap = BTreeMap<Vec<u8>, Vec<u8>>;

pub struct InMemoryBackend {
    // Partition -> (Key -> Value)
    data: RwLock<HashMap<String, PartitionMap>>,
}

impl InMemoryBackend {
    /// Creates a new empty in-memory backend.
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for InMemoryBackend {
    fn get(
        &self,
        partition: &Partition,
        key: &[u8],
    ) -> crate::storage_trait::Result<Option<Vec<u8>>> {
        let data = self.data.read().unwrap();
        Ok(data.get(partition.name()).and_then(|map| map.get(key)).cloned())
    }

    fn put(
        &self,
        partition: &Partition,
        key: &[u8],
        value: &[u8],
    ) -> crate::storage_trait::Result<()> {
        let mut data = self.data.write().unwrap();
        let map = data.entry(partition.name().to_string()).or_default();
        map.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, partition: &Partition, key: &[u8]) -> crate::storage_trait::Result<()> {
        let mut data = self.data.write().unwrap();
        if let Some(map) = data.get_mut(partition.name()) {
            map.remove(key);
        }
        Ok(())
    }

    fn batch(&self, operations: Vec<Operation>) -> crate::storage_trait::Result<()> {
        for op in operations {
            match op {
                Operation::Put {
                    partition,
                    key,
                    value,
                } => {
                    self.put(&partition, &key, &value)?;
                },
                Operation::Delete { partition, key } => {
                    self.delete(&partition, &key)?;
                },
            }
        }
        Ok(())
    }

    fn scan(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> crate::storage_trait::Result<kalamdb_commons::storage::KvIterator<'_>> {
        let data = self.data.read().unwrap();
        let limit = limit.unwrap_or(usize::MAX);

        let items: Vec<(Vec<u8>, Vec<u8>)> = data
            .get(partition.name())
            .map(|map| {
                if limit == 0 {
                    return Vec::new();
                }

                let start_bound = match (prefix, start_key) {
                    (Some(prefix), Some(start)) => {
                        if start >= prefix {
                            Some(start.to_vec())
                        } else {
                            Some(prefix.to_vec())
                        }
                    },
                    (Some(prefix), None) => Some(prefix.to_vec()),
                    (None, Some(start)) => Some(start.to_vec()),
                    (None, None) => None,
                };

                let iter: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = match start_bound {
                    Some(start) => Box::new(map.range(start..)),
                    None => Box::new(map.iter()),
                };

                let mut items = Vec::new();
                for (k, v) in iter {
                    if let Some(prefix) = prefix {
                        if !k.starts_with(prefix) {
                            break;
                        }
                    }
                    items.push((k.clone(), v.clone()));
                    if items.len() >= limit {
                        break;
                    }
                }
                items
            })
            .unwrap_or_default();

        Ok(Box::new(items.into_iter()))
    }

    fn partition_exists(&self, partition: &Partition) -> bool {
        let data = self.data.read().unwrap();
        data.contains_key(partition.name())
    }

    fn create_partition(&self, partition: &Partition) -> crate::storage_trait::Result<()> {
        let mut data = self.data.write().unwrap();
        data.entry(partition.name().to_string()).or_default();
        Ok(())
    }

    fn list_partitions(&self) -> crate::storage_trait::Result<Vec<Partition>> {
        let data = self.data.read().unwrap();
        Ok(data.keys().map(|k| Partition::new(k.clone())).collect())
    }

    fn drop_partition(&self, partition: &Partition) -> crate::storage_trait::Result<()> {
        let mut data = self.data.write().unwrap();
        data.remove(partition.name());
        Ok(())
    }

    fn compact_partition(&self, _partition: &Partition) -> crate::storage_trait::Result<()> {
        // No-op for in-memory backend (no compaction needed)
        Ok(())
    }

    fn stats(&self) -> StorageStats {
        let partition_count = match self.data.read() {
            Ok(data) => data.len(),
            Err(_) => 0,
        };

        StorageStats::from([
            ("storage_backend".to_string(), "in_memory".to_string()),
            ("storage_partition_count".to_string(), partition_count.to_string()),
        ])
    }
}
