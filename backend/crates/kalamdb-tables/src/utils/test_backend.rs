//! Test backend wrapper that records scan parameters.

use kalamdb_commons::storage::KvIterator;
use kalamdb_store::storage_trait::{Operation, Partition, StorageBackend};
use kalamdb_store::test_utils::InMemoryBackend;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanArgs {
    pub prefix: Option<Vec<u8>>,
    pub start_key: Option<Vec<u8>>,
    pub limit: Option<usize>,
}

/// StorageBackend wrapper that records the last scan call.
pub struct RecordingBackend {
    inner: InMemoryBackend,
    last_scan: Mutex<Option<ScanArgs>>,
    scan_calls: AtomicUsize,
}

impl RecordingBackend {
    pub fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            last_scan: Mutex::new(None),
            scan_calls: AtomicUsize::new(0),
        }
    }

    pub fn last_scan(&self) -> Option<ScanArgs> {
        self.last_scan.lock().unwrap().clone()
    }

    pub fn scan_calls(&self) -> usize {
        self.scan_calls.load(Ordering::SeqCst)
    }
}

impl Default for RecordingBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for RecordingBackend {
    fn get(
        &self,
        partition: &Partition,
        key: &[u8],
    ) -> kalamdb_store::storage_trait::Result<Option<Vec<u8>>> {
        self.inner.get(partition, key)
    }

    fn put(
        &self,
        partition: &Partition,
        key: &[u8],
        value: &[u8],
    ) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.put(partition, key, value)
    }

    fn delete(
        &self,
        partition: &Partition,
        key: &[u8],
    ) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.delete(partition, key)
    }

    fn batch(&self, operations: Vec<Operation>) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.batch(operations)
    }

    fn scan(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> kalamdb_store::storage_trait::Result<KvIterator<'_>> {
        let args = ScanArgs {
            prefix: prefix.map(|p| p.to_vec()),
            start_key: start_key.map(|k| k.to_vec()),
            limit,
        };
        *self.last_scan.lock().unwrap() = Some(args);
        self.scan_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.scan(partition, prefix, start_key, limit)
    }

    fn partition_exists(&self, partition: &Partition) -> bool {
        self.inner.partition_exists(partition)
    }

    fn create_partition(&self, partition: &Partition) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.create_partition(partition)
    }

    fn list_partitions(&self) -> kalamdb_store::storage_trait::Result<Vec<Partition>> {
        self.inner.list_partitions()
    }

    fn drop_partition(&self, partition: &Partition) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.drop_partition(partition)
    }

    fn compact_partition(&self, partition: &Partition) -> kalamdb_store::storage_trait::Result<()> {
        self.inner.compact_partition(partition)
    }

    fn stats(&self) -> kalamdb_store::storage_trait::StorageStats {
        self.inner.stats()
    }
}
