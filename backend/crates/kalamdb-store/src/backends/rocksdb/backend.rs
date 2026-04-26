//! RocksDB implementation of the StorageBackend trait.
//!
//! This module provides a concrete implementation of `StorageBackend` using RocksDB
//! as the underlying storage engine. Logical partitions are encoded as key prefixes
//! inside a small fixed set of physical RocksDB column families.

use std::sync::Arc;

use kalamdb_configs::RocksDbSettings;
use rocksdb::{BoundColumnFamily, Cache, IteratorMode, Options, PrefixRange, WriteOptions, DB};

use super::{
    cf_tuning::apply_cf_settings,
    init::create_block_options_with_cache,
    keyspace::{
        decode_logical_partition_registry_key, logical_partition_registry_key,
        logical_partition_registry_prefix, next_prefix_bound, partition_key_prefix,
        physical_cf_for_partition, physical_key, SYSTEM_META_CF,
    },
};
use crate::storage_trait::{
    Operation, Partition, Result, StorageBackend, StorageError, StorageStats,
};

const ESTIMATE_NUM_KEYS_PROPERTY: &str = "rocksdb.estimate-num-keys";
const ESTIMATE_LIVE_DATA_SIZE_PROPERTY: &str = "rocksdb.estimate-live-data-size";
const ACTIVE_MEMTABLE_ENTRIES_PROPERTY: &str = "rocksdb.num-entries-active-mem-table";
const IMM_MEMTABLE_ENTRIES_PROPERTY: &str = "rocksdb.num-entries-imm-mem-tables";
const ACTIVE_MEMTABLE_SIZE_PROPERTY: &str = "rocksdb.cur-size-active-mem-table";
const LIVE_SST_FILES_SIZE_PROPERTY: &str = "rocksdb.live-sst-files-size";
const TOTAL_SST_FILES_SIZE_PROPERTY: &str = "rocksdb.total-sst-files-size";
const ALL_MEMTABLES_SIZE_PROPERTY: &str = "rocksdb.cur-size-all-mem-tables";
const PENDING_COMPACTION_BYTES_PROPERTY: &str = "rocksdb.estimate-pending-compaction-bytes";

/// RocksDB implementation of the StorageBackend trait.
pub struct RocksDBBackend {
    db: Arc<DB>,
    write_opts: WriteOptions,
    settings: RocksDbSettings,
    block_cache: Cache,
    known_cf_names: std::sync::RwLock<Vec<String>>,
    logical_partition_names: std::sync::RwLock<Vec<String>>,
}

impl RocksDBBackend {
    fn new_internal(
        db: Arc<DB>,
        sync_writes: bool,
        disable_wal: bool,
        settings: RocksDbSettings,
    ) -> Self {
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(sync_writes);
        write_opts.disable_wal(disable_wal);
        let block_cache =
            Cache::new_lru_cache(std::cmp::min(settings.block_cache_size, 1024 * 1024));
        let backend = Self {
            db,
            write_opts,
            settings,
            block_cache,
            known_cf_names: std::sync::RwLock::new(Vec::new()),
            logical_partition_names: std::sync::RwLock::new(Vec::new()),
        };
        backend.load_logical_partitions();
        backend
    }

    /// Creates a new RocksDB backend with the given database handle.
    pub fn new(db: Arc<DB>) -> Self {
        Self::new_internal(db, false, false, RocksDbSettings::default())
    }

    /// Creates a new backend with write options and explicit RocksDB tuning settings.
    pub fn with_options_and_settings(
        db: Arc<DB>,
        sync_writes: bool,
        disable_wal: bool,
        settings: RocksDbSettings,
    ) -> Self {
        Self::new_internal(db, sync_writes, disable_wal, settings)
    }

    /// Set the known physical column family names.
    pub fn set_known_cf_names(&self, names: Vec<String>) {
        *self.known_cf_names.write().unwrap() = names;
    }

    fn get_cf(&self, partition: &Partition) -> Result<Arc<BoundColumnFamily<'_>>> {
        let cf_name = physical_cf_for_partition(partition.name());
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| StorageError::PartitionNotFound(cf_name.to_string()))
    }

    fn ensure_physical_cf(&self, cf_name: &str) -> Result<()> {
        if self.db.cf_handle(cf_name).is_some() {
            self.track_physical_cf(cf_name);
            return Ok(());
        }

        let mut opts = Options::default();
        apply_cf_settings(&mut opts, &self.settings, cf_name);
        opts.set_block_based_table_factory(&create_block_options_with_cache(&self.block_cache));

        match self.db.create_cf(cf_name, &opts) {
            Ok(()) => {
                self.track_physical_cf(cf_name);
                Ok(())
            },
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Column family already exists")
                    || msg.contains("column family already exists")
                {
                    self.track_physical_cf(cf_name);
                    return Ok(());
                }
                Err(StorageError::IoError(msg))
            },
        }
    }

    fn track_physical_cf(&self, cf_name: &str) {
        if let Ok(mut names) = self.known_cf_names.write() {
            if !names.iter().any(|name| name == cf_name) {
                names.push(cf_name.to_string());
            }
        }
    }

    fn track_logical_partition(&self, partition_name: &str) {
        if let Ok(mut names) = self.logical_partition_names.write() {
            if !names.iter().any(|name| name == partition_name) {
                names.push(partition_name.to_string());
            }
        }
    }

    fn load_logical_partitions(&self) {
        let Some(cf) = self.db.cf_handle(SYSTEM_META_CF) else {
            return;
        };
        let prefix = logical_partition_registry_prefix();
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_range(PrefixRange(prefix.clone()));

        let names: Vec<String> = self
            .db
            .iterator_cf_opt(
                &cf,
                readopts,
                IteratorMode::From(prefix.as_slice(), rocksdb::Direction::Forward),
            )
            .filter_map(|item| {
                item.ok().and_then(|(key, _)| {
                    decode_logical_partition_registry_key(&key).map(str::to_string)
                })
            })
            .collect();

        if let Ok(mut tracked) = self.logical_partition_names.write() {
            *tracked = names;
        }
    }

    fn persist_logical_partition(&self, partition_name: &str) -> Result<()> {
        let cf = self
            .db
            .cf_handle(SYSTEM_META_CF)
            .ok_or_else(|| StorageError::PartitionNotFound(SYSTEM_META_CF.to_string()))?;
        self.db
            .put_cf_opt(&cf, logical_partition_registry_key(partition_name), b"", &self.write_opts)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn remove_logical_partition(&self, partition_name: &str) -> Result<()> {
        let cf = self
            .db
            .cf_handle(SYSTEM_META_CF)
            .ok_or_else(|| StorageError::PartitionNotFound(SYSTEM_META_CF.to_string()))?;
        self.db
            .delete_cf_opt(&cf, logical_partition_registry_key(partition_name), &self.write_opts)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn tracked_cf_names(&self) -> Vec<String> {
        match self.known_cf_names.read() {
            Ok(names) if names.is_empty() => vec!["default".to_string()],
            Ok(names) => names.clone(),
            Err(_) => vec!["default".to_string()],
        }
    }

    fn tracked_partition_count(&self) -> usize {
        match self.logical_partition_names.read() {
            Ok(names) => names.iter().filter(|name| name.as_str() != "default").count(),
            Err(_) => 0,
        }
    }

    fn sum_cf_property(&self, property_name: &str) -> u64 {
        self.tracked_cf_names()
            .into_iter()
            .filter_map(|cf_name| self.db.cf_handle(&cf_name))
            .filter_map(|cf| self.db.property_int_value_cf(&cf, property_name).ok().flatten())
            .sum()
    }
}

impl StorageBackend for RocksDBBackend {
    fn get(&self, partition: &Partition, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _span = tracing::trace_span!(
            "rocksdb.get",
            partition = %partition.name(),
            physical_cf = physical_cf_for_partition(partition.name())
        )
        .entered();
        let cf = self.get_cf(partition)?;
        let physical_key = physical_key(partition.name(), key);
        self.db
            .get_cf(&cf, physical_key)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn put(&self, partition: &Partition, key: &[u8], value: &[u8]) -> Result<()> {
        let _span = tracing::trace_span!(
            "rocksdb.put",
            partition = %partition.name(),
            physical_cf = physical_cf_for_partition(partition.name()),
            value_len = value.len()
        )
        .entered();
        let cf = self.get_cf(partition)?;
        let physical_key = physical_key(partition.name(), key);
        self.db
            .put_cf_opt(&cf, physical_key, value, &self.write_opts)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn delete(&self, partition: &Partition, key: &[u8]) -> Result<()> {
        let _span = tracing::trace_span!(
            "rocksdb.delete",
            partition = %partition.name(),
            physical_cf = physical_cf_for_partition(partition.name())
        )
        .entered();
        let cf = self.get_cf(partition)?;
        let physical_key = physical_key(partition.name(), key);
        self.db
            .delete_cf_opt(&cf, physical_key, &self.write_opts)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn batch(&self, operations: Vec<Operation>) -> Result<()> {
        let _span = tracing::debug_span!("rocksdb.batch", op_count = operations.len()).entered();
        use rocksdb::WriteBatch;

        let mut batch = WriteBatch::default();

        for op in operations {
            match op {
                Operation::Put {
                    partition,
                    key,
                    value,
                } => {
                    let cf = self.get_cf(&partition)?;
                    batch.put_cf(&cf, physical_key(partition.name(), &key), value);
                },
                Operation::Delete { partition, key } => {
                    let cf = self.get_cf(&partition)?;
                    batch.delete_cf(&cf, physical_key(partition.name(), &key));
                },
            }
        }

        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| StorageError::IoError(e.to_string()))
    }

    fn scan(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send + '_>> {
        let _span = tracing::trace_span!(
            "rocksdb.scan",
            partition = %partition.name(),
            physical_cf = physical_cf_for_partition(partition.name()),
            has_prefix = prefix.is_some(),
            limit = ?limit
        )
        .entered();
        use rocksdb::Direction;

        let cf = self.get_cf(partition)?;
        let snapshot = self.db.snapshot();
        let partition_prefix = partition_key_prefix(partition.name());
        let user_prefix = prefix.map(|p| p.to_vec());
        let physical_prefix = user_prefix.as_ref().map_or_else(
            || partition_prefix.clone(),
            |prefix| {
                let mut physical_prefix = partition_prefix.clone();
                physical_prefix.extend_from_slice(prefix);
                physical_prefix
            },
        );
        let physical_start = start_key.map(|start| {
            let mut physical_start = partition_prefix.clone();
            physical_start.extend_from_slice(start);
            physical_start
        });

        let iter_mode = if let Some(start) = &physical_start {
            IteratorMode::From(start.as_slice(), Direction::Forward)
        } else {
            IteratorMode::From(physical_prefix.as_slice(), Direction::Forward)
        };

        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_range(PrefixRange(physical_prefix.clone()));
        let inner = self.db.iterator_cf_opt(&cf, readopts, iter_mode);

        struct SnapshotScanIter<'a, D: rocksdb::DBAccess> {
            _snapshot: rocksdb::SnapshotWithThreadMode<'a, D>,
            inner: rocksdb::DBIteratorWithThreadMode<'a, D>,
            partition_prefix: Vec<u8>,
            user_prefix: Option<Vec<u8>>,
            remaining: Option<usize>,
        }

        impl<'a, D: rocksdb::DBAccess> Iterator for SnapshotScanIter<'a, D> {
            type Item = (Vec<u8>, Vec<u8>);

            fn next(&mut self) -> Option<Self::Item> {
                if let Some(0) = self.remaining {
                    return None;
                }

                match self.inner.next()? {
                    Ok((k, v)) => {
                        if !k.starts_with(&self.partition_prefix) {
                            return None;
                        }
                        let logical_key = &k[self.partition_prefix.len()..];
                        if let Some(ref prefix) = self.user_prefix {
                            if !logical_key.starts_with(prefix) {
                                return None;
                            }
                        }
                        if let Some(ref mut left) = self.remaining {
                            if *left > 0 {
                                *left -= 1;
                            }
                        }
                        Some((logical_key.to_vec(), v.to_vec()))
                    },
                    Err(_) => None,
                }
            }
        }

        let iter = SnapshotScanIter::<DB> {
            _snapshot: snapshot,
            inner,
            partition_prefix,
            user_prefix,
            remaining: limit,
        };

        Ok(Box::new(iter))
    }

    fn scan_reverse(
        &self,
        partition: &Partition,
        prefix: Option<&[u8]>,
        start_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send + '_>> {
        let _span = tracing::trace_span!(
            "rocksdb.scan_reverse",
            partition = %partition.name(),
            has_prefix = prefix.is_some(),
            limit = ?limit
        )
        .entered();
        use rocksdb::Direction;

        let cf = self.get_cf(partition)?;
        let snapshot = self.db.snapshot();
        let partition_prefix = partition_key_prefix(partition.name());
        let user_prefix = prefix.map(|p| p.to_vec());
        let physical_prefix = user_prefix.as_ref().map_or_else(
            || partition_prefix.clone(),
            |prefix| {
                let mut physical_prefix = partition_prefix.clone();
                physical_prefix.extend_from_slice(prefix);
                physical_prefix
            },
        );
        let physical_start = start_key.map(|start| {
            let mut physical_start = partition_prefix.clone();
            physical_start.extend_from_slice(start);
            physical_start
        });

        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_range(PrefixRange(physical_prefix.clone()));

        let iter_mode = if let Some(start) = &physical_start {
            IteratorMode::From(start.as_slice(), Direction::Reverse)
        } else {
            IteratorMode::End
        };

        let inner = self.db.iterator_cf_opt(&cf, readopts, iter_mode);

        struct SnapshotReverseScanIter<'a, D: rocksdb::DBAccess> {
            _snapshot: rocksdb::SnapshotWithThreadMode<'a, D>,
            inner: rocksdb::DBIteratorWithThreadMode<'a, D>,
            partition_prefix: Vec<u8>,
            user_prefix: Option<Vec<u8>>,
            remaining: Option<usize>,
        }

        impl<'a, D: rocksdb::DBAccess> Iterator for SnapshotReverseScanIter<'a, D> {
            type Item = (Vec<u8>, Vec<u8>);

            fn next(&mut self) -> Option<Self::Item> {
                if let Some(0) = self.remaining {
                    return None;
                }

                match self.inner.next()? {
                    Ok((k, v)) => {
                        if !k.starts_with(&self.partition_prefix) {
                            return None;
                        }
                        let logical_key = &k[self.partition_prefix.len()..];
                        if let Some(ref prefix) = self.user_prefix {
                            if !logical_key.starts_with(prefix) {
                                return None;
                            }
                        }
                        if let Some(ref mut left) = self.remaining {
                            if *left > 0 {
                                *left -= 1;
                            }
                        }
                        Some((logical_key.to_vec(), v.to_vec()))
                    },
                    Err(_) => None,
                }
            }
        }

        let iter = SnapshotReverseScanIter::<DB> {
            _snapshot: snapshot,
            inner,
            partition_prefix,
            user_prefix,
            remaining: limit,
        };

        Ok(Box::new(iter))
    }

    fn partition_exists(&self, partition: &Partition) -> bool {
        self.logical_partition_names
            .read()
            .map(|names| names.iter().any(|name| name == partition.name()))
            .unwrap_or(false)
    }

    fn create_partition(&self, partition: &Partition) -> Result<()> {
        self.ensure_physical_cf(physical_cf_for_partition(partition.name()))?;
        self.persist_logical_partition(partition.name())?;
        self.track_logical_partition(partition.name());
        Ok(())
    }

    fn list_partitions(&self) -> Result<Vec<Partition>> {
        let names = self.logical_partition_names.read().unwrap();
        let partitions = names
            .iter()
            .filter(|name| *name != "default")
            .map(|name| Partition::new(name.as_str()))
            .collect();
        Ok(partitions)
    }

    fn drop_partition(&self, partition: &Partition) -> Result<()> {
        let cf = self.get_cf(partition)?;
        let prefix = partition_key_prefix(partition.name());
        let snapshot = self.db.snapshot();
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_range(PrefixRange(prefix.clone()));

        let keys: Vec<Vec<u8>> = self
            .db
            .iterator_cf_opt(
                &cf,
                readopts,
                IteratorMode::From(prefix.as_slice(), rocksdb::Direction::Forward),
            )
            .filter_map(|item| item.ok().map(|(key, _)| key.to_vec()))
            .take_while(|key| key.starts_with(&prefix))
            .collect();

        if !keys.is_empty() {
            let mut batch = rocksdb::WriteBatch::default();
            for key in keys {
                batch.delete_cf(&cf, key);
            }
            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| StorageError::IoError(e.to_string()))?;
        }

        if let Ok(mut names) = self.logical_partition_names.write() {
            names.retain(|n| n != partition.name());
        }
        self.remove_logical_partition(partition.name())?;

        Ok(())
    }

    fn compact_partition(&self, partition: &Partition) -> Result<()> {
        let cf = self.get_cf(partition)?;
        let start = partition_key_prefix(partition.name());
        if let Some(end) = next_prefix_bound(&start) {
            self.db.compact_range_cf(&cf, Some(start.as_slice()), Some(end.as_slice()));
        } else {
            self.db.compact_range_cf(&cf, Some(start.as_slice()), None::<&[u8]>);
        }
        Ok(())
    }

    fn flush_all_memtables(&self) -> Result<()> {
        let names = self
            .known_cf_names
            .read()
            .map_err(|e| StorageError::Other(format!("lock poisoned: {}", e)))?;

        let mut flush_opts = rocksdb::FlushOptions::default();
        flush_opts.set_wait(true);

        for cf_name in names.iter() {
            if let Some(cf) = self.db.cf_handle(cf_name) {
                if let Err(e) = self.db.flush_cf_opt(&cf, &flush_opts) {
                    log::warn!("flush_all_memtables: failed to flush CF '{}': {}", cf_name, e);
                }
            }
        }
        if let Err(e) = self.db.flush_opt(&flush_opts) {
            log::warn!("flush_all_memtables: failed to flush default CF: {}", e);
        }

        Ok(())
    }

    fn backup_to(&self, backup_dir: &std::path::Path) -> crate::storage_trait::Result<()> {
        use rocksdb::{
            backup::{BackupEngine, BackupEngineOptions},
            Env,
        };

        std::fs::create_dir_all(backup_dir).map_err(|e| {
            crate::storage_trait::StorageError::Other(format!(
                "Failed to create backup directory '{}': {}",
                backup_dir.display(),
                e
            ))
        })?;

        let opts = BackupEngineOptions::new(backup_dir).map_err(|e| {
            crate::storage_trait::StorageError::Other(format!(
                "BackupEngineOptions::new failed: {}",
                e
            ))
        })?;
        let env = Env::new().map_err(|e| {
            crate::storage_trait::StorageError::Other(format!(
                "Failed to create RocksDB Env: {}",
                e
            ))
        })?;
        let mut engine = BackupEngine::open(&opts, &env).map_err(|e| {
            crate::storage_trait::StorageError::Other(format!("Failed to open BackupEngine: {}", e))
        })?;
        engine.create_new_backup_flush(&*self.db, true).map_err(|e| {
            crate::storage_trait::StorageError::Other(format!(
                "RocksDB create_new_backup_flush failed: {}",
                e
            ))
        })?;
        Ok(())
    }

    fn restore_from(&self, backup_dir: &std::path::Path) -> crate::storage_trait::Result<()> {
        use rocksdb::{
            backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
            Env,
        };

        let opts = BackupEngineOptions::new(backup_dir).map_err(|e| {
            crate::storage_trait::StorageError::Other(format!(
                "BackupEngineOptions::new failed: {}",
                e
            ))
        })?;
        let env = Env::new().map_err(|e| {
            crate::storage_trait::StorageError::Other(format!(
                "Failed to create RocksDB Env: {}",
                e
            ))
        })?;
        let mut engine = BackupEngine::open(&opts, &env).map_err(|e| {
            crate::storage_trait::StorageError::Other(format!("Failed to open BackupEngine: {}", e))
        })?;
        let restore_opts = RestoreOptions::default();

        let db_path = self.db.path();
        let staging_path = {
            let mut p = db_path.as_os_str().to_os_string();
            p.push("_restore_pending");
            std::path::PathBuf::from(p)
        };

        if staging_path.exists() {
            std::fs::remove_dir_all(&staging_path).map_err(|e| {
                crate::storage_trait::StorageError::Other(format!(
                    "Failed to clear stale restore staging dir '{}': {}",
                    staging_path.display(),
                    e
                ))
            })?;
        }

        engine
            .restore_from_latest_backup(&staging_path, &staging_path, &restore_opts)
            .map_err(|e| {
                crate::storage_trait::StorageError::Other(format!(
                    "RocksDB restore_from_latest_backup failed: {}",
                    e
                ))
            })?;
        Ok(())
    }

    fn stats(&self) -> StorageStats {
        StorageStats::from([
            ("storage_backend".to_string(), "rocksdb".to_string()),
            (
                "storage_partition_count".to_string(),
                self.tracked_partition_count().to_string(),
            ),
            (
                "rocksdb_physical_cf_count".to_string(),
                self.tracked_cf_names().len().to_string(),
            ),
            (
                "rocksdb_estimate_num_keys".to_string(),
                self.sum_cf_property(ESTIMATE_NUM_KEYS_PROPERTY).to_string(),
            ),
            (
                "rocksdb_estimate_live_data_size_bytes".to_string(),
                self.sum_cf_property(ESTIMATE_LIVE_DATA_SIZE_PROPERTY).to_string(),
            ),
            (
                "rocksdb_active_memtable_entries".to_string(),
                self.sum_cf_property(ACTIVE_MEMTABLE_ENTRIES_PROPERTY).to_string(),
            ),
            (
                "rocksdb_immutable_memtable_entries".to_string(),
                self.sum_cf_property(IMM_MEMTABLE_ENTRIES_PROPERTY).to_string(),
            ),
            (
                "rocksdb_active_memtable_size_bytes".to_string(),
                self.sum_cf_property(ACTIVE_MEMTABLE_SIZE_PROPERTY).to_string(),
            ),
            (
                "rocksdb_live_sst_files_size_bytes".to_string(),
                self.sum_cf_property(LIVE_SST_FILES_SIZE_PROPERTY).to_string(),
            ),
            (
                "rocksdb_total_sst_files_size_bytes".to_string(),
                self.sum_cf_property(TOTAL_SST_FILES_SIZE_PROPERTY).to_string(),
            ),
            (
                "rocksdb_memtables_size_bytes".to_string(),
                self.sum_cf_property(ALL_MEMTABLES_SIZE_PROPERTY).to_string(),
            ),
            (
                "rocksdb_pending_compaction_bytes".to_string(),
                self.sum_cf_property(PENDING_COMPACTION_BYTES_PROPERTY).to_string(),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{encode_key, encode_prefix};
    use kalamdb_configs::RocksDbSettings;
    use tempfile::TempDir;

    use super::super::init::RocksDbInit;
    use super::*;

    fn create_test_db() -> (Arc<DB>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open(&opts, temp_dir.path()).unwrap();
        (Arc::new(db), temp_dir)
    }

    #[test]
    fn test_create_and_get_partition() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        assert!(backend.partition_exists(&partition));
    }

    #[test]
    fn test_put_and_get() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        backend.put(&partition, b"key1", b"value1").unwrap();
        let value = backend.get(&partition, b"key1").unwrap();

        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_delete() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        backend.put(&partition, b"key1", b"value1").unwrap();
        backend.delete(&partition, b"key1").unwrap();

        let value = backend.get(&partition, b"key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_batch_operations() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        let ops = vec![
            Operation::Put {
                partition: partition.clone(),
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            },
            Operation::Put {
                partition: partition.clone(),
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            },
            Operation::Delete {
                partition: partition.clone(),
                key: b"key1".to_vec(),
            },
        ];

        backend.batch(ops).unwrap();

        assert_eq!(backend.get(&partition, b"key1").unwrap(), None);
        assert_eq!(backend.get(&partition, b"key2").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_scan_all() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        backend.put(&partition, b"key1", b"value1").unwrap();
        backend.put(&partition, b"key2", b"value2").unwrap();
        backend.put(&partition, b"key3", b"value3").unwrap();

        let results: Vec<_> = backend.scan(&partition, None, None, None).unwrap().collect();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_scan_with_prefix() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        backend.put(&partition, b"user:1", b"value1").unwrap();
        backend.put(&partition, b"user:2", b"value2").unwrap();
        backend.put(&partition, b"admin:1", b"value3").unwrap();

        let results: Vec<_> =
            backend.scan(&partition, Some(b"user:"), None, None).unwrap().collect();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_scan_with_limit() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        backend.put(&partition, b"key1", b"value1").unwrap();
        backend.put(&partition, b"key2", b"value2").unwrap();
        backend.put(&partition, b"key3", b"value3").unwrap();

        let results: Vec<_> = backend.scan(&partition, None, None, Some(2)).unwrap().collect();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_scan_reverse_with_storekey_prefix_returns_latest_match() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();

        let prefix = encode_prefix(&("user1", 42_i64));
        let older_key = encode_key(&("user1", 42_i64, 100_i64));
        let newer_key = encode_key(&("user1", 42_i64, 200_i64));
        let other_key = encode_key(&("user1", 99_i64, 300_i64));

        backend.put(&partition, &older_key, b"older").unwrap();
        backend.put(&partition, &newer_key, b"newer").unwrap();
        backend.put(&partition, &other_key, b"other").unwrap();

        let results: Vec<_> = backend
            .scan_reverse(&partition, Some(&prefix), None, Some(1))
            .unwrap()
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, newer_key);
        assert_eq!(results[0].1, b"newer".to_vec());
    }

    #[test]
    fn test_list_partitions() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        backend.create_partition(&Partition::new("cf1")).unwrap();
        backend.create_partition(&Partition::new("cf2")).unwrap();

        assert!(backend.partition_exists(&Partition::new("cf1")));
        assert!(backend.partition_exists(&Partition::new("cf2")));

        let partitions = backend.list_partitions().unwrap();
        let _ = partitions.len();
    }

    #[test]
    fn test_logical_partitions_survive_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_string_lossy().into_owned();
        let partition = Partition::new("shared_default:profiles");

        {
            let init = RocksDbInit::with_defaults(db_path.clone());
            let (db, cf_names) = init.open_with_cf_names().unwrap();
            let backend = RocksDBBackend::with_options_and_settings(
                db,
                false,
                false,
                RocksDbSettings::default(),
            );
            backend.set_known_cf_names(cf_names);

            backend.create_partition(&partition).unwrap();
            backend.put(&partition, b"key1", b"value1").unwrap();

            assert!(backend.partition_exists(&partition));
        }

        {
            let init = RocksDbInit::with_defaults(db_path);
            let (db, cf_names) = init.open_with_cf_names().unwrap();
            let backend = RocksDBBackend::with_options_and_settings(
                db,
                false,
                false,
                RocksDbSettings::default(),
            );
            backend.set_known_cf_names(cf_names);

            assert!(backend.partition_exists(&partition));
            assert_eq!(backend.get(&partition, b"key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(backend.list_partitions().unwrap(), vec![partition]);
        }
    }

    #[test]
    fn test_drop_partition() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("test_cf");
        backend.create_partition(&partition).unwrap();
        assert!(backend.partition_exists(&partition));

        backend.drop_partition(&partition).unwrap();
        assert!(!backend.partition_exists(&partition));
    }

    #[test]
    fn test_stats_include_storage_and_rocksdb_metrics() {
        let (db, _temp) = create_test_db();
        let backend = RocksDBBackend::new(db);

        let partition = Partition::new("stats_cf");
        backend.create_partition(&partition).unwrap();
        backend.put(&partition, b"key1", b"value1").unwrap();
        backend.flush_all_memtables().unwrap();

        let stats = backend.stats();

        assert_eq!(stats.get("storage_backend").map(String::as_str), Some("rocksdb"));
        assert_eq!(stats.get("storage_partition_count").map(String::as_str), Some("1"));
        assert!(stats.contains_key("rocksdb_estimate_num_keys"));
        assert!(stats.contains_key("rocksdb_estimate_live_data_size_bytes"));
        assert!(stats.contains_key("rocksdb_active_memtable_entries"));
        assert!(stats.contains_key("rocksdb_immutable_memtable_entries"));
        assert!(stats.contains_key("rocksdb_active_memtable_size_bytes"));
        assert!(stats.contains_key("rocksdb_live_sst_files_size_bytes"));
        assert!(stats.contains_key("rocksdb_total_sst_files_size_bytes"));
        assert!(stats.contains_key("rocksdb_memtables_size_bytes"));
        assert!(stats.contains_key("rocksdb_pending_compaction_bytes"));
    }
}
