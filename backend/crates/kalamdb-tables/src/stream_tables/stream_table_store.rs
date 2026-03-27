//! Stream table store implementation backed by in-memory storage.
//!
//! Stream tables now use in-memory BTreeMap storage for fast ephemeral access.
//! In the future, we will support specifying persistence mode when creating a stream table.
//!
//! **MVCC Architecture (Phase 13.2)**:
//! - StreamTableRowId: Composite struct with user_id and _seq fields
//! - StreamTableRow: Minimal structure with user_id, _seq, fields (JSON)
//! - Ordering: (user_id, _seq) for deterministic scans

use crate::common::partition_name;
use kalamdb_commons::ids::{SeqId, StreamTableRowId};
use kalamdb_commons::models::{StreamTableRow, UserId};
use kalamdb_commons::storage::Partition;
use kalamdb_commons::TableId;
use kalamdb_sharding::ShardRouter;
use kalamdb_store::entity_store::ScanDirection;
use kalamdb_store::storage_trait::{Result, StorageError};
use kalamdb_streams::{
    bucket_for_ttl, FileStreamLogStore, MemoryStreamLogStore, StreamLogConfig, StreamLogStore,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

const MAX_SCAN_LIMIT: usize = 100000;

/// Configuration for StreamTableStore.
#[derive(Debug, Clone)]
pub struct StreamTableStoreConfig {
    pub base_dir: PathBuf,
    pub max_rows_per_user: usize,
    pub shard_router: ShardRouter,
    pub ttl_seconds: Option<u64>,
    pub storage_mode: StreamTableStorageMode,
}

impl StreamTableStoreConfig {
    fn bucket(&self) -> kalamdb_streams::StreamTimeBucket {
        bucket_for_ttl(self.ttl_seconds.unwrap_or(3600))
    }
}

/// Backing store mode for stream tables.
#[derive(Debug, Clone, Copy)]
pub enum StreamTableStorageMode {
    Memory,
    File,
}

impl Default for StreamTableStorageMode {
    fn default() -> Self {
        Self::File
    }
}

#[derive(Clone)]
enum StreamLogStoreBackend {
    Memory(Arc<MemoryStreamLogStore>),
    File(Arc<FileStreamLogStore>),
}

impl StreamLogStoreBackend {
    fn append_rows(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        rows: HashMap<StreamTableRowId, StreamTableRow>,
    ) -> Result<()> {
        match self {
            Self::Memory(store) => {
                store.append_rows(table_id, user_id, rows).map_err(map_stream_error)
            },
            Self::File(store) => {
                store.append_rows(table_id, user_id, rows).map_err(map_stream_error)
            },
        }
    }

    fn read_in_time_range(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        start_time: u64,
        end_time: u64,
        limit: usize,
    ) -> Result<HashMap<StreamTableRowId, StreamTableRow>> {
        match self {
            Self::Memory(store) => store
                .read_in_time_range(table_id, user_id, start_time, end_time, limit)
                .map_err(map_stream_error),
            Self::File(store) => store
                .read_in_time_range(table_id, user_id, start_time, end_time, limit)
                .map_err(map_stream_error),
        }
    }

    fn delete_old_logs_with_count(&self, before_time: u64) -> Result<usize> {
        match self {
            Self::Memory(store) => {
                store.delete_old_logs_with_count(before_time).map_err(map_stream_error)
            },
            Self::File(store) => {
                store.delete_old_logs_with_count(before_time).map_err(map_stream_error)
            },
        }
    }

    fn append_delete(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        key: &StreamTableRowId,
    ) -> Result<()> {
        match self {
            Self::Memory(store) => {
                store.append_delete(table_id, user_id, key).map_err(map_stream_error)
            },
            Self::File(store) => {
                store.append_delete(table_id, user_id, key).map_err(map_stream_error)
            },
        }
    }

    fn has_logs_before(&self, before_time: u64) -> Result<bool> {
        match self {
            Self::Memory(store) => store.has_logs_before(before_time).map_err(map_stream_error),
            Self::File(store) => store.has_logs_before(before_time).map_err(map_stream_error),
        }
    }

    fn list_user_ids(&self) -> Result<Vec<UserId>> {
        match self {
            Self::Memory(store) => store.list_user_ids().map_err(map_stream_error),
            Self::File(store) => store.list_user_ids().map_err(map_stream_error),
        }
    }
}

fn map_stream_error(error: kalamdb_streams::StreamLogError) -> StorageError {
    StorageError::IoError(error.to_string())
}

/// Store for stream tables (in-memory BTreeMap storage for fast ephemeral access).
#[derive(Clone)]
pub struct StreamTableStore {
    table_id: TableId,
    partition: Partition,
    log_store: StreamLogStoreBackend,
}

impl StreamTableStore {
    /// Create a new stream table store (in-memory backed)
    pub fn new(
        table_id: TableId,
        partition: impl Into<Partition>,
        config: StreamTableStoreConfig,
    ) -> Self {
        let log_store = match config.storage_mode {
            StreamTableStorageMode::Memory => StreamLogStoreBackend::Memory(Arc::new(
                MemoryStreamLogStore::with_table_id_and_limit(
                    table_id.clone(),
                    config.max_rows_per_user,
                ),
            )),
            StreamTableStorageMode::File => {
                StreamLogStoreBackend::File(Arc::new(FileStreamLogStore::new(StreamLogConfig {
                    base_dir: config.base_dir.clone(),
                    shard_router: config.shard_router.clone(),
                    bucket: config.bucket(),
                    table_id: table_id.clone(),
                })))
            },
        };

        Self {
            table_id,
            partition: partition.into(),
            log_store,
        }
    }

    /// Returns the partition name.
    pub fn partition(&self) -> &Partition {
        &self.partition
    }

    /// Append a row.
    pub fn put(&self, key: &StreamTableRowId, entity: &StreamTableRow) -> Result<()> {
        let mut rows = HashMap::new();
        rows.insert(key.clone(), entity.clone());
        self.log_store.append_rows(&self.table_id, key.user_id(), rows)
    }

    /// Retrieve a row by key.
    pub fn get(&self, key: &StreamTableRowId) -> Result<Option<StreamTableRow>> {
        let ts = key.seq().timestamp_millis();
        let rows = self.log_store.read_in_time_range(
            &self.table_id,
            key.user_id(),
            ts,
            ts,
            MAX_SCAN_LIMIT,
        )?;
        Ok(rows.get(key).cloned())
    }

    /// Delete a row by key (append tombstone).
    pub fn delete(&self, key: &StreamTableRowId) -> Result<()> {
        self.log_store.append_delete(&self.table_id, key.user_id(), key)
    }

    /// Delete old logs for this table before a timestamp.
    pub fn delete_old_logs(&self, before_time: u64) -> Result<usize> {
        self.log_store.delete_old_logs_with_count(before_time)
    }

    /// Check if any logs exist before a timestamp.
    pub fn has_logs_before(&self, before_time: u64) -> Result<bool> {
        self.log_store.has_logs_before(before_time)
    }

    /// Scan all rows with an optional limit.
    pub fn scan_all(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        let limit = limit.unwrap_or(MAX_SCAN_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let users = self.log_store.list_user_ids()?;
        let mut results: Vec<(StreamTableRowId, StreamTableRow)> = Vec::new();

        for user_id in users {
            let remaining = limit.saturating_sub(results.len());
            if remaining == 0 {
                break;
            }
            let rows = self.log_store.read_in_time_range(
                &self.table_id,
                &user_id,
                0,
                u64::MAX,
                remaining,
            )?;
            results.extend(rows.into_iter());
        }

        results.sort_by(|(a, _), (b, _)| a.cmp(b));
        if results.len() > limit {
            results.truncate(limit);
        }
        Ok(results)
    }

    /// Scan rows for a single user, starting at an optional sequence ID (inclusive).
    pub fn scan_user(
        &self,
        user_id: &UserId,
        start_seq: Option<SeqId>,
        limit: usize,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start_time = start_seq.map(|seq| seq.timestamp_millis()).unwrap_or(0);
        let rows = self.log_store.read_in_time_range(
            &self.table_id,
            user_id,
            start_time,
            u64::MAX,
            limit,
        )?;

        let mut vec: Vec<(StreamTableRowId, StreamTableRow)> = rows
            .into_iter()
            .filter(|(key, _)| start_seq.map(|seq| key.seq() >= seq).unwrap_or(true))
            .collect();
        vec.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(vec)
    }

    /// Streaming scan for a single user with TTL filtering and early termination.
    ///
    /// This method is optimized for LIMIT queries - it stops scanning as soon as
    /// enough rows are collected, without loading all data into memory first.
    ///
    /// **Performance benefits over `scan_user`:**
    /// - Early termination when limit is reached
    /// - TTL filtering applied during iteration (not after collection)
    /// - Memory-efficient for large datasets with small LIMIT
    ///
    /// # Arguments
    /// * `user_id` - User ID to scope the scan
    /// * `start_seq` - Optional starting sequence (inclusive)
    /// * `limit` - Maximum rows to return
    /// * `ttl_ms` - Optional TTL in milliseconds (rows older than now - ttl_ms are filtered)
    /// * `now_ms` - Current time in milliseconds (for TTL calculation)
    ///
    /// # Returns
    /// Vector of (key, row) pairs, filtered by TTL and limited
    pub fn scan_user_streaming(
        &self,
        user_id: &UserId,
        start_seq: Option<SeqId>,
        limit: usize,
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start_time = start_seq.map(|seq| seq.timestamp_millis()).unwrap_or(0);
        let rows = self.log_store.read_in_time_range(
            &self.table_id,
            user_id,
            start_time,
            u64::MAX,
            MAX_SCAN_LIMIT,
        )?;

        // Filter by start_seq, apply TTL, and collect with early termination
        let mut result = Vec::with_capacity(limit.min(1024));

        // Sort by key for deterministic ordering
        let mut sorted: Vec<_> = rows.into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (key, row) in sorted {
            // Filter by start_seq
            if let Some(seq) = start_seq {
                if key.seq() < seq {
                    continue;
                }
            }

            // TTL filtering: skip expired rows
            if let Some(ttl) = ttl_ms {
                let row_ts = key.seq().timestamp_millis();
                if row_ts + ttl <= now_ms {
                    continue; // Row has expired
                }
            }

            result.push((key, row));

            // Early termination on limit
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    /// Async version of scan_user_streaming to avoid blocking the async runtime.
    ///
    /// Uses `spawn_blocking` internally.
    pub async fn scan_user_streaming_async(
        &self,
        user_id: &UserId,
        start_seq: Option<SeqId>,
        limit: usize,
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        let store = self.clone();
        let user_id = user_id.clone();
        tokio::task::spawn_blocking(move || {
            store.scan_user_streaming(&user_id, start_seq, limit, ttl_ms, now_ms)
        })
        .await
        .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
    }

    /// Scan row keys for a single user, starting at an optional sequence ID (inclusive).
    pub fn scan_user_keys(
        &self,
        user_id: &UserId,
        start_seq: Option<SeqId>,
        limit: usize,
    ) -> Result<Vec<StreamTableRowId>> {
        let rows = self.scan_user(user_id, start_seq, limit)?;
        Ok(rows.into_iter().map(|(key, _)| key).collect())
    }

    /// Scan keys relative to a starting key in a specified direction.
    ///
    /// Note: This operation scans logs across users and is intended for maintenance jobs.
    pub fn scan_keys_directional(
        &self,
        _start_key: Option<&StreamTableRowId>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<StreamTableRowId>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut rows = self.scan_all(Some(limit))?;
        rows.sort_by(|(a, _), (b, _)| match direction {
            ScanDirection::Newer => a.cmp(b),
            ScanDirection::Older => b.cmp(a),
        });

        Ok(rows.into_iter().map(|(k, _)| k).collect())
    }
}

/// Helper function to create a new stream table store (in-memory backed).
///
/// # Arguments
/// * `table_id` - Table identifier
/// * `config` - Stream log configuration
///
/// # Returns
/// A new StreamTableStore instance configured for the stream table
pub fn new_stream_table_store(
    table_id: &TableId,
    config: StreamTableStoreConfig,
) -> StreamTableStore {
    let partition_name = partition_name(
        kalamdb_commons::constants::ColumnFamilyNames::STREAM_TABLE_PREFIX,
        table_id,
    );
    StreamTableStore::new(table_id.clone(), partition_name, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::models::{rows::Row, NamespaceId, TableName};
    use kalamdb_sharding::ShardRouter;
    use std::collections::BTreeMap;

    fn create_test_store(_base_dir: &std::path::Path) -> StreamTableStore {
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_stream"));
        let config = StreamTableStoreConfig {
            base_dir: _base_dir.join("streams").join("test_ns").join("test_stream"),
            max_rows_per_user: MemoryStreamLogStore::DEFAULT_MAX_ROWS_PER_USER,
            shard_router: ShardRouter::default_config(),
            ttl_seconds: Some(3600),
            storage_mode: StreamTableStorageMode::File,
        };
        new_stream_table_store(&table_id, config)
    }

    fn create_test_row(user_id: &UserId, seq: i64) -> StreamTableRow {
        let mut values = BTreeMap::new();
        values.insert("event".to_string(), ScalarValue::Utf8(Some("click".to_string())));
        values.insert("data".to_string(), ScalarValue::Int64(Some(123)));
        StreamTableRow {
            user_id: user_id.clone(),
            _seq: SeqId::new(seq),
            fields: Row::new(values),
        }
    }

    #[test]
    fn test_stream_table_store_create() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = create_test_store(temp_dir.path());
        assert!(store.partition().name().contains("stream_"));
    }

    #[test]
    fn test_stream_table_store_put_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = create_test_store(temp_dir.path());
        let key = StreamTableRowId::new(UserId::new("user1"), SeqId::new(100));
        let row = create_test_row(&UserId::new("user1"), 100);

        store.put(&key, &row).unwrap();
        let retrieved = store.get(&key).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), row);
    }

    #[test]
    fn test_stream_table_store_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = create_test_store(temp_dir.path());
        let key = StreamTableRowId::new(UserId::new("user1"), SeqId::new(200));
        let row = create_test_row(&UserId::new("user1"), 200);

        store.put(&key, &row).unwrap();
        store.delete(&key).unwrap();
        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn test_stream_table_store_scan_all() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = create_test_store(temp_dir.path());

        for user_i in 1..=2 {
            for seq_i in 1..=3 {
                let key = StreamTableRowId::new(
                    UserId::new(format!("user{}", user_i)),
                    SeqId::new((user_i * 1000 + seq_i) as i64),
                );
                let row = create_test_row(
                    &UserId::new(&format!("user{}", user_i)),
                    (user_i * 1000 + seq_i) as i64,
                );
                store.put(&key, &row).unwrap();
            }
        }

        let all_rows = store.scan_all(None).unwrap();
        assert_eq!(all_rows.len(), 6);
    }

    #[test]
    fn test_scan_user_streaming_respects_limit_and_start_seq() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = create_test_store(temp_dir.path());
        let user_id = UserId::new("user1");

        for i in 0..10 {
            let key = StreamTableRowId::new(user_id.clone(), SeqId::new(100 + i));
            let row = create_test_row(&user_id, 100 + i);
            store.put(&key, &row).unwrap();
        }

        let start_seq = SeqId::new(105);
        let results = store.scan_user_streaming(&user_id, Some(start_seq), 3, None, 0).unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|(key, _)| key.seq() >= start_seq));
    }

    #[test]
    fn test_stream_table_store_persists_rows_across_reopen() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_stream"));
        let config = StreamTableStoreConfig {
            base_dir: temp_dir.path().join("streams").join("test_ns").join("test_stream"),
            max_rows_per_user: MemoryStreamLogStore::DEFAULT_MAX_ROWS_PER_USER,
            shard_router: ShardRouter::default_config(),
            ttl_seconds: Some(3600),
            storage_mode: StreamTableStorageMode::File,
        };

        let key = StreamTableRowId::new(UserId::new("user1"), SeqId::new(100));
        let row = create_test_row(&UserId::new("user1"), 100);

        let store = new_stream_table_store(&table_id, config.clone());
        store.put(&key, &row).unwrap();
        drop(store);

        let reopened = new_stream_table_store(&table_id, config);
        let retrieved = reopened.get(&key).unwrap();
        assert_eq!(retrieved, Some(row));
    }
}
