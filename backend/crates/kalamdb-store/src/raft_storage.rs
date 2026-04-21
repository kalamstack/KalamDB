//! Raft log and metadata persistence via StorageBackend.
//!
//! This module provides durable storage for Raft consensus state using the
//! generic `StorageBackend` abstraction. All Raft groups share a single partition
//! (`raft_data`) with keys prefixed by group ID to avoid Column Family explosion.
//!
//! ## Key Layout
//!
//! ```text
//! [GroupPrefix]:[KeyType]:[Suffix]
//!
//! Group Prefixes:
//!   - Meta:             "meta"
//!   - DataUserShard(N): "u:{N:05}"
//!   - DataSharedShard(N): "s:{N:05}"
//!
//! Key Types:
//!   - log:{index:020}  → RaftLogEntry (KSerializable binary payload)
//!   - meta:vote        → Vote<u64> (KSerializable binary payload)
//!   - meta:commit      → Option<LogId<u64>> (KSerializable binary payload)
//!   - meta:purge       → Option<LogId<u64>> (KSerializable binary payload)
//!   - snap:meta        → SnapshotMeta (KSerializable binary payload)
//!   - snap:data        → Snapshot file path (string) or inline bytes if small
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use kalamdb_store::raft_storage::{RaftPartitionStore, GroupId};
//! use kalamdb_store::StorageBackend;
//! use std::sync::Arc;
//!
//! let backend: Arc<dyn StorageBackend> = /* ... */;
//! let store = RaftPartitionStore::new(backend, GroupId::Meta);
//!
//! // Append log entries
//! store.append_logs(&entries)?;
//!
//! // Save vote
//! store.save_vote(&vote)?;
//! ```

use crate::storage_trait::{Operation, Partition, Result, StorageBackend};
use kalamdb_commons::KSerializable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Re-export GroupId from kalamdb-sharding
pub use kalamdb_sharding::GroupId;

/// The single partition name for all Raft data.
pub const RAFT_PARTITION_NAME: &str = "raft_data";

// ============================================================================
// Types
// ============================================================================

/// A single Raft log entry for persistent storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    /// Log index (1-based, monotonically increasing)
    pub index: u64,
    /// Raft term when this entry was created
    pub term: u64,
    /// Serialized entry payload (command bytes)
    pub payload: Vec<u8>,
}

impl KSerializable for RaftLogEntry {}

/// Raft vote state for leader election.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RaftVote {
    /// The term in which the vote was cast
    pub term: u64,
    /// Node ID that received the vote (None if no vote cast)
    pub voted_for: Option<u64>,
    /// Whether this node is the leader for this term
    pub committed: bool,
}

impl KSerializable for RaftVote {}

/// Raft log ID (term + index pair).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLogId {
    /// Leader term
    pub term: u64,
    /// Log index
    pub index: u64,
}

impl KSerializable for RaftLogId {}

/// Snapshot metadata (stored separately from snapshot data).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSnapshotMeta {
    /// Last log ID included in the snapshot
    pub last_log_id: Option<RaftLogId>,
    /// Snapshot ID (unique identifier)
    pub snapshot_id: String,
    /// Size of the snapshot data in bytes
    pub size_bytes: u64,
}

impl KSerializable for RaftSnapshotMeta {}

/// Reference to snapshot data (file path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftSnapshotData {
    /// Snapshot data stored in external file (path)
    FilePath(String),
}

impl KSerializable for RaftSnapshotData {}

// ============================================================================
// RaftPartitionStore
// ============================================================================

/// Persistent storage for a single Raft group's log and metadata.
///
/// All operations are synchronous and use the `StorageBackend` trait.
/// Thread-safe via internal `Arc<dyn StorageBackend>`.
pub struct RaftPartitionStore {
    backend: Arc<dyn StorageBackend>,
    group_id: GroupId,
    partition: Partition,
}

impl RaftPartitionStore {
    /// Creates a new store for the given Raft group.
    ///
    /// The partition `raft_data` must already exist in the backend.
    pub fn new(backend: Arc<dyn StorageBackend>, group_id: GroupId) -> Self {
        Self {
            backend,
            group_id,
            partition: Partition::new(RAFT_PARTITION_NAME),
        }
    }

    /// Returns the group ID this store is for.
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    // ------------------------------------------------------------------------
    // Key construction helpers
    // ------------------------------------------------------------------------

    fn make_key(&self, suffix: &str) -> Vec<u8> {
        format!("{}:{}", self.group_id.key_prefix(), suffix).into_bytes()
    }

    fn log_key(&self, index: u64) -> Vec<u8> {
        self.make_key(&format!("log:{:020}", index))
    }

    fn vote_key(&self) -> Vec<u8> {
        self.make_key("meta:vote")
    }

    fn commit_key(&self) -> Vec<u8> {
        self.make_key("meta:commit")
    }

    fn purge_key(&self) -> Vec<u8> {
        self.make_key("meta:purge")
    }

    fn snap_meta_key(&self) -> Vec<u8> {
        self.make_key("snap:meta")
    }

    fn snap_data_key(&self) -> Vec<u8> {
        self.make_key("snap:data")
    }

    fn last_applied_key(&self) -> Vec<u8> {
        self.make_key("meta:last_applied")
    }

    fn last_membership_key(&self) -> Vec<u8> {
        self.make_key("meta:membership")
    }

    // ------------------------------------------------------------------------
    // Log operations
    // ------------------------------------------------------------------------

    /// Appends log entries to storage.
    ///
    /// Entries must have consecutive indices. Uses batch write for atomicity.
    pub fn append_logs(&self, entries: &[RaftLogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let ops: Vec<Operation> = entries
            .iter()
            .map(|entry| {
                let key = self.log_key(entry.index);
                let value = entry.encode()?;
                Ok(Operation::Put {
                    partition: self.partition.clone(),
                    key,
                    value,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.backend.batch(ops)
    }

    /// Retrieves a single log entry by index.
    pub fn get_log(&self, index: u64) -> Result<Option<RaftLogEntry>> {
        let key = self.log_key(index);
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftLogEntry::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Scans log entries in the range [start, end).
    pub fn scan_logs(&self, start: u64, end: u64) -> Result<Vec<RaftLogEntry>> {
        if start >= end {
            return Ok(Vec::new());
        }

        let prefix = self.make_key("log:");
        let start_key = self.log_key(start);

        let iter = self.backend.scan(&self.partition, Some(&prefix), Some(&start_key), None)?;

        let mut entries = Vec::with_capacity((end - start) as usize);
        for (key, value) in iter {
            // Parse index and check if < end
            if let Some(index) = Self::parse_log_index_from_key(&key) {
                if index >= end {
                    break; // Keys are sorted, so we can stop
                }
                entries.push(RaftLogEntry::decode(&value)?);
            }
        }

        Ok(entries)
    }

    /// Deletes all log entries with index < `before_index`.
    ///
    /// Used for log compaction after a snapshot is taken.
    pub fn delete_logs_before(&self, before_index: u64) -> Result<()> {
        if before_index == 0 {
            return Ok(());
        }

        // Scan all log keys with log prefix
        let prefix = self.make_key("log:");

        let iter = self.backend.scan(&self.partition, Some(&prefix), None, None)?;

        let ops: Vec<Operation> = iter
            .filter_map(|(key, _)| {
                // Parse index and check if < before_index
                if let Some(index) = Self::parse_log_index_from_key(&key) {
                    if index < before_index {
                        return Some(Operation::Delete {
                            partition: self.partition.clone(),
                            key,
                        });
                    }
                }
                None
            })
            .collect();

        if !ops.is_empty() {
            self.backend.batch(ops)?;
        }

        Ok(())
    }

    /// Returns the last (highest) log index, or None if log is empty.
    pub fn last_log_index(&self) -> Result<Option<u64>> {
        // Scan all log entries and find the max index
        let prefix = self.make_key("log:");

        let iter = self.backend.scan(&self.partition, Some(&prefix), None, None)?;

        // Get the last entry (keys are sorted, so last is highest)
        let mut last_index: Option<u64> = None;
        for (key, _) in iter {
            if let Some(index) = Self::parse_log_index_from_key(&key) {
                last_index = Some(index);
            }
        }

        Ok(last_index)
    }

    fn parse_log_index_from_key(key: &[u8]) -> Option<u64> {
        let key_str = std::str::from_utf8(key).ok()?;
        // Key format: "{prefix}:log:{index:020}"
        let mut parts = key_str.rsplitn(3, ':');
        let index = parts.next()?;
        let marker = parts.next()?;
        if marker == "log" {
            index.parse().ok()
        } else {
            None
        }
    }

    // ------------------------------------------------------------------------
    // Vote operations
    // ------------------------------------------------------------------------

    /// Saves the current vote state.
    pub fn save_vote(&self, vote: &RaftVote) -> Result<()> {
        let key = self.vote_key();
        let value = vote.encode()?;
        self.backend.put(&self.partition, &key, &value)
    }

    /// Reads the current vote state.
    pub fn read_vote(&self) -> Result<Option<RaftVote>> {
        let key = self.vote_key();
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftVote::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    // ------------------------------------------------------------------------
    // Commit/Purge operations
    // ------------------------------------------------------------------------

    /// Saves the committed log ID.
    pub fn save_commit(&self, commit: Option<RaftLogId>) -> Result<()> {
        let key = self.commit_key();
        match commit {
            Some(log_id) => {
                let value = log_id.encode()?;
                self.backend.put(&self.partition, &key, &value)
            },
            None => self.backend.delete(&self.partition, &key),
        }
    }

    /// Reads the committed log ID.
    pub fn read_commit(&self) -> Result<Option<RaftLogId>> {
        let key = self.commit_key();
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftLogId::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Saves the last purged log ID.
    pub fn save_purge(&self, purge: Option<RaftLogId>) -> Result<()> {
        let key = self.purge_key();
        match purge {
            Some(log_id) => {
                let value = log_id.encode()?;
                self.backend.put(&self.partition, &key, &value)
            },
            None => self.backend.delete(&self.partition, &key),
        }
    }

    /// Reads the last purged log ID.
    pub fn read_purge(&self) -> Result<Option<RaftLogId>> {
        let key = self.purge_key();
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftLogId::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    // ------------------------------------------------------------------------
    // State Machine State operations
    // ------------------------------------------------------------------------

    /// Saves the last applied log ID.
    ///
    /// This is critical for crash recovery - OpenRaft uses this to determine
    /// which log entries have already been applied to the state machine and
    /// should not be replayed.
    pub fn save_last_applied(&self, last_applied: Option<RaftLogId>) -> Result<()> {
        let key = self.last_applied_key();
        match last_applied {
            Some(log_id) => {
                let value = log_id.encode()?;
                self.backend.put(&self.partition, &key, &value)
            },
            None => self.backend.delete(&self.partition, &key),
        }
    }

    /// Reads the last applied log ID.
    pub fn read_last_applied(&self) -> Result<Option<RaftLogId>> {
        let key = self.last_applied_key();
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftLogId::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Saves the last membership configuration.
    ///
    /// Membership changes are persisted separately from snapshots to ensure
    /// consistent cluster configuration across restarts.
    pub fn save_last_membership(&self, membership: &[u8]) -> Result<()> {
        let key = self.last_membership_key();
        self.backend.put(&self.partition, &key, membership)
    }

    /// Reads the last membership configuration.
    pub fn read_last_membership(&self) -> Result<Option<Vec<u8>>> {
        let key = self.last_membership_key();
        self.backend.get(&self.partition, &key)
    }

    // ------------------------------------------------------------------------
    // Snapshot operations
    // ------------------------------------------------------------------------

    /// Saves snapshot metadata.
    pub fn save_snapshot_meta(&self, meta: &RaftSnapshotMeta) -> Result<()> {
        let key = self.snap_meta_key();
        let value = meta.encode()?;
        self.backend.put(&self.partition, &key, &value)
    }

    /// Reads snapshot metadata.
    pub fn read_snapshot_meta(&self) -> Result<Option<RaftSnapshotMeta>> {
        let key = self.snap_meta_key();
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftSnapshotMeta::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Saves snapshot data reference (inline or file path).
    pub fn save_snapshot_data(&self, data: &RaftSnapshotData) -> Result<()> {
        let key = self.snap_data_key();
        let value = data.encode()?;
        self.backend.put(&self.partition, &key, &value)
    }

    /// Reads snapshot data reference.
    pub fn read_snapshot_data(&self) -> Result<Option<RaftSnapshotData>> {
        let key = self.snap_data_key();
        match self.backend.get(&self.partition, &key)? {
            Some(bytes) => Ok(Some(RaftSnapshotData::decode(&bytes)?)),
            None => Ok(None),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::InMemoryBackend;

    fn create_test_store(group_id: GroupId) -> RaftPartitionStore {
        let backend = Arc::new(InMemoryBackend::new());
        // Create the raft_data partition
        backend.create_partition(&Partition::new(RAFT_PARTITION_NAME)).unwrap();
        RaftPartitionStore::new(backend, group_id)
    }

    #[test]
    fn test_append_and_get_log() {
        let store = create_test_store(GroupId::Meta);

        let entries = vec![
            RaftLogEntry {
                index: 1,
                term: 1,
                payload: b"cmd1".to_vec(),
            },
            RaftLogEntry {
                index: 2,
                term: 1,
                payload: b"cmd2".to_vec(),
            },
            RaftLogEntry {
                index: 3,
                term: 2,
                payload: b"cmd3".to_vec(),
            },
        ];

        store.append_logs(&entries).unwrap();

        // Get individual entries
        let entry1 = store.get_log(1).unwrap().unwrap();
        assert_eq!(entry1.index, 1);
        assert_eq!(entry1.term, 1);
        assert_eq!(entry1.payload, b"cmd1");

        let entry3 = store.get_log(3).unwrap().unwrap();
        assert_eq!(entry3.term, 2);

        // Non-existent entry
        assert!(store.get_log(99).unwrap().is_none());
    }

    #[test]
    fn test_scan_logs() {
        let store = create_test_store(GroupId::Meta);

        let entries: Vec<RaftLogEntry> = (1..=10)
            .map(|i| RaftLogEntry {
                index: i,
                term: 1,
                payload: format!("cmd{}", i).into_bytes(),
            })
            .collect();

        store.append_logs(&entries).unwrap();

        // Scan range [3, 7)
        let scanned = store.scan_logs(3, 7).unwrap();
        assert_eq!(scanned.len(), 4);
        assert_eq!(scanned[0].index, 3);
        assert_eq!(scanned[3].index, 6);

        // Empty range
        assert!(store.scan_logs(5, 5).unwrap().is_empty());
        assert!(store.scan_logs(10, 5).unwrap().is_empty());
    }

    #[test]
    fn test_first_last_log_index() {
        let store = create_test_store(GroupId::Meta);

        // Empty log
        assert!(store.last_log_index().unwrap().is_none());

        // Add some entries
        let entries = vec![
            RaftLogEntry {
                index: 5,
                term: 1,
                payload: vec![],
            },
            RaftLogEntry {
                index: 6,
                term: 1,
                payload: vec![],
            },
            RaftLogEntry {
                index: 7,
                term: 1,
                payload: vec![],
            },
        ];
        store.append_logs(&entries).unwrap();

        let scanned = store.scan_logs(5, 6).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].index, 5);
        assert_eq!(store.last_log_index().unwrap(), Some(7));
    }

    #[test]
    fn test_delete_logs_before() {
        let store = create_test_store(GroupId::DataUserShard(5));

        let entries: Vec<RaftLogEntry> = (1..=10)
            .map(|i| RaftLogEntry {
                index: i,
                term: 1,
                payload: vec![],
            })
            .collect();

        store.append_logs(&entries).unwrap();

        // Delete logs before index 5
        store.delete_logs_before(5).unwrap();

        // Entries 1-4 should be gone
        for i in 1..5 {
            assert!(store.get_log(i).unwrap().is_none());
        }

        // Entries 5-10 should still exist
        for i in 5..=10 {
            assert!(store.get_log(i).unwrap().is_some());
        }

        let scanned = store.scan_logs(5, 6).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].index, 5);
    }

    #[test]
    fn test_vote_operations() {
        let store = create_test_store(GroupId::Meta);

        // No vote initially
        assert!(store.read_vote().unwrap().is_none());

        // Save a vote
        let vote = RaftVote {
            term: 5,
            voted_for: Some(2),
            committed: false,
        };
        store.save_vote(&vote).unwrap();

        let read_vote = store.read_vote().unwrap().unwrap();
        assert_eq!(read_vote.term, 5);
        assert_eq!(read_vote.voted_for, Some(2));
        assert!(!read_vote.committed);

        // Update vote
        let vote2 = RaftVote {
            term: 6,
            voted_for: Some(3),
            committed: true,
        };
        store.save_vote(&vote2).unwrap();

        let read_vote2 = store.read_vote().unwrap().unwrap();
        assert_eq!(read_vote2.term, 6);
        assert!(read_vote2.committed);
    }

    #[test]
    fn test_commit_purge_operations() {
        let store = create_test_store(GroupId::DataSharedShard(0));

        // No commit/purge initially
        assert!(store.read_commit().unwrap().is_none());
        assert!(store.read_purge().unwrap().is_none());

        // Save commit
        let commit = RaftLogId { term: 3, index: 42 };
        store.save_commit(Some(commit)).unwrap();

        let read_commit = store.read_commit().unwrap().unwrap();
        assert_eq!(read_commit.term, 3);
        assert_eq!(read_commit.index, 42);

        // Save purge
        let purge = RaftLogId { term: 2, index: 30 };
        store.save_purge(Some(purge)).unwrap();

        let read_purge = store.read_purge().unwrap().unwrap();
        assert_eq!(read_purge.index, 30);

        // Clear commit
        store.save_commit(None).unwrap();
        assert!(store.read_commit().unwrap().is_none());
    }

    #[test]
    fn test_snapshot_operations() {
        let store = create_test_store(GroupId::Meta);

        // No snapshot initially
        assert!(store.read_snapshot_meta().unwrap().is_none());
        assert!(store.read_snapshot_data().unwrap().is_none());

        // Save snapshot meta
        let meta = RaftSnapshotMeta {
            last_log_id: Some(RaftLogId {
                term: 5,
                index: 100,
            }),
            snapshot_id: "snap-001".to_string(),
            size_bytes: 1024,
        };
        store.save_snapshot_meta(&meta).unwrap();

        let read_meta = store.read_snapshot_meta().unwrap().unwrap();
        assert_eq!(read_meta.snapshot_id, "snap-001");
        assert_eq!(read_meta.size_bytes, 1024);

        // Save file path snapshot data
        let data_path = RaftSnapshotData::FilePath("/var/data/snap-001.bin".to_string());
        store.save_snapshot_data(&data_path).unwrap();

        let read_data = store.read_snapshot_data().unwrap().unwrap();
        match read_data {
            RaftSnapshotData::FilePath(path) => assert_eq!(path, "/var/data/snap-001.bin"),
        }
    }

    #[test]
    fn test_different_groups_isolated() {
        let backend = Arc::new(InMemoryBackend::new());
        backend.create_partition(&Partition::new(RAFT_PARTITION_NAME)).unwrap();

        let store1 = RaftPartitionStore::new(backend.clone(), GroupId::Meta);
        let store2 = RaftPartitionStore::new(backend.clone(), GroupId::DataUserShard(0));

        // Add entries to store1
        store1
            .append_logs(&[RaftLogEntry {
                index: 1,
                term: 1,
                payload: b"sys".to_vec(),
            }])
            .unwrap();

        // Add entries to store2 (different group)
        store2
            .append_logs(&[RaftLogEntry {
                index: 1,
                term: 2,
                payload: b"users".to_vec(),
            }])
            .unwrap();

        // Each store should see only its own entries
        let entry1 = store1.get_log(1).unwrap().unwrap();
        assert_eq!(entry1.term, 1);
        assert_eq!(entry1.payload, b"sys");

        let entry2 = store2.get_log(1).unwrap().unwrap();
        assert_eq!(entry2.term, 2);
        assert_eq!(entry2.payload, b"users");
    }

    #[test]
    fn test_group_id_key_prefixes() {
        assert_eq!(GroupId::Meta.key_prefix(), "meta");
        assert_eq!(GroupId::DataUserShard(0).key_prefix(), "u:00000");
        assert_eq!(GroupId::DataUserShard(31).key_prefix(), "u:00031");
        assert_eq!(GroupId::DataSharedShard(0).key_prefix(), "s:00000");
    }
}
