//! Row ID types for MVCC architecture
//!
//! This module provides composite row ID types for different table types:
//! - UserTableRowId: Composite key with user_id and _seq for user-scoped tables
//! - SharedTableRowId: Alias to SeqId for shared tables (no user scoping)

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::{
    ids::SeqId,
    models::UserId,
    storage_key::{decode_key, encode_key, encode_prefix},
    StorageKey,
};

/// Composite key for user table rows: {user_id}:{_seq}
///
/// **MVCC Architecture**: Similar to TableId pattern, this is a composite struct
/// with two fields that implements StorageKey trait for RocksDB storage.
///
/// **Storage Format**: Uses `storekey` order-preserving encoding as tuple (user_id, seq).
/// This ensures proper lexicographic ordering: all rows for "alice" come before "bob"
/// regardless of user_id length.
///
/// # Examples
///
/// ```ignore
/// use kalamdb_commons::ids::{UserTableRowId, SeqId};
/// use kalamdb_commons::models::UserId;
///
/// let user_id = UserId::new("alice");
/// let seq = SeqId::new(12345);
/// let row_id = UserTableRowId::new(user_id, seq);
///
/// // Serialize to storage key
/// let key_bytes = row_id.storage_key();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserTableRowId {
    pub user_id: UserId,
    pub seq: SeqId,
}

impl UserTableRowId {
    /// Create a new user table row ID
    pub fn new(user_id: UserId, seq: SeqId) -> Self {
        Self { user_id, seq }
    }

    /// Get the user_id component
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Get the sequence ID component
    pub fn seq(&self) -> SeqId {
        self.seq
    }

    /// Prefix for scanning all rows for a user.
    pub fn user_prefix(user_id: &UserId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(),))
    }
}

impl StorageKey for UserTableRowId {
    fn storage_key(&self) -> Vec<u8> {
        // Use storekey's order-preserving encoding for (user_id, seq) tuple
        encode_key(&(self.user_id.as_str(), self.seq.as_i64()))
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let (user_id_str, seq_val): (String, i64) = decode_key(bytes)?;
        Ok(Self::new(UserId::new(user_id_str), SeqId::new(seq_val)))
    }
}

/// Type alias for shared table row ID (just SeqId, no user scoping)
///
/// **MVCC Architecture**: Shared tables use SeqId directly as the storage key
/// since they are accessible across all users.
pub type SharedTableRowId = SeqId;

/// Composite key for stream table rows: {user_id}:{_seq}
///
/// **MVCC Architecture (Phase 13.2)**: Stream tables now use the same MVCC
/// architecture as user tables with composite keys for user isolation.
///
/// **Storage Format**: Uses `storekey` order-preserving encoding as tuple (user_id, seq).
/// This ensures proper lexicographic ordering: all rows for "alice" come before "bob"
/// regardless of user_id length.
///
/// # Examples
///
/// ```ignore
/// use kalamdb_commons::ids::{StreamTableRowId, SeqId};
/// use kalamdb_commons::models::UserId;
///
/// let user_id = UserId::new("alice");
/// let seq = SeqId::new(12345);
/// let row_id = StreamTableRowId::new(user_id, seq);
///
/// // Serialize to storage key
/// let key_bytes = row_id.storage_key();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamTableRowId {
    pub user_id: UserId,
    pub seq: SeqId,
}

impl StreamTableRowId {
    /// Create a new stream table row ID
    pub fn new(user_id: UserId, seq: SeqId) -> Self {
        Self { user_id, seq }
    }

    /// Get the user_id component
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Get the sequence ID component
    pub fn seq(&self) -> SeqId {
        self.seq
    }

    /// Prefix for scanning all rows for a user.
    pub fn user_prefix(user_id: &UserId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(),))
    }
}

impl Ord for StreamTableRowId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_id.as_str().cmp(other.user_id.as_str()) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            other => other,
        }
    }
}

impl PartialOrd for StreamTableRowId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl StorageKey for StreamTableRowId {
    fn storage_key(&self) -> Vec<u8> {
        // Use storekey's order-preserving encoding for (user_id, seq) tuple
        encode_key(&(self.user_id.as_str(), self.seq.as_i64()))
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let (user_id_str, seq_val): (String, i64) = decode_key(bytes)?;
        Ok(Self::new(UserId::new(user_id_str), SeqId::new(seq_val)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_table_row_id_storage_key_round_trip() {
        let user_id = UserId::new("user1");
        let seq = SeqId::new(12345);
        let row_id = UserTableRowId::new(user_id.clone(), seq);

        // Serialize and deserialize using StorageKey trait
        let key_bytes = row_id.storage_key();
        let parsed = UserTableRowId::from_storage_key(&key_bytes).unwrap();

        assert_eq!(parsed.user_id(), &user_id);
        assert_eq!(parsed.seq(), seq);
    }

    #[test]
    fn test_user_table_row_id_ordering() {
        // Critical test: verify lexicographic ordering is correct
        let alice = UserTableRowId::new(UserId::new("alice"), SeqId::new(100));
        let bob = UserTableRowId::new(UserId::new("bob"), SeqId::new(50));

        // "alice" < "bob" lexicographically, so alice's key should sort first
        assert!(alice.storage_key() < bob.storage_key(), "alice should sort before bob");

        // Same user, different seq: should sort by seq
        let alice_100 = UserTableRowId::new(UserId::new("alice"), SeqId::new(100));
        let alice_200 = UserTableRowId::new(UserId::new("alice"), SeqId::new(200));
        assert!(
            alice_100.storage_key() < alice_200.storage_key(),
            "alice:100 should sort before alice:200"
        );
    }

    #[test]
    fn test_variable_length_user_id_ordering() {
        // This tests the bug that was fixed: "bob" (3 chars) vs "alice" (5 chars)
        // Previously, bob would sort before alice due to length-first encoding
        let bob = UserTableRowId::new(UserId::new("bob"), SeqId::new(100));
        let alice = UserTableRowId::new(UserId::new("alice"), SeqId::new(100));

        // "alice" < "bob" lexicographically
        assert!(
            alice.storage_key() < bob.storage_key(),
            "alice should sort before bob despite shorter length"
        );
    }

    #[test]
    fn test_shared_table_row_id_alias() {
        // SharedTableRowId is just SeqId
        let seq: SharedTableRowId = SeqId::new(12345);
        assert_eq!(seq.as_i64(), 12345);
    }

    #[test]
    fn test_stream_table_row_id_round_trip() {
        let user_id = UserId::new("bob");
        let seq = SeqId::new(88888);
        let row_id = StreamTableRowId::new(user_id.clone(), seq);

        // Serialize and deserialize using StorageKey trait
        let bytes = row_id.storage_key();
        let parsed = StreamTableRowId::from_storage_key(&bytes).unwrap();

        assert_eq!(parsed.user_id, user_id);
        assert_eq!(parsed.seq, seq);
    }

    #[test]
    fn test_stream_table_row_id_ordering() {
        // Same ordering tests for StreamTableRowId
        let alice = StreamTableRowId::new(UserId::new("alice"), SeqId::new(100));
        let bob = StreamTableRowId::new(UserId::new("bob"), SeqId::new(50));

        assert!(alice.storage_key() < bob.storage_key(), "alice should sort before bob");
    }
}
