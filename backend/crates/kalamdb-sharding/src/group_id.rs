//! Raft Group ID definitions
//!
//! KalamDB uses Multi-Raft with 34 groups:
//! - 1 unified metadata group (Meta)
//! - 32 user data shards (user tables)
//! - 1 shared data shard (shared tables)

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Default number of user data shards
pub const DEFAULT_USER_SHARDS: u32 = 32;

/// Default number of shared data shards (future: increase for scale)
pub const DEFAULT_SHARED_SHARDS: u32 = 1;

/// Identifies a Raft group in the Multi-Raft architecture.
///
/// Each group has its own Raft instance with independent leader election.
///
/// ## Structure
///
/// - **Meta**: Unified metadata group (namespaces, tables, storages, users, jobs)
/// - **DataUserShard(0..31)**: User table data shards
/// - **DataSharedShard(0)**: Shared table data shard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GroupId {
    // === Unified Metadata Group (1) ===
    /// Unified metadata: namespaces, tables, storages, users, jobs
    Meta,

    // === Data Groups (33) ===
    /// User table data shard (0..31)
    /// Routes by: user_id % num_user_shards
    DataUserShard(u32),
    /// Shared table data shard (0 for Phase 1)
    /// Future: shard by table_id or row key
    DataSharedShard(u32),
}

impl GroupId {
    /// Total number of metadata groups (post-018: just 1)
    pub const METADATA_GROUP_COUNT: usize = 1;

    /// Returns true if this is a metadata group
    pub fn is_metadata(&self) -> bool {
        matches!(self, GroupId::Meta)
    }

    /// Returns true if this is a data group
    pub fn is_data(&self) -> bool {
        matches!(self, GroupId::DataUserShard(_) | GroupId::DataSharedShard(_))
    }

    /// Returns the partition prefix for RocksDB column family naming
    pub fn partition_prefix(&self) -> String {
        match self {
            GroupId::Meta => "raft_meta".to_string(),
            GroupId::DataUserShard(id) => format!("raft_data_user_{:02}", id),
            GroupId::DataSharedShard(id) => format!("raft_data_shared_{:02}", id),
        }
    }

    /// Returns the key prefix for storage.
    ///
    /// All keys in `raft_data` partition are prefixed with this string.
    pub fn key_prefix(&self) -> String {
        match self {
            GroupId::Meta => "meta".to_string(),
            GroupId::DataUserShard(n) => format!("u:{:05}", n),
            GroupId::DataSharedShard(n) => format!("s:{:05}", n),
        }
    }

    /// Returns a numeric ID for openraft (must be unique across all groups)
    pub fn as_u64(&self) -> u64 {
        match self {
            GroupId::Meta => 10, // New ID for unified Meta
            GroupId::DataUserShard(id) => 100 + (*id as u64),
            GroupId::DataSharedShard(id) => 200 + (*id as u64),
        }
    }

    /// Create from numeric ID
    pub fn from_u64(id: u64) -> Option<Self> {
        match id {
            10 => Some(GroupId::Meta),
            100..=131 => Some(GroupId::DataUserShard((id - 100) as u32)),
            200..=231 => Some(GroupId::DataSharedShard((id - 200) as u32)),
            _ => None,
        }
    }

    /// Returns the unified metadata group ID
    pub fn meta() -> GroupId {
        GroupId::Meta
    }

    /// Returns all metadata group IDs (post-018: just Meta)
    pub fn all_metadata() -> Vec<GroupId> {
        vec![GroupId::Meta]
    }

    /// Returns all user data shard IDs for given shard count
    pub fn all_user_shards(num_shards: u32) -> Vec<GroupId> {
        (0..num_shards).map(GroupId::DataUserShard).collect()
    }

    /// Returns all shared data shard IDs for given shard count
    pub fn all_shared_shards(num_shards: u32) -> Vec<GroupId> {
        (0..num_shards).map(GroupId::DataSharedShard).collect()
    }

    /// Returns all group IDs for given configuration (post-018)
    pub fn all_groups(num_user_shards: u32, num_shared_shards: u32) -> Vec<GroupId> {
        let mut groups = Self::all_metadata();
        groups.extend(Self::all_user_shards(num_user_shards));
        groups.extend(Self::all_shared_shards(num_shared_shards));
        groups
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupId::Meta => write!(f, "meta"),
            GroupId::DataUserShard(id) => write!(f, "data:user:{:02}", id),
            GroupId::DataSharedShard(id) => write!(f, "data:shared:{:02}", id),
        }
    }
}

impl FromStr for GroupId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "meta" => Ok(GroupId::Meta),
            _ if s.starts_with("data:user:") => {
                let id_str = s.strip_prefix("data:user:").unwrap();
                id_str
                    .parse::<u32>()
                    .map(GroupId::DataUserShard)
                    .map_err(|e| format!("Invalid user shard ID: {}", e))
            },
            _ if s.starts_with("data:shared:") => {
                let id_str = s.strip_prefix("data:shared:").unwrap();
                id_str
                    .parse::<u32>()
                    .map(GroupId::DataSharedShard)
                    .map_err(|e| format!("Invalid shared shard ID: {}", e))
            },
            _ => Err(format!("Unknown group ID format: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_id_display() {
        assert_eq!(GroupId::Meta.to_string(), "meta");
        assert_eq!(GroupId::DataUserShard(5).to_string(), "data:user:05");
        assert_eq!(GroupId::DataSharedShard(0).to_string(), "data:shared:00");
    }

    #[test]
    fn test_group_id_roundtrip() {
        for id in [10u64, 100, 115, 131, 200] {
            let group = GroupId::from_u64(id).unwrap();
            assert_eq!(group.as_u64(), id);
        }
    }

    #[test]
    fn test_all_groups_count() {
        let groups = GroupId::all_groups(32, 1);
        // 1 metadata (Meta) + 32 user + 1 shared = 34
        assert_eq!(groups.len(), 34);
    }

    #[test]
    fn test_meta_group() {
        assert!(GroupId::Meta.is_metadata());
        assert!(!GroupId::Meta.is_data());
    }

    #[test]
    fn test_partition_prefix() {
        assert_eq!(GroupId::Meta.partition_prefix(), "raft_meta");
        assert_eq!(GroupId::DataUserShard(5).partition_prefix(), "raft_data_user_05");
        assert_eq!(GroupId::DataSharedShard(0).partition_prefix(), "raft_data_shared_00");
    }

    #[test]
    fn test_key_prefix() {
        assert_eq!(GroupId::Meta.key_prefix(), "meta");
        assert_eq!(GroupId::DataUserShard(5).key_prefix(), "u:00005");
        assert_eq!(GroupId::DataSharedShard(0).key_prefix(), "s:00000");
    }
}
