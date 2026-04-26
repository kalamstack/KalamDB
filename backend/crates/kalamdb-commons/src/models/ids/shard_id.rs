//! Shard ID type for sharded table storage.
//!
//! Used in storage path templates for sharded data distribution.
//! Format: `shard_{number}` (e.g., `shard_0`, `shard_42`)

use std::fmt;

use serde::{Deserialize, Serialize};

/// Type-safe shard identifier.
///
/// Wraps a u32 shard number. When formatted for storage paths,
/// produces `shard_N` format (e.g., "shard_0", "shard_1").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(u32);

impl ShardId {
    /// Create a new ShardId from a shard number.
    #[inline]
    pub const fn new(shard_number: u32) -> Self {
        Self(shard_number)
    }

    /// Get the raw shard number.
    #[inline]
    pub const fn number(&self) -> u32 {
        self.0
    }

    /// Format as storage path segment (e.g., "shard_0").
    #[inline]
    pub fn as_path_segment(&self) -> String {
        format!("shard_{}", self.0)
    }
}

impl From<u32> for ShardId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<ShardId> for u32 {
    fn from(shard: ShardId) -> Self {
        shard.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard_{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_id_new() {
        let shard = ShardId::new(42);
        assert_eq!(shard.number(), 42);
    }

    #[test]
    fn test_shard_id_display() {
        let shard = ShardId::new(0);
        assert_eq!(shard.to_string(), "shard_0");

        let shard = ShardId::new(123);
        assert_eq!(shard.to_string(), "shard_123");
    }

    #[test]
    fn test_shard_id_path_segment() {
        let shard = ShardId::new(5);
        assert_eq!(shard.as_path_segment(), "shard_5");
    }

    #[test]
    fn test_shard_id_from_u32() {
        let shard: ShardId = 10.into();
        assert_eq!(shard.number(), 10);
    }

    #[test]
    fn test_shard_id_into_u32() {
        let shard = ShardId::new(99);
        let num: u32 = shard.into();
        assert_eq!(num, 99);
    }
}
