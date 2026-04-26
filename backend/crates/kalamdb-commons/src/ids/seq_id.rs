//! SeqId - Sequence ID based on Snowflake ID for MVCC versioning
//!
//! This module provides a wrapper around Snowflake IDs for use as sequence identifiers
//! in the MVCC architecture. Each SeqId represents a unique version of a row.

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{ids::SnowflakeGenerator, StorageKey};

/// Sequence ID for MVCC versioning
///
/// Internally uses Snowflake ID format (64 bits):
/// - 41 bits: timestamp in milliseconds since custom epoch
/// - 10 bits: machine/worker ID
/// - 12 bits: sequence number
///
/// **MVCC Architecture**: Used as `_seq` column for version tracking
/// Storage key format: `{user_id}:{_seq}` or just `{_seq}` for shared tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeqId(i64);

impl SeqId {
    /// Custom epoch: 2024-01-01 00:00:00 UTC (same as SnowflakeGenerator)
    pub const EPOCH: u64 = 1704067200000;

    /// Create a new SeqId from a Snowflake ID
    pub fn new(snowflake_id: i64) -> Self {
        Self(snowflake_id)
    }

    /// Create SeqId from raw i64 value
    pub fn from_i64(value: i64) -> Self {
        Self(value)
    }

    /// Get the raw i64 value
    pub fn as_i64(&self) -> i64 {
        self.0
    }

    /// Extract timestamp in milliseconds since Unix epoch
    ///
    /// This is useful for debugging, logging, and time-based queries.
    pub fn timestamp_millis(&self) -> u64 {
        let id = self.0 as u64;
        (id >> 22) + Self::EPOCH
    }

    /// Extract timestamp in whole seconds since Unix epoch
    pub fn timestamp_seconds(&self) -> u64 {
        self.timestamp_millis() / 1000
    }

    /// Compute how many whole seconds old this SeqId is relative to `now_millis`
    pub fn age_seconds(&self, now_millis: u64) -> u64 {
        let ts = self.timestamp_millis();
        now_millis.saturating_sub(ts) / 1000
    }

    /// Extract timestamp as SystemTime
    pub fn timestamp(&self) -> SystemTime {
        let millis = self.timestamp_millis();
        UNIX_EPOCH + std::time::Duration::from_millis(millis)
    }

    /// Extract worker ID (0-1023)
    pub fn worker_id(&self) -> u16 {
        let id = self.0 as u64;
        ((id >> 12) & 0x3FF) as u16
    }

    /// Extract sequence number (0-4095)
    pub fn sequence(&self) -> u16 {
        let id = self.0 as u64;
        (id & 0xFFF) as u16
    }

    // String conversion is provided by Display/ToString; no inherent method needed

    /// Parse from string representation
    pub fn from_string(s: &str) -> Result<Self, String> {
        s.parse::<i64>()
            .map(Self::new)
            .map_err(|e| format!("Failed to parse SeqId: {}", e))
    }

    /// Convert to bytes (big-endian for consistent ordering in RocksDB)
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Parse from bytes (big-endian)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 8 {
            return Err(format!("Invalid byte length: expected 8, got {}", bytes.len()));
        }
        let mut array = [0u8; 8];
        array.copy_from_slice(bytes);
        Ok(Self::new(i64::from_be_bytes(array)))
    }

    /// Return the maximum possible SeqId for the provided timestamp.
    ///
    /// This packs the timestamp together with the largest worker/sequence values
    /// so the returned SeqId encompasses every Snowflake generated at or before
    /// `timestamp_millis`.
    pub fn max_id_for_timestamp(timestamp_millis: u64) -> Result<Self, String> {
        // let normalized = timestamp_millis.max(Self::EPOCH);
        let id = SnowflakeGenerator::max_id_for_timestamp(timestamp_millis)?;
        Ok(Self::new(id))
    }
}

impl fmt::Display for SeqId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for SeqId {
    fn from(value: i64) -> Self {
        Self::new(value)
    }
}

impl From<SeqId> for i64 {
    fn from(seq_id: SeqId) -> Self {
        seq_id.0
    }
}

impl StorageKey for SeqId {
    fn storage_key(&self) -> Vec<u8> {
        self.to_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        Self::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_id_creation() {
        let seq_id = SeqId::new(123456789);
        assert_eq!(seq_id.as_i64(), 123456789);
    }

    #[test]
    fn test_seq_id_timestamp_extraction() {
        // Create a SeqId with known timestamp component
        let timestamp_offset = 1000u64; // 1000ms after epoch
        let worker_id = 5u64;
        let sequence = 42u64;

        let id = (timestamp_offset << 22) | (worker_id << 12) | sequence;
        let seq_id = SeqId::new(id as i64);

        assert_eq!(seq_id.timestamp_millis(), SeqId::EPOCH + timestamp_offset);
        assert_eq!(seq_id.worker_id(), 5);
        assert_eq!(seq_id.sequence(), 42);
    }

    #[test]
    fn test_seq_id_timestamp_seconds() {
        let timestamp_offset = 5000u64; // 5 seconds after epoch
        let id = (timestamp_offset << 22) as i64;
        let seq_id = SeqId::new(id);

        assert_eq!(seq_id.timestamp_seconds(), (SeqId::EPOCH + timestamp_offset) / 1000);
    }

    #[test]
    fn test_seq_id_age_seconds() {
        let timestamp_offset = 2000u64;
        let id = (timestamp_offset << 22) as i64;
        let seq_id = SeqId::new(id);
        let now_millis = SeqId::EPOCH + timestamp_offset + 7000; // 7s later

        assert_eq!(seq_id.age_seconds(now_millis), 7);
    }

    #[test]
    fn test_seq_id_string_conversion() {
        let seq_id = SeqId::new(987654321);
        let s = seq_id.to_string();
        assert_eq!(s, "987654321");

        let parsed = SeqId::from_string(&s).unwrap();
        assert_eq!(parsed, seq_id);
    }

    #[test]
    fn test_seq_id_bytes_conversion() {
        let seq_id = SeqId::new(123456789);
        let bytes = seq_id.to_bytes();
        let parsed = SeqId::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, seq_id);
    }

    #[test]
    fn test_seq_id_max_id_for_timestamp() {
        let ts = SeqId::EPOCH + 5000;
        let seq_id = SeqId::max_id_for_timestamp(ts).expect("seq id");
        assert!(seq_id.timestamp_millis() >= ts);
        assert!(seq_id >= SeqId::new(0));
    }

    #[test]
    fn test_seq_id_ordering() {
        let seq1 = SeqId::new(100);
        let seq2 = SeqId::new(200);
        let seq3 = SeqId::new(300);

        assert!(seq1 < seq2);
        assert!(seq2 < seq3);
        assert!(seq3 > seq1);
    }

    #[test]
    fn test_seq_id_from_i64() {
        let seq_id: SeqId = 42i64.into();
        assert_eq!(seq_id.as_i64(), 42);

        let value: i64 = seq_id.into();
        assert_eq!(value, 42);
    }
}
