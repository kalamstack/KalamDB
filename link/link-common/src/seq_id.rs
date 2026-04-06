//! Lightweight SeqId wrapper shared by the KalamDB SDK.
//!
//! Mirrors the backend's Snowflake-based sequence identifier so that
//! the client can serialize/deserialize `_seq` values without depending
//! on the heavy `kalamdb-commons` crate.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Sequence ID for MVCC versioning (Snowflake layout: timestamp | worker | seq)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct SeqId(i64);

impl SeqId {
    /// Custom epoch: 2024-01-01 00:00:00 UTC (matches backend)
    pub const EPOCH: u64 = 1704067200000;

    /// Create a new SeqId from the raw Snowflake ID
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    /// Create SeqId from raw i64 value
    pub fn from_i64(value: i64) -> Self {
        Self(value)
    }

    /// Raw i64 representation
    pub fn as_i64(&self) -> i64 {
        self.0
    }

    /// Extract timestamp in milliseconds since Unix epoch
    pub fn timestamp_millis(&self) -> u64 {
        let id = self.0 as u64;
        (id >> 22) + Self::EPOCH
    }

    /// Extract timestamp in whole seconds since Unix epoch
    pub fn timestamp_seconds(&self) -> u64 {
        self.timestamp_millis() / 1000
    }

    /// Compute age in seconds relative to `now_millis`
    pub fn age_seconds(&self, now_millis: u64) -> u64 {
        let ts = self.timestamp_millis();
        now_millis.saturating_sub(ts) / 1000
    }

    /// Convert to `SystemTime`
    pub fn timestamp(&self) -> SystemTime {
        let millis = self.timestamp_millis();
        UNIX_EPOCH + std::time::Duration::from_millis(millis)
    }

    /// Extract worker ID (0-1023)
    pub fn worker_id(&self) -> u16 {
        let id = self.0 as u64;
        ((id >> 12) & 0x3FF) as u16
    }

    /// Extract intra-millisecond sequence number (0-4095)
    pub fn sequence(&self) -> u16 {
        let id = self.0 as u64;
        (id & 0xFFF) as u16
    }

    /// Parse from decimal string
    pub fn from_string(s: &str) -> Result<Self, String> {
        s.parse::<i64>()
            .map(Self::new)
            .map_err(|e| format!("Failed to parse SeqId: {}", e))
    }

    /// Convert to big-endian bytes (preserves ordering)
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Parse from big-endian bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 8 {
            return Err(format!("Invalid byte length: expected 8, got {}", bytes.len()));
        }
        let mut array = [0u8; 8];
        array.copy_from_slice(bytes);
        Ok(Self::new(i64::from_be_bytes(array)))
    }
}

impl fmt::Display for SeqId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for SeqId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for SeqId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SeqIdVisitor;

        impl Visitor<'_> for SeqIdVisitor {
            type Value = SeqId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an i64 sequence ID or a decimal string")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SeqId::new(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = i64::try_from(value)
                    .map_err(|_| E::custom(format!("sequence ID {} exceeds i64 range", value)))?;
                Ok(SeqId::new(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SeqId::from_string(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(SeqIdVisitor)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let seq = SeqId::new(123456789);
        assert_eq!(seq.as_i64(), 123456789);
        assert_eq!(SeqId::from_i64(seq.as_i64()), seq);
    }

    #[test]
    fn test_timestamp_helpers() {
        let timestamp_offset = 5000u64; // 5 seconds after epoch
        let worker_id = 7u64;
        let sequence = 21u64;
        let id = (timestamp_offset << 22) | (worker_id << 12) | sequence;
        let seq = SeqId::new(id as i64);

        assert_eq!(seq.timestamp_millis(), SeqId::EPOCH + timestamp_offset);
        assert_eq!(seq.worker_id(), worker_id as u16);
        assert_eq!(seq.sequence(), sequence as u16);
    }

    #[test]
    fn test_bytes_round_trip() {
        let seq = SeqId::new(987654321);
        let bytes = seq.to_bytes();
        let parsed = SeqId::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, seq);
    }
}
