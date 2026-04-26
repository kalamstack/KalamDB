//! Storage key trait for type-safe key serialization with lexicographic ordering
//!
//! This module uses the `storekey` crate from SurrealDB to ensure proper
//! lexicographic ordering of serialized keys in RocksDB.
//!
//! # Why storekey?
//!
//! RocksDB stores keys in lexicographic (byte-by-byte) order. Naive encoding
//! strategies like `{len:1byte}{string_bytes}` break ordering:
//!
//! - "bob" → [3, b, o, b] sorts BEFORE "alice" → [5, a, l, i, c, e] because 3 < 5, even though
//!   "alice" < "bob" lexicographically
//!
//! The `storekey` crate uses escape-sequence encoding that preserves the
//! natural lexicographic order of strings and tuples.
//!
//! # Design Rationale
//!
//! Previously, SystemTableStore relied on `AsRef<[u8]>` for key serialization.
//! This caused bugs with composite keys (e.g., TableId, UserRowId) where
//! `AsRef<[u8]>` returned only the first component instead of the full
//! composite key (e.g., `b"namespace"` instead of `b"namespace:table"`).
//!
//! The StorageKey trait provides an explicit contract for storage serialization,
//! separate from AsRef which may be used for other purposes.
//!
//! # Usage for Composite Keys
//!
//! For composite keys, use the `encode_composite!` macro or the helper functions:
//!
//! ```rust,ignore
//! use kalamdb_commons::{StorageKey, encode_key, decode_key};
//!
//! impl StorageKey for UserTableRowId {
//!     fn storage_key(&self) -> Vec<u8> {
//!         encode_key(&(self.user_id.as_str(), self.seq.as_i64()))
//!     }
//!
//!     fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
//!         let (user_id_str, seq_val): (String, i64) = decode_key(bytes)?;
//!         Ok(Self::new(UserId::new(user_id_str), SeqId::new(seq_val)))
//!     }
//! }
//! ```

use storekey::{Decode, Encode};

/// Encode a value to bytes using storekey's order-preserving format.
///
/// The encoded bytes will sort in the same order as the original values
/// when compared lexicographically.
///
/// # Type Requirements
///
/// The value must implement `storekey::Encode`. Supported types include:
/// - All Rust primitives (u8, u16, u32, u64, i8, i16, i32, i64, etc.)
/// - Strings and &str
/// - Options
/// - Tuples (for composite keys)
/// - Vecs
/// - Custom types with `#[derive(storekey::Encode)]`
///
/// # Examples
///
/// ```rust,ignore
/// // Simple key
/// let key = encode_key(&"alice");
///
/// // Composite key (tuple)
/// let key = encode_key(&("alice", 12345_i64));
/// ```
pub fn encode_key<T: Encode>(value: &T) -> Vec<u8> {
    storekey::encode_vec(value).expect("storekey encoding should not fail for valid types")
}

/// Encode a value as a prefix for range scans.
///
/// This is identical to `encode_key` but makes the intent clear when used for prefix scans.
/// For tuple encodings like `(user_id, seq)`, encode just the prefix tuple `(user_id,)`.
///
/// # Examples
///
/// ```rust,ignore
/// // For scanning all rows for a user
/// let prefix = encode_prefix(&("alice",));
/// ```
pub fn encode_prefix<T: Encode>(value: &T) -> Vec<u8> {
    encode_key(value)
}

/// Decode a value from storekey-encoded bytes.
///
/// # Errors
///
/// Returns an error if the bytes cannot be decoded to the expected type.
///
/// # Examples
///
/// ```rust,ignore
/// let bytes = encode_key(&("alice", 12345_i64));
/// let (user, seq): (String, i64) = decode_key(&bytes)?;
/// ```
pub fn decode_key<T: Decode>(bytes: &[u8]) -> Result<T, String> {
    storekey::decode(&mut std::io::Cursor::new(bytes))
        .map_err(|e| format!("storekey decode error: {:?}", e))
}

/// Compute the next lexicographic key after the provided encoded bytes.
///
/// This is used to make range scans exclusive of a specific complete key. It
/// appends a null byte which is safe for storekey-encoded values because the
/// encoded bytes are prefix-free for complete keys.
///
/// Do not use this as an upper bound for encoded prefixes. For composite
/// storekey prefixes like `encode_prefix(&(user_id, pk))`, the resulting bytes
/// are not guaranteed to sort after every longer key that starts with that
/// prefix.
pub fn next_storage_key_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(bytes.len() + 1);
    next.extend_from_slice(bytes);
    next.push(0);
    next
}

/// Trait for keys that can be serialized for storage in EntityStore
///
/// All keys used with SystemTableStore must implement this trait to ensure
/// correct serialization to bytes for RocksDB/Parquet storage.
///
/// # Ordering Guarantees
///
/// Keys are serialized using `storekey` which preserves lexicographic ordering.
/// This means:
/// - Strings sort alphabetically
/// - Numbers sort numerically
/// - Tuples sort element-by-element
///
/// # Examples
///
/// Composite key (TableId):
/// ```rust,ignore
/// impl StorageKey for TableId {
///     fn storage_key(&self) -> Vec<u8> {
///         encode_key(&(self.namespace.as_str(), self.table_name.as_str()))
///     }
///
///     fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
///         let (ns, table): (String, String) = decode_key(bytes)?;
///         Ok(TableId::new(NamespaceId::new(ns), TableName::new(table)))
///     }
/// }
/// ```
///
/// Simple key (UserId):
/// ```rust,ignore
/// impl StorageKey for UserId {
///     fn storage_key(&self) -> Vec<u8> {
///         encode_key(&self.as_str())
///     }
///
///     fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
///         let s: String = decode_key(bytes)?;
///         Ok(UserId::new(s))
///     }
/// }
/// ```
pub trait StorageKey: Clone + Send + Sync + 'static {
    /// Serialize this key to bytes for storage using order-preserving encoding.
    ///
    /// For composite keys, this MUST return the full composite representation
    /// using `encode_key()` with a tuple.
    fn storage_key(&self) -> Vec<u8>;

    /// Deserialize this key from bytes
    fn from_storage_key(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized;
}

// --- Standard Implementations ---

impl StorageKey for String {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(&self.as_str())
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for Vec<u8> {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for u8 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for u16 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for u32 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for u64 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for u128 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for i8 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for i16 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for i32 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for i64 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

impl StorageKey for i128 {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(self)
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        decode_key(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_ordering_preserved() {
        // This test verifies that storekey preserves lexicographic ordering
        let alice_key = encode_key(&"alice");
        let bob_key = encode_key(&"bob");

        // "alice" < "bob" lexicographically, so encoded bytes should maintain this
        assert!(
            alice_key < bob_key,
            "alice should sort before bob: {:?} vs {:?}",
            alice_key,
            bob_key
        );
    }

    #[test]
    fn test_variable_length_string_ordering() {
        // Critical test: different length strings should sort correctly
        let short = encode_key(&"ab");
        let long = encode_key(&"aaa");

        // "aaa" < "ab" lexicographically (first char same, second char 'a' < 'b')
        assert!(long < short, "aaa should sort before ab: {:?} vs {:?}", long, short);
    }

    #[test]
    fn test_composite_key_ordering() {
        // Test tuple ordering
        let key1 = encode_key(&("alice", 100_i64));
        let key2 = encode_key(&("alice", 200_i64));
        let key3 = encode_key(&("bob", 50_i64));

        // Same user, different seq: should sort by seq
        assert!(key1 < key2, "alice:100 should sort before alice:200");

        // Different users: should sort by user first
        assert!(key1 < key3, "alice:100 should sort before bob:50");
        assert!(key2 < key3, "alice:200 should sort before bob:50");
    }

    #[test]
    fn test_numeric_ordering() {
        let key1 = encode_key(&100_u64);
        let key2 = encode_key(&200_u64);
        let key3 = encode_key(&1000_u64);

        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn test_round_trip_string() {
        let original = "hello world".to_string();
        let encoded = original.storage_key();
        let decoded: String = String::from_storage_key(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_round_trip_numbers() {
        let val: u64 = 12345;
        let encoded = val.storage_key();
        let decoded = u64::from_storage_key(&encoded).unwrap();
        assert_eq!(val, decoded);

        let val: i64 = -12345;
        let encoded = val.storage_key();
        let decoded = i64::from_storage_key(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_round_trip_composite() {
        let user = "alice";
        let seq = 12345_i64;
        let encoded = encode_key(&(user, seq));
        let (dec_user, dec_seq): (String, i64) = decode_key(&encoded).unwrap();
        assert_eq!(user, dec_user);
        assert_eq!(seq, dec_seq);
    }
}
