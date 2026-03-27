//! Table version identifier for schema versioning
//!
//! This module provides `TableVersionId` - a composite key that combines a TableId
//! with a version number for storing and retrieving specific schema versions.
//!
//! Storage key format (storekey tuple encoding):
//! - Latest pointer: (namespace, table, VERSION_KIND_LATEST)
//! - Versioned:      (namespace, table, VERSION_KIND_VERSIONED, version)

use serde::{Deserialize, Serialize};
use std::fmt;

use super::table_id::TableId;
use crate::storage_key::{decode_key, encode_key, encode_prefix};
use crate::StorageKey;

/// Marker for the "latest" version pointer
pub const LATEST_MARKER: &str = "<lat>";

/// Marker for versioned entries
pub const VERSION_MARKER: &str = "<ver>";

/// Discriminator for latest pointer keys
pub const VERSION_KIND_LATEST: u8 = 0;

/// Discriminator for versioned keys
pub const VERSION_KIND_VERSIONED: u8 = 1;

/// Composite key for versioned table schema storage
///
/// Supports two key types:
/// 1. **Latest pointer** (version = None): Points to current version number
/// 2. **Versioned entry** (version = Some(N)): Full TableDefinition at version N
///
/// # Storage Key Format
///
/// ```text
/// Latest:    ("default", "users", 0u8)
/// Versioned: ("default", "users", 1u8, 1u32)
/// ```
///
/// Storekey preserves lexicographic ordering for efficient range scans.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableVersionId {
    /// The base table identifier
    table_id: TableId,

    /// Version number: None = latest pointer, Some(N) = specific version
    version: Option<u32>,
}

impl TableVersionId {
    /// Create a latest pointer key (for looking up current version)
    ///
    /// # Example
    /// ```rust,ignore
    /// let table_id = TableId::from_strings("default", "users");
    /// let latest = TableVersionId::latest(table_id);
    /// // Storage key: ("default", "users", 0u8)
    /// ```
    pub fn latest(table_id: TableId) -> Self {
        Self {
            table_id,
            version: None,
        }
    }

    /// Create a versioned key (for storing/retrieving specific version)
    ///
    /// # Example
    /// ```rust,ignore
    /// let table_id = TableId::from_strings("default", "users");
    /// let v1 = TableVersionId::versioned(table_id, 1);
    /// // Storage key: ("default", "users", 1u8, 1u32)
    /// ```
    pub fn versioned(table_id: TableId, version: u32) -> Self {
        Self {
            table_id,
            version: Some(version),
        }
    }

    /// Get the base table ID
    pub fn table_id(&self) -> &TableId {
        &self.table_id
    }

    /// Get the version number (None for latest pointer)
    pub fn version(&self) -> Option<u32> {
        self.version
    }

    /// Check if this is a latest pointer
    pub fn is_latest(&self) -> bool {
        self.version.is_none()
    }

    /// Check if this is a versioned entry
    pub fn is_versioned(&self) -> bool {
        self.version.is_some()
    }

    /// Consume and return inner components
    pub fn into_parts(self) -> (TableId, Option<u32>) {
        (self.table_id, self.version)
    }

    /// Create prefix for scanning all versions of a table
    ///
    /// Returns prefix that matches all versioned entries for a table.
    /// Use with range scan to list all versions.
    ///
    /// # Example
    /// ```rust,ignore
    /// let table_id = TableId::from_strings("default", "users");
    /// let prefix = TableVersionId::version_scan_prefix(&table_id);
    /// // Returns storekey prefix for versioned entries
    /// ```
    pub fn version_scan_prefix(table_id: &TableId) -> Vec<u8> {
        encode_prefix(&(
            table_id.namespace_id().as_str(),
            table_id.table_name().as_str(),
            VERSION_KIND_VERSIONED,
        ))
    }

    /// Create prefix for scanning ALL entries (latest + versioned) of a table.
    ///
    /// Used by `delete_all_versions` and `list_versions` to find all storage
    /// entries for a single table without scanning the entire column family.
    pub fn table_scan_prefix(table_id: &TableId) -> Vec<u8> {
        encode_prefix(&(table_id.namespace_id().as_str(), table_id.table_name().as_str()))
    }

    /// Create prefix for scanning all tables in a namespace.
    ///
    /// Matches both latest pointers and versioned entries within the namespace.
    pub fn namespace_scan_prefix(namespace_id: &crate::models::ids::NamespaceId) -> Vec<u8> {
        encode_prefix(&(namespace_id.as_str(),))
    }

    /// Format as bytes for storage
    ///
    /// - Latest: (namespace, table, VERSION_KIND_LATEST)
    /// - Versioned: (namespace, table, VERSION_KIND_VERSIONED, version)
    pub fn as_storage_key(&self) -> Vec<u8> {
        let namespace_id = self.table_id.namespace_id().as_str();
        let table_name = self.table_id.table_name().as_str();

        match self.version {
            None => encode_key(&(namespace_id, table_name, VERSION_KIND_LATEST)),
            Some(v) => encode_key(&(namespace_id, table_name, VERSION_KIND_VERSIONED, v)),
        }
    }

    /// Parse from storage key bytes
    ///
    /// Handles both latest pointer and versioned formats.
    pub fn from_storage_key(key: &[u8]) -> Option<Self> {
        if let Ok((namespace_id, table_name, kind, version)) =
            decode_key::<(String, String, u8, u32)>(key)
        {
            if kind == VERSION_KIND_VERSIONED {
                let table_id = TableId::from_strings(&namespace_id, &table_name);
                return Some(Self::versioned(table_id, version));
            }
        }

        if let Ok((namespace_id, table_name, kind)) = decode_key::<(String, String, u8)>(key) {
            if kind == VERSION_KIND_LATEST {
                let table_id = TableId::from_strings(&namespace_id, &table_name);
                return Some(Self::latest(table_id));
            }
        }

        None
    }
}

impl fmt::Display for TableVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.version {
            None => write!(f, "{}<lat>", self.table_id),
            Some(v) => write!(f, "{}<ver>{:08}", self.table_id, v),
        }
    }
}

impl StorageKey for TableVersionId {
    fn storage_key(&self) -> Vec<u8> {
        self.as_storage_key()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        Self::from_storage_key(bytes).ok_or_else(|| "Invalid TableVersionId format".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ids::NamespaceId;
    use crate::models::schemas::TableName;

    fn test_table_id() -> TableId {
        TableId::new(NamespaceId::default(), TableName::new("users"))
    }

    #[test]
    fn test_latest_key() {
        let latest = TableVersionId::latest(test_table_id());
        assert!(latest.is_latest());
        assert!(!latest.is_versioned());
        assert_eq!(latest.version(), None);

        let key = latest.as_storage_key();
        assert!(!key.is_empty());
        let parsed = TableVersionId::from_storage_key(&key).unwrap();
        assert_eq!(parsed, latest);
    }

    #[test]
    fn test_versioned_key() {
        let v1 = TableVersionId::versioned(test_table_id(), 1);
        assert!(!v1.is_latest());
        assert!(v1.is_versioned());
        assert_eq!(v1.version(), Some(1));

        let key = v1.as_storage_key();
        assert!(!key.is_empty());
        let parsed = TableVersionId::from_storage_key(&key).unwrap();
        assert_eq!(parsed, v1);
    }

    #[test]
    fn test_version_ordering() {
        // Test that lexicographic ordering matches numeric ordering
        let v1 = TableVersionId::versioned(test_table_id(), 1);
        let v2 = TableVersionId::versioned(test_table_id(), 2);
        let v10 = TableVersionId::versioned(test_table_id(), 10);
        let v100 = TableVersionId::versioned(test_table_id(), 100);

        let key1 = v1.as_storage_key();
        let key2 = v2.as_storage_key();
        let key10 = v10.as_storage_key();
        let key100 = v100.as_storage_key();

        assert!(key1 < key2);
        assert!(key2 < key10);
        assert!(key10 < key100);
    }

    #[test]
    fn test_from_storage_key_latest() {
        let original = TableVersionId::latest(test_table_id());
        let key = original.as_storage_key();
        let parsed = TableVersionId::from_storage_key(&key).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_from_storage_key_versioned() {
        let original = TableVersionId::versioned(test_table_id(), 42);
        let key = original.as_storage_key();
        let parsed = TableVersionId::from_storage_key(&key).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_version_scan_prefix() {
        let table_id = test_table_id();
        let prefix = TableVersionId::version_scan_prefix(&table_id);
        // Test that prefix matches versioned keys
        let v1 = TableVersionId::versioned(table_id.clone(), 1);
        let v1_key = v1.as_storage_key();
        assert!(v1_key.starts_with(&prefix));
    }

    #[test]
    fn test_display() {
        let latest = TableVersionId::latest(test_table_id());
        assert_eq!(format!("{}", latest), "default:users<lat>");

        let v42 = TableVersionId::versioned(test_table_id(), 42);
        assert_eq!(format!("{}", v42), "default:users<ver>00000042");
    }

    #[test]
    fn test_into_parts() {
        let v5 = TableVersionId::versioned(test_table_id(), 5);
        let (table_id, version) = v5.into_parts();
        assert_eq!(table_id, test_table_id());
        assert_eq!(version, Some(5));
    }

    #[test]
    fn test_invalid_key_parsing() {
        // Invalid format should return None
        assert!(TableVersionId::from_storage_key(b"invalid").is_none());
        assert!(TableVersionId::from_storage_key(b"default:users").is_none());
        assert!(TableVersionId::from_storage_key(b"default:users<bad>").is_none());
    }
}
