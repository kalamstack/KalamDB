// File: backend/crates/kalamdb-commons/src/models/table_id.rs
// Composite key for system.tables entries

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use super::namespace_id::NamespaceId;
use crate::models::schemas::TableName;
use crate::storage_key::{decode_key, encode_key, encode_prefix};
use crate::StorageKey;

/// Composite key for system.tables entries: (namespace_id, table_name)
///
/// This composite key provides type-safe access to table metadata,
/// ensuring namespace and table name are always paired correctly.
///
/// # Serialization
///
/// Serializes as "namespace.table" string format for JSON compatibility.
/// For example: `"flush_test_ns_mkav1q2g_3.metrics"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId {
    namespace_id: NamespaceId,
    table_name: TableName,
}

impl TableId {
    /// Create a new TableId from namespace ID and table name
    #[inline]
    pub fn new(namespace_id: NamespaceId, table_name: TableName) -> Self {
        Self {
            namespace_id,
            table_name,
        }
    }

    /// Get the namespace ID component
    #[inline]
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Get the table name component
    #[inline]
    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    /// Create from string components
    #[inline]
    pub fn from_strings(namespace_id: &str, table_name: &str) -> Self {
        Self {
            namespace_id: NamespaceId::new(namespace_id),
            table_name: TableName::new(table_name),
        }
    }

    /// Create from string components with validation errors instead of panics.
    #[inline]
    pub fn try_from_strings(namespace_id: &str, table_name: &str) -> Result<Self, String> {
        let namespace_id = NamespaceId::try_new(namespace_id)
            .map_err(|e| format!("invalid namespace_id '{}': {}", namespace_id, e))?;
        let table_name = TableName::try_new(table_name)
            .map_err(|e| format!("invalid table_name '{}': {}", table_name, e))?;

        Ok(Self {
            namespace_id,
            table_name,
        })
    }

    /// Create a prefix for scanning all tables in a namespace.
    #[inline]
    pub fn namespace_prefix(namespace_id: &NamespaceId) -> Vec<u8> {
        encode_prefix(&(namespace_id.as_str(),))
    }

    /// Format as bytes for storage using storekey tuple encoding
    #[inline]
    pub fn as_storage_key(&self) -> Vec<u8> {
        encode_key(&(self.namespace_id.as_str(), self.table_name.as_str()))
    }

    /// Parse from storage key bytes
    pub fn from_storage_key(key: &[u8]) -> Option<Self> {
        if let Ok((namespace_id, table_name)) = decode_key::<(String, String)>(key) {
            return Some(Self {
                namespace_id: NamespaceId::new(namespace_id),
                table_name: TableName::new(table_name),
            });
        }

        None
    }

    /// Consume and return inner components
    pub fn into_parts(self) -> (NamespaceId, TableName) {
        (self.namespace_id, self.table_name)
    }

    /// Returns the fully qualified table name in SQL format: "namespace.table"
    ///
    /// This is the format used in SQL queries (e.g., `SELECT * FROM app.users`).
    /// For storage key format (storekey tuple), use `as_storage_key()` instead.
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.namespace_id.as_str(), self.table_name.as_str())
    }
}

// Custom Serialize implementation: serialize as "namespace.table" string
impl Serialize for TableId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.full_name())
    }
}

// Custom Deserialize implementation: deserialize from "namespace.table" string
// Uses a Visitor pattern to avoid deserialize_any for codec compatibility.
impl<'de> Deserialize<'de> for TableId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct TableIdVisitor;

        impl<'de> Visitor<'de> for TableIdVisitor {
            type Value = TableId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string in the format 'namespace.table'")
            }

            fn visit_str<E>(self, value: &str) -> Result<TableId, E>
            where
                E: Error,
            {
                // Parse "namespace.table" format
                let mut parts = value.splitn(2, '.');
                let namespace = parts.next();
                let table = parts.next();
                match (namespace, table) {
                    (Some(namespace), Some(table)) => Ok(TableId {
                        namespace_id: NamespaceId::new(namespace),
                        table_name: TableName::new(table),
                    }),
                    _ => Err(E::custom(format!("Invalid table_id format: {}", value))),
                }
            }

            fn visit_string<E>(self, value: String) -> Result<TableId, E>
            where
                E: Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_str(TableIdVisitor)
    }
}

impl AsRef<str> for TableId {
    fn as_ref(&self) -> &str {
        // This creates a temporary allocation. For zero-copy access,
        // use as_storage_key() directly.
        // This implementation is primarily for trait compatibility.
        // In performance-critical paths, prefer as_storage_key().
        self.namespace_id.as_str()
    }
}

/// Implement AsRef<[u8]> for EntityStore compatibility
///
/// This allocates a new Vec on each call. For performance-critical paths,
/// consider using as_storage_key() directly instead.
impl AsRef<[u8]> for TableId {
    fn as_ref(&self) -> &[u8] {
        // We need to return a reference, but as_storage_key() creates a new Vec
        // The best we can do here is to use the namespace_id bytes as a prefix
        // In practice, the EntityStore will use as_storage_key() internally
        // This implementation satisfies the trait bound requirement
        self.namespace_id.as_str().as_bytes()
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.namespace_id, self.table_name)
    }
}

impl StorageKey for TableId {
    fn storage_key(&self) -> Vec<u8> {
        self.as_storage_key()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        Self::from_storage_key(bytes).ok_or_else(|| "Invalid TableId format".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_id_new() {
        let namespace_id = NamespaceId::new("ns1");
        let table_name = TableName::new("users");
        let table_id = TableId::new(namespace_id.clone(), table_name.clone());

        assert_eq!(table_id.namespace_id(), &namespace_id);
        assert_eq!(table_id.table_name(), &table_name);
    }

    #[test]
    fn test_table_id_from_strings() {
        let table_id = TableId::from_strings("ns1", "users");
        assert_eq!(table_id.namespace_id().as_str(), "ns1");
        assert_eq!(table_id.table_name().as_str(), "users");
    }

    #[test]
    fn test_table_id_try_from_strings() {
        let table_id = TableId::try_from_strings("ns1", "users").unwrap();
        assert_eq!(table_id.namespace_id().as_str(), "ns1");
        assert_eq!(table_id.table_name().as_str(), "users");
    }

    #[test]
    fn test_table_id_try_from_strings_invalid() {
        let err = TableId::try_from_strings("../ns1", "users").unwrap_err();
        assert!(err.contains("invalid namespace_id"));
    }

    #[test]
    fn test_table_id_as_storage_key() {
        let table_id = TableId::from_strings("ns1", "users");
        let key = table_id.as_storage_key();
        assert!(!key.is_empty());
        let parsed = TableId::from_storage_key(&key).unwrap();
        assert_eq!(parsed, table_id);
    }

    #[test]
    fn test_table_id_from_storage_key() {
        let key = TableId::from_strings("ns1", "users").as_storage_key();
        let table_id = TableId::from_storage_key(&key).unwrap();

        assert_eq!(table_id.namespace_id().as_str(), "ns1");
        assert_eq!(table_id.table_name().as_str(), "users");
    }

    #[test]
    fn test_table_id_roundtrip() {
        let original = TableId::from_strings("ns1", "users");
        let key = original.as_storage_key();
        let parsed = TableId::from_storage_key(&key).unwrap();

        assert_eq!(original, parsed);
    }

    #[test]
    fn test_table_id_display() {
        let table_id = TableId::from_strings("ns1", "users");
        assert_eq!(format!("{}", table_id), "ns1:users");
    }

    #[test]
    fn test_table_id_full_name() {
        let table_id = TableId::from_strings("app", "messages");
        assert_eq!(table_id.full_name(), "app.messages");

        let table_id2 = TableId::from_strings("my_namespace", "user_table");
        assert_eq!(table_id2.full_name(), "my_namespace.user_table");
    }

    #[test]
    fn test_table_id_serialization() {
        let table_id = TableId::from_strings("ns1", "users");
        let json = serde_json::to_string(&table_id).unwrap();
        let deserialized: TableId = serde_json::from_str(&json).unwrap();
        assert_eq!(table_id, deserialized);
    }

    #[test]
    fn test_table_id_into_parts() {
        let table_id = TableId::from_strings("ns1", "users");
        let (namespace_id, table_name) = table_id.into_parts();

        assert_eq!(namespace_id.as_str(), "ns1");
        assert_eq!(table_name.as_str(), "users");
    }

    #[test]
    fn test_table_id_with_special_chars() {
        let table_id = TableId::from_strings("my-namespace", "table_name");
        let key = table_id.as_storage_key();
        let parsed = TableId::from_storage_key(&key).unwrap();

        assert_eq!(table_id, parsed);
    }
}
