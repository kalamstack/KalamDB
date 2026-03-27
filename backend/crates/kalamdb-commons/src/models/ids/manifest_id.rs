//! Type-safe wrapper for manifest cache identifiers.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::storage_key::{decode_key, encode_key, encode_prefix};
use crate::{StorageKey, TableId, UserId};

/// Type-safe wrapper for manifest cache identifiers.
///
/// A ManifestId uniquely identifies a manifest cache entry by combining
/// a TableId with an optional UserId (for user tables vs shared tables).
///
/// Format: "namespace:table:scope" where scope is "shared" or user_id
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ManifestId {
    pub table_id: TableId,
    pub user_id: Option<UserId>,
}

impl ManifestId {
    /// Create a new manifest ID from TableId and optional UserId
    #[inline]
    pub fn new(table_id: TableId, user_id: Option<UserId>) -> Self {
        Self { table_id, user_id }
    }

    /// Create a new manifest ID for a shared table
    #[inline]
    pub fn for_shared_table(table_id: TableId) -> Self {
        Self {
            table_id,
            user_id: None,
        }
    }

    /// Create a new manifest ID for a user table
    #[inline]
    pub fn for_user_table(table_id: TableId, user_id: UserId) -> Self {
        Self {
            table_id,
            user_id: Some(user_id),
        }
    }

    /// Get the scope string ("shared" or user_id)
    pub fn scope_str(&self) -> String {
        match &self.user_id {
            Some(uid) => uid.as_str().to_string(),
            None => "shared".to_string(),
        }
    }

    /// Create a prefix for scanning all manifest entries for a table.
    pub fn table_prefix(table_id: &TableId) -> Vec<u8> {
        encode_prefix(&(table_id.namespace_id().as_str(), table_id.table_name().as_str()))
    }

    /// Get the ID as a string representation
    pub fn as_str(&self) -> String {
        format!(
            "{}:{}:{}",
            self.table_id.namespace_id().as_str(),
            self.table_id.table_name().as_str(),
            self.scope_str()
        )
    }

    /// Get the table ID
    #[inline]
    pub fn table_id(&self) -> &TableId {
        &self.table_id
    }

    /// Get the user ID
    #[inline]
    pub fn user_id(&self) -> Option<&UserId> {
        self.user_id.as_ref()
    }
}

impl From<String> for ManifestId {
    fn from(s: String) -> Self {
        // Parse format: "namespace:table:scope"
        let mut parts = s.splitn(3, ':');
        let namespace = parts.next().unwrap_or("default");
        let table = parts.next().unwrap_or("unknown");
        let scope = parts.next().unwrap_or("shared");

        let table_id = TableId::from_strings(namespace, table);
        let user_id = if scope == "shared" {
            None
        } else {
            Some(UserId::from(scope))
        };

        Self { table_id, user_id }
    }
}

impl From<&str> for ManifestId {
    fn from(s: &str) -> Self {
        Self::from(s.to_string())
    }
}

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl StorageKey for ManifestId {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(&(
            self.table_id.namespace_id().as_str(),
            self.table_id.table_name().as_str(),
            self.scope_str(),
        ))
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        if let Ok((namespace_id, table_name, scope)) = decode_key::<(String, String, String)>(bytes)
        {
            let table_id = TableId::from_strings(&namespace_id, &table_name);
            let user_id = if scope == "shared" {
                None
            } else {
                Some(UserId::from(scope.as_str()))
            };
            return Ok(Self { table_id, user_id });
        }

        Err("Invalid manifest ID storage key".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NamespaceId, TableName};

    #[test]
    fn test_manifest_id_shared_table() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let id = ManifestId::for_shared_table(table_id.clone());

        assert_eq!(id.table_id(), &table_id);
        assert_eq!(id.user_id(), None);
        assert_eq!(id.scope_str(), "shared");
        assert_eq!(id.as_str(), "test:table:shared");
    }

    #[test]
    fn test_manifest_id_user_table() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let user_id = UserId::from("user123");
        let id = ManifestId::for_user_table(table_id.clone(), user_id.clone());

        assert_eq!(id.table_id(), &table_id);
        assert_eq!(id.user_id(), Some(&user_id));
        assert_eq!(id.scope_str(), "user123");
        assert_eq!(id.as_str(), "test:table:user123");
    }

    #[test]
    fn test_manifest_id_from_string() {
        let id = ManifestId::from("test:table:shared".to_string());
        assert_eq!(id.scope_str(), "shared");
        assert_eq!(id.user_id(), None);

        let id2 = ManifestId::from("test:table:user456");
        assert_eq!(id2.scope_str(), "user456");
        assert!(id2.user_id().is_some());
    }

    #[test]
    fn test_manifest_id_display() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let id = ManifestId::for_shared_table(table_id);
        assert_eq!(format!("{}", id), "test:table:shared");
    }

    #[test]
    fn test_manifest_id_storage_key() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let id = ManifestId::for_shared_table(table_id);

        let key = id.storage_key();
        let restored = ManifestId::from_storage_key(&key).unwrap();

        assert_eq!(id, restored);
    }
}
