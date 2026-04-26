// File: backend/crates/kalamdb-commons/src/models/row_id.rs
// Type-safe row identifier for shared and stream tables

use std::fmt;

use serde::{Deserialize, Serialize};

/// Type-safe row identifier for shared and stream tables.
///
/// This newtype wrapper provides compile-time safety for row IDs,
/// preventing them from being confused with other string/byte identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowId(Vec<u8>);

impl RowId {
    /// Create a new RowId from any type that can be converted to Vec<u8>
    pub fn new(id: impl Into<Vec<u8>>) -> Self {
        Self(id.into())
    }

    /// Create a RowId from a string
    pub fn from_string(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }

    /// Get the row ID as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Convert to inner Vec<u8>
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for RowId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for RowId {
    fn from(id: Vec<u8>) -> Self {
        Self(id)
    }
}

impl From<String> for RowId {
    fn from(id: String) -> Self {
        Self(id.into_bytes())
    }
}

impl From<&str> for RowId {
    fn from(id: &str) -> Self {
        Self(id.as_bytes().to_vec())
    }
}

impl fmt::Display for RowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Try to display as UTF-8 string, fall back to hex if invalid
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "{}", hex::encode(&self.0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_id_from_string() {
        let id = RowId::from_string("test-row-123");
        assert_eq!(id.as_bytes(), b"test-row-123");
    }

    #[test]
    fn test_row_id_from_vec() {
        let id = RowId::new(vec![1, 2, 3, 4]);
        assert_eq!(id.as_bytes(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_row_id_as_ref() {
        let id = RowId::from_string("test");
        let bytes: &[u8] = id.as_ref();
        assert_eq!(bytes, b"test");
    }

    #[test]
    fn test_row_id_display() {
        let id = RowId::from_string("test-123");
        assert_eq!(format!("{}", id), "test-123");
    }

    #[test]
    fn test_row_id_clone() {
        let id1 = RowId::from_string("test");
        let id2 = id1.clone();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_row_id_serialization() {
        let id = RowId::from_string("test-row");
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: RowId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }
}
