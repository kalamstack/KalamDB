//! Type-safe wrapper for named SQL types (`CREATE TYPE` / implicit table row types).

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::namespace_id::NamespaceId;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// Schema-qualified type identity, e.g. `chat.message`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TypeId(String);

impl TypeId {
    /// Creates a type id. Panics if empty.
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(!id.is_empty(), "TypeId cannot be empty");
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// Build `schema.name` or unqualified `name`.
    pub fn from_parts(namespace: Option<&NamespaceId>, name: &str) -> Self {
        let name = name.trim();
        assert!(!name.is_empty(), "TypeId cannot be empty");
        match namespace {
            Some(namespace) => Self::new(format!("{}.{name}", namespace.as_str())),
            None => Self::new(name),
        }
    }
}

impl fmt::Display for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TypeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for TypeId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for TypeId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "storage")]
impl StorageKey for TypeId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        String::from_utf8(bytes.to_vec()).map(Self).map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_id_from_parts() {
        assert_eq!(
            TypeId::from_parts(Some(&NamespaceId::new("chat")), "message").as_str(),
            "chat.message"
        );
        assert_eq!(TypeId::from_parts(None, "address").as_str(), "address");
    }

    #[test]
    #[should_panic(expected = "TypeId cannot be empty")]
    fn type_id_empty_panics() {
        let _ = TypeId::new("");
    }
}
