//! Type-safe wrapper for SQL routines (`CREATE PROCEDURE`).

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::namespace_id::NamespaceId;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// Schema-qualified routine identity, e.g. `chat.create_message`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RoutineId(String);

impl RoutineId {
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(!id.is_empty(), "RoutineId cannot be empty");
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn from_parts(namespace: Option<&NamespaceId>, name: &str) -> Self {
        let name = name.trim().to_ascii_lowercase();
        assert!(!name.is_empty(), "RoutineId cannot be empty");
        match namespace {
            Some(namespace) => Self::new(format!("{}.{name}", namespace.as_str())),
            None => Self::new(name),
        }
    }

    pub fn namespace_id(&self) -> Option<NamespaceId> {
        self.as_str().rsplit_once('.').map(|(namespace, _)| NamespaceId::new(namespace))
    }
}

impl fmt::Display for RoutineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for RoutineId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for RoutineId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for RoutineId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "storage")]
impl StorageKey for RoutineId {
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
    fn routine_id_from_parts() {
        assert_eq!(
            RoutineId::from_parts(Some(&NamespaceId::new("chat")), "create_message").as_str(),
            "chat.create_message"
        );
    }
}
