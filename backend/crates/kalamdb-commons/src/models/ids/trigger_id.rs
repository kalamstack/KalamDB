//! Type-safe wrapper for SQL topic triggers (`CREATE TRIGGER`).

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::namespace_id::NamespaceId;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// Schema-qualified trigger identity, e.g. `chat.process_message`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TriggerId(String);

impl TriggerId {
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(!id.is_empty(), "TriggerId cannot be empty");
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
        assert!(!name.is_empty(), "TriggerId cannot be empty");
        match namespace {
            Some(namespace) => Self::new(format!("{}.{name}", namespace.as_str())),
            None => Self::new(name),
        }
    }
}

impl fmt::Display for TriggerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TriggerId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for TriggerId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for TriggerId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "storage")]
impl StorageKey for TriggerId {
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
    fn trigger_id_from_parts() {
        let id = TriggerId::from_parts(Some(&NamespaceId::new("chat")), "process_message");
        assert_eq!(id.as_str(), "chat.process_message");
    }
}
