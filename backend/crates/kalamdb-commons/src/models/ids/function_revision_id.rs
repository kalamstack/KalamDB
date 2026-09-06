//! Identity of an activated function module revision.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::{ArtifactId, FunctionModuleId};
#[cfg(feature = "storage")]
use crate::StorageKey;

/// `{module}:{artifact}` so a revision is unique per module and content hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FunctionRevisionId(String);

impl FunctionRevisionId {
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(!id.is_empty(), "FunctionRevisionId cannot be empty");
        Self(id)
    }

    pub fn from_module_artifact(module_id: &FunctionModuleId, artifact_id: &ArtifactId) -> Self {
        Self::new(format!("{}:{}", module_id.as_str(), artifact_id.as_str()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for FunctionRevisionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for FunctionRevisionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for FunctionRevisionId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for FunctionRevisionId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "storage")]
impl StorageKey for FunctionRevisionId {
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
    fn function_revision_id_from_module_artifact() {
        let revision = FunctionRevisionId::from_module_artifact(
            &FunctionModuleId::new("backend"),
            &ArtifactId::new("abc123"),
        );
        assert_eq!(revision.as_str(), "backend:abc123");
    }
}
