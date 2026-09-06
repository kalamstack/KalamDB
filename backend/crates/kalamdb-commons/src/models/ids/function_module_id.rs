//! Identity of a deployed function module (one module per project in V1).

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "storage")]
use crate::StorageKey;

/// Function module name, e.g. `backend`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FunctionModuleId(String);

impl FunctionModuleId {
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(!id.is_empty(), "FunctionModuleId cannot be empty");
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for FunctionModuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for FunctionModuleId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for FunctionModuleId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for FunctionModuleId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "storage")]
impl StorageKey for FunctionModuleId {
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
    fn function_module_id_round_trips() {
        assert_eq!(FunctionModuleId::new("backend").as_str(), "backend");
    }
}
