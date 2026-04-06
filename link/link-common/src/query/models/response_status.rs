use serde::{Deserialize, Serialize};

/// Response status enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ResponseStatus {
    Success,
    Error,
}

impl std::fmt::Display for ResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseStatus::Success => write!(f, "success"),
            ResponseStatus::Error => write!(f, "error"),
        }
    }
}
