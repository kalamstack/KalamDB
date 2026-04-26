use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Enum representing storage mode preferences for users.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageMode {
    Table,
    Region,
}

impl StorageMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageMode::Table => "table",
            StorageMode::Region => "region",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "table" => Some(StorageMode::Table),
            "region" => Some(StorageMode::Region),
            _ => None,
        }
    }
}

impl FromStr for StorageMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StorageMode::from_str_opt(s).ok_or_else(|| format!("Invalid StorageMode: {}", s))
    }
}

impl fmt::Display for StorageMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for StorageMode {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "table" => StorageMode::Table,
            "region" => StorageMode::Region,
            _ => StorageMode::Table,
        }
    }
}

impl From<String> for StorageMode {
    fn from(s: String) -> Self {
        StorageMode::from(s.as_str())
    }
}
