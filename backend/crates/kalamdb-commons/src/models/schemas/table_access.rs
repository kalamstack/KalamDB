use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Enum representing table access control in KalamDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TableAccess {
    Public,
    Private,
    Restricted,
    Dba,
}

impl TableAccess {
    pub fn as_str(&self) -> &'static str {
        match self {
            TableAccess::Public => "public",
            TableAccess::Private => "private",
            TableAccess::Restricted => "restricted",
            TableAccess::Dba => "dba",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "public" => Some(TableAccess::Public),
            "private" => Some(TableAccess::Private),
            "restricted" => Some(TableAccess::Restricted),
            "dba" => Some(TableAccess::Dba),
            _ => None,
        }
    }
}

impl FromStr for TableAccess {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TableAccess::from_str_opt(s).ok_or_else(|| format!("Invalid TableAccess: {}", s))
    }
}

impl fmt::Display for TableAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for TableAccess {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "public" => TableAccess::Public,
            "private" => TableAccess::Private,
            "restricted" => TableAccess::Restricted,
            "dba" => TableAccess::Dba,
            _ => TableAccess::Private,
        }
    }
}

impl From<String> for TableAccess {
    fn from(s: String) -> Self {
        TableAccess::from(s.as_str())
    }
}
