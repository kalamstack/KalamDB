use std::{fmt, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Enum representing user roles in KalamDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum Role {
    Anonymous,
    User,
    Service,
    Dba,
    System,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Anonymous => "anonymous",
            Role::User => "user",
            Role::Service => "service",
            Role::Dba => "dba",
            Role::System => "system",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "anonymous" => Some(Role::Anonymous),
            "user" => Some(Role::User),
            "service" => Some(Role::Service),
            "dba" => Some(Role::Dba),
            "system" => Some(Role::System),
            _ => None,
        }
    }
}

impl FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Role::from_str_opt(s).ok_or_else(|| format!("Invalid Role: {}", s))
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for Role {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "anonymous" => Role::Anonymous,
            "user" => Role::User,
            "service" => Role::Service,
            "dba" => Role::Dba,
            "system" => Role::System,
            _ => Role::User,
        }
    }
}

impl From<String> for Role {
    fn from(s: String) -> Self {
        Role::from(s.as_str())
    }
}
