//! EXECUTE ACL principal persisted on `system.routine_grants`.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Principal that can be granted `EXECUTE` on a routine.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RoutineGrantee {
    Public,
    User,
    Service,
    Role(String), // TODO: Use RoleId instead
}

impl RoutineGrantee {
    /// Stable catalog key used in `RoutineGrantId`.
    pub fn catalog_key(&self) -> String {
        match self {
            Self::Public => "public".to_string(),
            Self::User => "user".to_string(),
            Self::Service => "service".to_string(),
            Self::Role(name) => format!("role={name}"),
        }
    }

    pub fn from_catalog_key(key: &str) -> Result<Self, String> {
        match key {
            "public" => Ok(Self::Public),
            "user" => Ok(Self::User),
            "service" => Ok(Self::Service),
            other if let Some(name) = other.strip_prefix("role=") => {
                if name.is_empty() {
                    return Err("routine grantee role name cannot be empty".to_string());
                }
                Ok(Self::Role(name.to_string()))
            },
            _ => Err(format!("unknown routine grantee '{key}'")),
        }
    }
}

impl fmt::Display for RoutineGrantee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.catalog_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routine_grantee_catalog_key_round_trips() {
        for grantee in [
            RoutineGrantee::Public,
            RoutineGrantee::User,
            RoutineGrantee::Service,
            RoutineGrantee::Role("moderator".to_string()),
        ] {
            let key = grantee.catalog_key();
            assert_eq!(RoutineGrantee::from_catalog_key(&key).unwrap(), grantee);
        }
    }
}
