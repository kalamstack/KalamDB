//! Composite identity for one routine EXECUTE grant.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::RoutineId;
use crate::models::RoutineGrantee;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// `{routine_id}:{grantee_key}` identity for `system.routine_grants`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RoutineGrantId(String);

impl RoutineGrantId {
    pub fn new(routine_id: &RoutineId, grantee: &RoutineGrantee) -> Self {
        Self(format!("{}:{}", routine_id.as_str(), grantee.catalog_key()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn routine_id(&self) -> RoutineId {
        let (routine_id, _) = self.split();
        RoutineId::new(routine_id)
    }

    pub fn grantee(&self) -> Result<RoutineGrantee, String> {
        RoutineGrantee::from_catalog_key(self.split().1)
    }

    fn split(&self) -> (&str, &str) {
        self.0.split_once(':').expect("RoutineGrantId always contains ':'")
    }
}

impl fmt::Display for RoutineGrantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RoutineGrantId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(feature = "storage")]
impl StorageKey for RoutineGrantId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let value = String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())?;
        let (routine_id, grantee_key) = value
            .split_once(':')
            .ok_or_else(|| "invalid routine grant storage key".to_string())?;
        let grantee = RoutineGrantee::from_catalog_key(grantee_key)?;
        Ok(Self::new(&RoutineId::new(routine_id), &grantee))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routine_grant_id_round_trips_role_grantee() {
        let id = RoutineGrantId::new(
            &RoutineId::from_parts(Some(&crate::models::NamespaceId::new("api")), "create_order"),
            &RoutineGrantee::Role("moderator".to_string()),
        );
        assert_eq!(id.as_str(), "api.create_order:role=moderator");
        assert_eq!(id.grantee().unwrap(), RoutineGrantee::Role("moderator".to_string()));
    }
}
