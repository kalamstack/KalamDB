//! Composite identity for one routine parameter.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::RoutineId;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// `{routine_id}:{ordinal}` identity for `system.routine_parameters`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RoutineParameterId(String);

impl RoutineParameterId {
    pub fn new(routine_id: &RoutineId, ordinal: i32) -> Result<Self, String> {
        if ordinal < 0 {
            return Err("routine parameter ordinal cannot be negative".to_string());
        }
        Ok(Self(format!("{}:{ordinal}", routine_id.as_str())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn routine_id(&self) -> RoutineId {
        let (routine_id, _) = self.split();
        RoutineId::new(routine_id)
    }

    pub fn ordinal(&self) -> i32 {
        self.split().1.parse().expect("RoutineParameterId ordinal is numeric")
    }

    fn split(&self) -> (&str, &str) {
        self.0.rsplit_once(':').expect("RoutineParameterId always contains ':'")
    }
}

impl fmt::Display for RoutineParameterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RoutineParameterId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(feature = "storage")]
impl StorageKey for RoutineParameterId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let value = String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())?;
        let (routine_id, ordinal) = value
            .rsplit_once(':')
            .ok_or_else(|| "invalid routine parameter storage key".to_string())?;
        let ordinal: i32 =
            ordinal.parse().map_err(|_| "invalid routine parameter ordinal".to_string())?;
        Self::new(&RoutineId::new(routine_id), ordinal)
    }
}
