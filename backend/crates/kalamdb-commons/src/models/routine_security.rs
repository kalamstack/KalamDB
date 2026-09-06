//! Routine security mode persisted on `system.routines`.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// PostgreSQL-style routine security.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum RoutineSecurityMode {
    /// Run as the calling principal (`SECURITY INVOKER`).
    Invoker,
    /// Run as the routine owner (`SECURITY DEFINER`).
    Definer,
}

impl Default for RoutineSecurityMode {
    fn default() -> Self {
        Self::Invoker
    }
}

impl RoutineSecurityMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Invoker => "INVOKER",
            Self::Definer => "DEFINER",
        }
    }
}
