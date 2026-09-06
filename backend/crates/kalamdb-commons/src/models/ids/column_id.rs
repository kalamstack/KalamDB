//! Stable table column identity. Survives `ALTER TABLE RENAME COLUMN`.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Catalog identity of a table column (`ColumnDefinition.column_id`).
///
/// Names can change; this id is assigned at `CREATE`/`ADD COLUMN` and is never
/// reused after `DROP COLUMN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ColumnId(u64);

impl ColumnId {
    /// Wrap a catalog `column_id`.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Inner numeric id used on [`super::super::schemas::ColumnDefinition`].
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ColumnId {
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_id_roundtrips_as_u64() {
        let id = ColumnId::new(42);
        assert_eq!(id.as_u64(), 42);
        assert_eq!(ColumnId::from(42), id);
        assert_eq!(id.to_string(), "42");
    }
}
