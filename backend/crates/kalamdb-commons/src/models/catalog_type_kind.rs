//! Catalog kind stored on `system.types`.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Distinguishes implicit table row types, aliases, named composites, and enums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum CatalogTypeKind {
    /// Same-named row type generated for a table.
    ImplicitTableRow,
    /// Alias that records a source type id instead of copying fields.
    RowAlias,
    /// Named `CREATE TYPE ... AS (...)`.
    Composite,
    /// Named `CREATE TYPE ... AS ENUM (...)`.
    Enum,
}

impl CatalogTypeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ImplicitTableRow => "implicit_table_row",
            Self::RowAlias => "row_alias",
            Self::Composite => "composite",
            Self::Enum => "enum",
        }
    }

    pub fn from_str_opt(value: &str) -> Option<Self> {
        match value {
            "implicit_table_row" => Some(Self::ImplicitTableRow),
            "row_alias" => Some(Self::RowAlias),
            "composite" => Some(Self::Composite),
            "enum" => Some(Self::Enum),
            _ => None,
        }
    }
}

impl fmt::Display for CatalogTypeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_type_kind_round_trips_as_snake_case() {
        assert_eq!(CatalogTypeKind::RowAlias.as_str(), "row_alias");
        assert_eq!(
            CatalogTypeKind::from_str_opt("implicit_table_row"),
            Some(CatalogTypeKind::ImplicitTableRow)
        );
    }
}
