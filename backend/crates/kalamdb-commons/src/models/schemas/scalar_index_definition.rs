//! Catalog definition of a USER/SHARED scalar secondary index.

use serde::{Deserialize, Serialize};

use super::ColumnDefinition;
use crate::models::ColumnId;

/// Logical scalar index stored on [`super::TableDefinition`].
///
/// Indexes are non-unique by default. Unique is allowed only when the column
/// list is unique in the live MVCC winner set. The primary key remains the
/// unique live-row identity.
///
/// `columns` are stable [`ColumnId`]s so `RENAME COLUMN` does not invalidate
/// the catalog entry. Resolve to the current name with
/// [`Self::resolved_column_names`] when opening a store or matching SQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalarIndexDefinition {
    pub name:    String,
    pub columns: Vec<ColumnId>,
    #[serde(default)]
    pub unique:  bool,
}

impl ScalarIndexDefinition {
    pub fn new(name: impl Into<String>, columns: Vec<ColumnId>, unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            unique,
        }
    }

    /// Current SQL/storage names for this index, in key order.
    ///
    /// Returns `None` if any id is missing from `columns` (dropped without
    /// dropping the index).
    pub fn resolved_column_names<'a>(
        &self,
        columns: &'a [ColumnDefinition],
    ) -> Option<Vec<&'a str>> {
        self.columns
            .iter()
            .map(|column_id| {
                columns
                    .iter()
                    .find(|column| column.column_id == column_id.as_u64())
                    .map(|column| column.column_name.as_str())
            })
            .collect()
    }
}
