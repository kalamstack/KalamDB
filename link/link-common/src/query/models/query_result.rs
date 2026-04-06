use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::models::kalam_cell_value::KalamCellValue;
use crate::models::kalam_cell_value::RowData;
use crate::models::schema_field::SchemaField;

/// Individual query result within a SQL response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct QueryResult {
    /// Schema describing the columns in the result set.
    /// Each field contains: name, data_type (KalamDataType), and index.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema: Vec<SchemaField>,

    /// The result rows as arrays of values (ordered by schema index).
    /// Populated by the server; cleared client-side once `named_rows` is built.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<KalamCellValue>>>,

    /// Rows as maps keyed by column name (column → KalamCellValue).
    ///
    /// Populated client-side by [`populate_named_rows`] from `schema` + `rows`.
    /// When present, SDKs should prefer this over positional `rows`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub named_rows: Option<Vec<RowData>>,

    /// Number of rows affected or returned.
    pub row_count: usize,

    /// Optional message for non-query statements.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl QueryResult {
    /// Get column names from schema.
    pub fn column_names(&self) -> Vec<String> {
        self.schema.iter().map(|f| f.name.clone()).collect()
    }

    /// Get a row as a HashMap by index (for convenience).
    pub fn row_as_map(&self, row_idx: usize) -> Option<HashMap<String, KalamCellValue>> {
        let row = self.rows.as_ref()?.get(row_idx)?;
        let mut map = HashMap::with_capacity(self.schema.len());
        for (i, field) in self.schema.iter().enumerate() {
            if let Some(value) = row.get(i) {
                map.insert(field.name.clone(), value.clone());
            }
        }
        Some(map)
    }

    /// Get all rows as HashMaps (column name → value).
    pub fn rows_as_maps(&self) -> Vec<HashMap<String, KalamCellValue>> {
        let Some(rows) = &self.rows else {
            return vec![];
        };
        (0..rows.len()).filter_map(|i| self.row_as_map(i)).collect()
    }

    /// Build `named_rows` from `schema` + `rows`, then clear positional `rows`.
    ///
    /// After this call the result contains only the named-column representation
    /// so SDKs can iterate `named_rows` directly instead of performing the
    /// schema → map transformation themselves.
    pub fn populate_named_rows(&mut self) {
        if self.named_rows.is_some() {
            return;
        }
        self.named_rows = Some(self.rows_as_maps());
        // Clear positional rows to avoid sending redundant data
        self.rows = None;
    }
}
