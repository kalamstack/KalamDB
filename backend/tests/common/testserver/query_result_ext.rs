use kalam_link::models::QueryResult;
use kalam_link::KalamCellValue;
use std::collections::HashMap;

/// Extension trait for `QueryResult` to provide test-friendly row access.
#[allow(dead_code)]
pub trait QueryResultTestExt {
    fn row_as_map(&self, row_idx: usize) -> Option<HashMap<String, KalamCellValue>>;
    fn rows_as_maps(&self) -> Vec<HashMap<String, KalamCellValue>>;
}

impl QueryResultTestExt for QueryResult {
    fn row_as_map(&self, row_idx: usize) -> Option<HashMap<String, KalamCellValue>> {
        let row = self.rows.as_ref()?.get(row_idx)?;
        let mut map = HashMap::new();
        for (i, field) in self.schema.iter().enumerate() {
            if let Some(value) = row.get(i) {
                map.insert(field.name.clone(), value.clone());
            }
        }
        Some(map)
    }

    fn rows_as_maps(&self) -> Vec<HashMap<String, KalamCellValue>> {
        let Some(rows) = &self.rows else {
            return vec![];
        };
        rows.iter().enumerate().filter_map(|(i, _)| self.row_as_map(i)).collect()
    }
}
