/// Configuration for live-query row materialization.
#[derive(Debug, Clone, Default)]
pub struct LiveRowsConfig {
    /// Maximum number of rows to retain in the materialized result set.
    ///
    /// When set, the newest rows are kept and older rows are discarded.
    pub limit: Option<usize>,
    /// Column names that together identify a stable row.
    ///
    /// When omitted, live row materialization falls back to the `id` column.
    pub key_columns: Option<Vec<String>>,
}

impl LiveRowsConfig {
    pub(crate) fn normalized_key_columns(&self) -> Vec<String> {
        let mut normalized =
            Vec::with_capacity(self.key_columns.as_ref().map_or(1, |columns| columns.len().max(1)));

        if let Some(columns) = &self.key_columns {
            for column in columns {
                let trimmed = column.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if normalized.iter().any(|existing| existing == trimmed) {
                    continue;
                }

                normalized.push(trimmed.to_owned());
            }
        }

        if normalized.is_empty() {
            normalized.push("id".to_string());
        }

        normalized
    }
}
