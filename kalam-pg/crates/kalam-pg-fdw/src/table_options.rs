use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::{NamespaceId, TableName};
use kalamdb_commons::{TableId, TableType};
use std::collections::BTreeMap;
use std::str::FromStr;

/// Parsed foreign-table options for the FDW relation layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    pub table_id: TableId,
    pub table_type: TableType,
}

impl TableOptions {
    /// Parse typed table options from raw FDW option pairs.
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self, KalamPgError> {
        let namespace = options.get("namespace").ok_or_else(|| {
            KalamPgError::Validation("table option 'namespace' is required".to_string())
        })?;
        let table = options.get("table").ok_or_else(|| {
            KalamPgError::Validation("table option 'table' is required".to_string())
        })?;
        let table_type = options.get("table_type").ok_or_else(|| {
            KalamPgError::Validation("table option 'table_type' is required".to_string())
        })?;
        let table_type = TableType::from_str(table_type).map_err(KalamPgError::Validation)?;

        Ok(Self {
            table_id: TableId::new(
                NamespaceId::new(namespace.clone()),
                TableName::new(table.clone()),
            ),
            table_type,
        })
    }
}
