use kalamdb_commons::{TableId, TableType};

/// Parsed foreign-table options for the FDW relation layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    pub table_id: TableId,
    pub table_type: TableType,
}
