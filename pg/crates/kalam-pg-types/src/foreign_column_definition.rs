use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::schemas::ColumnDefinition;

use crate::pg_type_name::pg_type_name_for;

/// Build a PostgreSQL column definition for a foreign table column.
pub fn foreign_column_definition(column: &ColumnDefinition) -> Result<String, KalamPgError> {
    let type_name = pg_type_name_for(&column.data_type)?;
    let mut definition = format!("\"{}\" {}", column.column_name, type_name);

    if !column.is_nullable && !column.is_primary_key {
        definition.push_str(" NOT NULL");
    }

    Ok(definition)
}
