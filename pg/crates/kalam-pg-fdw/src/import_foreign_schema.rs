use kalam_pg_common::{KalamPgError, DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_types::foreign_column_definition;
use kalamdb_commons::models::schemas::TableDefinition;
use kalamdb_commons::TableType;

/// Build the SQL statement used by `IMPORT FOREIGN SCHEMA` for a Kalam table.
pub fn create_foreign_table_sql(
    server_name: &str,
    foreign_schema: &str,
    table_definition: &TableDefinition,
) -> Result<String, KalamPgError> {
    // Filter out virtual/system columns that we append ourselves below.
    let virtual_names: &[&str] = &[USER_ID_COLUMN, SEQ_COLUMN, DELETED_COLUMN];

    let mut columns: Vec<String> = table_definition
        .columns
        .iter()
        .filter(|col| !virtual_names.contains(&col.column_name.as_str()))
        .map(foreign_column_definition)
        .collect::<Result<Vec<_>, _>>()?;

    if matches!(table_definition.table_type, TableType::User | TableType::Stream) {
        columns.push(format!("\"{}\" TEXT", USER_ID_COLUMN));
    }
    columns.push(format!("\"{}\" BIGINT", SEQ_COLUMN));
    columns.push(format!("\"{}\" BOOLEAN", DELETED_COLUMN));

    Ok(format!(
        "CREATE FOREIGN TABLE \"{}\".\"{}\" ({}) SERVER \"{}\" OPTIONS (namespace '{}', table '{}', table_type '{}')",
        quote_identifier(foreign_schema),
        quote_identifier(table_definition.table_name.as_str()),
        columns.join(", "),
        quote_identifier(server_name),
        escape_literal(table_definition.namespace_id.as_str()),
        escape_literal(table_definition.table_name.as_str()),
        table_definition.table_type.as_str(),
    ))
}

fn quote_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}
