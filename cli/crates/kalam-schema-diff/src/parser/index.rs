use kalamdb_commons::models::NamespaceId;
use kalamdb_sql::ddl::{parse_create_index_on, AlterTableStatement, ColumnOperation};

use crate::{
    diff::SchemaDiffError,
    model::{Schema, TableIndex, TableIndexKind},
    sql::{eq_ci, normalize_ident_key, normalize_object_key, word_spans},
};

pub(super) struct PendingIndex {
    pub(super) table_sql: String,
    pub(super) table_key: String,
    pub(super) index:     TableIndex,
}

pub(super) fn is_index_ddl(sql: &str) -> bool {
    let words: Vec<&str> = word_spans(sql).into_iter().map(|word| word.text).collect();
    match words.as_slice() {
        ["CREATE", "INDEX", ..] | ["CREATE", "UNIQUE", "INDEX", ..] => true,
        ["ALTER", "TABLE", ..] => words.iter().any(|word| eq_ci(word, "INDEX")),
        _ => false,
    }
}

pub(super) fn parse_index_ddl(path: &str, sql: &str) -> Result<PendingIndex, SchemaDiffError> {
    let namespace = NamespaceId::new("public");
    let upper = sql.trim_start().to_ascii_uppercase();
    let stmt = if upper.starts_with("CREATE") {
        parse_create_index_on(sql, &namespace).map_err(|err| index_parse_error(path, &err))?
    } else {
        AlterTableStatement::parse(sql, &namespace).map_err(|err| index_parse_error(path, &err))?
    };

    let table_sql = format!("{}.{}", stmt.namespace_id.as_str(), stmt.table_name.as_str());
    let table_key = normalize_object_key(&table_sql);
    let index = table_index_from_operation(path, sql, &stmt.operation)?;
    Ok(PendingIndex {
        table_sql,
        table_key,
        index,
    })
}

pub(super) fn attach_indexes(
    path: &str,
    schema: &mut Schema,
    pending: Vec<PendingIndex>,
) -> Result<(), SchemaDiffError> {
    for item in pending {
        let resolved_key = resolve_table_key(schema, &item.table_key).ok_or_else(|| {
            index_parse_error(
                path,
                &format!("index {} references unknown table {}", item.index.name, item.table_sql),
            )
        })?;
        let table = schema.tables.get_mut(&resolved_key).ok_or_else(|| {
            index_parse_error(path, &format!("index {} references unknown table", item.index.name))
        })?;
        let key = normalize_ident_key(&item.index.name);
        if table.indexes.contains_key(&key) {
            return Err(index_parse_error(
                path,
                &format!("duplicate index {} on table {}", item.index.name, item.table_sql),
            ));
        }
        table.indexes.insert(key, item.index);
    }
    Ok(())
}

fn resolve_table_key(schema: &Schema, table_key: &str) -> Option<String> {
    if schema.tables.contains_key(table_key) {
        return Some(table_key.to_string());
    }
    let short = table_key.rsplit('.').next().unwrap_or(table_key);
    if schema.tables.contains_key(short) {
        return Some(short.to_string());
    }
    None
}

fn table_index_from_operation(
    path: &str,
    sql: &str,
    operation: &ColumnOperation,
) -> Result<TableIndex, SchemaDiffError> {
    match operation {
        ColumnOperation::CreateScalarIndex {
            name,
            columns,
            unique,
            ..
        } => Ok(TableIndex {
            name:    name.clone(),
            columns: columns.clone(),
            unique:  *unique,
            kind:    TableIndexKind::Scalar,
        }),
        ColumnOperation::CreateVectorIndex {
            column_name,
            metric,
        } => Ok(TableIndex {
            name:    column_name.clone(),
            columns: vec![column_name.clone()],
            unique:  false,
            kind:    TableIndexKind::Vector {
                metric: format!("{metric:?}").to_ascii_uppercase(),
            },
        }),
        ColumnOperation::DropIndex { .. } | ColumnOperation::DropVectorIndex { .. } => {
            Err(index_parse_error(
                path,
                "DROP INDEX is not valid in schema.sql; omit the CREATE INDEX instead",
            ))
        },
        _ => Err(index_parse_error(path, &format!("unsupported index statement:\n{sql}"))),
    }
}

fn index_parse_error(path: &str, message: &str) -> SchemaDiffError {
    SchemaDiffError::Parse {
        message: format!("{path}: {message}"),
    }
}
