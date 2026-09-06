use std::collections::BTreeMap;

use sqlparser::ast::{ColumnDef, ColumnOption, CreateTable};

use crate::{
    model::{Column, Table, TableKind},
    sql::{
        clean_identifier_token, eq_ci, normalize_ident_key, normalize_object_key,
        normalize_sql_fragment, word_spans,
    },
};

pub(super) fn table_from_create(
    create_table: CreateTable,
    kind_from_prefix: Option<TableKind>,
    mut options: BTreeMap<String, String>,
) -> Result<Table, String> {
    let name_sql = create_table.name.to_string();
    let key = normalize_object_key(&name_sql);
    let kind_from_option = options.get("TYPE").and_then(|value| TableKind::from_str(value));
    let kind = kind_from_prefix.or(kind_from_option);

    options.remove("TYPE");

    let mut columns = BTreeMap::new();
    let mut column_order = Vec::new();

    for column_def in &create_table.columns {
        let column = column_from_def(column_def)?;

        if columns.contains_key(&column.key) {
            return Err(format!("duplicate column {} in table {}", column.name_sql, name_sql));
        }

        column_order.push(column.key.clone());
        columns.insert(column.key.clone(), column);
    }

    let constraints = create_table
        .constraints
        .iter()
        .map(|constraint| normalize_sql_fragment(&constraint.to_string()))
        .collect::<Vec<_>>();

    Ok(Table {
        key,
        name_sql,
        kind,
        column_order,
        columns,
        constraints,
        options,
        indexes: BTreeMap::new(),
    })
}

fn column_from_def(column_def: &ColumnDef) -> Result<Column, String> {
    let name_sql = column_def.name.to_string();
    let key = normalize_ident_key(&column_def.name.value);
    let type_sql = normalize_sql_fragment(&column_def.data_type.to_string());
    let type_key = canonical_type_key(&type_sql)?;
    let mut not_null = false;
    let mut default_sql = None;
    let mut primary_key = false;
    let mut extra_options = Vec::new();

    for option_def in &column_def.options {
        match &option_def.option {
            ColumnOption::NotNull => not_null = true,
            ColumnOption::Null => not_null = false,
            ColumnOption::Default(expr) => {
                default_sql = Some(normalize_sql_fragment(&expr.to_string()));
            },
            ColumnOption::PrimaryKey(_) => {
                primary_key = true;
                not_null = true;
            },
            other => extra_options.push(normalize_sql_fragment(&other.to_string())),
        }
    }

    extra_options.sort();

    Ok(Column {
        key,
        name_sql,
        type_sql,
        type_key,
        not_null,
        default_sql,
        primary_key,
        extra_options,
        create_sql: normalize_sql_fragment(&column_def.to_string()),
    })
}

fn canonical_type_key(type_sql: &str) -> Result<String, String> {
    let mut normalized = normalize_sql_fragment(type_sql).to_ascii_uppercase();
    normalized = normalized.replace(" (", "(").replace("( ", "(").replace(" )", ")");
    normalized = normalized.replace(" ,", ",").replace(", ", ",");

    if normalized.starts_with("DECIMAL(") || normalized.starts_with("NUMERIC(") {
        return Ok(normalized);
    }

    if normalized.starts_with("EMBEDDING(") {
        let dim = normalized
            .trim_start_matches("EMBEDDING(")
            .trim_end_matches(')')
            .parse::<usize>()
            .map_err(|_| format!("invalid EMBEDDING type: {type_sql}"))?;

        if !(1..=8192).contains(&dim) {
            return Err(format!("EMBEDDING dimension out of range 1..=8192: {dim}"));
        }

        return Ok(format!("EMBEDDING({dim})"));
    }

    let canonical = match normalized.as_str() {
        "BOOLEAN" | "BOOL" => "BOOLEAN",
        "SMALLINT" | "INT2" => "SMALLINT",
        "INT" | "INTEGER" | "INT4" | "MEDIUMINT" => "INT",
        "BIGINT" | "INT8" => "BIGINT",
        "FLOAT" | "REAL" | "FLOAT4" => "FLOAT",
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT64" => "DOUBLE",
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" | "NVARCHAR" => "TEXT",
        "BYTES" | "BINARY" | "VARBINARY" | "BLOB" | "BYTEA" => "BYTES",
        "JSON" | "JSONB" => "JSON",
        "TIMESTAMP" | "DATE" | "DATETIME" | "TIME" => normalized.as_str(),
        "UUID" => "UUID",
        "FILE" => "FILE",
        other if other.starts_with("VARCHAR(") => "TEXT",
        other if other.starts_with("CHAR(") => "TEXT",
        other if other.starts_with("NVARCHAR(") => "TEXT",
        _ => {
            return Err(format!(
                "unsupported KalamDB data type: {type_sql}. Add it to canonical_type_key()."
            ));
        },
    };

    Ok(canonical.to_string())
}

pub(super) fn parse_create_namespace(raw_stmt: &str) -> Option<String> {
    let words = word_spans(raw_stmt);

    if words.len() < 3 || !eq_ci(words[0].text, "CREATE") {
        return None;
    }

    if !(eq_ci(words[1].text, "NAMESPACE") || eq_ci(words[1].text, "SCHEMA")) {
        return None;
    }

    let mut index = 2;

    if words.len() >= 5
        && eq_ci(words[2].text, "IF")
        && eq_ci(words[3].text, "NOT")
        && eq_ci(words[4].text, "EXISTS")
    {
        index = 5;
    }

    words.get(index).map(|word| clean_identifier_token(word.text))
}

pub(super) fn extract_kalam_table_kind(sql: &str) -> Option<TableKind> {
    let words = word_spans(sql);

    if words.len() < 3 || !eq_ci(words[0].text, "CREATE") || !eq_ci(words[2].text, "TABLE") {
        return None;
    }

    TableKind::from_str(words[1].text)
}

pub(super) fn remove_kalam_table_kind(sql: &str) -> String {
    let words = word_spans(sql);

    if words.len() < 3 || !eq_ci(words[0].text, "CREATE") || !eq_ci(words[2].text, "TABLE") {
        return sql.to_string();
    }

    if TableKind::from_str(words[1].text).is_none() {
        return sql.to_string();
    }

    let mut out = String::new();
    out.push_str(&sql[..words[1].start]);
    out.push_str(&sql[words[1].end..]);
    out
}
