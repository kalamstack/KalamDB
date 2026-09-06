//! Scalar `CREATE INDEX` / `DROP INDEX` parsing.
//!
//! Vector indexes stay on the no-parentheses `USING COSINE|L2|DOT` form in
//! [`super::alter_table`]. Parentheses select the scalar catalog path.

use kalamdb_commons::models::NamespaceId;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::ddl::{
    alter_table::{resolve_table_reference_from_str, ColumnOperation},
    AlterTableStatement, DdlResult,
};

static ALTER_CREATE_SCALAR_INDEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)^\s*ALTER\s+TABLE\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\s+CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][\w]*)\s*\(([^)]+)\)\s*;?\s*$",
    )
    .unwrap()
});

static ALTER_DROP_SCALAR_INDEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)^\s*ALTER\s+TABLE\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\s+DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][\w]*)\s*;?\s*$",
    )
    .unwrap()
});

static CREATE_INDEX_ON_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][\w]*)\s+ON\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\s*\(([^)]+)\)\s*;?\s*$",
    )
    .unwrap()
});

/// Parse `ALTER TABLE t CREATE INDEX name (cols)` / `DROP INDEX name`.
///
/// Returns `None` when the statement is not this form (including vector
/// `CREATE INDEX col USING COSINE` which has no parentheses).
pub fn parse_alter_table_scalar_index(
    sql: &str,
    current_namespace: &NamespaceId,
) -> DdlResult<Option<AlterTableStatement>> {
    let upper = sql.to_ascii_uppercase();
    if upper.contains("CREATE") && upper.contains(" INDEX") && sql.contains('(') {
        reject_vector_using(sql)?;
    }

    if let Some(caps) = ALTER_CREATE_SCALAR_INDEX_RE.captures(sql) {
        reject_vector_using(sql)?;
        let table_ref = caps
            .get(1)
            .map(|m| m.as_str())
            .ok_or_else(|| "missing table reference in CREATE INDEX".to_string())?;
        let unique = caps.get(2).is_some();
        let if_not_exists = sql.to_ascii_uppercase().contains("IF NOT EXISTS");
        let name = caps
            .get(3)
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| "missing index name in CREATE INDEX".to_string())?;
        let columns = parse_index_columns(
            caps.get(4)
                .map(|m| m.as_str())
                .ok_or_else(|| "missing column list in CREATE INDEX".to_string())?,
        )?;
        let (namespace_id, table_name) =
            resolve_table_reference_from_str(table_ref, current_namespace)?;
        return Ok(Some(AlterTableStatement {
            table_name,
            namespace_id,
            operation: ColumnOperation::CreateScalarIndex {
                name,
                columns,
                unique,
                if_not_exists,
            },
        }));
    }

    if sql.to_ascii_uppercase().contains("DROP VECTOR INDEX") {
        return Ok(None);
    }

    if let Some(caps) = ALTER_DROP_SCALAR_INDEX_RE.captures(sql) {
        let table_ref = caps
            .get(1)
            .map(|m| m.as_str())
            .ok_or_else(|| "missing table reference in DROP INDEX".to_string())?;
        let if_exists = sql.to_ascii_uppercase().contains("IF EXISTS");
        let name = caps
            .get(2)
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| "missing index name in DROP INDEX".to_string())?;
        let (namespace_id, table_name) =
            resolve_table_reference_from_str(table_ref, current_namespace)?;
        return Ok(Some(AlterTableStatement {
            table_name,
            namespace_id,
            operation: ColumnOperation::DropIndex { name, if_exists },
        }));
    }

    Ok(None)
}

/// Parse PostgreSQL-shaped `CREATE [UNIQUE] INDEX name ON table (cols)`.
pub fn parse_create_index_on(
    sql: &str,
    current_namespace: &NamespaceId,
) -> DdlResult<AlterTableStatement> {
    reject_vector_using(sql)?;
    let caps = CREATE_INDEX_ON_RE.captures(sql).ok_or_else(|| {
        "expected CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (columns)".to_string()
    })?;
    let unique = caps.get(1).is_some();
    let if_not_exists = sql.to_ascii_uppercase().contains("IF NOT EXISTS");
    let name = caps
        .get(2)
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| "missing index name in CREATE INDEX".to_string())?;
    let table_ref = caps
        .get(3)
        .map(|m| m.as_str())
        .ok_or_else(|| "missing table reference in CREATE INDEX".to_string())?;
    let columns = parse_index_columns(
        caps.get(4)
            .map(|m| m.as_str())
            .ok_or_else(|| "missing column list in CREATE INDEX".to_string())?,
    )?;
    let (namespace_id, table_name) =
        resolve_table_reference_from_str(table_ref, current_namespace)?;
    Ok(AlterTableStatement {
        table_name,
        namespace_id,
        operation: ColumnOperation::CreateScalarIndex {
            name,
            columns,
            unique,
            if_not_exists,
        },
    })
}

fn parse_index_columns(list: &str) -> DdlResult<Vec<String>> {
    let columns: Vec<String> = list
        .split(',')
        .map(|part| part.trim().trim_matches('"').trim_matches('`').to_string())
        .filter(|part| !part.is_empty())
        .collect();
    if columns.is_empty() {
        return Err("CREATE INDEX requires at least one column".to_string());
    }
    for column in &columns {
        if !column.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            || !column.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(format!("invalid index column name '{column}'"));
        }
    }
    let mut seen = Vec::with_capacity(columns.len());
    for column in &columns {
        if seen.iter().any(|existing: &String| existing.eq_ignore_ascii_case(column)) {
            return Err(format!("duplicate column '{column}' in CREATE INDEX"));
        }
        seen.push(column.clone());
    }
    Ok(columns)
}

fn reject_vector_using(sql: &str) -> DdlResult<()> {
    if sql.to_ascii_uppercase().contains(" USING ") {
        Err("scalar CREATE INDEX cannot mix USING COSINE/L2/DOT; that form is vector-only"
            .to_string())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;

    fn ns() -> NamespaceId {
        NamespaceId::new("public")
    }

    #[test]
    fn parse_alter_create_scalar_index() {
        let stmt = parse_alter_table_scalar_index(
            "ALTER TABLE app.messages CREATE INDEX idx_conv (conversation_id);",
            &ns(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(stmt.namespace_id, NamespaceId::new("app"));
        assert_eq!(stmt.table_name, TableName::new("messages"));
        match stmt.operation {
            ColumnOperation::CreateScalarIndex {
                name,
                columns,
                unique,
                if_not_exists,
            } => {
                assert_eq!(name, "idx_conv");
                assert_eq!(columns, vec!["conversation_id".to_string()]);
                assert!(!unique);
                assert!(!if_not_exists);
            },
            other => panic!("expected CreateScalarIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_index_on_unique_if_not_exists() {
        let stmt = parse_create_index_on(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_email ON users (email, status)",
            &ns(),
        )
        .unwrap();
        match stmt.operation {
            ColumnOperation::CreateScalarIndex {
                name,
                columns,
                unique,
                if_not_exists,
            } => {
                assert_eq!(name, "idx_email");
                assert_eq!(columns, vec!["email".to_string(), "status".to_string()]);
                assert!(unique);
                assert!(if_not_exists);
            },
            other => panic!("expected CreateScalarIndex, got {other:?}"),
        }
    }

    #[test]
    fn parentheses_required_for_scalar_alter() {
        assert!(parse_alter_table_scalar_index("ALTER TABLE docs CREATE INDEX embedding", &ns())
            .unwrap()
            .is_none());
    }

    #[test]
    fn mixing_using_metric_errors() {
        let err = parse_alter_table_scalar_index(
            "ALTER TABLE t CREATE INDEX idx (embedding) USING COSINE",
            &ns(),
        )
        .unwrap_err();
        assert!(err.contains("USING"));
    }
}
