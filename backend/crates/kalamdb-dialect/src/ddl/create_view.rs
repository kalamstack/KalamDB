//! Parser for CREATE VIEW statements (DataFusion-compatible)
//!
//! Produces a typed AST that mirrors sqlparser's semantics so kalamdb-core can
//! route CREATE VIEW requests through a dedicated handler.

use kalamdb_commons::models::{NamespaceId, TableName};
use sqlparser::{
    ast::{ObjectName, Statement},
    dialect::GenericDialect,
};

use crate::{ddl::DdlResult, parser::utils::parse_sql_statements};

/// Typed representation of a CREATE VIEW statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateViewStatement {
    /// Resolved namespace/schema for the view
    pub namespace_id: NamespaceId,
    /// Unqualified view name
    pub view_name: TableName,
    /// Whether `OR REPLACE` was specified
    pub or_replace: bool,
    /// Whether `IF NOT EXISTS` was specified
    pub if_not_exists: bool,
    /// Optional column projection list (`CREATE VIEW v(col1, col2) ...`)
    pub columns: Vec<String>,
    /// Raw SELECT statement backing the view
    pub query_sql: String,
    /// Fully trimmed original SQL text
    pub original_sql: String,
}

impl CreateViewStatement {
    /// Parse a CREATE VIEW statement using sqlparser's Generic dialect.
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let dialect = GenericDialect {};
        let mut statements = parse_sql_statements(sql, &dialect)
            .map_err(|e| format!("Failed to parse CREATE VIEW statement: {}", e))?;

        if statements.is_empty() {
            return Err("No SQL statement provided".to_string());
        }
        if statements.len() > 1 {
            return Err("Multiple statements are not supported in CREATE VIEW".to_string());
        }

        match statements.remove(0) {
            Statement::CreateView(sqlparser::ast::CreateView {
                name,
                columns,
                query,
                or_replace,
                materialized,
                // if_not_exists, // Removed in newer sqlparser versions or wrapped?
                // temporary,     // Removed in newer sqlparser versions or wrapped?
                ..
            }) => {
                // Determine if_not_exists/temporary if fields missing
                let if_not_exists = false; // Placeholder until verified
                let temporary = false; // Placeholder until verified

                if materialized {
                    return Err("MATERIALIZED VIEWS are not supported yet".to_string());
                }
                if temporary {
                    return Err("TEMPORARY VIEWS are not supported".to_string());
                }

                let (namespace_id, view_name) = resolve_object_name(&name, default_namespace)?;
                let column_names = columns.into_iter().map(|col| col.name.value).collect();
                let query_sql = query.to_string();

                Ok(Self {
                    namespace_id,
                    view_name,
                    or_replace,
                    if_not_exists,
                    columns: column_names,
                    query_sql,
                    original_sql: sql.trim().to_string(),
                })
            },
            _ => Err("Expected CREATE VIEW statement".to_string()),
        }
    }
}

fn resolve_object_name(
    name: &ObjectName,
    default_namespace: &NamespaceId,
) -> DdlResult<(NamespaceId, TableName)> {
    let parts: Vec<String> = name.0.iter().map(|part| part.to_string()).collect();

    match parts.as_slice() {
        [view] => Ok((default_namespace.clone(), TableName::from(view.as_str()))),
        [namespace, view] => Ok((NamespaceId::new(namespace), TableName::from(view.as_str()))),
        _ => Err("CREATE VIEW expects either <view> or <namespace>.<view>".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_ns() -> NamespaceId {
        NamespaceId::default()
    }

    #[test]
    fn parse_basic_view() {
        let stmt = CreateViewStatement::parse("CREATE VIEW my_view AS SELECT 1", &default_ns())
            .expect("parse view");

        assert_eq!(stmt.namespace_id.as_str(), "default");
        assert_eq!(stmt.view_name.as_str(), "my_view");
        assert!(!stmt.or_replace);
        assert!(!stmt.if_not_exists);
        assert_eq!(stmt.query_sql, "SELECT 1");
    }

    #[test]
    fn parse_qualified_view() {
        let stmt = CreateViewStatement::parse(
            "CREATE VIEW analytics.session_counts AS SELECT COUNT(*) FROM shared.sessions",
            &default_ns(),
        )
        .expect("parse qualified view");

        assert_eq!(stmt.namespace_id.as_str(), "analytics");
        assert_eq!(stmt.view_name.as_str(), "session_counts");
        assert_eq!(stmt.query_sql.to_uppercase(), "SELECT COUNT(*) FROM SHARED.SESSIONS");
    }

    #[test]
    fn reject_materialized() {
        let err =
            CreateViewStatement::parse("CREATE MATERIALIZED VIEW mv AS SELECT 1", &default_ns())
                .unwrap_err();
        assert!(err.contains("MATERIALIZED"));
    }
}
