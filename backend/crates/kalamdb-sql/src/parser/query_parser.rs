//! SQL query parsing utilities for live queries and subscriptions
//!
//! Uses sqlparser-rs for safe SQL parsing to prevent SQL injection attacks
//! and ensure proper handling of edge cases.

use crate::parser::utils::parse_sql_statements;
use kalamdb_commons::constants::SystemColumnNames;
use sqlparser::ast::{
    Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;

/// Error type for query parsing
#[derive(Debug, Clone)]
pub enum QueryParseError {
    /// Failed to parse SQL
    ParseError(String),
    /// Invalid SQL for the operation
    InvalidSql(String),
}

impl std::fmt::Display for QueryParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::InvalidSql(msg) => write!(f, "Invalid SQL: {}", msg),
        }
    }
}

impl std::error::Error for QueryParseError {}

/// Utilities for parsing SQL queries for live subscriptions
pub struct QueryParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionQueryAnalysis {
    pub table_name: String,
    pub where_clause: Option<String>,
    pub projections: Option<Vec<String>>,
}

impl QueryParser {
    /// Parse and validate a subscription query once, returning the extracted parts.
    pub fn analyze_subscription_query(
        query: &str,
    ) -> Result<SubscriptionQueryAnalysis, QueryParseError> {
        let dialect = GenericDialect {};
        let statements = parse_sql_statements(query, &dialect)
            .map_err(|e| QueryParseError::ParseError(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Err(QueryParseError::InvalidSql("Empty SQL statement".to_string()));
        }

        if statements.len() != 1 {
            return Err(QueryParseError::InvalidSql(
                "Subscription query must contain exactly one SELECT statement".to_string(),
            ));
        }

        match &statements[0] {
            Statement::Query(query) => Self::analyze_subscription_query_ast(query),
            _ => Err(QueryParseError::InvalidSql(
                "Only SELECT queries are supported for subscriptions".to_string(),
            )),
        }
    }

    /// Validate that a subscription query uses only supported clauses.
    ///
    /// Supported shape:
    /// `SELECT <columns>|* FROM namespace.table [WHERE ...]`
    pub fn validate_subscription_query(query: &str) -> Result<(), QueryParseError> {
        Self::analyze_subscription_query(query).map(|_| ())
    }

    /// Extract table name from SQL query using sqlparser-rs
    ///
    /// This uses proper SQL parsing to safely extract the table name,
    /// preventing SQL injection attacks and handling edge cases like:
    /// - Quoted identifiers
    /// - Schema-qualified names (schema.table)
    /// - Complex FROM clauses
    ///
    /// # Security
    /// Uses sqlparser-rs instead of string manipulation to prevent
    /// SQL injection attacks in live query subscriptions.
    pub fn extract_table_name(query: &str) -> Result<String, QueryParseError> {
        Self::analyze_subscription_query(query).map(|analysis| analysis.table_name)
    }

    /// Extract table name from a parsed Query AST
    fn extract_table_from_query(query: &Query) -> Result<String, QueryParseError> {
        match query.body.as_ref() {
            SetExpr::Select(select) => Self::extract_table_from_select(select),
            _ => Err(QueryParseError::InvalidSql(
                "Unsupported query type for subscriptions".to_string(),
            )),
        }
    }

    /// Extract table name from a SELECT statement
    fn extract_table_from_select(select: &Select) -> Result<String, QueryParseError> {
        if select.from.is_empty() {
            return Err(QueryParseError::InvalidSql("No FROM clause in SELECT".to_string()));
        }

        let table_with_joins = &select.from[0];
        Self::extract_table_from_table_with_joins(table_with_joins)
    }

    /// Extract table name from TableWithJoins
    fn extract_table_from_table_with_joins(
        table_with_joins: &TableWithJoins,
    ) -> Result<String, QueryParseError> {
        match &table_with_joins.relation {
            TableFactor::Table { name, .. } => {
                use sqlparser::ast::ObjectNamePart;
                // Handle schema-qualified names (namespace.table)
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .filter_map(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .collect();

                Ok(parts.join("."))
            },
            _ => Err(QueryParseError::InvalidSql(
                "Unsupported table type in FROM clause".to_string(),
            )),
        }
    }

    /// Extract WHERE clause from SQL query
    ///
    /// Returns the WHERE clause as a string for filter matching.
    /// Uses sqlparser-rs to safely parse and extract the clause.
    pub fn extract_where_clause(query: &str) -> Result<Option<String>, QueryParseError> {
        Self::analyze_subscription_query(query).map(|analysis| analysis.where_clause)
    }

    /// Extract WHERE clause from a parsed Query AST
    fn extract_where_from_query(query: &Query) -> Result<Option<String>, QueryParseError> {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                Ok(select.selection.as_ref().map(|expr| format!("{}", expr)))
            },
            _ => Ok(None),
        }
    }

    /// Extract projection columns from SQL query
    ///
    /// Returns a list of column names requested in the SELECT clause.
    /// Returns None for SELECT * queries.
    pub fn extract_projections(query: &str) -> Result<Option<Vec<String>>, QueryParseError> {
        Self::analyze_subscription_query(query).map(|analysis| analysis.projections)
    }

    /// Resolve placeholders like CURRENT_USER() in WHERE clause
    ///
    /// SECURITY: Escapes single quotes in user_id to prevent SQL injection.
    /// A malicious user_id like `foo' OR 1=1 --` would be escaped to
    /// `foo'' OR 1=1 --`, producing a safe string literal.
    pub fn resolve_where_clause_placeholders(
        where_clause: &str,
        user_id: &kalamdb_commons::models::UserId,
    ) -> String {
        let escaped_user_id = user_id.as_str().replace('\'', "''");
        where_clause.replace("CURRENT_USER()", &format!("'{}'", escaped_user_id))
    }

    /// Extract projection columns from a parsed Query AST
    fn extract_projections_from_query(
        query: &Query,
    ) -> Result<Option<Vec<String>>, QueryParseError> {
        match query.body.as_ref() {
            SetExpr::Select(select) => Self::extract_projections_from_select(select),
            _ => Ok(None),
        }
    }

    /// Extract projections from a SELECT statement
    fn extract_projections_from_select(
        select: &Select,
    ) -> Result<Option<Vec<String>>, QueryParseError> {
        let mut columns = Vec::new();
        let mut has_wildcard = false;

        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    has_wildcard = true;
                    break;
                },
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(Self::expr_to_column_name(expr));
                },
                SelectItem::ExprWithAlias { expr: _, alias } => {
                    columns.push(alias.value.clone());
                },
                SelectItem::QualifiedWildcard(_, _) => {
                    has_wildcard = true;
                    break;
                },
            }
        }

        if has_wildcard {
            Ok(None) // SELECT * or table.* returns None
        } else {
            Ok(Some(columns))
        }
    }

    /// Convert an expression to a column name string
    fn expr_to_column_name(expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => {
                parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join(".")
            },
            _ => format!("{}", expr),
        }
    }

    /// Parse parameterized WHERE clause expression
    ///
    /// Converts expressions like "user_id = $1" into a structured format
    /// for parameter substitution.
    pub fn parse_parameterized_expr(
        expr_str: &str,
    ) -> Result<(String, Vec<String>), QueryParseError> {
        let dialect = GenericDialect {};

        // Wrap in a dummy SELECT to parse the expression
        let dummy_query = format!("SELECT * FROM dummy WHERE {}", expr_str);
        let statements = parse_sql_statements(&dummy_query, &dialect).map_err(|e| {
            QueryParseError::ParseError(format!("Failed to parse expression: {}", e))
        })?;

        if statements.is_empty() {
            return Err(QueryParseError::InvalidSql("Failed to parse expression".to_string()));
        }

        // Extract parameters ($1, $2, etc.)
        let mut params = Vec::new();
        Self::extract_parameters_from_expr_str(expr_str, &mut params);

        Ok((expr_str.to_string(), params))
    }

    /// Extract parameter placeholders from expression string
    fn extract_parameters_from_expr_str(expr_str: &str, params: &mut Vec<String>) {
        let re = regex::Regex::new(r"\$\d+").unwrap();
        for cap in re.captures_iter(expr_str) {
            if let Some(param) = cap.get(0) {
                let param_str = param.as_str().to_string();
                if !params.contains(&param_str) {
                    params.push(param_str);
                }
            }
        }
    }

    pub(crate) fn analyze_subscription_query_ast(
        query: &Query,
    ) -> Result<SubscriptionQueryAnalysis, QueryParseError> {
        Self::validate_subscription_query_ast(query)?;

        Ok(SubscriptionQueryAnalysis {
            table_name: Self::extract_table_from_query(query)?,
            where_clause: Self::extract_where_from_query(query)?,
            projections: Self::extract_projections_from_query(query)?,
        })
    }

    fn validate_subscription_query_ast(query: &Query) -> Result<(), QueryParseError> {
        if query.with.is_some() {
            return Err(QueryParseError::InvalidSql(
                "Subscription query does not support WITH clauses. Only SELECT ... FROM ... [WHERE ...] is supported.".to_string(),
            ));
        }

        if query.order_by.is_some() {
            return Err(QueryParseError::InvalidSql(
                "Subscription query does not support ORDER BY. Only SELECT ... FROM ... [WHERE ...] is supported.".to_string(),
            ));
        }

        if query.limit_clause.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Err(QueryParseError::InvalidSql(
                "Subscription query supports only SELECT ... FROM ... [WHERE ...].".to_string(),
            ));
        }

        match query.body.as_ref() {
            SetExpr::Select(select) => Self::validate_subscription_select(select),
            _ => Err(QueryParseError::InvalidSql(
                "Subscription query supports only simple SELECT statements.".to_string(),
            )),
        }
    }

    fn validate_subscription_select(select: &Select) -> Result<(), QueryParseError> {
        if select.distinct.is_some()
            || select.select_modifiers.is_some()
            || select.top.is_some()
            || select.exclude.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
        {
            return Err(QueryParseError::InvalidSql(
                "Subscription query supports only SELECT ... FROM ... [WHERE ...].".to_string(),
            ));
        }

        if Self::has_group_by(&select.group_by) {
            return Err(QueryParseError::InvalidSql(
                "Subscription query does not support GROUP BY. Only SELECT ... FROM ... [WHERE ...] is supported.".to_string(),
            ));
        }

        if select.from.len() != 1 {
            return Err(QueryParseError::InvalidSql(
                "Subscription query requires exactly one table in the FROM clause.".to_string(),
            ));
        }

        let table_with_joins = &select.from[0];
        if !table_with_joins.joins.is_empty() {
            return Err(QueryParseError::InvalidSql(
                "Subscription query does not support JOIN clauses. Only SELECT ... FROM ... [WHERE ...] is supported.".to_string(),
            ));
        }

        match &table_with_joins.relation {
            TableFactor::Table { .. } => {},
            _ => {
                return Err(QueryParseError::InvalidSql(
                    "Subscription query requires a direct table reference in FROM.".to_string(),
                ));
            },
        }

        Self::validate_subscription_projection_items(&select.projection)
    }

    fn validate_subscription_projection_items(
        projection: &[SelectItem],
    ) -> Result<(), QueryParseError> {
        for item in projection {
            let column_name = match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => continue,
                SelectItem::UnnamedExpr(expr) => Some(Self::expr_to_column_name(expr)),
                SelectItem::ExprWithAlias { expr, .. } => Some(Self::expr_to_column_name(expr)),
            };

            if let Some(column_name) = column_name {
                let base_name = column_name.rsplit('.').next().unwrap_or(&column_name);
                if base_name.eq_ignore_ascii_case(SystemColumnNames::SEQ)
                    || base_name.eq_ignore_ascii_case(SystemColumnNames::DELETED)
                {
                    return Err(QueryParseError::InvalidSql(format!(
                        "Subscription query cannot project system columns '{}' or '{}'.",
                        SystemColumnNames::SEQ,
                        SystemColumnNames::DELETED
                    )));
                }
            }
        }

        Ok(())
    }

    fn has_group_by(group_by: &GroupByExpr) -> bool {
        match group_by {
            GroupByExpr::All(_) => true,
            GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name_simple() {
        let result = QueryParser::extract_table_name("SELECT * FROM users");
        assert_eq!(result.unwrap(), "users");
    }

    #[test]
    fn test_extract_table_name_qualified() {
        let result = QueryParser::extract_table_name("SELECT * FROM chat.messages");
        assert_eq!(result.unwrap(), "chat.messages");
    }

    #[test]
    fn test_extract_where_clause() {
        let result = QueryParser::extract_where_clause("SELECT * FROM users WHERE id = 1").unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().contains("id"));
    }

    #[test]
    fn test_extract_projections_wildcard() {
        let result = QueryParser::extract_projections("SELECT * FROM users").unwrap();
        assert!(result.is_none()); // Wildcard returns None
    }

    #[test]
    fn test_extract_projections_specific() {
        let result = QueryParser::extract_projections("SELECT id, name, email FROM users").unwrap();
        assert!(result.is_some());
        let cols = result.unwrap();
        assert_eq!(cols, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_validate_subscription_query_accepts_select_where() {
        QueryParser::validate_subscription_query(
            "SELECT id, name FROM chat.messages WHERE conversation_id = 1",
        )
        .unwrap();
    }

    #[test]
    fn test_validate_subscription_query_rejects_order_by() {
        let err = QueryParser::validate_subscription_query(
            "SELECT id FROM chat.messages WHERE conversation_id = 1 ORDER BY id",
        )
        .unwrap_err();

        assert!(err.to_string().contains("ORDER BY"));
    }

    #[test]
    fn test_validate_subscription_query_rejects_group_by() {
        let err = QueryParser::validate_subscription_query(
            "SELECT user_id FROM chat.messages GROUP BY user_id",
        )
        .unwrap_err();

        assert!(err.to_string().contains("GROUP BY"));
    }

    #[test]
    fn test_validate_subscription_query_rejects_seq_projection() {
        let err = QueryParser::validate_subscription_query("SELECT _seq FROM chat.messages")
            .unwrap_err();

        assert!(err.to_string().contains(SystemColumnNames::SEQ));
    }

    #[test]
    fn test_validate_subscription_query_rejects_deleted_projection() {
        let err = QueryParser::validate_subscription_query(
            "SELECT chat.messages._deleted FROM chat.messages",
        )
        .unwrap_err();

        assert!(err.to_string().contains(SystemColumnNames::DELETED));
    }

    #[test]
    fn test_analyze_subscription_query_extracts_all_parts() {
        let analysis = QueryParser::analyze_subscription_query(
            "SELECT id, name FROM chat.messages WHERE conversation_id = 1",
        )
        .unwrap();

        assert_eq!(analysis.table_name, "chat.messages");
        assert_eq!(analysis.where_clause, Some("conversation_id = 1".to_string()));
        assert_eq!(analysis.projections, Some(vec!["id".to_string(), "name".to_string()]));
    }
}
