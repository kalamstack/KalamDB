//! DESCRIBE TABLE statement parser
//!
//! Parses SQL statements like:
//! - DESCRIBE TABLE table_name
//! - DESCRIBE TABLE namespace.table_name
//! - DESC TABLE table_name
//! - DESCRIBE TABLE table_name HISTORY (show schema versions)

use kalamdb_commons::models::{NamespaceId, TableName};

use crate::ddl::DdlResult;

/// DESCRIBE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTableStatement {
    /// Optional namespace (if qualified name used)
    pub namespace_id: Option<NamespaceId>,

    /// Table name to describe
    pub table_name: TableName,

    /// If true, show schema history (all versions)
    pub show_history: bool,
}

impl DescribeTableStatement {
    /// Parse a DESCRIBE TABLE statement from SQL
    ///
    /// Supports syntax:
    /// - DESCRIBE TABLE table_name
    /// - DESCRIBE TABLE namespace.table_name
    /// - DESC TABLE table_name (shorthand)
    /// - DESCRIBE TABLE table_name HISTORY
    pub fn parse(sql: &str) -> DdlResult<Self> {
        use crate::ddl::parsing;

        let sql_trimmed = sql.trim().trim_end_matches(';');
        let sql_upper = sql_trimmed.to_uppercase();

        // Determine which prefix is used
        let prefix_len = if sql_upper.starts_with("DESCRIBE TABLE ") {
            15
        } else if sql_upper.starts_with("DESC TABLE ") {
            11
        } else {
            return Err("Expected DESCRIBE TABLE or DESC TABLE".to_string());
        };

        // Extract remainder after prefix
        let remainder = sql_trimmed[prefix_len..].trim();

        // Check for HISTORY keyword at the end
        let remainder_upper = remainder.to_uppercase();
        let (table_ref, show_history) = if remainder_upper.ends_with(" HISTORY") {
            (remainder[..remainder.len() - 8].trim(), true)
        } else {
            (remainder, false)
        };

        let (namespace, table) = parsing::parse_table_reference(table_ref)?;

        Ok(Self {
            namespace_id: namespace.map(|ns| NamespaceId::new(&ns)),
            table_name: TableName::new(&table),
            show_history,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_describe_table() {
        let stmt = DescribeTableStatement::parse("DESCRIBE TABLE users").unwrap();
        assert!(stmt.namespace_id.is_none());
        assert_eq!(stmt.table_name.as_str(), "users");
    }

    #[test]
    fn test_parse_describe_table_qualified() {
        let stmt = DescribeTableStatement::parse("DESCRIBE TABLE app.users").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "app");
        assert_eq!(stmt.table_name.as_str(), "users");
    }

    #[test]
    fn test_parse_desc_table() {
        let stmt = DescribeTableStatement::parse("DESC TABLE users").unwrap();
        assert!(stmt.namespace_id.is_none());
        assert_eq!(stmt.table_name.as_str(), "users");
    }

    #[test]
    fn test_parse_describe_table_lowercase() {
        let stmt = DescribeTableStatement::parse("describe table myapp.messages").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "myapp");
        assert_eq!(stmt.table_name.as_str(), "messages");
    }

    #[test]
    fn test_parse_describe_table_missing_name() {
        let result = DescribeTableStatement::parse("DESCRIBE TABLE");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_qualified_name() {
        let result = DescribeTableStatement::parse("DESCRIBE TABLE a.b.c");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_statement() {
        let result = DescribeTableStatement::parse("DESCRIBE NAMESPACE app");
        assert!(result.is_err());
    }

    #[test]
    fn test_describe_table_history() {
        let stmt = DescribeTableStatement::parse("DESCRIBE TABLE users HISTORY").unwrap();
        assert!(stmt.namespace_id.is_none());
        assert_eq!(stmt.table_name.as_str(), "users");
        assert!(stmt.show_history);
    }

    #[test]
    fn test_desc_table_history() {
        let stmt = DescribeTableStatement::parse("DESC TABLE users HISTORY").unwrap();
        assert!(stmt.namespace_id.is_none());
        assert_eq!(stmt.table_name.as_str(), "users");
        assert!(stmt.show_history);
    }

    #[test]
    fn test_describe_qualified_table_history() {
        let stmt = DescribeTableStatement::parse("DESCRIBE TABLE myapp.messages HISTORY").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "myapp");
        assert_eq!(stmt.table_name.as_str(), "messages");
        assert!(stmt.show_history);
    }

    #[test]
    fn test_describe_table_no_history() {
        let stmt = DescribeTableStatement::parse("DESCRIBE TABLE users").unwrap();
        assert!(!stmt.show_history);
    }
}
