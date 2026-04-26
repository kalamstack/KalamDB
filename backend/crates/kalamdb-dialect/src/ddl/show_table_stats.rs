//! SHOW TABLE STATS statement parser
//!
//! Parses SQL statements like:
//! - SHOW STATS FOR TABLE table_name
//! - SHOW STATS FOR TABLE namespace.table_name

use kalamdb_commons::models::{NamespaceId, TableName};

use crate::ddl::DdlResult;

/// SHOW TABLE STATS statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTableStatsStatement {
    /// Optional namespace (if qualified name used)
    pub namespace_id: Option<NamespaceId>, // TODO: consider making this mandatory

    /// Table name to show statistics for
    pub table_name: TableName,
}

impl ShowTableStatsStatement {
    /// Parse a SHOW TABLE STATS statement from SQL
    ///
    /// Supports syntax:
    /// - SHOW STATS FOR TABLE table_name
    /// - SHOW STATS FOR TABLE namespace.table_name
    pub fn parse(sql: &str) -> DdlResult<Self> {
        use crate::ddl::parsing;

        let table_ref = parsing::extract_after_prefix(sql, "SHOW STATS FOR TABLE")?;
        let (namespace, table) = parsing::parse_table_reference(&table_ref)?;

        Ok(Self {
            namespace_id: namespace.map(|ns| NamespaceId::new(&ns)),
            table_name: TableName::new(&table),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_show_stats() {
        let stmt = ShowTableStatsStatement::parse("SHOW STATS FOR TABLE users").unwrap();
        assert!(stmt.namespace_id.is_none());
        assert_eq!(stmt.table_name.as_str(), "users");
    }

    #[test]
    fn test_parse_show_stats_qualified() {
        let stmt = ShowTableStatsStatement::parse("SHOW STATS FOR TABLE app.users").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "app");
        assert_eq!(stmt.table_name.as_str(), "users");
    }

    #[test]
    fn test_parse_show_stats_lowercase() {
        let stmt = ShowTableStatsStatement::parse("show stats for table myapp.messages").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "myapp");
        assert_eq!(stmt.table_name.as_str(), "messages");
    }

    #[test]
    fn test_parse_show_stats_missing_name() {
        let result = ShowTableStatsStatement::parse("SHOW STATS FOR TABLE");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_qualified_name() {
        let result = ShowTableStatsStatement::parse("SHOW STATS FOR TABLE a.b.c");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_statement() {
        let result = ShowTableStatsStatement::parse("SHOW STATISTICS FOR TABLE users");
        assert!(result.is_err());
    }
}
