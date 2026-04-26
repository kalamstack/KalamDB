//! SHOW TABLES statement parser
//!
//! Parses SQL statements like:
//! - SHOW TABLES
//! - SHOW TABLES IN namespace

use kalamdb_commons::models::NamespaceId;

use crate::ddl::{parsing, DdlResult};

/// SHOW TABLES statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablesStatement {
    /// Optional namespace to filter tables
    pub namespace_id: Option<NamespaceId>,
}

impl ShowTablesStatement {
    /// Parse a SHOW TABLES statement from SQL
    ///
    /// Supports syntax:
    /// - SHOW TABLES
    /// - SHOW TABLES IN namespace
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let namespace = parsing::parse_optional_in_clause(sql, "SHOW TABLES")?;

        Ok(Self {
            namespace_id: namespace.map(NamespaceId::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_show_tables() {
        let stmt = ShowTablesStatement::parse("SHOW TABLES").unwrap();
        assert!(stmt.namespace_id.is_none());
    }

    #[test]
    fn test_parse_show_tables_in_namespace() {
        let stmt = ShowTablesStatement::parse("SHOW TABLES IN app").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "app");
    }

    #[test]
    fn test_parse_show_tables_lowercase() {
        let stmt = ShowTablesStatement::parse("show tables in myapp").unwrap();
        assert_eq!(stmt.namespace_id.unwrap().as_str(), "myapp");
    }

    #[test]
    fn test_parse_show_tables_missing_namespace() {
        let result = ShowTablesStatement::parse("SHOW TABLES IN");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_statement() {
        let result = ShowTablesStatement::parse("SHOW DATABASES");
        assert!(result.is_err());
    }
}
