//! CREATE NAMESPACE statement parser
//!
//! Parses SQL statements like:
//! - CREATE NAMESPACE app
//! - CREATE NAMESPACE IF NOT EXISTS app

use kalamdb_commons::models::NamespaceId;

use crate::ddl::{parsing, DdlResult};

/// CREATE NAMESPACE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateNamespaceStatement {
    /// Namespace name to create
    pub name: NamespaceId,

    /// If true, don't error if namespace already exists
    pub if_not_exists: bool,
}

impl CreateNamespaceStatement {
    /// Parse a CREATE NAMESPACE statement from SQL
    ///
    /// Supports syntax:
    /// - CREATE NAMESPACE name
    /// - CREATE NAMESPACE IF NOT EXISTS name
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let (name, if_not_exists) =
            parsing::parse_create_drop_statement(sql, "CREATE NAMESPACE", "IF NOT EXISTS")?;

        Ok(Self {
            name: NamespaceId::new(name),
            if_not_exists,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_namespace() {
        let stmt = CreateNamespaceStatement::parse("CREATE NAMESPACE app").unwrap();
        assert_eq!(stmt.name.as_str(), "app");
        assert!(!stmt.if_not_exists);
    }

    #[test]
    fn test_parse_create_namespace_if_not_exists() {
        let stmt = CreateNamespaceStatement::parse("CREATE NAMESPACE IF NOT EXISTS app").unwrap();
        assert_eq!(stmt.name.as_str(), "app");
        assert!(stmt.if_not_exists);
    }

    #[test]
    fn test_parse_create_namespace_lowercase() {
        let stmt = CreateNamespaceStatement::parse("create namespace myapp").unwrap();
        assert_eq!(stmt.name.as_str(), "myapp");
    }

    #[test]
    fn test_parse_create_namespace_missing_name() {
        let result = CreateNamespaceStatement::parse("CREATE NAMESPACE");
        assert!(result.is_err());
    }
}
