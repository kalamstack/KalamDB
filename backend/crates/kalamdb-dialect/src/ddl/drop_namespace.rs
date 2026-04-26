//! DROP NAMESPACE statement parser
//!
//! Parses SQL statements like:
//! - DROP NAMESPACE app
//! - DROP NAMESPACE IF EXISTS app
//! - DROP NAMESPACE app CASCADE
//! - DROP NAMESPACE IF EXISTS app CASCADE

use kalamdb_commons::models::NamespaceId;

use crate::ddl::{parsing, DdlResult};

/// DROP NAMESPACE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropNamespaceStatement {
    /// Namespace name to drop
    pub name: NamespaceId,

    /// If true, don't error if namespace doesn't exist
    pub if_exists: bool,

    /// If true, drop all tables in the namespace first
    pub cascade: bool,
}

impl DropNamespaceStatement {
    /// Parse a DROP NAMESPACE statement from SQL
    ///
    /// Supports syntax:
    /// - DROP NAMESPACE name
    /// - DROP NAMESPACE IF EXISTS name
    /// - DROP NAMESPACE name CASCADE
    /// - DROP NAMESPACE IF EXISTS name CASCADE
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();

        // Check for CASCADE at the end
        let cascade = tokens.last().map(|t| t.eq_ignore_ascii_case("CASCADE")).unwrap_or(false);

        // Remove CASCADE token if present for parsing the rest
        let sql_without_cascade = if cascade {
            tokens[..tokens.len() - 1].join(" ")
        } else {
            trimmed.to_string()
        };

        let (name, if_exists) = parsing::parse_create_drop_statement(
            &sql_without_cascade,
            "DROP NAMESPACE",
            "IF EXISTS",
        )?;

        Ok(Self {
            name: NamespaceId::new(name),
            if_exists,
            cascade,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_drop_namespace() {
        let stmt = DropNamespaceStatement::parse("DROP NAMESPACE app").unwrap();
        assert_eq!(stmt.name.as_str(), "app");
        assert!(!stmt.if_exists);
        assert!(!stmt.cascade);
    }

    #[test]
    fn test_parse_drop_namespace_if_exists() {
        let stmt = DropNamespaceStatement::parse("DROP NAMESPACE IF EXISTS app").unwrap();
        assert_eq!(stmt.name.as_str(), "app");
        assert!(stmt.if_exists);
        assert!(!stmt.cascade);
    }

    #[test]
    fn test_parse_drop_namespace_cascade() {
        let stmt = DropNamespaceStatement::parse("DROP NAMESPACE app CASCADE").unwrap();
        assert_eq!(stmt.name.as_str(), "app");
        assert!(!stmt.if_exists);
        assert!(stmt.cascade);
    }

    #[test]
    fn test_parse_drop_namespace_if_exists_cascade() {
        let stmt = DropNamespaceStatement::parse("DROP NAMESPACE IF EXISTS app CASCADE").unwrap();
        assert_eq!(stmt.name.as_str(), "app");
        assert!(stmt.if_exists);
        assert!(stmt.cascade);
    }

    #[test]
    fn test_parse_drop_namespace_cascade_lowercase() {
        let stmt = DropNamespaceStatement::parse("drop namespace myapp cascade").unwrap();
        assert_eq!(stmt.name.as_str(), "myapp");
        assert!(stmt.cascade);
    }

    #[test]
    fn test_parse_drop_namespace_lowercase() {
        let stmt = DropNamespaceStatement::parse("drop namespace myapp").unwrap();
        assert_eq!(stmt.name.as_str(), "myapp");
        assert!(!stmt.cascade);
    }

    #[test]
    fn test_parse_drop_namespace_missing_name() {
        let result = DropNamespaceStatement::parse("DROP NAMESPACE");
        assert!(result.is_err());
    }
}
