//! USE NAMESPACE statement parser
//!
//! Parses SQL statements like:
//! - USE namespace_name
//! - USE NAMESPACE namespace_name
//! - SET NAMESPACE namespace_name
//!
//! This sets the default schema for the current session using DataFusion's
//! native configuration: `datafusion.catalog.default_schema`

use kalamdb_commons::models::NamespaceId;

use crate::ddl::DdlResult;

/// USE NAMESPACE statement
///
/// Changes the default schema for unqualified table names in the current session.
/// After executing `USE namespace1`, a query like `SELECT * FROM users` will
/// resolve to `kalam.namespace1.users`.
#[derive(Debug, Clone, PartialEq)]
pub struct UseNamespaceStatement {
    /// Namespace name to switch to
    pub namespace: NamespaceId,
}

impl UseNamespaceStatement {
    /// Parse a USE NAMESPACE statement from SQL
    ///
    /// Supports syntax:
    /// - USE namespace_name
    /// - USE NAMESPACE namespace_name
    /// - SET NAMESPACE namespace_name
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let sql_upper = sql.trim().to_uppercase();
        let words: Vec<&str> = sql.split_whitespace().collect();

        // Match patterns: USE <name>, USE NAMESPACE <name>, SET NAMESPACE <name>
        let namespace_name = if sql_upper.starts_with("USE NAMESPACE") {
            // USE NAMESPACE namespace_name
            if words.len() < 3 {
                return Err("USE NAMESPACE requires a namespace name".to_string());
            }
            words[2]
        } else if sql_upper.starts_with("SET NAMESPACE") {
            // SET NAMESPACE namespace_name
            if words.len() < 3 {
                return Err("SET NAMESPACE requires a namespace name".to_string());
            }
            words[2]
        } else if sql_upper.starts_with("USE ") {
            // USE namespace_name (shorthand) - only when not followed by NAMESPACE keyword
            if words.len() < 2 {
                return Err("USE requires a namespace name".to_string());
            }
            // Check if this is actually "USE NAMESPACE" without a name
            if words[1].eq_ignore_ascii_case("NAMESPACE") {
                return Err("USE NAMESPACE requires a namespace name".to_string());
            }
            words[1]
        } else {
            return Err(format!(
                "Expected USE, USE NAMESPACE, or SET NAMESPACE statement, got: {}",
                sql
            ));
        };

        // Remove quotes if present
        let namespace_name = namespace_name.trim_matches('\'').trim_matches('"').trim_matches('`');

        Ok(Self {
            namespace: NamespaceId::new(namespace_name),
        })
    }
}

// Implement DdlAst marker trait
impl crate::DdlAst for UseNamespaceStatement {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_use_namespace() {
        let stmt = UseNamespaceStatement::parse("USE NAMESPACE myapp").unwrap();
        assert_eq!(stmt.namespace.as_str(), "myapp");
    }

    #[test]
    fn test_parse_use_shorthand() {
        let stmt = UseNamespaceStatement::parse("USE myapp").unwrap();
        assert_eq!(stmt.namespace.as_str(), "myapp");
    }

    #[test]
    fn test_parse_set_namespace() {
        let stmt = UseNamespaceStatement::parse("SET NAMESPACE production").unwrap();
        assert_eq!(stmt.namespace.as_str(), "production");
    }

    #[test]
    fn test_parse_lowercase() {
        let stmt = UseNamespaceStatement::parse("use namespace myapp").unwrap();
        assert_eq!(stmt.namespace.as_str(), "myapp");
    }

    #[test]
    fn test_parse_quoted() {
        let stmt = UseNamespaceStatement::parse("USE 'my_app'").unwrap();
        assert_eq!(stmt.namespace.as_str(), "my_app");
    }

    #[test]
    fn test_parse_missing_name() {
        let result = UseNamespaceStatement::parse("USE NAMESPACE");
        assert!(result.is_err());
    }
}
