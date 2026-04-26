//! STORAGE COMPACT TABLE and STORAGE COMPACT ALL command parsing
//!
//! This module provides SQL command parsing for manual RocksDB compaction operations.
//! Compaction removes tombstones from RocksDB column families for faster reads and
//! reclaimed space.
//!
//! ## Commands
//!
//! ### STORAGE COMPACT TABLE
//!
//! Triggers an asynchronous compaction for a single table:
//!
//! ```sql
//! STORAGE COMPACT TABLE namespace.table_name;
//! ```
//!
//! ### STORAGE COMPACT ALL
//!
//! Triggers asynchronous compaction for all user/shared tables in a namespace:
//!
//! ```sql
//! STORAGE COMPACT ALL IN namespace;
//! ```

use kalamdb_commons::{NamespaceId, TableName};

use crate::{ddl::parsing, parser::utils::normalize_sql};

const ERR_EXPECTED_NAMESPACE: &str = "Expected STORAGE COMPACT ALL IN namespace";

/// STORAGE COMPACT TABLE statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTableStatement {
    /// Namespace containing the table
    pub namespace: NamespaceId,
    /// Table name to compact
    pub table_name: TableName,
}

impl CompactTableStatement {
    /// Parse STORAGE COMPACT TABLE command from SQL string
    ///
    /// # Syntax
    ///
    /// ```sql
    /// STORAGE COMPACT TABLE <namespace>.<table_name>;
    /// ```
    pub fn parse(sql: &str) -> Result<Self, String> {
        let normalized = normalize_sql(sql);

        let table_ref = parsing::extract_after_prefix(&normalized, "STORAGE COMPACT TABLE")?;
        let (namespace, table_name) = parsing::parse_table_reference(&table_ref)?;

        let namespace = namespace.ok_or_else(|| {
            "Table name must be qualified (namespace.table) for STORAGE COMPACT TABLE".to_string()
        })?;

        parsing::validate_no_extra_tokens(&normalized, 4, "STORAGE COMPACT TABLE")?;

        Ok(Self {
            namespace: NamespaceId::from(namespace),
            table_name: TableName::from(table_name),
        })
    }
}

/// STORAGE COMPACT ALL statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactAllTablesStatement {
    /// Namespace to compact all tables from
    pub namespace: NamespaceId,
}

impl CompactAllTablesStatement {
    /// Parse STORAGE COMPACT ALL command from SQL string
    ///
    /// # Syntax
    ///
    /// ```sql
    /// STORAGE COMPACT ALL IN <namespace>;
    /// ```
    pub fn parse(sql: &str) -> Result<Self, String> {
        let normalized = normalize_sql(sql);

        let namespace = parsing::parse_optional_in_clause(&normalized, "STORAGE COMPACT ALL")?
            .ok_or_else(|| ERR_EXPECTED_NAMESPACE.to_string())?;

        let normalized_upper = normalized.to_uppercase();
        let has_namespace_keyword = normalized_upper.contains(" IN NAMESPACE ");
        let expected_tokens = if has_namespace_keyword { 6 } else { 5 };
        let command_for_error = if has_namespace_keyword {
            "STORAGE COMPACT ALL IN NAMESPACE"
        } else {
            "STORAGE COMPACT ALL IN"
        };
        parsing::validate_no_extra_tokens(&normalized, expected_tokens, command_for_error)?;

        Ok(Self {
            namespace: NamespaceId::from(namespace),
        })
    }

    /// Parse and fall back to the default namespace when no `IN` clause is provided.
    pub fn parse_with_default(sql: &str, default_namespace: &NamespaceId) -> Result<Self, String> {
        match Self::parse(sql) {
            Ok(stmt) => Ok(stmt),
            Err(err) if err == ERR_EXPECTED_NAMESPACE => {
                if default_namespace.as_str().is_empty() {
                    Err(err)
                } else {
                    Ok(Self {
                        namespace: default_namespace.clone(),
                    })
                }
            },
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_compact_table_basic() {
        let stmt = CompactTableStatement::parse("STORAGE COMPACT TABLE prod.events").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
        assert_eq!(stmt.table_name, TableName::from("events"));
    }

    #[test]
    fn test_parse_compact_table_with_semicolon() {
        let stmt = CompactTableStatement::parse("STORAGE COMPACT TABLE prod.events;").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
        assert_eq!(stmt.table_name, TableName::from("events"));
    }

    #[test]
    fn test_parse_compact_table_unqualified_error() {
        let result = CompactTableStatement::parse("STORAGE COMPACT TABLE events");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be qualified"));
    }

    #[test]
    fn test_parse_compact_table_extra_tokens_error() {
        let result =
            CompactTableStatement::parse("STORAGE COMPACT TABLE prod.events WHERE id > 100");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected tokens"));
    }

    #[test]
    fn test_parse_compact_all_tables_basic() {
        let stmt = CompactAllTablesStatement::parse("STORAGE COMPACT ALL IN prod").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
    }

    #[test]
    fn test_parse_compact_all_tables_with_semicolon() {
        let stmt = CompactAllTablesStatement::parse("STORAGE COMPACT ALL IN prod;").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
    }

    #[test]
    fn test_parse_compact_all_tables_in_namespace_keyword() {
        let stmt =
            CompactAllTablesStatement::parse("STORAGE COMPACT ALL IN NAMESPACE prod").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
    }

    #[test]
    fn test_parse_compact_all_tables_missing_in_error() {
        let result = CompactAllTablesStatement::parse("STORAGE COMPACT ALL prod");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("STORAGE COMPACT ALL IN"));
    }

    #[test]
    fn test_parse_compact_all_tables_extra_tokens_error() {
        let result = CompactAllTablesStatement::parse("STORAGE COMPACT ALL IN prod WHERE id > 100");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected tokens"));
    }

    #[test]
    fn test_parse_compact_all_tables_with_default_namespace() {
        let default_ns = NamespaceId::from("default");
        let stmt =
            CompactAllTablesStatement::parse_with_default("STORAGE COMPACT ALL", &default_ns)
                .unwrap();
        assert_eq!(stmt.namespace, default_ns);
    }
}
