//! SQL parser for system table queries
//!
//! Parses SQL statements targeting system tables using sqlparser-rs.

use anyhow::{anyhow, Result};
pub use kalamdb_system::SystemTable;
use sqlparser::ast::Statement;

use crate::{dialect::KalamDbDialect, parser::utils::parse_sql_statements};

/// SQL statement types supported for system tables
#[derive(Debug, Clone)]
pub enum SystemStatement {
    Select {
        table: SystemTable,
        columns: Vec<String>,
        where_clause: Option<String>,
    },
    Insert {
        table: SystemTable,
        columns: Vec<String>,
        values: Vec<serde_json::Value>,
    },
    Update {
        table: SystemTable,
        updates: Vec<(String, serde_json::Value)>,
        where_clause: Option<String>,
    },
    Delete {
        table: SystemTable,
        where_clause: Option<String>,
    },
}

/// SQL parser for system tables
pub struct SqlParser {
    dialect: KalamDbDialect,
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            dialect: KalamDbDialect::default(),
        }
    }

    /// Parse a SQL statement
    pub fn parse(&self, sql: &str) -> Result<SystemStatement> {
        let statements = parse_sql_statements(sql, &self.dialect)?;

        if statements.is_empty() {
            return Err(anyhow!("No SQL statement found"));
        }

        if statements.len() > 1 {
            return Err(anyhow!("Multiple statements not supported"));
        }

        self.parse_statement(&statements[0])
    }

    fn parse_statement(&self, statement: &Statement) -> Result<SystemStatement> {
        match statement {
            Statement::Query(query) => self.parse_select(query),
            Statement::Insert { .. } => self.parse_insert(statement),
            Statement::Update { .. } => self.parse_update(statement),
            Statement::Delete { .. } => self.parse_delete(statement),
            _ => Err(anyhow!("Unsupported statement type")),
        }
    }

    fn parse_select(&self, _query: &sqlparser::ast::Query) -> Result<SystemStatement> {
        // Simplified implementation for now
        // Full implementation would parse SELECT fields, FROM clause, WHERE clause, etc.
        Err(anyhow!("SELECT parsing not yet implemented"))
    }

    fn parse_insert(&self, _statement: &Statement) -> Result<SystemStatement> {
        // Simplified implementation for now
        Err(anyhow!("INSERT parsing not yet implemented"))
    }

    fn parse_update(&self, _statement: &Statement) -> Result<SystemStatement> {
        // Simplified implementation for now
        Err(anyhow!("UPDATE parsing not yet implemented"))
    }

    fn parse_delete(&self, _statement: &Statement) -> Result<SystemStatement> {
        // Simplified implementation for now
        Err(anyhow!("DELETE parsing not yet implemented"))
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_table_from_name() {
        assert_eq!(SystemTable::from_name("users").unwrap(), SystemTable::Users);
        assert_eq!(SystemTable::from_name("system_users").unwrap(), SystemTable::Users);
        assert_eq!(SystemTable::from_name("namespaces").unwrap(), SystemTable::Namespaces);
    }

    // #[test]
    // fn test_system_table_column_family_name() {
    //     assert_eq!(SystemTable::Users.column_family_name(), "system_users");
    //     assert_eq!(SystemTable::Live.column_family_name(), None);
    //     assert_eq!(
    //         SystemTable::Namespaces.column_family_name(),
    //         "system_namespaces"
    //     );
    // }

    #[test]
    fn test_parser_creation() {
        let parser = SqlParser::new();
        assert!(parser.parse("").is_err());
    }
}
