//! DROP TABLE statement parser
//!
//! Parses SQL statements like:
//! - DROP USER TABLE messages
//! - DROP SHARED TABLE conversations
//! - DROP STREAM TABLE events
//! - DROP TABLE IF EXISTS messages

use kalamdb_commons::{
    models::{NamespaceId, TableName},
    schemas::TableType,
};

use crate::ddl::DdlResult;

/// Table categories supported by DROP TABLE statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    User,
    Shared,
    Stream,
}

impl From<TableKind> for TableType {
    fn from(kind: TableKind) -> Self {
        match kind {
            TableKind::User => TableType::User,
            TableKind::Shared => TableType::Shared,
            TableKind::Stream => TableType::Stream,
        }
    }
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    /// Table name to drop
    pub table_name: TableName,

    /// Namespace ID (defaults to current namespace)
    pub namespace_id: NamespaceId,

    /// Table type (User, Shared, or Stream)
    pub table_type: TableKind,

    /// If true, don't error if table doesn't exist
    pub if_exists: bool,
}

impl DropTableStatement {
    /// Parse a DROP TABLE statement from SQL
    ///
    /// Supports syntax:
    /// - DROP USER TABLE name
    /// - DROP SHARED TABLE name
    /// - DROP STREAM TABLE name
    /// - DROP TABLE IF EXISTS name (defaults to USER TABLE)
    /// - DROP USER TABLE IF EXISTS name
    pub fn parse(sql: &str, current_namespace: &NamespaceId) -> DdlResult<Self> {
        use crate::ddl::parsing;

        let sql_upper = sql.trim().to_uppercase();

        if !sql_upper.starts_with("DROP") {
            return Err("Expected DROP TABLE statement".to_string());
        }

        // Determine table type
        let (table_type, type_keyword) = if sql_upper.contains("DROP USER TABLE") {
            (TableKind::User, "DROP USER TABLE")
        } else if sql_upper.contains("DROP SHARED TABLE") {
            (TableKind::Shared, "DROP SHARED TABLE")
        } else if sql_upper.contains("DROP STREAM TABLE") {
            (TableKind::Stream, "DROP STREAM TABLE")
        } else if sql_upper.contains("DROP TABLE") {
            (TableKind::User, "DROP TABLE") // Default to USER TABLE
        } else {
            return Err("Expected DROP [USER|SHARED|STREAM] TABLE statement".to_string());
        };

        // Parse using shared utility (handles IF EXISTS)
        let (name, if_exists) =
            parsing::parse_create_drop_statement(sql, type_keyword, "IF EXISTS")?;

        // Handle qualified name (namespace.table)
        let (namespace_id, table_name) = if let Some(dot_pos) = name.find('.') {
            let ns = &name[..dot_pos];
            let tbl = &name[dot_pos + 1..];
            (NamespaceId::new(ns), TableName::new(tbl))
        } else {
            (current_namespace.clone(), TableName::new(&name))
        };

        Ok(Self {
            table_name,
            namespace_id,
            table_type,
            if_exists,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_namespace() -> NamespaceId {
        NamespaceId::new("test_app")
    }

    #[test]
    fn test_parse_drop_user_table() {
        let stmt =
            DropTableStatement::parse("DROP USER TABLE messages", &test_namespace()).unwrap();
        assert_eq!(stmt.table_name.as_str(), "messages");
        assert_eq!(stmt.table_type, TableKind::User);
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_shared_table() {
        let stmt = DropTableStatement::parse("DROP SHARED TABLE conversations", &test_namespace())
            .unwrap();
        assert_eq!(stmt.table_name.as_str(), "conversations");
        assert_eq!(stmt.table_type, TableKind::Shared);
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_stream_table() {
        let stmt =
            DropTableStatement::parse("DROP STREAM TABLE events", &test_namespace()).unwrap();
        assert_eq!(stmt.table_name.as_str(), "events");
        assert_eq!(stmt.table_type, TableKind::Stream);
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_table_defaults_to_user() {
        let stmt = DropTableStatement::parse("DROP TABLE messages", &test_namespace()).unwrap();
        assert_eq!(stmt.table_name.as_str(), "messages");
        assert_eq!(stmt.table_type, TableKind::User);
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let stmt =
            DropTableStatement::parse("DROP USER TABLE IF EXISTS messages", &test_namespace())
                .unwrap();
        assert_eq!(stmt.table_name.as_str(), "messages");
        assert_eq!(stmt.table_type, TableKind::User);
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_shared_table_if_exists() {
        let stmt = DropTableStatement::parse(
            "DROP SHARED TABLE IF EXISTS conversations",
            &test_namespace(),
        )
        .unwrap();
        assert_eq!(stmt.table_name.as_str(), "conversations");
        assert_eq!(stmt.table_type, TableKind::Shared);
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_parse_drop_table_lowercase() {
        let stmt = DropTableStatement::parse("drop user table mydata", &test_namespace()).unwrap();
        assert_eq!(stmt.table_name.as_str(), "mydata");
        assert_eq!(stmt.table_type, TableKind::User);
    }

    #[test]
    fn test_parse_drop_table_missing_name() {
        let result = DropTableStatement::parse("DROP USER TABLE", &test_namespace());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_statement() {
        let result = DropTableStatement::parse("SELECT * FROM messages", &test_namespace());
        assert!(result.is_err());
    }
}
