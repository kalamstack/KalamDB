//! STORAGE FLUSH TABLE and STORAGE FLUSH ALL command parsing
//!
//! This module provides SQL command parsing for manual flush operations. Flush commands
//! trigger immediate data migration from RocksDB to Parquet files asynchronously,
//! returning a job_id for monitoring via system.jobs.
//!
//! ## Commands
//!
//! ### STORAGE FLUSH TABLE
//!
//! Triggers an asynchronous flush for a single table:
//!
//! ```sql
//! STORAGE FLUSH TABLE namespace.table_name;
//! ```
//!
//! **Response**: Returns job_id immediately (< 100ms)
//! **Monitoring**: Poll `SELECT * FROM system.jobs WHERE job_id = 'returned_id'`
//! **Result**: When complete, system.jobs.result contains records_flushed and storage_location
//!
//! ### STORAGE FLUSH ALL
//!
//! Triggers asynchronous flush for all user tables in a namespace:
//!
//! ```sql
//! STORAGE FLUSH ALL IN namespace;
//! ```
//!
//! **Response**: Returns array of job_ids (one per table)
//! **Monitoring**: Poll system.jobs for each job_id
//! **Concurrent Safety**: Multiple flush jobs for different tables can run concurrently
//!
//! ## Asynchronous Execution
//!
//! STORAGE FLUSH commands return immediately with a job_id. The actual flush operation runs
//! in the background via the JobsManager. This design prevents long-running SQL
//! commands from blocking the API.
//!
//! **Job Lifecycle**:
//! 1. Parse STORAGE FLUSH command → validate table/namespace exists
//! 2. Create job record in system.jobs with status='running'
//! 3. Return job_id to client (< 100ms)
//! 4. JobsManager spawns async task for flush execution
//! 5. Flush task updates system.jobs.result with metrics
//! 6. Final status: 'completed' (success) or 'failed' (error)
//!
//! ## Error Handling
//!
//! **Parse Errors**:
//! - Invalid syntax → immediate error response
//! - Non-existent table/namespace → immediate error response
//!
//! **Execution Errors**:
//! - Parquet write failure → job status='failed', error in system.jobs.result
//! - In-progress detection → Allow both jobs or return existing job_id (T252)
//!
//! ## Examples
//!
//! ```rust
//! use kalamdb_dialect::ddl::flush_commands::{FlushAllTablesStatement, FlushTableStatement};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Parse STORAGE FLUSH TABLE
//! let stmt = FlushTableStatement::parse("STORAGE FLUSH TABLE prod.events")?;
//! assert_eq!(stmt.namespace, "prod".into());
//! assert_eq!(stmt.table_name, "events".into());
//!
//! // Parse STORAGE FLUSH ALL
//! let stmt = FlushAllTablesStatement::parse("STORAGE FLUSH ALL IN prod")?;
//! assert_eq!(stmt.namespace, "prod".into());
//! # Ok(())
//! # }
//! ```

//! Parsers for STORAGE FLUSH TABLE and STORAGE FLUSH ALL commands (US4).

use kalamdb_commons::{NamespaceId, TableName};

use crate::{ddl::parsing, parser::utils::normalize_sql};

const ERR_EXPECTED_NAMESPACE: &str = "Expected STORAGE FLUSH ALL IN namespace";

/// STORAGE FLUSH TABLE statement
///
/// Triggers asynchronous flush for a single table, returning job_id immediately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushTableStatement {
    /// Namespace containing the table
    pub namespace: NamespaceId,
    /// Table name to flush
    pub table_name: TableName,
}

impl FlushTableStatement {
    /// Parse STORAGE FLUSH TABLE command from SQL string
    ///
    /// # Syntax
    ///
    /// ```sql
    /// STORAGE FLUSH TABLE <namespace>.<table_name>;
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// # use kalamdb_dialect::ddl::flush_commands::FlushTableStatement;
    /// let stmt = FlushTableStatement::parse("STORAGE FLUSH TABLE prod.events").unwrap();
    /// assert_eq!(stmt.namespace, "prod".into());
    /// assert_eq!(stmt.table_name, "events".into());
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error string if:
    /// - Syntax is invalid
    /// - Table name is not qualified (missing namespace)
    pub fn parse(sql: &str) -> Result<Self, String> {
        // Normalize SQL: remove extra whitespace, semicolons
        let normalized = normalize_sql(sql);

        // Extract table reference after STORAGE FLUSH TABLE
        let table_ref = parsing::extract_after_prefix(&normalized, "STORAGE FLUSH TABLE")?;
        let (namespace, table_name) = parsing::parse_table_reference(&table_ref)?;

        // STORAGE FLUSH TABLE requires qualified names
        let namespace = namespace.ok_or_else(|| {
            "Table name must be qualified (namespace.table) for STORAGE FLUSH TABLE".to_string()
        })?;

        // Check for extra tokens (should be exactly: STORAGE FLUSH TABLE namespace.table)
        parsing::validate_no_extra_tokens(&normalized, 4, "STORAGE FLUSH TABLE")?;

        Ok(Self {
            namespace: NamespaceId::from(namespace),
            table_name: TableName::from(table_name),
        })
    }
}

/// STORAGE FLUSH ALL statement
///
/// Triggers asynchronous flush for all user tables in a namespace, returning
/// array of job_ids (one per table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushAllTablesStatement {
    /// Namespace to flush all tables from
    pub namespace: NamespaceId,
}

impl FlushAllTablesStatement {
    /// Parse STORAGE FLUSH ALL command from SQL string
    ///
    /// # Syntax
    ///
    /// ```sql
    /// STORAGE FLUSH ALL IN <namespace>;
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// # use kalamdb_dialect::ddl::flush_commands::FlushAllTablesStatement;
    /// let stmt = FlushAllTablesStatement::parse("STORAGE FLUSH ALL IN prod").unwrap();
    /// assert_eq!(stmt.namespace, "prod".into());
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error string if:
    /// - Syntax is invalid
    /// - Missing IN keyword
    /// - Extra tokens after namespace
    pub fn parse(sql: &str) -> Result<Self, String> {
        // Normalize SQL: remove extra whitespace, semicolons
        let normalized = normalize_sql(sql);

        // Parse using utility
        let namespace = parsing::parse_optional_in_clause(&normalized, "STORAGE FLUSH ALL")?
            .ok_or_else(|| ERR_EXPECTED_NAMESPACE.to_string())?;

        // Check for extra tokens, supporting both "IN <ns>" and "IN NAMESPACE <ns>"
        let normalized_upper = normalized.to_uppercase();
        let has_namespace_keyword = normalized_upper.contains(" IN NAMESPACE ");
        let expected_tokens = if has_namespace_keyword { 6 } else { 5 };
        let command_for_error = if has_namespace_keyword {
            "STORAGE FLUSH ALL IN NAMESPACE"
        } else {
            "STORAGE FLUSH ALL IN"
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
    fn test_parse_flush_table_basic() {
        let stmt = FlushTableStatement::parse("STORAGE FLUSH TABLE prod.events").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
        assert_eq!(stmt.table_name, TableName::from("events"));
    }

    #[test]
    fn test_parse_flush_table_with_semicolon() {
        let stmt = FlushTableStatement::parse("STORAGE FLUSH TABLE prod.events;").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
        assert_eq!(stmt.table_name, TableName::from("events"));
    }

    #[test]
    fn test_parse_flush_table_unqualified_error() {
        let result = FlushTableStatement::parse("STORAGE FLUSH TABLE events");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be qualified"));
    }

    #[test]
    fn test_parse_flush_table_extra_tokens_error() {
        let result = FlushTableStatement::parse("STORAGE FLUSH TABLE prod.events WHERE id > 100");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected tokens"));
    }

    #[test]
    fn test_parse_flush_all_tables_basic() {
        let stmt = FlushAllTablesStatement::parse("STORAGE FLUSH ALL IN prod").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
    }

    #[test]
    fn test_parse_flush_all_tables_with_semicolon() {
        let stmt = FlushAllTablesStatement::parse("STORAGE FLUSH ALL IN prod;").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
    }

    #[test]
    fn test_parse_flush_all_tables_in_namespace_keyword() {
        let stmt = FlushAllTablesStatement::parse("STORAGE FLUSH ALL IN NAMESPACE prod").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("prod"));
    }

    #[test]
    fn test_parse_flush_all_tables_missing_in_error() {
        let result = FlushAllTablesStatement::parse("STORAGE FLUSH ALL prod");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("STORAGE FLUSH ALL IN"));
    }

    #[test]
    fn test_parse_flush_all_tables_extra_tokens_error() {
        let result = FlushAllTablesStatement::parse("STORAGE FLUSH ALL IN prod WHERE id > 100");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected tokens"));
    }

    #[test]
    fn test_parse_with_default_namespace_when_missing_in_clause() {
        let default_ns = NamespaceId::from("fallback_ns");
        let stmt = FlushAllTablesStatement::parse_with_default("STORAGE FLUSH ALL", &default_ns)
            .expect("default namespace should be applied");
        assert_eq!(stmt.namespace, default_ns);
    }
}
