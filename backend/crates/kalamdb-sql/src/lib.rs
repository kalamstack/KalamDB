//! KalamSQL - Unified SQL interface for KalamDB system tables
//!
//! This crate provides a SQL-based interface for managing KalamDB's system tables:
//! - system_users: User management
//! - system_live: Live subscription observability  
//! - system_jobs: Background job tracking
//! - system_namespaces: Namespace metadata
//! - system_tables: Table metadata
//! - system_storages: Storage backend configurations
//! - information_schema_tables: Unified table metadata (replaces system_table_schemas + system_columns)
//!
//! All system metadata is stored in RocksDB column families, eliminating JSON config files.
//!
//! # Example
//!
//! ```ignore
//! use kalamdb_store::storage_trait::StorageBackend;
//! use std::sync::Arc;
//!
//! # fn example() -> anyhow::Result<()> {
//! # let backend: Arc<dyn StorageBackend> = todo!();
//! // Execute SQL against system tables
//! let results = kalamdb.execute("SELECT * FROM system.users WHERE username = 'alice'")?;
//!
//! // Typed helpers for common operations
//! let user = kalamdb.get_user("alice")?;
//! kalamdb.insert_namespace("my_app", "{}")?;
//! # Ok(())
//! # }
//! ```

pub mod batch_execution;
pub mod classifier;
pub mod compatibility;
pub mod ddl;
pub mod ddl_parent;
pub mod execute_as;
pub mod parser;
pub mod query_cache;
pub mod validation;

// Re-export system models from kalamdb-commons (single source of truth)
pub use kalamdb_system::{AuditLogEntry, Namespace, Storage, User};
// Job and LiveQuery are now in kalamdb-system
pub use kalamdb_system::providers::jobs::models::{Job, JobFilter, JobOptions};
pub use kalamdb_system::LiveQuery;

pub use batch_execution::split_statements;
pub use compatibility::{
    format_mysql_column_not_found, format_mysql_error, format_mysql_syntax_error,
    format_mysql_table_not_found, format_postgres_column_not_found, format_postgres_error,
    format_postgres_syntax_error, format_postgres_table_not_found, map_sql_type_to_arrow,
    ErrorStyle,
};
pub use ddl::{
    parse_job_command, AlterStorageStatement, CheckStorageStatement, CompactAllTablesStatement,
    CompactTableStatement, CreateStorageStatement, DropStorageStatement, FlushAllTablesStatement,
    FlushTableStatement, JobCommand, ShowManifestStatement, ShowStoragesStatement,
    SubscribeStatement, SubscriptionOptions,
};
pub use ddl_parent::DdlAst;
pub use parser::SqlParser;
pub use parser::{
    extract_dml_table_id, extract_dml_table_id_from_statement,
    normalize_context_keyword_calls_for_sqlparser, parse_single_statement,
    rewrite_context_functions_for_datafusion,
};
pub use query_cache::{QueryCache, QueryCacheKey, QueryCacheTtlConfig};
pub use validation::{
    validate_column_name, validate_namespace_name, validate_table_name, ValidationError,
    RESERVED_COLUMN_NAMES, RESERVED_NAMESPACES,
};
