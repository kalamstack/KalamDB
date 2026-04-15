//! KalamDB dialect, parser, and statement classification surface.
//!
//! This crate is the extraction point for KalamDB SQL parsing concerns.

use kalamdb_commons::models::NamespaceId;
use kalamdb_commons::Role;

pub mod batch_execution;
pub mod classifier;
pub mod compatibility;
pub mod ddl;
pub mod ddl_parent;
pub mod dialect;
pub mod execute_as;
pub mod parser;
pub mod validation;

pub use batch_execution::{
    parse_batch_statements, parse_execution_batch, parse_execution_statement,
    prepare_execution_batch, split_statements, BatchParseError, ExecutionBatchParseError,
    ExecutionBatchPrepareError, ParsedExecutionStatement, PreparedExecutionBatchStatement,
};
pub use classifier::{SqlStatement, SqlStatementKind, StatementClassificationError};
pub use compatibility::{
    format_mysql_column_not_found, format_mysql_error, format_mysql_syntax_error,
    format_mysql_table_not_found, format_postgres_column_not_found, format_postgres_error,
    format_postgres_syntax_error, format_postgres_table_not_found, map_sql_type_to_arrow,
    map_sql_type_to_kalam, ErrorStyle,
};
pub use ddl::{
    parse_job_command, AlterStorageStatement, CheckStorageStatement, CompactAllTablesStatement,
    CompactTableStatement, CreateStorageStatement, DropStorageStatement, FlushAllTablesStatement,
    FlushTableStatement, JobCommand, ShowManifestStatement, ShowStoragesStatement,
    SubscribeStatement, SubscriptionOptions,
};
pub use ddl_parent::DdlAst;
pub use dialect::KalamDbDialect;
pub use execute_as::{extract_inner_sql, parse_execute_as, ExecuteAsEnvelope};
pub use parser::query_parser::{QueryParseError, QueryParser, SubscriptionQueryAnalysis};
pub use parser::SqlParser;
pub use parser::{
    extract_dml_table_id, extract_dml_table_id_fast, extract_dml_table_id_from_statement,
    insert_column_names_from_statement, insert_columns_match,
    normalize_context_keyword_calls_for_sqlparser, parse_single_statement,
    rewrite_context_functions_for_datafusion,
};
pub use validation::{
    validate_column_name, validate_namespace_name, validate_table_name, ValidationError,
    RESERVED_COLUMN_NAMES, RESERVED_NAMESPACES,
};

pub fn classify_statement(
    sql: &str,
    default_namespace: &NamespaceId,
    role: Role,
) -> Result<SqlStatement, StatementClassificationError> {
    SqlStatement::classify_and_parse(sql, default_namespace, role)
}
