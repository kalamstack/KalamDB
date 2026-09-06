//! KalamDB dialect, parser, and statement classification surface.
//!
//! This crate is the extraction point for KalamDB SQL parsing concerns.

use kalamdb_commons::{models::NamespaceId, Role};

pub mod batch_execution;
pub mod classifier;
pub mod compatibility;
pub mod contracts;
pub mod ddl;
pub mod ddl_parent;
pub mod dialect;
pub mod execute_as;
pub mod parser;
pub mod query_features;
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
pub use contracts::{
    canonical_contract_hash, compile_contract, compile_contract_sql, diff_contracts, ContractDiff,
    ContractError, ContractSnapshot, ContractSource,
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
pub use parser::{
    build_on_conflict_update_assignments, build_on_conflict_update_assignments_with_params,
    conflict_target_is_primary_key, expr_to_scalar, expr_to_scalar_with_params,
    extract_dml_table_id, extract_dml_table_id_fast, extract_dml_table_id_from_statement,
    insert_column_names_from_statement, insert_columns_match, insert_from_statement,
    insert_has_on_conflict, insert_returning_items, is_default_expr,
    normalize_context_keyword_calls_for_sqlparser, object_name_to_string,
    on_conflict_update_should_apply, on_conflict_values_insert, parse_on_conflict_action,
    parse_on_conflict_action_with_params, parse_single_statement,
    query_parser::{QueryParseError, QueryParser, SubscriptionQueryAnalysis},
    rewrite_context_functions_for_datafusion, rewrite_explain_for_datafusion,
    single_values_insert_row, sql_value_to_scalar, strip_nested_expr,
    validate_primary_key_conflict_target, values_insert_view, values_insert_view_from_statement,
    values_rows_from_insert, values_to_rows, OnConflictUpdateAssignment, OnConflictUpdateValue,
    ParsedOnConflictAction, PostgresExplainFormat, RewrittenPostgresExplain, SqlParser,
    ValuesInsertShapeOptions, ValuesInsertView,
};
pub use query_features::{
    supports_general_query_feature, supports_subscription_query_feature, GeneralQueryFeature,
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
