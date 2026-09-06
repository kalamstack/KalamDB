//! DDL statement definitions shared across KalamDB components.
//!
//! This module consolidates DDL statement parsers (CREATE, DROP, ALTER, SHOW, etc.)
//! so they can be reused without depending on `kalamdb-core`.

pub mod parsing;
pub mod policy_commands;

#[cfg(test)]
mod policy_commands_tests;

pub mod alter_namespace;
pub mod alter_table;
pub mod alter_type;
pub mod backup_namespace;
pub mod call;
pub mod column_default;
pub mod compact_commands;
pub mod create_index;
pub mod create_namespace;
pub mod create_procedure;
pub mod create_schema;
pub mod create_table; // Unified parser for all table types (USER, SHARED, STREAM)
pub mod create_trigger;
pub mod create_type;
pub mod create_view;
pub mod describe_table;
pub mod drop_namespace;
pub mod drop_table;
pub mod export_commands;
pub mod flush_commands;
pub mod grant_execute;
pub mod job_commands;
pub mod kill_live_query;
pub mod manifest_commands;
pub mod restore_namespace;
pub mod set_search_path;
pub mod show_namespaces;
pub mod show_table_stats;
pub mod show_tables;
pub mod storage_commands;
pub mod subscribe_commands;
pub mod topic_commands;
pub mod use_namespace;
pub mod user_commands;

/// Result type used by the DDL parsers.
/// Returns String errors to avoid dependencies and allow easy conversion to KalamDbError.
pub type DdlResult<T> = Result<T, String>;

pub(crate) const ACCESS_LEVEL_UNSUPPORTED: &str = "ACCESS_LEVEL is not supported. Shared tables \
                                                   use FORCE row-level security; grant access \
                                                   with CREATE POLICY";

pub(crate) fn reject_access_level_sql(sql: &str) -> DdlResult<()> {
    static ACCESS_LEVEL_SQL_RE: once_cell::sync::Lazy<regex::Regex> =
        once_cell::sync::Lazy::new(|| {
            regex::Regex::new(r"(?i)\bACCESS[_\s]+LEVEL\b").expect("access-level reject regex")
        });
    if ACCESS_LEVEL_SQL_RE.is_match(sql) {
        Err(ACCESS_LEVEL_UNSUPPORTED.to_string())
    } else {
        Ok(())
    }
}

pub use alter_namespace::AlterNamespaceStatement;
pub use alter_table::{AlterTableStatement, ColumnOperation, TablePropertyUpdates};
pub use alter_type::{AlterTypeOperation, AlterTypeStatement};
pub use backup_namespace::BackupDatabaseStatement;
pub use call::CallStatement;
pub use compact_commands::{CompactAllTablesStatement, CompactTableStatement};
pub use create_index::parse_create_index_on;
pub use create_namespace::CreateNamespaceStatement;
pub use create_procedure::{CreateProcedureStatement, DropProcedureStatement, ProcedureParameter};
pub use create_schema::CreateSchemaStatement;
pub use create_table::CreateTableStatement;
pub use create_trigger::{AlterTriggerStatement, CreateTriggerStatement, DropTriggerStatement};
pub use create_type::{
    CompositeTypeField, CreateTypeBody, CreateTypeStatement, DropTypeStatement, TypeReference,
};
pub use create_view::CreateViewStatement;
pub use describe_table::DescribeTableStatement;
pub use drop_namespace::DropNamespaceStatement;
pub use drop_table::{DropTableStatement, TableKind};
pub use export_commands::{ExportUserDataStatement, ShowExportStatement};
pub use flush_commands::{FlushAllTablesStatement, FlushTableStatement};
pub use grant_execute::{ExecuteGrantee, GrantExecuteStatement, RevokeExecuteStatement};
pub use job_commands::{parse_job_command, JobCommand};
// Re-export SubscriptionOptions from kalamdb_commons for convenience
pub use kalamdb_commons::websocket::SubscriptionOptions;
pub use kalamdb_commons::CallArgument;
pub use kill_live_query::KillLiveQueryStatement;
pub use manifest_commands::ShowManifestStatement;
pub use policy_commands::{
    AlterPolicyOperation, AlterPolicyStatement, CreatePolicyStatement, DropPolicyStatement,
    PolicyCommand, PolicyTarget,
};
pub use restore_namespace::RestoreDatabaseStatement;
pub use set_search_path::SetSearchPathStatement;
pub use show_namespaces::ShowNamespacesStatement;
pub use show_table_stats::ShowTableStatsStatement;
pub use show_tables::ShowTablesStatement;
pub use storage_commands::{
    AlterStorageStatement, CheckStorageStatement, CreateStorageStatement, DropStorageStatement,
    ShowStoragesStatement,
};
pub use subscribe_commands::SubscribeStatement;
pub use topic_commands::{
    AckStatement, AddTopicSourceStatement, AlterTopicRetentionStatement,
    ClearTopicRetentionStatement, ClearTopicStatement, ConsumePosition, ConsumeStatement,
    CreateTopicStatement, DropTopicStatement, ResetConsumerGroupStatement,
};
pub use use_namespace::UseNamespaceStatement;
pub use user_commands::{
    AlterUserStatement, CreateUserMode, CreateUserStatement, DropUserStatement, UserModification,
};

/// DML statement markers for TypedStatementHandler pattern.
///
/// Classification uses these markers; INSERT shape, VALUES, RETURNING, and ON CONFLICT
/// syntax parsing lives in [`crate::parser::dml`], while execution stays in
/// `kalamdb-core`.
#[derive(Debug, Clone)]
pub struct InsertStatement;

/// Marker for UPDATE statements (parsed in handler using sqlparser)
#[derive(Debug, Clone)]
pub struct UpdateStatement;

/// Marker for DELETE statements (parsed in handler using sqlparser)
#[derive(Debug, Clone)]
pub struct DeleteStatement;

// Implement DdlAst for DML markers
impl crate::DdlAst for InsertStatement {}
impl crate::DdlAst for UpdateStatement {}
impl crate::DdlAst for DeleteStatement {}
