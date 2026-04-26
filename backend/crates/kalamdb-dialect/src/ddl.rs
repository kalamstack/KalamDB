//! DDL statement definitions shared across KalamDB components.
//!
//! This module consolidates DDL statement parsers (CREATE, DROP, ALTER, SHOW, etc.)
//! so they can be reused without depending on `kalamdb-core`.

pub mod parsing;

pub mod alter_namespace;
pub mod alter_table;
pub mod backup_namespace;
pub mod compact_commands;
pub mod create_namespace;
pub mod create_table; // Unified parser for all table types (USER, SHARED, STREAM)
pub mod create_view;
pub mod describe_table;
pub mod drop_namespace;
pub mod drop_table;
pub mod export_commands;
pub mod flush_commands;
pub mod job_commands;
pub mod kill_live_query;
pub mod manifest_commands;
pub mod restore_namespace;
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

pub use alter_namespace::AlterNamespaceStatement;
pub use alter_table::{AlterTableStatement, ColumnOperation};
pub use backup_namespace::BackupDatabaseStatement;
pub use compact_commands::{CompactAllTablesStatement, CompactTableStatement};
pub use create_namespace::CreateNamespaceStatement;
pub use create_table::CreateTableStatement;
pub use create_view::CreateViewStatement;
pub use describe_table::DescribeTableStatement;
pub use drop_namespace::DropNamespaceStatement;
pub use drop_table::{DropTableStatement, TableKind};
pub use export_commands::{ExportUserDataStatement, ShowExportStatement};
pub use flush_commands::{FlushAllTablesStatement, FlushTableStatement};
pub use job_commands::{parse_job_command, JobCommand};
// Re-export SubscriptionOptions from kalamdb_commons for convenience
pub use kalamdb_commons::websocket::SubscriptionOptions;
pub use kill_live_query::KillLiveQueryStatement;
pub use manifest_commands::ShowManifestStatement;
pub use restore_namespace::RestoreDatabaseStatement;
pub use show_namespaces::ShowNamespacesStatement;
pub use show_table_stats::ShowTableStatsStatement;
pub use show_tables::ShowTablesStatement;
pub use storage_commands::{
    AlterStorageStatement, CheckStorageStatement, CreateStorageStatement, DropStorageStatement,
    ShowStoragesStatement,
};
pub use subscribe_commands::SubscribeStatement;
pub use topic_commands::{
    AckStatement, AddTopicSourceStatement, ClearTopicStatement, ConsumePosition, ConsumeStatement,
    CreateTopicStatement, DropTopicStatement,
};
pub use use_namespace::UseNamespaceStatement;
pub use user_commands::{
    AlterUserStatement, CreateUserStatement, DropUserStatement, UserModification,
};

/// DML statement markers for TypedStatementHandler pattern
/// These are empty markers since the actual SQL parsing happens in the handlers
/// Marker for INSERT statements (parsed in handler using sqlparser)
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
