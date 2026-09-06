//! Parent trait for parsed DDL statements and blanket impls

use crate::ddl::*;

/// Marker trait implemented by all parsed DDL statement types.
///
/// Enables writing generic handlers over a common parent type `T: DdlAst`.
pub trait DdlAst: core::fmt::Debug + Send + Sync {}

// Implement the marker trait for all exported DDL statement types
impl DdlAst for AlterNamespaceStatement {}
impl DdlAst for AlterTableStatement {}
impl DdlAst for BackupDatabaseStatement {}
impl DdlAst for CreateNamespaceStatement {}
impl DdlAst for CreateTableStatement {}
impl DdlAst for DescribeTableStatement {}
impl DdlAst for DropNamespaceStatement {}
impl DdlAst for DropTableStatement {}
impl DdlAst for CreatePolicyStatement {}
impl DdlAst for AlterPolicyStatement {}
impl DdlAst for DropPolicyStatement {}
impl DdlAst for CompactAllTablesStatement {}
impl DdlAst for CompactTableStatement {}
impl DdlAst for FlushAllTablesStatement {}
impl DdlAst for FlushTableStatement {}
impl DdlAst for KillLiveQueryStatement {}
impl DdlAst for RestoreDatabaseStatement {}
impl DdlAst for ShowNamespacesStatement {}
impl DdlAst for ShowTableStatsStatement {}
impl DdlAst for ShowTablesStatement {}
impl DdlAst for AlterStorageStatement {}
impl DdlAst for CreateStorageStatement {}
impl DdlAst for DropStorageStatement {}
impl DdlAst for ShowStoragesStatement {}
impl DdlAst for CheckStorageStatement {}
impl DdlAst for SubscribeStatement {}
impl DdlAst for CreateUserStatement {}
impl DdlAst for AlterUserStatement {}
impl DdlAst for DropUserStatement {}
impl DdlAst for JobCommand {}
impl DdlAst for ShowManifestStatement {}
impl DdlAst for CreateViewStatement {}
impl DdlAst for CreateTypeStatement {}
impl DdlAst for DropTypeStatement {}
impl DdlAst for AlterTypeStatement {}
impl DdlAst for CreateProcedureStatement {}
impl DdlAst for DropProcedureStatement {}
impl DdlAst for CreateTriggerStatement {}
impl DdlAst for DropTriggerStatement {}
impl DdlAst for AlterTriggerStatement {}
impl DdlAst for GrantExecuteStatement {}
impl DdlAst for RevokeExecuteStatement {}
impl DdlAst for CallStatement {}
impl DdlAst for CreateSchemaStatement {}
impl DdlAst for SetSearchPathStatement {}
impl DdlAst for ExportUserDataStatement {}
impl DdlAst for ShowExportStatement {}
