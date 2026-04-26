pub mod backup;
pub mod cluster;
pub mod compact;
pub mod export;
pub mod flush;
mod helpers;
pub mod jobs;
pub mod system;

use std::sync::Arc;

use kalamdb_core::{app_context::AppContext, sql::executor::handler_registry::HandlerRegistry};
use kalamdb_handlers_support::{register_dynamic_handler, register_typed_handler};
use kalamdb_sql::classifier::SqlStatementKind;

pub fn register_admin_handlers(registry: &HandlerRegistry, app_context: Arc<AppContext>) {
    use kalamdb_commons::models::{LiveQueryId, NamespaceId, TableName};
    use kalamdb_sql::ddl::{
        BackupDatabaseStatement, ExportUserDataStatement, FlushAllTablesStatement,
        FlushTableStatement, JobCommand, KillLiveQueryStatement, RestoreDatabaseStatement,
        ShowExportStatement,
    };

    register_typed_handler!(
        registry,
        SqlStatementKind::ShowManifest(kalamdb_sql::ddl::ShowManifestStatement),
        system::ShowManifestCacheHandler::new(app_context.clone()),
        SqlStatementKind::ShowManifest,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::FlushTable(FlushTableStatement {
            namespace: NamespaceId::new("_placeholder"),
            table_name: TableName::new("_placeholder"),
        }),
        flush::FlushTableHandler::new(app_context.clone()),
        SqlStatementKind::FlushTable,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::FlushAllTables(FlushAllTablesStatement {
            namespace: NamespaceId::new("_placeholder"),
        }),
        flush::FlushAllTablesHandler::new(app_context.clone()),
        SqlStatementKind::FlushAllTables,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::CompactTable(kalamdb_sql::ddl::CompactTableStatement {
            namespace: NamespaceId::new("_placeholder"),
            table_name: TableName::new("_placeholder"),
        }),
        compact::CompactTableHandler::new(app_context.clone()),
        SqlStatementKind::CompactTable,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::CompactAllTables(kalamdb_sql::ddl::CompactAllTablesStatement {
            namespace: NamespaceId::new("_placeholder"),
        }),
        compact::CompactAllTablesHandler::new(app_context.clone()),
        SqlStatementKind::CompactAllTables,
    );

    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterSnapshot,
        cluster::ClusterSnapshotHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterPurge(0),
        cluster::ClusterPurgeHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterTriggerElection,
        cluster::ClusterTriggerElectionHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterTransferLeader(0),
        cluster::ClusterTransferLeaderHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterJoin {
            node_id: 0,
            rpc_addr: String::new(),
            api_addr: String::new(),
        },
        cluster::ClusterJoinHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterRebalance,
        cluster::ClusterRebalanceHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterStepdown,
        cluster::ClusterStepdownHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterClear,
        cluster::ClusterClearHandler::new(app_context.clone()),
    );
    register_dynamic_handler!(
        registry,
        SqlStatementKind::ClusterList,
        cluster::ClusterListHandler::new(app_context.clone()),
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::KillJob(JobCommand::Kill {
            job_id: String::new(),
        }),
        jobs::KillJobHandler::new(app_context.clone()),
        SqlStatementKind::KillJob,
    );

    let placeholder_live = LiveQueryId::from_string("user123-conn_abc-q1").unwrap_or_else(|_| {
        kalamdb_commons::models::LiveQueryId::new(
            kalamdb_commons::models::UserId::new("user123"),
            kalamdb_commons::models::ConnectionId::new("conn_abc"),
            "q1".to_string(),
        )
    });
    register_typed_handler!(
        registry,
        SqlStatementKind::KillLiveQuery(KillLiveQueryStatement {
            live_id: placeholder_live,
        }),
        jobs::KillLiveQueryHandler::new(app_context.clone()),
        SqlStatementKind::KillLiveQuery,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::BackupDatabase(BackupDatabaseStatement {
            backup_path: "_placeholder".to_string(),
        }),
        backup::BackupDatabaseHandler::new(app_context.clone()),
        SqlStatementKind::BackupDatabase,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::RestoreDatabase(RestoreDatabaseStatement {
            backup_path: "_placeholder".to_string(),
        }),
        backup::RestoreDatabaseHandler::new(app_context.clone()),
        SqlStatementKind::RestoreDatabase,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ExportUserData(ExportUserDataStatement),
        export::ExportUserDataHandler::new(app_context.clone()),
        SqlStatementKind::ExportUserData,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ShowExport(ShowExportStatement),
        export::ShowExportHandler::new(app_context),
        SqlStatementKind::ShowExport,
    );
}
