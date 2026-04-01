//! SQL statement handler implementations for KalamDB
//!
//! This crate contains all SQL handler implementations (CREATE TABLE, DROP NAMESPACE, etc.)
//! and shared helpers used by those handlers. Extracting these from `kalamdb-core` enables
//! parallel compilation, significantly reducing build times.
//!
//! # Architecture
//!
//! Handlers implement [`TypedStatementHandler<T>`] or [`StatementHandler`] traits defined
//! in `kalamdb-core`. Registration into the [`HandlerRegistry`] happens via
//! [`register_all_handlers`], which is called from the server lifecycle code.

// Handler modules (organized by SQL statement category)
pub mod backup;
pub mod cluster;
pub mod compact;
pub mod export;
pub mod flush;
pub mod helpers;
pub mod jobs;
pub mod namespace;
pub mod storage;
pub mod subscription;
pub mod system;
pub mod table;
pub mod topics;
pub mod user;
pub mod view;

use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use std::sync::Arc;

/// Register all SQL statement handlers into the given registry.
///
/// This function is called once during server startup from lifecycle code.
/// It populates the `HandlerRegistry` with concrete handler implementations
/// for every supported SQL statement type.
pub fn register_all_handlers(
    registry: &HandlerRegistry,
    app_context: Arc<AppContext>,
    enforce_password_complexity: bool,
) {
    use kalamdb_commons::models::{NamespaceId, StorageId};
    use kalamdb_commons::TableType;
    use kalamdb_sql::classifier::SqlStatementKind;

    // ============================================================================
    // NAMESPACE HANDLERS
    // ============================================================================

    registry.register_typed(
        SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
            name: NamespaceId::new("_placeholder"),
            if_not_exists: false,
        }),
        namespace::CreateNamespaceHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CreateNamespace(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::AlterNamespace(kalamdb_sql::ddl::AlterNamespaceStatement {
            name: NamespaceId::new("_placeholder"),
            options: std::collections::HashMap::new(),
        }),
        namespace::AlterNamespaceHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::AlterNamespace(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::DropNamespace(kalamdb_sql::ddl::DropNamespaceStatement {
            name: NamespaceId::new("_placeholder"),
            if_exists: false,
            cascade: false,
        }),
        namespace::DropNamespaceHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::DropNamespace(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ShowNamespaces(kalamdb_sql::ddl::ShowNamespacesStatement),
        namespace::ShowNamespacesHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ShowNamespaces(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::UseNamespace(kalamdb_sql::ddl::UseNamespaceStatement {
            namespace: NamespaceId::new("_placeholder"),
        }),
        namespace::UseNamespaceHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::UseNamespace(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // STORAGE HANDLERS
    // ============================================================================

    registry.register_typed(
        SqlStatementKind::CreateStorage(kalamdb_sql::ddl::CreateStorageStatement {
            storage_id: StorageId::new("_placeholder"),
            storage_type: kalamdb_system::providers::storages::models::StorageType::Filesystem,
            storage_name: String::new(),
            description: None,
            base_directory: String::new(),
            shared_tables_template: String::new(),
            user_tables_template: String::new(),
            credentials: None,
            config_json: None,
        }),
        storage::CreateStorageHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CreateStorage(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::AlterStorage(kalamdb_sql::ddl::AlterStorageStatement {
            storage_id: StorageId::new("_placeholder"),
            storage_name: None,
            description: None,
            shared_tables_template: None,
            user_tables_template: None,
            config_json: None,
        }),
        storage::AlterStorageHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::AlterStorage(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::DropStorage(kalamdb_sql::ddl::DropStorageStatement {
            storage_id: kalamdb_commons::StorageId::from(""),
            if_exists: false,
        }),
        storage::DropStorageHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::DropStorage(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ShowStorages(kalamdb_sql::ddl::ShowStoragesStatement),
        storage::ShowStoragesHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ShowStorages(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::CheckStorage(kalamdb_sql::ddl::CheckStorageStatement {
            storage_id: kalamdb_commons::StorageId::from(""),
            extended: false,
        }),
        storage::CheckStorageHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CheckStorage(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // TABLE HANDLERS
    // ============================================================================
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use kalamdb_commons::models::TableName;
    use kalamdb_sql::ddl::{
        AlterTableStatement, CreateTableStatement, CreateViewStatement, DescribeTableStatement,
        DropTableStatement, ShowTableStatsStatement, ShowTablesStatement,
    };
    use std::collections::HashMap;

    registry.register_typed(
        SqlStatementKind::CreateTable(CreateTableStatement {
            table_name: TableName::new("_placeholder"),
            namespace_id: NamespaceId::new("_placeholder"),
            table_type: TableType::Shared,
            schema: Arc::new(ArrowSchema::empty()),
            column_defaults: HashMap::new(),
            primary_key_column: None,
            storage_id: None,
            use_user_storage: false,
            flush_policy: None,
            deleted_retention_hours: None,
            ttl_seconds: None,
            if_not_exists: false,
            access_level: None,
        }),
        table::CreateTableHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CreateTable(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::CreateView(CreateViewStatement {
            namespace_id: NamespaceId::new("_placeholder"),
            view_name: TableName::new("_placeholder"),
            or_replace: false,
            if_not_exists: false,
            columns: Vec::new(),
            query_sql: String::new(),
            original_sql: String::new(),
        }),
        view::CreateViewHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CreateView(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::AlterTable(AlterTableStatement {
            table_name: TableName::new("_placeholder"),
            namespace_id: NamespaceId::new("_placeholder"),
            operation: kalamdb_sql::ddl::ColumnOperation::Drop {
                column_name: "_placeholder".to_string(),
            },
        }),
        table::AlterTableHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::AlterTable(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::DropTable(DropTableStatement {
            table_name: TableName::new("_placeholder"),
            namespace_id: NamespaceId::new("_placeholder"),
            table_type: kalamdb_sql::ddl::TableKind::Shared,
            if_exists: false,
        }),
        table::DropTableHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::DropTable(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ShowTables(ShowTablesStatement { namespace_id: None }),
        table::ShowTablesHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ShowTables(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::DescribeTable(DescribeTableStatement {
            namespace_id: None,
            table_name: TableName::new("_placeholder"),
            show_history: false,
        }),
        table::DescribeTableHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::DescribeTable(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ShowStats(ShowTableStatsStatement {
            namespace_id: None,
            table_name: TableName::new("_placeholder"),
        }),
        table::ShowStatsHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ShowStats(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // SYSTEM HANDLERS
    // ============================================================================
    registry.register_typed(
        SqlStatementKind::ShowManifest(kalamdb_sql::ddl::ShowManifestStatement),
        system::ShowManifestCacheHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ShowManifest(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // FLUSH HANDLERS
    // ============================================================================
    registry.register_typed(
        SqlStatementKind::FlushTable(kalamdb_sql::ddl::FlushTableStatement {
            namespace: NamespaceId::new("_placeholder"),
            table_name: TableName::new("_placeholder"),
        }),
        flush::FlushTableHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::FlushTable(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::FlushAllTables(kalamdb_sql::ddl::FlushAllTablesStatement {
            namespace: NamespaceId::new("_placeholder"),
        }),
        flush::FlushAllTablesHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::FlushAllTables(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::CompactTable(kalamdb_sql::ddl::CompactTableStatement {
            namespace: NamespaceId::new("_placeholder"),
            table_name: TableName::new("_placeholder"),
        }),
        compact::CompactTableHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CompactTable(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::CompactAllTables(kalamdb_sql::ddl::CompactAllTablesStatement {
            namespace: NamespaceId::new("_placeholder"),
        }),
        compact::CompactAllTablesHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CompactAllTables(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // CLUSTER HANDLERS
    // ============================================================================
    registry.register_dynamic(
        SqlStatementKind::ClusterSnapshot,
        cluster::ClusterSnapshotHandler::new(app_context.clone()),
    );

    registry.register_dynamic(
        SqlStatementKind::ClusterPurge(0),
        cluster::ClusterPurgeHandler::new(app_context.clone()),
    );

    registry.register_dynamic(
        SqlStatementKind::ClusterTriggerElection,
        cluster::ClusterTriggerElectionHandler::new(app_context.clone()),
    );

    registry.register_dynamic(
        SqlStatementKind::ClusterTransferLeader(0),
        cluster::ClusterTransferLeaderHandler::new(app_context.clone()),
    );

    registry.register_dynamic(
        SqlStatementKind::ClusterStepdown,
        cluster::ClusterStepdownHandler::new(app_context.clone()),
    );

    registry.register_dynamic(
        SqlStatementKind::ClusterClear,
        cluster::ClusterClearHandler::new(app_context.clone()),
    );

    registry.register_dynamic(
        SqlStatementKind::ClusterList,
        cluster::ClusterListHandler::new(app_context.clone()),
    );

    // ============================================================================
    // JOB HANDLERS
    // ============================================================================
    registry.register_typed(
        SqlStatementKind::KillJob(kalamdb_sql::ddl::JobCommand::Kill {
            job_id: String::new(),
        }),
        jobs::KillJobHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::KillJob(s) => Some(s.clone()),
            _ => None,
        },
    );

    let placeholder_live = kalamdb_commons::models::LiveQueryId::from_string("user123-conn_abc-q1")
        .unwrap_or_else(|_| {
            kalamdb_commons::models::LiveQueryId::new(
                kalamdb_commons::models::UserId::new("user123"),
                kalamdb_commons::models::ConnectionId::new("conn_abc"),
                "q1".to_string(),
            )
        });
    registry.register_typed(
        SqlStatementKind::KillLiveQuery(kalamdb_sql::ddl::KillLiveQueryStatement {
            live_id: placeholder_live,
        }),
        jobs::KillLiveQueryHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::KillLiveQuery(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // USER HANDLERS
    // ============================================================================
    use kalamdb_commons::AuthType;
    use kalamdb_sql::ddl::{
        AlterUserStatement, CreateUserStatement, DropUserStatement, UserModification,
    };

    registry.register_typed(
        SqlStatementKind::CreateUser(CreateUserStatement {
            username: "_placeholder".to_string(),
            auth_type: AuthType::Internal,
            role: kalamdb_commons::Role::User,
            email: None,
            password: None,
        }),
        user::CreateUserHandler::new(app_context.clone(), enforce_password_complexity),
        |stmt| match stmt.kind() {
            SqlStatementKind::CreateUser(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::AlterUser(AlterUserStatement {
            username: "_placeholder".to_string(),
            modification: UserModification::SetEmail("_placeholder".to_string()),
        }),
        user::AlterUserHandler::new(app_context.clone(), enforce_password_complexity),
        |stmt| match stmt.kind() {
            SqlStatementKind::AlterUser(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::DropUser(DropUserStatement {
            username: "_placeholder".to_string(),
            if_exists: false,
        }),
        user::DropUserHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::DropUser(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // SUBSCRIPTION HANDLER
    // ============================================================================
    use kalamdb_sql::ddl::{SubscribeStatement, SubscriptionOptions};

    registry.register_typed(
        SqlStatementKind::Subscribe(SubscribeStatement {
            select_query: "SELECT * FROM _placeholder._placeholder".to_string(),
            namespace: NamespaceId::new("_placeholder"),
            table_name: TableName::new("_placeholder"),
            options: SubscriptionOptions::default(),
        }),
        subscription::SubscribeHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::Subscribe(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // TOPIC PUB/SUB HANDLERS
    // ============================================================================
    use kalamdb_commons::models::{PayloadMode, TableId, TopicId};
    use kalamdb_sql::ddl::{
        AckStatement, AddTopicSourceStatement, ClearTopicStatement, ConsumePosition,
        ConsumeStatement, CreateTopicStatement, DropTopicStatement,
    };

    registry.register_typed(
        SqlStatementKind::CreateTopic(CreateTopicStatement {
            topic_name: "_placeholder".to_string(),
            partitions: None,
        }),
        topics::CreateTopicHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::CreateTopic(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::DropTopic(DropTopicStatement {
            topic_name: "_placeholder".to_string(),
        }),
        topics::DropTopicHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::DropTopic(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ClearTopic(ClearTopicStatement {
            topic_id: TopicId::new("_placeholder"),
        }),
        topics::ClearTopicHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ClearTopic(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::AddTopicSource(AddTopicSourceStatement {
            topic_name: "_placeholder".to_string(),
            table_id: TableId::from_strings("_placeholder", "_placeholder"),
            operation: kalamdb_commons::models::TopicOp::Insert,
            filter_expr: None,
            payload_mode: PayloadMode::Full,
        }),
        topics::AddTopicSourceHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::AddTopicSource(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ConsumeTopic(ConsumeStatement {
            topic_name: "_placeholder".to_string(),
            group_id: None,
            position: ConsumePosition::Latest,
            limit: None,
        }),
        topics::ConsumeHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ConsumeTopic(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::AckTopic(AckStatement {
            topic_name: "_placeholder".to_string(),
            group_id: "_placeholder".to_string(),
            partition_id: 0,
            upto_offset: 0,
        }),
        topics::AckHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::AckTopic(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // BACKUP & RESTORE HANDLERS
    // ============================================================================
    use kalamdb_sql::ddl::{BackupDatabaseStatement, RestoreDatabaseStatement};

    registry.register_typed(
        SqlStatementKind::BackupDatabase(BackupDatabaseStatement {
            backup_path: "_placeholder".to_string(),
        }),
        backup::BackupDatabaseHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::BackupDatabase(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::RestoreDatabase(RestoreDatabaseStatement {
            backup_path: "_placeholder".to_string(),
        }),
        backup::RestoreDatabaseHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::RestoreDatabase(s) => Some(s.clone()),
            _ => None,
        },
    );

    // ============================================================================
    // USER DATA EXPORT HANDLERS
    // ============================================================================
    use kalamdb_sql::ddl::{ExportUserDataStatement, ShowExportStatement};

    registry.register_typed(
        SqlStatementKind::ExportUserData(ExportUserDataStatement),
        export::ExportUserDataHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ExportUserData(s) => Some(s.clone()),
            _ => None,
        },
    );

    registry.register_typed(
        SqlStatementKind::ShowExport(ShowExportStatement),
        export::ShowExportHandler::new(app_context.clone()),
        |stmt| match stmt.kind() {
            SqlStatementKind::ShowExport(s) => Some(s.clone()),
            _ => None,
        },
    );
}
