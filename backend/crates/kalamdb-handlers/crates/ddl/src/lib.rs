pub mod helpers;
pub mod namespace;
pub mod storage;
pub mod table;
pub mod view;

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use kalamdb_commons::{
    models::{NamespaceId, StorageId, TableName},
    TableType,
};
use kalamdb_core::{app_context::AppContext, sql::executor::handler_registry::HandlerRegistry};
use kalamdb_handlers_support::register_typed_handler;
use kalamdb_sql::{
    classifier::SqlStatementKind,
    ddl::{
        AlterTableStatement, CreateTableStatement, CreateViewStatement, DescribeTableStatement,
        DropTableStatement, ShowTableStatsStatement, ShowTablesStatement,
    },
};

pub fn register_ddl_handlers(registry: &HandlerRegistry, app_context: Arc<AppContext>) {
    register_typed_handler!(
        registry,
        SqlStatementKind::CreateNamespace(kalamdb_sql::ddl::CreateNamespaceStatement {
            name: NamespaceId::new("_placeholder"),
            if_not_exists: false,
        }),
        namespace::CreateNamespaceHandler::new(app_context.clone()),
        SqlStatementKind::CreateNamespace,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::AlterNamespace(kalamdb_sql::ddl::AlterNamespaceStatement {
            name: NamespaceId::new("_placeholder"),
            options: HashMap::new(),
        }),
        namespace::AlterNamespaceHandler::new(app_context.clone()),
        SqlStatementKind::AlterNamespace,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::DropNamespace(kalamdb_sql::ddl::DropNamespaceStatement {
            name: NamespaceId::new("_placeholder"),
            if_exists: false,
            cascade: false,
        }),
        namespace::DropNamespaceHandler::new(app_context.clone()),
        SqlStatementKind::DropNamespace,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ShowNamespaces(kalamdb_sql::ddl::ShowNamespacesStatement),
        namespace::ShowNamespacesHandler::new(app_context.clone()),
        SqlStatementKind::ShowNamespaces,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::UseNamespace(kalamdb_sql::ddl::UseNamespaceStatement {
            namespace: NamespaceId::new("_placeholder"),
        }),
        namespace::UseNamespaceHandler::new(app_context.clone()),
        SqlStatementKind::UseNamespace,
    );

    register_typed_handler!(
        registry,
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
        SqlStatementKind::CreateStorage,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::AlterStorage(kalamdb_sql::ddl::AlterStorageStatement {
            storage_id: StorageId::new("_placeholder"),
            storage_name: None,
            description: None,
            shared_tables_template: None,
            user_tables_template: None,
            config_json: None,
        }),
        storage::AlterStorageHandler::new(app_context.clone()),
        SqlStatementKind::AlterStorage,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::DropStorage(kalamdb_sql::ddl::DropStorageStatement {
            storage_id: kalamdb_commons::StorageId::from(""),
            if_exists: false,
        }),
        storage::DropStorageHandler::new(app_context.clone()),
        SqlStatementKind::DropStorage,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ShowStorages(kalamdb_sql::ddl::ShowStoragesStatement),
        storage::ShowStoragesHandler::new(app_context.clone()),
        SqlStatementKind::ShowStorages,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::CheckStorage(kalamdb_sql::ddl::CheckStorageStatement {
            storage_id: kalamdb_commons::StorageId::from(""),
            extended: false,
        }),
        storage::CheckStorageHandler::new(app_context.clone()),
        SqlStatementKind::CheckStorage,
    );

    register_typed_handler!(
        registry,
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
        SqlStatementKind::CreateTable,
    );

    register_typed_handler!(
        registry,
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
        SqlStatementKind::CreateView,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::AlterTable(AlterTableStatement {
            table_name: TableName::new("_placeholder"),
            namespace_id: NamespaceId::new("_placeholder"),
            operation: kalamdb_sql::ddl::ColumnOperation::Drop {
                column_name: "_placeholder".to_string(),
            },
        }),
        table::AlterTableHandler::new(app_context.clone()),
        SqlStatementKind::AlterTable,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::DropTable(DropTableStatement {
            table_name: TableName::new("_placeholder"),
            namespace_id: NamespaceId::new("_placeholder"),
            table_type: kalamdb_sql::ddl::TableKind::Shared,
            if_exists: false,
        }),
        table::DropTableHandler::new(app_context.clone()),
        SqlStatementKind::DropTable,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ShowTables(ShowTablesStatement { namespace_id: None }),
        table::ShowTablesHandler::new(app_context.clone()),
        SqlStatementKind::ShowTables,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::DescribeTable(DescribeTableStatement {
            namespace_id: None,
            table_name: TableName::new("_placeholder"),
            show_history: false,
        }),
        table::DescribeTableHandler::new(app_context.clone()),
        SqlStatementKind::DescribeTable,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ShowStats(ShowTableStatsStatement {
            namespace_id: None,
            table_name: TableName::new("_placeholder"),
        }),
        table::ShowStatsHandler::new(app_context),
        SqlStatementKind::ShowStats,
    );
}
