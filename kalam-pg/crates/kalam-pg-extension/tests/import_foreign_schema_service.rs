#![cfg(feature = "embedded")]

use chrono::Utc;
use pg_kalam::{ImportForeignSchemaRequest, ImportForeignSchemaService};
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::schemas::{
    ColumnDefinition, SharedTableOptions, TableDefinition, TableOptions,
};
use kalamdb_commons::models::{NamespaceId, StorageId, TableName};
use kalamdb_commons::{NodeId, TableAccess, TableId, TableType};
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_store::test_utils::TestDb;
use kalamdb_store::StorageBackend;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::{Storage, StoragePartition, SystemTable};
use std::sync::Arc;

fn test_app_context_simple() -> Arc<AppContext> {
    let mut column_families: Vec<&'static str> = SystemTable::all_tables()
        .iter()
        .filter_map(|table| table.column_family_name())
        .collect();
    column_families.push(StoragePartition::InformationSchemaTables.name());
    column_families.push("shared_table:app:config");
    column_families.push("stream_table:app:events");

    let test_db = TestDb::new(&column_families).expect("create test db");
    let storage_backend: Arc<dyn StorageBackend> = test_db.backend();
    let storage_base_path = test_db.storage_dir().expect("create storage base path");
    let data_path = test_db.path().to_path_buf();

    let mut test_config = ServerConfig::default();
    test_config.storage.data_path = data_path.to_string_lossy().to_string();
    test_config.execution.max_parameters = 50;
    test_config.execution.max_parameter_size_bytes = 512 * 1024;

    let app_context = AppContext::init(
        storage_backend,
        NodeId::new(1),
        storage_base_path.to_string_lossy().to_string(),
        test_config,
    );

    let storages = app_context.system_tables().storages();
    let storage_id = StorageId::from("local");
    if storages.get_storage_by_id(&storage_id).unwrap().is_none() {
        storages
            .create_storage(Storage {
                storage_id,
                storage_name: "Local Storage".to_string(),
                description: Some("Default local storage for tests".to_string()),
                storage_type: StorageType::Filesystem,
                base_directory: storage_base_path.to_string_lossy().to_string(),
                credentials: None,
                config_json: None,
                shared_tables_template: "shared/{namespace}/{table}".to_string(),
                user_tables_template: "user/{namespace}/{table}/{userId}".to_string(),
                created_at: Utc::now().timestamp_millis(),
                updated_at: Utc::now().timestamp_millis(),
            })
            .expect("create default local storage");
    }

    std::mem::forget(test_db);
    app_context
}

fn register_table(app_context: &Arc<AppContext>, table_definition: TableDefinition) {
    app_context
        .schema_registry()
        .put(table_definition)
        .expect("register table definition");
}

#[tokio::test]
async fn import_service_generates_sql_for_supported_tables() {
    let app_context = test_app_context_simple();
    let service = ImportForeignSchemaService::new(app_context.clone());

    register_table(
        &app_context,
        TableDefinition::new(
            NamespaceId::new("app"),
            TableName::new("messages"),
            TableType::User,
            vec![ColumnDefinition::primary_key(
                1,
                "id",
                1,
                KalamDataType::BigInt,
            )],
            TableOptions::user(),
            None,
        )
        .expect("user table"),
    );
    register_table(
        &app_context,
        TableDefinition::new(
            NamespaceId::new("app"),
            TableName::new("shared_docs"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
            ],
            TableOptions::Shared(SharedTableOptions {
                access_level: Some(TableAccess::Public),
                ..Default::default()
            }),
            None,
        )
        .expect("shared table"),
    );
    register_table(
        &app_context,
        TableDefinition::new(
            NamespaceId::new("events"),
            TableName::new("timeline"),
            TableType::Stream,
            vec![ColumnDefinition::simple(
                1,
                "payload",
                1,
                KalamDataType::Json,
            )],
            TableOptions::stream(3600),
            None,
        )
        .expect("stream table"),
    );

    let sql_statements = service
        .generate_sql(&ImportForeignSchemaRequest::new("kalam_server", "chat"))
        .expect("generate import sql");

    assert_eq!(sql_statements.len(), 3);
    assert!(sql_statements.iter().any(|sql| sql.contains("\"chat\".\"messages\"")));
    assert!(sql_statements.iter().any(|sql| sql.contains("\"chat\".\"shared_docs\"")));
    assert!(sql_statements.iter().any(|sql| sql.contains("\"chat\".\"timeline\"")));
}

#[tokio::test]
async fn import_service_filters_namespace_type_and_exclusions() {
    let app_context = test_app_context_simple();
    let service = ImportForeignSchemaService::new(app_context.clone());

    let user_table_id = TableId::new(NamespaceId::new("app"), TableName::new("messages"));
    let shared_table_id = TableId::new(NamespaceId::new("app"), TableName::new("shared_docs"));

    register_table(
        &app_context,
        TableDefinition::new(
            user_table_id.namespace_id().clone(),
            user_table_id.table_name().clone(),
            TableType::User,
            vec![ColumnDefinition::primary_key(
                1,
                "id",
                1,
                KalamDataType::BigInt,
            )],
            TableOptions::user(),
            None,
        )
        .expect("user table"),
    );
    register_table(
        &app_context,
        TableDefinition::new(
            shared_table_id.namespace_id().clone(),
            shared_table_id.table_name().clone(),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
            ],
            TableOptions::Shared(SharedTableOptions {
                access_level: Some(TableAccess::Public),
                ..Default::default()
            }),
            None,
        )
        .expect("shared table"),
    );
    register_table(
        &app_context,
        TableDefinition::new(
            NamespaceId::new("events"),
            TableName::new("timeline"),
            TableType::Stream,
            vec![ColumnDefinition::simple(
                1,
                "payload",
                1,
                KalamDataType::Json,
            )],
            TableOptions::stream(3600),
            None,
        )
        .expect("stream table"),
    );

    let mut request = ImportForeignSchemaRequest::new("kalam_server", "chat");
    request.source_namespace = Some(NamespaceId::new("app"));
    request.included_table_types = Some(vec![TableType::User, TableType::Shared]);
    request.excluded_tables.insert(shared_table_id);

    let sql_statements = service.generate_sql(&request).expect("generate filtered import sql");

    assert_eq!(sql_statements.len(), 1);
    assert!(sql_statements[0].contains("\"chat\".\"messages\""));
    assert!(!sql_statements[0].contains("\"chat\".\"shared_docs\""));
    assert!(!sql_statements[0].contains("\"chat\".\"timeline\""));
}
