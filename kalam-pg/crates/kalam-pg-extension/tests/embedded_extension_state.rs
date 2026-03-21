#![cfg(feature = "embedded")]

use chrono::Utc;
use pg_kalam::{EmbeddedExtensionState, ImportForeignSchemaRequest};
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
use kalamdb_commons::models::{NamespaceId, StorageId, TableName};
use kalamdb_commons::{NodeId, TableType};
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

#[test]
fn embedded_state_builds_executor_and_import_sql() {
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let app_context = {
        let _guard = runtime.enter();
        test_app_context_simple()
    };
    app_context
        .schema_registry()
        .put(
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
        )
        .expect("register table");

    let state = EmbeddedExtensionState::new(app_context).expect("create embedded extension state");
    let executor = state.executor().expect("build embedded executor");
    let sql = state
        .import_foreign_schema_sql(&ImportForeignSchemaRequest::new("kalam_server", "chat"))
        .expect("generate import sql");

    assert_eq!(sql.len(), 1);
    assert!(sql[0].contains("\"chat\".\"messages\""));

    drop(executor);
    drop(state);
    drop(runtime);
}
