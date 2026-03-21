#![cfg(feature = "embedded")]

use chrono::Utc;
use pg_kalam::{EmbeddedExtensionState, EmbeddedSqlService};
use kalamdb_commons::models::{StorageId, UserId};
use kalamdb_commons::{NodeId, Role};
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_store::test_utils::TestDb;
use kalamdb_store::StorageBackend;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::{Storage, StoragePartition, SystemTable};
use ntest::timeout;
use serde_json::Value;
use std::sync::Arc;

fn test_app_context_simple() -> Arc<AppContext> {
    let mut column_families: Vec<&'static str> = SystemTable::all_tables()
        .iter()
        .filter_map(|table| table.column_family_name())
        .collect();
    column_families.push(StoragePartition::InformationSchemaTables.name());

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
#[timeout(60000)]
fn embedded_sql_service_executes_admin_and_user_sql() {
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let extension_state = Arc::new({
        let _guard = runtime.enter();
        EmbeddedExtensionState::new(test_app_context_simple())
            .expect("create embedded extension state")
    });
    let service = EmbeddedSqlService::new(extension_state);

    assert_eq!(
        service
            .execute_json("CREATE NAMESPACE pg_poc", Some(UserId::new("pg_admin")), Role::System)
            .expect("create namespace")["kind"],
        Value::String("success".to_string())
    );
    assert_eq!(
        service
            .execute_json(
                "CREATE USER TABLE pg_poc.messages (id INT PRIMARY KEY, body TEXT)",
                Some(UserId::new("pg_admin")),
                Role::System,
            )
            .expect("create table")["kind"],
        Value::String("success".to_string())
    );
    assert_eq!(
        service
            .execute_json(
                "INSERT INTO pg_poc.messages (id, body) VALUES (1, 'hello from postgres')",
                Some(UserId::new("u_sql")),
                Role::User,
            )
            .expect("insert row")["rows_affected"],
        Value::Number(1.into())
    );

    let query_result = service
        .execute_json(
            "SELECT id, body FROM pg_poc.messages",
            Some(UserId::new("u_sql")),
            Role::User,
        )
        .expect("query rows");

    assert_eq!(query_result["kind"], Value::String("rows".to_string()));
    assert_eq!(query_result["row_count"], Value::Number(1.into()));
    assert_eq!(query_result["rows"][0]["id"], Value::Number(1.into()));
    assert_eq!(
        query_result["rows"][0]["body"],
        Value::String("hello from postgres".to_string())
    );

    drop(service);
    drop(runtime);
}
