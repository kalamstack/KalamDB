#![cfg(feature = "embedded")]

use chrono::Utc;
use datafusion::scalar::ScalarValue;
use pg_kalam::EmbeddedFdwService;
use kalam_pg_common::{DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_fdw::{DeleteInput, InsertInput, ScanInput, UpdateInput};
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
use kalamdb_commons::models::{NamespaceId, StorageId, TableName, UserId};
use kalamdb_commons::{NodeId, TableId, TableType};
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
    let mut table_definition = table_definition;
    app_context
        .system_columns_service()
        .add_system_columns(&mut table_definition)
        .expect("add system columns");
    app_context
        .schema_registry()
        .put(table_definition)
        .expect("register table definition");
}

#[test]
#[ntest::timeout(60000)]
fn embedded_fdw_service_scans_user_rows_with_virtual_and_system_columns() {
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let app_context = {
        let _guard = runtime.enter();
        test_app_context_simple()
    };
    let service =
        EmbeddedFdwService::new(app_context.clone()).expect("create embedded fdw service");
    let table_id = TableId::new(NamespaceId::new("pg_poc"), TableName::new("messages"));
    let session_user_id = Some(UserId::new("u_fdw"));

    register_table(
        &app_context,
        TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            TableType::User,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Int),
                ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
            ],
            TableOptions::user(),
            None,
        )
        .expect("user table definition"),
    );

    runtime
        .block_on(service.insert(InsertInput {
            table_id: table_id.clone(),
            table_type: TableType::User,
            rows: vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Int32(Some(1))),
                ("body".to_string(), ScalarValue::Utf8(Some("hello".to_string()))),
            ])],
            session_user_id: session_user_id.clone(),
        }))
        .expect("insert through fdw service");

    let rows = runtime
        .block_on(service.scan(ScanInput {
            table_id,
            table_type: TableType::User,
            projected_columns: vec![
                "id".to_string(),
                USER_ID_COLUMN.to_string(),
                SEQ_COLUMN.to_string(),
                DELETED_COLUMN.to_string(),
            ],
            filters: Vec::new(),
            limit: Some(1),
            session_user_id,
        }))
        .expect("scan through fdw service");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int32(Some(1))));
    assert_eq!(rows[0].get(USER_ID_COLUMN), Some(&ScalarValue::Utf8(Some("u_fdw".to_string()))));
    assert!(matches!(rows[0].get(SEQ_COLUMN), Some(ScalarValue::Int64(Some(_)))));
    assert_eq!(rows[0].get(DELETED_COLUMN), Some(&ScalarValue::Boolean(Some(false))));

    drop(service);
    drop(runtime);
}

#[test]
#[ntest::timeout(60000)]
fn embedded_fdw_service_updates_and_deletes_user_rows() {
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let app_context = {
        let _guard = runtime.enter();
        test_app_context_simple()
    };
    let service =
        EmbeddedFdwService::new(app_context.clone()).expect("create embedded fdw service");
    let table_id = TableId::new(NamespaceId::new("pg_poc"), TableName::new("messages"));
    let session_user_id = Some(UserId::new("u_fdw"));

    register_table(
        &app_context,
        TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            TableType::User,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
            ],
            TableOptions::user(),
            None,
        )
        .expect("user table definition"),
    );

    runtime
        .block_on(service.insert(InsertInput {
            table_id: table_id.clone(),
            table_type: TableType::User,
            rows: vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Utf8(Some("msg-1".to_string()))),
                ("body".to_string(), ScalarValue::Utf8(Some("hello".to_string()))),
            ])],
            session_user_id: session_user_id.clone(),
        }))
        .expect("insert row");

    let update_response = runtime
        .block_on(service.update(UpdateInput {
            table_id: table_id.clone(),
            table_type: TableType::User,
            pk_value: "msg-1".to_string(),
            updates: Row::from_vec(vec![(
                "body".to_string(),
                ScalarValue::Utf8(Some("updated".to_string())),
            )]),
            session_user_id: session_user_id.clone(),
        }))
        .expect("update row");

    assert_eq!(update_response.affected_rows, 1);

    let updated_rows = runtime
        .block_on(service.scan(ScanInput {
            table_id: table_id.clone(),
            table_type: TableType::User,
            projected_columns: vec![
                "id".to_string(),
                "body".to_string(),
                DELETED_COLUMN.to_string(),
            ],
            filters: Vec::new(),
            limit: Some(1),
            session_user_id: session_user_id.clone(),
        }))
        .expect("scan updated row");

    assert_eq!(updated_rows.len(), 1);
    assert_eq!(
        updated_rows[0].get("body"),
        Some(&ScalarValue::Utf8(Some("updated".to_string())))
    );
    assert_eq!(updated_rows[0].get(DELETED_COLUMN), Some(&ScalarValue::Boolean(Some(false))));

    let delete_response = runtime
        .block_on(service.delete(DeleteInput {
            table_id: table_id.clone(),
            table_type: TableType::User,
            pk_value: "msg-1".to_string(),
            explicit_user_id: None,
            session_user_id: session_user_id.clone(),
        }))
        .expect("delete row");

    assert_eq!(delete_response.affected_rows, 1);

    let rows_after_delete = runtime
        .block_on(service.scan(ScanInput {
            table_id,
            table_type: TableType::User,
            projected_columns: vec!["id".to_string(), "body".to_string()],
            filters: Vec::new(),
            limit: None,
            session_user_id,
        }))
        .expect("scan after delete");

    assert!(rows_after_delete.is_empty(), "deleted rows should be filtered from scans");

    drop(service);
    drop(runtime);
}
