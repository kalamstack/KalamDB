use chrono::Utc;
use datafusion::scalar::ScalarValue;
use kalam_pg_api::{InsertRequest, KalamBackendExecutor, ScanRequest, TenantContext};
use kalam_pg_embedded::EmbeddedKalamRuntime;
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::schemas::{
    ColumnDefinition, SharedTableOptions, TableDefinition, TableOptions,
};
use kalamdb_commons::models::{NamespaceId, StorageId, TableName, UserId};
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

#[tokio::test]
#[ntest::timeout(60000)]
async fn runtime_wraps_existing_app_context() {
    let app_context = test_app_context_simple();
    let runtime = EmbeddedKalamRuntime::from_app_context(app_context.clone()).unwrap();

    assert_eq!(runtime.app_context().node_id().as_u64(), app_context.node_id().as_u64());
}

fn register_table(app_context: &Arc<AppContext>, table_def: TableDefinition) {
    app_context.schema_registry().put(table_def).expect("register table definition");
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn embedded_runtime_poc_round_trips_user_rows() {
    let app_context = test_app_context_simple();
    let runtime = EmbeddedKalamRuntime::from_app_context(app_context.clone()).unwrap();
    let table_id = TableId::new(NamespaceId::new("pg_poc"), TableName::new("messages"));

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

    let tenant = TenantContext::with_user_id(UserId::new("u_poc"));
    runtime
        .insert(InsertRequest::new(
            table_id.clone(),
            TableType::User,
            tenant.clone(),
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Int32(Some(1))),
                ("body".to_string(), ScalarValue::Utf8(Some("hello".to_string()))),
            ])],
        ))
        .await
        .expect("insert user row");

    let response = runtime
        .scan(ScanRequest::new(table_id, TableType::User, tenant))
        .await
        .expect("scan user table");

    assert_eq!(response.batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn embedded_runtime_poc_round_trips_shared_rows() {
    let app_context = test_app_context_simple();
    let runtime = EmbeddedKalamRuntime::from_app_context(app_context.clone()).unwrap();
    let table_id = TableId::new(NamespaceId::new("pg_poc"), TableName::new("shared_docs"));

    register_table(
        &app_context,
        TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Int),
                ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
            ],
            TableOptions::Shared(SharedTableOptions {
                access_level: Some(TableAccess::Public),
                ..Default::default()
            }),
            None,
        )
        .expect("shared table definition"),
    );

    runtime
        .insert(InsertRequest::new(
            table_id.clone(),
            TableType::Shared,
            TenantContext::anonymous(),
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Int32(Some(7))),
                ("body".to_string(), ScalarValue::Utf8(Some("shared".to_string()))),
            ])],
        ))
        .await
        .expect("insert shared row");

    let response = runtime
        .scan(ScanRequest::new(table_id, TableType::Shared, TenantContext::anonymous()))
        .await
        .expect("scan shared table");

    assert_eq!(response.batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn embedded_runtime_poc_round_trips_stream_rows() {
    let app_context = test_app_context_simple();
    let runtime = EmbeddedKalamRuntime::from_app_context(app_context.clone()).unwrap();
    let table_id = TableId::new(NamespaceId::new("pg_poc"), TableName::new("events"));

    register_table(
        &app_context,
        TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            TableType::Stream,
            vec![
                ColumnDefinition::primary_key(1, "event_id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "payload", 2, KalamDataType::Text),
            ],
            TableOptions::stream(3600),
            None,
        )
        .expect("stream table definition"),
    );

    let tenant = TenantContext::with_user_id(UserId::new("u_stream"));
    runtime
        .insert(InsertRequest::new(
            table_id.clone(),
            TableType::Stream,
            tenant.clone(),
            vec![Row::from_vec(vec![
                ("event_id".to_string(), ScalarValue::Utf8(Some("evt-1".to_string()))),
                ("payload".to_string(), ScalarValue::Utf8(Some("typing".to_string()))),
            ])],
        ))
        .await
        .expect("insert stream row");

    let response = runtime
        .scan(ScanRequest::new(table_id, TableType::Stream, tenant))
        .await
        .expect("scan stream table");

    assert_eq!(response.batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}
