#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

use chrono::Utc;
use datafusion_common::ScalarValue;
use kalamdb_commons::{
    conversions::arrow_json_conversion::record_batch_to_json_rows,
    models::{
        datatypes::KalamDataType,
        rows::Row,
        schemas::{ColumnDefinition, TableDefinition, TableOptions},
        NamespaceId, StorageId, TableId, TableName, UserId,
    },
    schemas::ColumnDefault,
    NodeId, Role, TableAccess, TableType,
};
use kalamdb_configs::ServerConfig;
use kalamdb_core::{
    app_context::AppContext,
    sql::{
        context::{ExecutionContext, ExecutionResult},
        executor::{
            handler_registry::HandlerRegistry, request_transaction_state::RequestTransactionState,
            SqlExecutor,
        },
    },
};
use kalamdb_store::test_utils::TestDb;
use kalamdb_system::{providers::storages::models::StorageType, Storage};
use uuid::Uuid;

static TEST_PORT_OFFSET: AtomicU16 = AtomicU16::new(0);

fn next_test_ports() -> (u16, u16) {
    let pid_component = ((std::process::id() % 1000) as u16) * 20;
    let offset = TEST_PORT_OFFSET.fetch_add(2, Ordering::Relaxed);
    (20_000 + pid_component + offset, 30_000 + pid_component + offset)
}

pub fn create_executor(app_ctx: Arc<AppContext>) -> SqlExecutor {
    SqlExecutor::new(app_ctx, Arc::new(HandlerRegistry::new()))
}

pub async fn create_cluster_app_context() -> (Arc<AppContext>, TestDb) {
    create_cluster_app_context_with_config(ServerConfig::default()).await
}

pub async fn create_cluster_app_context_with_config(
    config: ServerConfig,
) -> (Arc<AppContext>, TestDb) {
    let mut config = config;
    if config.cluster.is_none() {
        let (rpc_port, api_port) = next_test_ports();
        config.cluster = Some(kalamdb_configs::ClusterConfig {
            cluster_id: "test-cluster".to_string(),
            node_id: 1,
            rpc_addr: format!("127.0.0.1:{}", rpc_port),
            api_addr: format!("http://127.0.0.1:{}", api_port),
            peers: Vec::new(),
            user_shards: 32,
            shared_shards: 1,
            heartbeat_interval_ms: 50,
            election_timeout_ms: (150, 300),
            snapshot_policy: "LogsSinceLast(1000)".to_string(),
            max_snapshots_to_keep: 3,
            replication_timeout_ms: 5_000,
            reconnect_interval_ms: 3_000,
            peer_wait_max_retries: None,
            peer_wait_initial_delay_ms: None,
            peer_wait_max_delay_ms: None,
        });
    }

    let test_db = TestDb::with_system_tables().expect("create test db");
    let storage_base_path = test_db.storage_dir().expect("storage dir");
    let app_ctx = AppContext::create_isolated(
        test_db.backend(),
        NodeId::new(1),
        storage_base_path.to_string_lossy().into_owned(),
        config,
    );
    app_ctx.executor().start().await.expect("start raft");
    app_ctx.executor().initialize_cluster().await.expect("initialize raft cluster");
    app_ctx.wire_raft_appliers();

    let storages = app_ctx.system_tables().storages();
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

    (app_ctx, test_db)
}

pub fn unique_namespace(prefix: &str) -> NamespaceId {
    NamespaceId::new(format!("{}_{}", prefix, Uuid::now_v7().simple()))
}

pub async fn create_shared_table(
    app_ctx: &Arc<AppContext>,
    namespace: &NamespaceId,
    table_name: &str,
) -> TableId {
    let table_id = TableId::new(namespace.clone(), TableName::new(table_name));
    let id_col = ColumnDefinition::new(
        1,
        "id".to_string(),
        1,
        KalamDataType::BigInt,
        false,
        true,
        false,
        ColumnDefault::None,
        None,
    );
    let name_col = ColumnDefinition::simple(2, "name", 2, KalamDataType::Text);

    let mut table_options = TableOptions::shared();
    if let TableOptions::Shared(options) = &mut table_options {
        options.access_level = Some(TableAccess::Public);
    }

    let mut table_def = TableDefinition::new(
        namespace.clone(),
        table_id.table_name().clone(),
        TableType::Shared,
        vec![id_col, name_col],
        table_options,
        None,
    )
    .expect("create shared table definition");
    app_ctx
        .system_columns_service()
        .add_system_columns(&mut table_def)
        .expect("add system columns");

    app_ctx
        .schema_registry()
        .register_table(table_def)
        .expect("register shared table");

    table_id
}

pub async fn create_user_table(
    app_ctx: &Arc<AppContext>,
    namespace: &NamespaceId,
    table_name: &str,
) -> TableId {
    let table_id = TableId::new(namespace.clone(), TableName::new(table_name));
    let id_col = ColumnDefinition::new(
        1,
        "id".to_string(),
        1,
        KalamDataType::BigInt,
        false,
        true,
        false,
        ColumnDefault::None,
        None,
    );
    let name_col = ColumnDefinition::simple(2, "name", 2, KalamDataType::Text);

    let mut table_def = TableDefinition::new(
        namespace.clone(),
        table_id.table_name().clone(),
        TableType::User,
        vec![id_col, name_col],
        TableOptions::user(),
        None,
    )
    .expect("create user table definition");
    app_ctx
        .system_columns_service()
        .add_system_columns(&mut table_def)
        .expect("add system columns");

    app_ctx
        .schema_registry()
        .register_table(table_def)
        .expect("register user table");

    table_id
}

pub async fn setup_shared_table(
    namespace_prefix: &str,
    table_name: &str,
) -> (Arc<AppContext>, SqlExecutor, TableId, TestDb) {
    let (app_ctx, test_db) = create_cluster_app_context().await;

    let namespace = unique_namespace(namespace_prefix);
    let table_id = create_shared_table(&app_ctx, &namespace, table_name).await;
    let executor = create_executor(Arc::clone(&app_ctx));
    (app_ctx, executor, table_id, test_db)
}

pub fn request_exec_ctx(app_ctx: &Arc<AppContext>, request_id: &str) -> ExecutionContext {
    ExecutionContext::new(UserId::from("sql-tx-user"), Role::Dba, app_ctx.base_session_context())
        .with_request_id(request_id.to_string())
}

pub fn observer_exec_ctx(app_ctx: &Arc<AppContext>) -> ExecutionContext {
    ExecutionContext::new(UserId::from("sql-observer"), Role::Dba, app_ctx.base_session_context())
}

pub async fn execute_ok(
    executor: &SqlExecutor,
    exec_ctx: &ExecutionContext,
    sql: &str,
) -> ExecutionResult {
    executor.execute(sql, exec_ctx, vec![]).await.expect(sql)
}

pub async fn execute_err(executor: &SqlExecutor, exec_ctx: &ExecutionContext, sql: &str) -> String {
    executor.execute(sql, exec_ctx, vec![]).await.unwrap_err().to_string()
}

pub async fn select_names(
    executor: &SqlExecutor,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
) -> Vec<String> {
    let sql = format!(
        "SELECT name FROM {}.{} ORDER BY id",
        table_id.namespace_id(),
        table_id.table_name()
    );
    let result = execute_ok(executor, exec_ctx, &sql).await;
    let ExecutionResult::Rows { batches, .. } = result else {
        panic!("expected rows result");
    };

    let mut names = Vec::new();
    for batch in batches {
        for row in record_batch_to_json_rows(&batch).expect("json rows") {
            if let Some(name) = row.get("name").and_then(|value| value.as_str()) {
                names.push(name.to_string());
            }
        }
    }
    names
}

pub fn insert_sql(table_id: &TableId, id: i64, name: &str) -> String {
    format!(
        "INSERT INTO {}.{} (id, name) VALUES ({}, '{}')",
        table_id.namespace_id(),
        table_id.table_name(),
        id,
        name
    )
}

pub fn update_sql(table_id: &TableId, id: i64, name: &str) -> String {
    format!(
        "UPDATE {}.{} SET name = '{}' WHERE id = {}",
        table_id.namespace_id(),
        table_id.table_name(),
        name,
        id
    )
}

pub fn row(id: i64, name: &str) -> Row {
    Row::new(BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(id))),
        ("name".to_string(), ScalarValue::Utf8(Some(name.to_string()))),
    ]))
}

pub fn request_transaction_state(exec_ctx: &ExecutionContext) -> RequestTransactionState<'_> {
    RequestTransactionState::from_execution_context(exec_ctx)
        .expect("request transaction state")
        .expect("request transaction state present")
}
