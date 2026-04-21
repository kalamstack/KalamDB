#![allow(dead_code)]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use datafusion_common::ScalarValue;
use kalamdb_commons::conversions::arrow_json_conversion::record_batch_to_json_rows;
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
use kalamdb_commons::models::KalamCellValue;
use kalamdb_commons::models::{NamespaceId, TableId, TableName, TransactionId};
use kalamdb_commons::schemas::ColumnDefault;
use kalamdb_commons::{TableAccess, TableType};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::operations::service::OperationService;
use kalamdb_core::test_helpers::{test_app_context, test_app_context_simple};
use kalamdb_pg::{
    BeginTransactionRequest, CloseSessionRequest, CommitTransactionRequest, InsertRpcRequest,
    KalamPgService, OpenSessionRequest, PgService, RollbackTransactionRequest, ScanRpcRequest,
    ScanRpcResponse,
};
use tokio::time::{sleep, Duration};
use tonic::Request;
use uuid::Uuid;

pub fn request<T>(payload: T) -> Request<T> {
    Request::new(payload)
}

pub fn unique_namespace(prefix: &str) -> NamespaceId {
    NamespaceId::new(format!("{}_{}", prefix, Uuid::now_v7().simple()))
}

pub fn build_service(app_ctx: Arc<AppContext>) -> KalamPgService {
    KalamPgService::new(false, None)
        .with_operation_executor(Arc::new(OperationService::new(app_ctx)))
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

pub async fn new_service_with_tables(
    namespace_prefix: &str,
    table_names: &[&str],
) -> (Arc<AppContext>, KalamPgService, NamespaceId, Vec<TableId>) {
    let app_ctx = test_app_context_simple();
    let namespace = unique_namespace(namespace_prefix);
    let mut table_ids = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        table_ids.push(create_shared_table(&app_ctx, &namespace, table_name).await);
    }
    let service = build_service(Arc::clone(&app_ctx));
    (app_ctx, service, namespace, table_ids)
}

pub async fn new_cluster_service_with_tables(
    namespace_prefix: &str,
    table_names: &[&str],
) -> (Arc<AppContext>, KalamPgService, NamespaceId, Vec<TableId>) {
    let app_ctx = test_app_context();
    let namespace = unique_namespace(namespace_prefix);
    let mut table_ids = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        table_ids.push(create_shared_table(&app_ctx, &namespace, table_name).await);
    }
    let service = build_service(Arc::clone(&app_ctx));
    (app_ctx, service, namespace, table_ids)
}

pub async fn new_cluster_user_service_with_tables(
    namespace_prefix: &str,
    table_names: &[&str],
) -> (Arc<AppContext>, KalamPgService, NamespaceId, Vec<TableId>) {
    let app_ctx = test_app_context();
    let namespace = unique_namespace(namespace_prefix);
    let mut table_ids = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        table_ids.push(create_user_table(&app_ctx, &namespace, table_name).await);
    }
    let service = build_service(Arc::clone(&app_ctx));
    (app_ctx, service, namespace, table_ids)
}

pub async fn await_user_leader(app_ctx: &Arc<AppContext>, user_id: &str) {
    let user_id = kalamdb_commons::UserId::new(user_id);
    for _attempt in 0..100 {
        if app_ctx.is_leader_for_user(&user_id).await {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }

    panic!("user shard leader not ready for {}", user_id);
}

pub async fn await_shared_leader(app_ctx: &Arc<AppContext>) {
    for _attempt in 0..100 {
        if app_ctx.is_leader_for_shared().await {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }

    panic!("shared shard leader not ready");
}

pub async fn open_session(service: &KalamPgService, _session_id: &str) -> String {
    service
        .open_session(request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .expect("open session")
        .into_inner()
        .session_id
}

pub async fn begin_transaction(service: &KalamPgService, session_id: &str) -> String {
    service
        .begin_transaction(request(BeginTransactionRequest {
            session_id: session_id.to_string(),
        }))
        .await
        .expect("begin transaction")
        .into_inner()
        .transaction_id
}

pub async fn commit_transaction(
    service: &KalamPgService,
    session_id: &str,
    transaction_id: &str,
) -> String {
    service
        .commit_transaction(request(CommitTransactionRequest {
            session_id: session_id.to_string(),
            transaction_id: transaction_id.to_string(),
        }))
        .await
        .expect("commit transaction")
        .into_inner()
        .transaction_id
}

pub async fn rollback_transaction(
    service: &KalamPgService,
    session_id: &str,
    transaction_id: &str,
) -> String {
    service
        .rollback_transaction(request(RollbackTransactionRequest {
            session_id: session_id.to_string(),
            transaction_id: transaction_id.to_string(),
        }))
        .await
        .expect("rollback transaction")
        .into_inner()
        .transaction_id
}

pub async fn close_session(service: &KalamPgService, session_id: &str) {
    service
        .close_session(request(CloseSessionRequest {
            session_id: session_id.to_string(),
        }))
        .await
        .expect("close session");
}

pub async fn insert_shared_row(
    service: &KalamPgService,
    table_id: &TableId,
    session_id: &str,
    id: i64,
    name: &str,
) {
    let row_json = serde_json::to_string(&Row::new(std::collections::BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(id))),
        ("name".to_string(), ScalarValue::Utf8(Some(name.to_string()))),
    ])))
    .expect("serialize typed row");

    service
        .insert(request(InsertRpcRequest {
            namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            table_type: "shared".to_string(),
            session_id: session_id.to_string(),
            user_id: None,
            rows_json: vec![row_json],
        }))
        .await
        .expect("insert row");
}

pub async fn scan_shared_rows(
    service: &KalamPgService,
    table_id: &TableId,
    session_id: &str,
) -> Vec<HashMap<String, KalamCellValue>> {
    let response = service
        .scan(request(ScanRpcRequest {
            namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            table_type: "shared".to_string(),
            session_id: session_id.to_string(),
            user_id: None,
            columns: vec![],
            filters: vec![],
            limit: None,
        }))
        .await
        .expect("scan rows")
        .into_inner();

    decode_scan_rows(&response)
}

pub async fn insert_user_row(
    service: &KalamPgService,
    table_id: &TableId,
    session_id: &str,
    user_id: &str,
    id: i64,
    name: &str,
) {
    let row_json = serde_json::to_string(&Row::new(std::collections::BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(id))),
        ("name".to_string(), ScalarValue::Utf8(Some(name.to_string()))),
    ])))
    .expect("serialize typed row");

    service
        .insert(request(InsertRpcRequest {
            namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            table_type: "user".to_string(),
            session_id: session_id.to_string(),
            user_id: Some(user_id.to_string()),
            rows_json: vec![row_json],
        }))
        .await
        .expect("insert user row");
}

pub async fn scan_user_rows(
    service: &KalamPgService,
    table_id: &TableId,
    session_id: &str,
    user_id: &str,
) -> Vec<HashMap<String, KalamCellValue>> {
    let response = service
        .scan(request(ScanRpcRequest {
            namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            table_type: "user".to_string(),
            session_id: session_id.to_string(),
            user_id: Some(user_id.to_string()),
            columns: vec![],
            filters: vec![],
            limit: None,
        }))
        .await
        .expect("scan user rows")
        .into_inner();

    decode_scan_rows(&response)
}

pub fn decode_scan_rows(response: &ScanRpcResponse) -> Vec<HashMap<String, KalamCellValue>> {
    let mut rows = Vec::new();
    for ipc_batch in &response.ipc_batches {
        let reader = StreamReader::try_new(Cursor::new(ipc_batch.clone()), None)
            .expect("create Arrow IPC reader");
        for batch in reader {
            let batch: RecordBatch = batch.expect("decode Arrow batch");
            rows.extend(record_batch_to_json_rows(&batch).expect("rows from batch"));
        }
    }
    rows
}

pub fn parse_transaction_id(transaction_id: &str) -> TransactionId {
    TransactionId::try_new(transaction_id.to_string()).expect("valid transaction id")
}
