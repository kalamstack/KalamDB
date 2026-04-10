mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion_common::ScalarValue;
use kalamdb_commons::conversions::arrow_json_conversion::record_batch_to_json_rows;
use kalamdb_commons::models::KalamCellValue;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::pg_operations::InsertRequest;
use kalamdb_commons::models::TransactionId;
use kalamdb_commons::TableType;
use kalamdb_configs::ServerConfig;
use kalamdb_core::operations::service::OperationService;
use kalamdb_core::sql::context::ExecutionResult;
use kalamdb_core::transactions::ExecutionOwnerKey;
use kalamdb_pg::{
    BeginTransactionRequest, InsertRpcRequest, KalamPgService, OpenSessionRequest,
    OperationExecutor, PgService, RollbackTransactionRequest, ScanRpcRequest,
};
use kalamdb_raft::RaftExecutor;
use support::{
    create_cluster_app_context, create_cluster_app_context_with_config, create_executor,
    create_shared_table, execute_ok, insert_sql, observer_exec_ctx, request_exec_ctx,
    request_transaction_state, row, unique_namespace,
};
use tonic::Request;

fn json_rows(result: ExecutionResult) -> Vec<HashMap<String, KalamCellValue>> {
    let ExecutionResult::Rows { batches, .. } = result else {
        panic!("expected row result");
    };

    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(record_batch_to_json_rows(&batch).expect("json rows"));
    }
    rows
}

fn string_field(row: &HashMap<String, KalamCellValue>, field: &str) -> String {
    row.get(field)
        .and_then(|value| value.as_str())
        .unwrap_or_else(|| panic!("missing string field {field}: {row:?}"))
        .to_string()
}

fn i64_field(row: &HashMap<String, KalamCellValue>, field: &str) -> i64 {
    row.get(field)
    .and_then(|value| value.as_i64().or_else(|| value.as_str().and_then(|raw| raw.parse().ok())))
        .unwrap_or_else(|| panic!("missing i64 field {field}: {row:?}"))
}

fn optional_string_field(
    row: &HashMap<String, KalamCellValue>,
    field: &str,
) -> Option<String> {
    row.get(field).and_then(|value| value.as_str()).map(ToString::to_string)
}

fn shared_insert_request(
    table_id: &kalamdb_commons::TableId,
    session_id: &str,
    id: i64,
    name: &str,
) -> InsertRpcRequest {
    let row_json = serde_json::to_string(&Row::new(std::collections::BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(id))),
        ("name".to_string(), ScalarValue::Utf8(Some(name.to_string()))),
    ])))
    .expect("serialize typed row");

    InsertRpcRequest {
        namespace: table_id.namespace_id().to_string(),
        table_name: table_id.table_name().to_string(),
        table_type: "shared".to_string(),
        session_id: session_id.to_string(),
        user_id: None,
        rows_json: vec![row_json],
    }
}

async fn open_session(service: &KalamPgService, session_id: &str) {
    service
        .open_session(Request::new(OpenSessionRequest {
            session_id: session_id.to_string(),
            current_schema: None,
        }))
        .await
        .expect("open session succeeds");
}

async fn begin_transaction(service: &KalamPgService, session_id: &str) -> String {
    service
        .begin_transaction(Request::new(BeginTransactionRequest {
            session_id: session_id.to_string(),
        }))
        .await
        .expect("begin transaction succeeds")
        .into_inner()
        .transaction_id
}

async fn rollback_transaction(service: &KalamPgService, session_id: &str, transaction_id: &str) {
    service
        .rollback_transaction(Request::new(RollbackTransactionRequest {
            session_id: session_id.to_string(),
            transaction_id: transaction_id.to_string(),
        }))
        .await
        .expect("rollback transaction succeeds");
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn system_transactions_shows_active_pg_and_sql_transactions_while_sessions_remain_pg_only() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let table_id = create_shared_table(&app_ctx, &unique_namespace("system_transactions"), "items")
        .await;
    let executor = create_executor(Arc::clone(&app_ctx));
    let operation_service = OperationService::new(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let request_ctx = request_exec_ctx(&app_ctx, "txn-view-request");
    let session_id = "pg-4101-abcd1234";

    let executor_handle = app_ctx.executor();
    let raft_executor = executor_handle
        .as_any()
        .downcast_ref::<RaftExecutor>()
        .expect("raft executor available");
    let pg_service = raft_executor.pg_service().expect("pg service is wired");

    open_session(&pg_service, session_id).await;
    let pg_transaction_id = begin_transaction(&pg_service, session_id).await;

    operation_service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_id.to_string()),
            user_id: None,
            rows: vec![row(1, "from-pg")],
        })
        .await
        .expect("pg-origin write stages successfully");

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 2, "from-sql")).await;
    let mut sql_request_state = request_transaction_state(&request_ctx);
    sql_request_state.sync_from_coordinator(&app_ctx);
    let sql_transaction_id = sql_request_state
        .active_transaction_id()
        .expect("sql transaction id present")
        .to_string();

    let transaction_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            "SELECT transaction_id, owner_id, origin, state, write_count FROM system.transactions ORDER BY origin, transaction_id",
        )
        .await,
    );
    assert_eq!(transaction_rows.len(), 2);

    let origins = transaction_rows
        .iter()
        .map(|row| string_field(row, "origin"))
        .collect::<Vec<_>>();
    assert_eq!(origins, vec!["PgRpc".to_string(), "SqlBatch".to_string()]);

    assert!(transaction_rows.iter().any(|row| {
        string_field(row, "transaction_id") == pg_transaction_id
            && string_field(row, "owner_id") == session_id
            && string_field(row, "state") == "open_write"
            && i64_field(row, "write_count") == 1
    }));
    assert!(transaction_rows.iter().any(|row| {
        string_field(row, "transaction_id") == sql_transaction_id
            && string_field(row, "owner_id") == "sql-req-txn-view-request"
            && string_field(row, "state") == "open_write"
            && i64_field(row, "write_count") == 1
    }));

    let session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            "SELECT session_id, transaction_id, transaction_state FROM system.sessions ORDER BY session_id",
        )
        .await,
    );
    assert_eq!(session_rows.len(), 1);
    assert_eq!(string_field(&session_rows[0], "session_id"), session_id);
    assert_eq!(string_field(&session_rows[0], "transaction_id"), pg_transaction_id);
    assert_eq!(string_field(&session_rows[0], "transaction_state"), "active");

    rollback_transaction(&pg_service, session_id, &pg_transaction_id).await;
    execute_ok(&executor, &request_ctx, "ROLLBACK").await;

    let cleared_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            "SELECT transaction_id FROM system.transactions",
        )
        .await,
    );
    assert!(cleared_rows.is_empty());
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn stale_idle_pg_sessions_drop_out_of_sessions_view() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-4202-idlefade";

    let executor_handle = app_ctx.executor();
    let raft_executor = executor_handle
        .as_any()
        .downcast_ref::<RaftExecutor>()
        .expect("raft executor available");
    let pg_service = raft_executor.pg_service().expect("pg service is wired");

    open_session(&pg_service, session_id).await;

    let active_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id, state FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert_eq!(active_session_rows.len(), 1);
    assert_eq!(string_field(&active_session_rows[0], "session_id"), session_id);
    assert_eq!(string_field(&active_session_rows[0], "state"), "idle");

    tokio::time::sleep(Duration::from_millis(5_200)).await;

    let cleared_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert!(cleared_session_rows.is_empty());
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn pg_passive_timeout_hides_stale_transaction_fields_from_sessions_view() {
    let mut config = ServerConfig::default();
    config.transaction_timeout_secs = 1;

    let (app_ctx, _test_db) = create_cluster_app_context_with_config(config).await;
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-4300-feedcafe";

    let executor_handle = app_ctx.executor();
    let raft_executor = executor_handle
        .as_any()
        .downcast_ref::<RaftExecutor>()
        .expect("raft executor available");
    let pg_service = raft_executor.pg_service().expect("pg service is wired");

    open_session(&pg_service, session_id).await;
    let transaction_id = begin_transaction(&pg_service, session_id).await;

    let active_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id, state, transaction_id, transaction_state FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert_eq!(active_session_rows.len(), 1);
    assert_eq!(string_field(&active_session_rows[0], "state"), "idle in transaction");
    assert_eq!(
        optional_string_field(&active_session_rows[0], "transaction_id"),
        Some(transaction_id.clone())
    );
    assert_eq!(
        optional_string_field(&active_session_rows[0], "transaction_state"),
        Some("active".to_string())
    );

    tokio::time::sleep(Duration::from_millis(2200)).await;

    let timed_out_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id, state, transaction_id, transaction_state FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert_eq!(timed_out_session_rows.len(), 1);
    assert_eq!(string_field(&timed_out_session_rows[0], "state"), "idle");
    assert_eq!(optional_string_field(&timed_out_session_rows[0], "transaction_id"), None);
    assert_eq!(
        optional_string_field(&timed_out_session_rows[0], "transaction_state"),
        None
    );

    let timed_out_transaction_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT transaction_id FROM system.transactions WHERE transaction_id = '{transaction_id}'"
            ),
        )
        .await,
    );
    assert!(timed_out_transaction_rows.is_empty());

    let replacement_transaction_id = begin_transaction(&pg_service, session_id).await;
    assert_ne!(replacement_transaction_id, transaction_id);
    rollback_transaction(&pg_service, session_id, &replacement_transaction_id).await;
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn pg_timeout_after_write_clears_sessions_and_transactions_views() {
    let mut config = ServerConfig::default();
    config.transaction_timeout_secs = 1;

    let (app_ctx, _test_db) = create_cluster_app_context_with_config(config).await;
    let table_id = create_shared_table(&app_ctx, &unique_namespace("pg_timeout_write"), "items")
        .await;
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-4301-deadbeef";

    let executor_handle = app_ctx.executor();
    let raft_executor = executor_handle
        .as_any()
        .downcast_ref::<RaftExecutor>()
        .expect("raft executor available");
    let pg_service = raft_executor.pg_service().expect("pg service is wired");

    open_session(&pg_service, session_id).await;
    let transaction_id = begin_transaction(&pg_service, session_id).await;

    pg_service
        .insert(Request::new(shared_insert_request(
            &table_id,
            session_id,
            1,
            "pending",
        )))
        .await
        .expect("initial staged write succeeds");

    let active_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id, state, transaction_id, transaction_state FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert_eq!(active_session_rows.len(), 1);
    assert_eq!(string_field(&active_session_rows[0], "state"), "idle in transaction");
    assert_eq!(
        optional_string_field(&active_session_rows[0], "transaction_id"),
        Some(transaction_id.clone())
    );
    assert_eq!(
        optional_string_field(&active_session_rows[0], "transaction_state"),
        Some("active".to_string())
    );

    let active_transaction_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT transaction_id, state, write_count FROM system.transactions WHERE transaction_id = '{transaction_id}'"
            ),
        )
        .await,
    );
    assert_eq!(active_transaction_rows.len(), 1);
    assert_eq!(string_field(&active_transaction_rows[0], "state"), "open_write");
    assert_eq!(i64_field(&active_transaction_rows[0], "write_count"), 1);

    tokio::time::sleep(Duration::from_millis(2200)).await;

    let timeout_error = pg_service
        .insert(Request::new(shared_insert_request(
            &table_id,
            session_id,
            2,
            "late",
        )))
        .await
        .expect_err("follow-up write should fail after timeout");
    assert_eq!(timeout_error.code(), tonic::Code::FailedPrecondition);
    assert!(timeout_error.message().contains("timed out"), "{timeout_error}");

    let owner_key =
        ExecutionOwnerKey::from_pg_session_id(session_id).expect("pg owner key should parse");
    let parsed_transaction_id =
        TransactionId::try_new(transaction_id.clone()).expect("transaction id should parse");
    assert!(app_ctx.transaction_coordinator().active_for_owner(&owner_key).is_none());
    assert!(
        app_ctx
            .transaction_coordinator()
            .get_handle(&parsed_transaction_id)
            .is_none(),
        "timed out PG write should not leave a handle behind"
    );
    assert!(
        app_ctx
            .transaction_coordinator()
            .get_overlay(&parsed_transaction_id)
            .is_none(),
        "timed out PG write should not leave staged rows behind"
    );

    let cleared_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id, state, transaction_id, transaction_state FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert_eq!(cleared_session_rows.len(), 1);
    assert_eq!(string_field(&cleared_session_rows[0], "state"), "idle");
    assert_eq!(optional_string_field(&cleared_session_rows[0], "transaction_id"), None);
    assert_eq!(
        optional_string_field(&cleared_session_rows[0], "transaction_state"),
        None
    );

    let cleared_transaction_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT transaction_id FROM system.transactions WHERE transaction_id = '{transaction_id}'"
            ),
        )
        .await,
    );
    assert!(cleared_transaction_rows.is_empty());

    let replacement_transaction_id = begin_transaction(&pg_service, session_id).await;
    assert_ne!(replacement_transaction_id, transaction_id);
    rollback_transaction(&pg_service, session_id, &replacement_transaction_id).await;
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn pg_timeout_after_read_clears_sessions_and_transactions_views() {
    let mut config = ServerConfig::default();
    config.transaction_timeout_secs = 1;

    let (app_ctx, _test_db) = create_cluster_app_context_with_config(config).await;
    let table_id = create_shared_table(&app_ctx, &unique_namespace("pg_timeout_read"), "items")
        .await;
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-4302-cafef00d";

    let executor_handle = app_ctx.executor();
    let raft_executor = executor_handle
        .as_any()
        .downcast_ref::<RaftExecutor>()
        .expect("raft executor available");
    let pg_service = raft_executor.pg_service().expect("pg service is wired");

    open_session(&pg_service, session_id).await;
    let transaction_id = begin_transaction(&pg_service, session_id).await;

    let active_transaction_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT transaction_id, state, write_count FROM system.transactions WHERE transaction_id = '{transaction_id}'"
            ),
        )
        .await,
    );
    assert_eq!(active_transaction_rows.len(), 1);
    assert_eq!(string_field(&active_transaction_rows[0], "state"), "open_read");
    assert_eq!(i64_field(&active_transaction_rows[0], "write_count"), 0);

    tokio::time::sleep(Duration::from_millis(2200)).await;

    let timeout_error = pg_service
        .scan(Request::new(ScanRpcRequest {
            namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            table_type: "shared".to_string(),
            session_id: session_id.to_string(),
            user_id: None,
            columns: vec![],
            limit: None,
        }))
        .await
        .expect_err("scan should fail after timeout");
    assert_eq!(timeout_error.code(), tonic::Code::FailedPrecondition);
    assert!(
        timeout_error.message().contains("timed_out")
            || timeout_error.message().contains("timed out"),
        "{timeout_error}"
    );

    let owner_key =
        ExecutionOwnerKey::from_pg_session_id(session_id).expect("pg owner key should parse");
    let parsed_transaction_id =
        TransactionId::try_new(transaction_id.clone()).expect("transaction id should parse");
    assert!(app_ctx.transaction_coordinator().active_for_owner(&owner_key).is_none());
    assert!(
        app_ctx
            .transaction_coordinator()
            .get_handle(&parsed_transaction_id)
            .is_none(),
        "timed out PG read should not leave a handle behind"
    );

    let cleared_session_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT session_id, state, transaction_id, transaction_state FROM system.sessions WHERE session_id = '{session_id}'"
            ),
        )
        .await,
    );
    assert_eq!(cleared_session_rows.len(), 1);
    assert_eq!(string_field(&cleared_session_rows[0], "state"), "idle");
    assert_eq!(optional_string_field(&cleared_session_rows[0], "transaction_id"), None);
    assert_eq!(
        optional_string_field(&cleared_session_rows[0], "transaction_state"),
        None
    );

    let cleared_transaction_rows = json_rows(
        execute_ok(
            &executor,
            &observer_ctx,
            &format!(
                "SELECT transaction_id FROM system.transactions WHERE transaction_id = '{transaction_id}'"
            ),
        )
        .await,
    );
    assert!(cleared_transaction_rows.is_empty());

    let replacement_transaction_id = begin_transaction(&pg_service, session_id).await;
    assert_ne!(replacement_transaction_id, transaction_id);
    rollback_transaction(&pg_service, session_id, &replacement_transaction_id).await;
}
