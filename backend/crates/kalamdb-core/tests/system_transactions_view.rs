mod support;

use std::collections::HashMap;
use std::sync::Arc;

use kalamdb_commons::conversions::arrow_json_conversion::record_batch_to_json_rows;
use kalamdb_commons::models::KalamCellValue;
use kalamdb_commons::models::pg_operations::InsertRequest;
use kalamdb_commons::TableType;
use kalamdb_core::operations::service::OperationService;
use kalamdb_core::sql::context::ExecutionResult;
use kalamdb_pg::{
    BeginTransactionRequest, KalamPgService, OpenSessionRequest, OperationExecutor, PgService,
    RollbackTransactionRequest,
};
use kalamdb_raft::RaftExecutor;
use support::{
    create_cluster_app_context, create_executor, create_shared_table, execute_ok, insert_sql,
    observer_exec_ctx, request_exec_ctx, request_transaction_state, row, unique_namespace,
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
