mod support;

use std::sync::Arc;

use kalamdb_commons::models::pg_operations::InsertRequest;
use kalamdb_commons::models::{TableId, TableName};
use kalamdb_commons::TableType;
use kalamdb_core::operations::service::OperationService;
use kalamdb_pg::OperationExecutor;
use support::{create_cluster_app_context, row, unique_namespace};

#[tokio::test]
#[ntest::timeout(6000)]
async fn explicit_transaction_rejects_stream_table_writes() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let service = OperationService::new(Arc::clone(&app_ctx));
    let session_id = "pg-3101-feedbeef";
    let table_id = TableId::new(unique_namespace("tx_stream"), TableName::new("events"));

    let transaction_id = service
        .begin_transaction(session_id)
        .await
        .expect("begin transaction succeeds")
        .expect("transaction id returned");

    let error = service
        .execute_insert(InsertRequest {
            table_id,
            table_type: TableType::Stream,
            session_id: Some(session_id.to_string()),
            user_id: None,
            rows: vec![row(1, "event")],
        })
        .await
        .expect_err("stream-table write should be rejected");
    assert!(
        error.message().contains("stream tables are not supported inside explicit transactions"),
        "{error}"
    );

    service
        .rollback_transaction(session_id, &transaction_id)
        .await
        .expect("rollback succeeds after stream-table rejection");
}
