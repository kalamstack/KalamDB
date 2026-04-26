mod support;

use std::{sync::Arc, time::Duration};

use kalamdb_commons::{models::pg_operations::InsertRequest, TableType};
use kalamdb_configs::ServerConfig;
use kalamdb_core::operations::service::OperationService;
use kalamdb_pg::OperationExecutor;
use support::{
    create_cluster_app_context_with_config, create_executor, create_shared_table,
    observer_exec_ctx, row, select_names, unique_namespace,
};

#[tokio::test]
#[ntest::timeout(6000)]
async fn transaction_timeout_aborts_staged_writes_and_blocks_follow_up_operations() {
    let mut config = ServerConfig::default();
    config.transaction_timeout_secs = 1;

    let (app_ctx, _test_db) = create_cluster_app_context_with_config(config).await;
    let table_id = create_shared_table(&app_ctx, &unique_namespace("tx_timeout"), "items").await;
    let service = OperationService::new(Arc::clone(&app_ctx));
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-1001-abcd";

    let transaction_id = service
        .begin_transaction(session_id)
        .await
        .expect("begin transaction succeeds")
        .expect("transaction id returned");

    service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_id.to_string()),
            user_id: None,
            rows: vec![row(1, "pending")],
        })
        .await
        .expect("initial staged write succeeds");

    tokio::time::sleep(Duration::from_millis(2200)).await;

    let timeout_error = service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_id.to_string()),
            user_id: None,
            rows: vec![row(2, "late")],
        })
        .await
        .expect_err("follow-up write should fail after timeout");
    assert!(timeout_error.message().contains("timed out"), "{timeout_error}");

    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());

    service
        .rollback_transaction(session_id, &transaction_id)
        .await
        .expect("rollback clears timed out transaction");

    let replacement_transaction_id = service
        .begin_transaction(session_id)
        .await
        .expect("begin transaction succeeds after cleanup")
        .expect("replacement transaction id returned");
    assert_ne!(replacement_transaction_id, transaction_id);
}
