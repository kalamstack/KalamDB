mod support;

use std::sync::Arc;

use kalamdb_commons::models::pg_operations::InsertRequest;
use kalamdb_commons::TableType;
use kalamdb_configs::ServerConfig;
use kalamdb_core::operations::service::OperationService;
use kalamdb_pg::OperationExecutor;
use support::{
    create_cluster_app_context_with_config, create_executor, create_shared_table, observer_exec_ctx,
    row, select_names, unique_namespace,
};

#[tokio::test]
#[ntest::timeout(4000)]
async fn transaction_buffer_limit_aborts_transaction_and_rejects_follow_up_writes() {
    let mut config = ServerConfig::default();
    config.max_transaction_buffer_bytes = 256;

    let (app_ctx, _test_db) = create_cluster_app_context_with_config(config).await;
    let table_id = create_shared_table(&app_ctx, &unique_namespace("tx_buffer_limit"), "items").await;
    let service = OperationService::new(Arc::clone(&app_ctx));
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-1002-abcd";
    let oversized_name = "x".repeat(1024);

    let transaction_id = service
        .begin_transaction(session_id)
        .await
        .expect("begin transaction succeeds")
        .expect("transaction id returned");

    let limit_error = service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_id.to_string()),
            user_id: None,
            rows: vec![row(1, &oversized_name)],
        })
        .await
        .expect_err("oversized write should fail");
    assert!(limit_error.message().contains("buffer limit"), "{limit_error}");
    assert!(limit_error.message().contains("aborted"), "{limit_error}");

    let follow_up_error = service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            session_id: Some(session_id.to_string()),
            user_id: None,
            rows: vec![row(2, "still-blocked")],
        })
        .await
        .expect_err("aborted transaction should keep rejecting writes");
    assert!(follow_up_error.message().contains("aborted"), "{follow_up_error}");

    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());

    service
        .rollback_transaction(session_id, &transaction_id)
        .await
        .expect("rollback clears aborted transaction");

    let replacement_transaction_id = service
        .begin_transaction(session_id)
        .await
        .expect("begin transaction succeeds after cleanup")
        .expect("replacement transaction id returned");
    assert_ne!(replacement_transaction_id, transaction_id);
}
