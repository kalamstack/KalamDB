mod support;

use support::{execute_ok, insert_sql, observer_exec_ctx, request_exec_ctx, request_transaction_state, select_names, setup_shared_table};

#[tokio::test]
#[ntest::timeout(15000)]
async fn request_cleanup_rolls_back_unclosed_sql_transaction() {
    let (app_ctx, executor, table_id, _test_db) =
        setup_shared_table("sql_tx_unclosed", "items").await;
    let request_ctx = request_exec_ctx(&app_ctx, "sql-request-unclosed");

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 1, "staged")).await;

    let mut tx_state = request_transaction_state(&request_ctx);
    tx_state.sync_from_coordinator(&app_ctx);
    assert!(tx_state.rollback_if_active(&app_ctx).expect("rollback cleanup").is_some());

    let observer_ctx = observer_exec_ctx(&app_ctx);
    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());
}