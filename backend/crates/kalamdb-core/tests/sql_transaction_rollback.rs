mod support;

use support::{execute_ok, insert_sql, observer_exec_ctx, request_exec_ctx, select_names, setup_shared_table};

#[tokio::test]
#[ntest::timeout(15000)]
async fn sql_request_transaction_rollback_discards_rows() {
    let (app_ctx, executor, table_id, _test_db) =
        setup_shared_table("sql_tx_rollback", "items").await;
    let request_ctx = request_exec_ctx(&app_ctx, "sql-request-rollback");

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 1, "alpha")).await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 2, "beta")).await;
    execute_ok(&executor, &request_ctx, "ROLLBACK").await;

    let observer_ctx = observer_exec_ctx(&app_ctx);
    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());
}