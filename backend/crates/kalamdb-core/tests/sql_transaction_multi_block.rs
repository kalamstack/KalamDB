mod support;

use support::{execute_ok, insert_sql, observer_exec_ctx, request_exec_ctx, select_names, setup_shared_table};

#[tokio::test]
#[ntest::timeout(15000)]
async fn sql_request_supports_multiple_sequential_transaction_blocks() {
    let (app_ctx, executor, table_id, _test_db) =
        setup_shared_table("sql_tx_multi", "items").await;
    let request_ctx = request_exec_ctx(&app_ctx, "sql-request-multi");

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 1, "first")).await;
    execute_ok(&executor, &request_ctx, "COMMIT").await;

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 2, "second")).await;
    execute_ok(&executor, &request_ctx, "ROLLBACK").await;

    let observer_ctx = observer_exec_ctx(&app_ctx);
    assert_eq!(
        select_names(&executor, &observer_ctx, &table_id).await,
        vec!["first".to_string()]
    );
}